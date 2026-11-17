"""本机 UDP 生产/处理子进程，实际产生输入、输出、心跳和结构化日志。"""

import argparse
import json
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path


def now():
    """输出带时区的 UTC 时间，用于日志关联；耗时计算另用单调时钟。"""
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path):
    """读取本会话文件；未创建时返回空字典，损坏 JSON 等错误继续抛出。"""
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return {}


def write_json(path: Path, value: dict):
    """先完整写同目录临时文件，再原子替换，供另一进程读取完整快照。

    依赖每个目标文件只有一个写者；没有 fsync，不承诺断电后的磁盘持久性。
    """
    # 同目录原子替换，避免观测线程读到只写了一半的 JSON；每个文件只有一个写者。
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    temporary.replace(path)


def run_worker(component: str, root: Path, instance_id: str, port: int):
    """运行一个组件，持续将实际状态写入本会话目录，直到由父进程终止。

    producer 向 port 指定的本机处理端约每 50ms 发送样本；processor 自行绑定空闲
    UDP 端口，接收后根据实际配置处理或拒绝样本。instance_id 关联本次启动的遥测。
    私有 controls 用于故障驱动；公开观测仅包含进程、日志、计数和配置变更事实。
    """
    metrics_path = root / f"{component}.json"
    log_path = root / f"{component}.jsonl"
    control_path = root / "private-controls.json"
    config_path = root / "config.json"

    def log(level, message, **fields):
        """每行写一个 JSON 事件，带组件与启动实例，便于跨来源关联和按时间检索。"""
        with log_path.open("a", encoding="utf-8") as output:
            output.write(
                json.dumps(
                    {
                        "at": now(),
                        "component": component,
                        "instance_id": instance_id,
                        "level": level,
                        "message": message,
                        **fields,
                    }
                )
                + "\n"
            )

    # 各计数只在当前启动实例内递增；未出现的输入/输出时间保持 None。
    metrics = {
        "component": component,
        "instance_id": instance_id,
        "pid": os.getpid(),
        "started_at": now(),
        "heartbeat_at": now(),
        "sent_count": 0,
        "received_count": 0,
        "processed_count": 0,
        "rejected_count": 0,
        "last_input_at": None,
        "last_output_at": None,
        "processing_started_at": None,
        "processing_age_ms": 0.0,
    }
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as channel:
        if component == "processor":
            # 超时用于定期检查控制文件并刷新心跳，不把“没有收到样本”当作程序退出。
            channel.bind(("127.0.0.1", 0))
            channel.settimeout(0.05)
            metrics["bound_port"] = channel.getsockname()[1]
        write_json(metrics_path, metrics)
        log("INFO", "component_started")
        pending = None
        processing_start = 0.0
        last_warning = 0.0
        while True:
            controls = read_json(control_path)
            config = read_json(config_path)
            metrics["heartbeat_at"] = now()
            metrics["config_version"] = config.get("version")
            if component == "producer":
                if not controls.get("pause_source", False):
                    sample = {
                        "seq": metrics["sent_count"] + 1,
                        "channel": "scan",
                        "sent_at": now(),
                        "sent_monotonic": time.monotonic(),
                    }
                    channel.sendto(json.dumps(sample).encode(), ("127.0.0.1", port))
                    metrics["sent_count"] += 1
                    metrics["last_output_at"] = sample["sent_at"]
                write_json(metrics_path, metrics)
                time.sleep(0.05)
                continue

            if pending is None:
                # pending 保存当前待处理样本；注入停滞后不会继续接收并覆盖它。
                try:
                    body, _ = channel.recvfrom(65536)
                    pending = json.loads(body)
                except socket.timeout:
                    write_json(metrics_path, metrics)
                    continue
                metrics["received_count"] += 1
                metrics["last_input_at"] = now()
                processing_start = time.monotonic()
                metrics["processing_started_at"] = now()

            metrics["processing_age_ms"] = (time.monotonic() - processing_start) * 1000
            if controls.get("stall_processor", False):
                # 保留当前样本并持续心跳，真实形成“进程存活但输出停滞”的状态。
                write_json(metrics_path, metrics)
                time.sleep(0.05)
                continue

            if pending["channel"] != config.get("expected_channel"):
                metrics["rejected_count"] += 1
                # 每个拒绝都计数，错误日志按时间节流，避免同类错误淹没诊断输入。
                if time.monotonic() - last_warning > 1:
                    log(
                        "ERROR",
                        "input_channel_mismatch",
                        expected=config.get("expected_channel"),
                        received=pending["channel"],
                        config_version=config.get("version"),
                    )
                    last_warning = time.monotonic()
            else:
                completed = now()
                write_json(root / "output.json", {"seq": pending["seq"], "processed_at": completed})
                metrics["processed_count"] += 1
                metrics["last_output_at"] = completed
                metrics["processing_latency_ms"] = metrics["processing_age_ms"]
                metrics["end_to_end_latency_ms"] = (
                    # 两个子进程在同一主机运行，共用单调时钟基准；不能直接套用到跨主机。
                    time.monotonic() - pending["sent_monotonic"]
                ) * 1000
            pending = None
            metrics["processing_started_at"] = None
            metrics["processing_age_ms"] = 0.0
            write_json(metrics_path, metrics)


def main():
    """解析父进程传入的固定组件和会话信息，不作为接收任意程序的执行入口。"""
    parser = argparse.ArgumentParser()
    parser.add_argument("component", choices=["producer", "processor"])
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    run_worker(args.component, args.runtime, args.instance, args.port)


if __name__ == "__main__":
    main()
