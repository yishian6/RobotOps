"""
    本机 UDP 生产/处理子进程，实际产生输入、输出、心跳和结构化日志。
"""

import argparse
import json
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path


def now():
    """
        输出带时区的 UTC 时间，用于日志关联；耗时计算另用单调时钟。

        Args:
            无显式参数。

        Returns:
            str: 带 UTC 偏移的 ISO 8601 时间，用于跨文件日志关联。
    """
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path):
    """
        读取本会话文件；未创建时返回空字典，损坏 JSON 等错误继续抛出。

        Args:
            path (Path): 当前会话内要读取的 JSON 文件路径。

        Returns:
            object: 已解析的 JSON 内容；仅文件尚未存在时返回空字典。
    """
    # 首次启动文件可能尚未出现，只对缺失返回空值；损坏内容不能按默认正常状态继续。
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return {}


def write_json(path: Path, value: dict):
    """
        先完整写同目录临时文件，再原子替换，供另一进程读取完整快照。

        依赖每个目标文件只有一个写者；没有 fsync，不承诺断电后的磁盘持久性。

        Args:
            path (Path): 只有一个写者负责的目标快照路径。
            value (dict): 要写入的 JSON 数据字典；本例的进程与控制数据满足有限数值约定。

        Returns:
            None: 完整的新快照已替换目标路径，不承诺断电持久性。
    """
    # 同目录原子替换，避免观测线程读到只写了一半的 JSON；每个文件只有一个写者。
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    temporary.replace(path)


def run_worker(component: str, root: Path, instance_id: str, port: int):
    """
        运行一个组件，持续将实际状态写入本会话目录，直到由父进程终止。

        producer 向 port 指定的本机处理端约每 50ms 发送样本；processor 自行绑定空闲
        UDP 端口，接收后根据实际配置处理或拒绝样本。instance_id 关联本次启动的遥测。
        私有 controls 用于故障驱动；公开观测仅包含进程、日志、计数和配置变更事实。

        Args:
            component (str): 固定的 producer 或 processor，由命令行 choices 约束。
            root (Path): 本次会话的私有控制、配置、遥测与日志目录。
            instance_id (str): 父进程为本次组件启动生成的唯一标识，区分旧遥测与新进程。
            port (int): producer 向本机该 UDP 端口发包；processor 忽略此值并自行分配端口。

        Returns:
            None: 正常工作时持续循环，不主动返回；由父进程结束子进程。
    """
    metrics_path = root / f"{component}.json"
    log_path = root / f"{component}.jsonl"
    control_path = root / "private-controls.json"
    config_path = root / "config.json"

    def log(level, message, **fields):
        """
            每行写一个 JSON 事件，带组件与启动实例，便于跨来源关联和按时间检索。

            Args:
                level: 日志级别字符串，如 INFO 或 ERROR。
                message: 结构化事件名，不作为格式模板或命令执行。
                **fields: 可选键值字段，记录通道、配置版本等诊断参数；由本 worker 内部调用者提供。

            Returns:
                None: 在当前组件的 JSONL 文件追加一条带实例信息的事件。
        """
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
        # 心跳证明循环仍在刷新状态，输入/输出时间则反映数据是否流动，不能混为一个指标。
        "heartbeat_at": now(),
        "sent_count": 0,
        # 接收、处理成功、配置拒绝分别计数，才能区分无输入、卡住与输入不匹配。
        "received_count": 0,
        "processed_count": 0,
        "rejected_count": 0,
        "last_input_at": None,
        "last_output_at": None,
        # age 表示当前在途样本的等待时长；处理完成后归零，完成耗时另存 latency 字段。
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
        # 处理端使用单个在途槽位；同一循环继续处理该样本，便于真实呈现积压与停滞。
        pending = None
        processing_start = 0.0
        last_warning = 0.0
        while True:
            # 每轮重新读取原子快照，使场景与配置变更无需重启；诊断端不读取私有控制文件。
            controls = read_json(control_path)
            config = read_json(config_path)
            metrics["heartbeat_at"] = now()
            metrics["config_version"] = config.get("version")
            if component == "producer":
                if not controls.get("pause_source", False):
                    sample = {
                        "seq": metrics["sent_count"] + 1,
                        "channel": "scan",
                        # UTC 时间用于日志关联，单调时间只用于同主机端到端耗时，避开系统校时影响。
                        "sent_at": now(),
                        "sent_monotonic": time.monotonic(),
                    }
                    channel.sendto(json.dumps(sample).encode(), ("127.0.0.1", port))
                    metrics["sent_count"] += 1
                    metrics["last_output_at"] = sample["sent_at"]
                # 暂停发包时仍刷新遥测，保留“源进程存活但数据断流”的可观察区别。
                write_json(metrics_path, metrics)
                time.sleep(0.05)
                continue

            if pending is None:
                # pending 保存当前待处理样本；注入停滞后不会继续接收并覆盖它。
                try:
                    # 接收缓冲覆盖单个 UDP 数据报；样本只来自本例生产端，未实现通用网络输入协议。
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
                # 只有通道匹配才写真实输出并递增成功计数，不能凭输入到达就判定处理功能正常。
                completed = now()
                write_json(root / "output.json", {"seq": pending["seq"], "processed_at": completed})
                metrics["processed_count"] += 1
                metrics["last_output_at"] = completed
                metrics["processing_latency_ms"] = metrics["processing_age_ms"]
                metrics["end_to_end_latency_ms"] = (
                    # 两个子进程在同一主机运行，共用单调时钟基准；不能直接套用到跨主机。
                    time.monotonic() - pending["sent_monotonic"]
                ) * 1000
            # 完成或拒绝当前样本后才释放槽位；停滞分支不走到这里，因此不会用新输入覆盖正在调查的卡住样本。
            pending = None
            metrics["processing_started_at"] = None
            metrics["processing_age_ms"] = 0.0
            write_json(metrics_path, metrics)


def main():
    """
        解析父进程传入的固定组件和会话信息，不作为接收任意程序的执行入口。

        Args:
            无显式参数。

        Returns:
            None: 解析参数后进入持续运行的组件循环。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("component", choices=["producer", "processor"])
    parser.add_argument("--runtime", required=True, type=Path)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    run_worker(args.component, args.runtime, args.instance, args.port)


if __name__ == "__main__":
    main()
