"""
    本地 pipeline 示例的故障注入与真实工具烟测。
"""

import tempfile
import time
from pathlib import Path

from robotops.models import ApplicationContract, IncidentCreate
from robotops.storage import Store
from robotops.tools import ToolGateway

from .observer import PipelineObserver
from .runtime import PipelineRuntime


def smoke_check():
    """
        在临时目录启动真实组件，验证故障会引起预期的进程、指标和日志变化。

        依次覆盖正常、输入断流、处理停滞、配置错误和处理进程退出；每次观测经真实工具
        取得并保存证据。finally 始终清理组件，临时目录随上下文退出删除。
        返回包含各场景观测的汇总；此流程不调用模型，不能据此宣称模型诊断正确。

        Args:
            无显式参数。

        Returns:
            dict: 含 passed 状态、model_called=False 和各故障观测的汇总。
    """
    with tempfile.TemporaryDirectory(prefix="robotops-smoke-") as temporary:
        root = Path(temporary)
        store = Store(root)
        store.register(
            ApplicationContract.model_validate_json(
                Path(__file__).with_name("contract.json").read_text()
            )
        )
        incident = store.create_incident(IncidentCreate(app_id="pipeline", symptom="Smoke check"))
        run_id = store.start_run(incident["id"], "no-model-tool-check")
        demo = PipelineRuntime(root)
        gateway = ToolGateway(store, PipelineObserver(demo), run_id)
        results = {}
        try:
            demo.start()
            for mode in ("normal", "input_stopped", "processing_stalled", "config_mismatch"):
                # 先解除上一个场景的控制，给真实进程时间刷新状态，再观察当前故障。
                demo.inject("normal")
                time.sleep(0.2)
                demo.inject(mode)
                time.sleep(0.3)
                metrics = gateway.execute("query_metrics", {"sample_seconds": 0.4})
                if metrics["status"] != "ok":
                    raise RuntimeError("Demo metrics unavailable")
                rates = metrics["data"]["rates"]
                # 场景名只选择验收条件，现象值必须来自真实计数，不能用注入真值代替观测结果。
                checks = {
                    "normal": rates["processor"]["processed_hz"] > 0,
                    "input_stopped": rates["producer"]["sent_hz"] == 0,
                    "processing_stalled": rates["producer"]["sent_hz"] > 0
                    and rates["processor"]["processed_hz"] == 0,
                    "config_mismatch": rates["processor"]["rejected_hz"] > 0,
                }
                if not checks[mode]:
                    raise RuntimeError(f"Unexpected actual telemetry for {mode}: {rates}")
                results[mode] = {"rates": rates, "evidence_id": metrics["evidence_id"]}
            # 速率之外再检查实际错误日志与进程退出，覆盖不同观测来源，避免仅验证控制文件写入。
            logs = gateway.execute("search_logs", {"keyword": "input_channel_mismatch"})
            if not logs.get("data", {}).get("rows"):
                raise RuntimeError("Configuration mismatch did not produce a real error log")
            demo.inject("processor_exit")
            if demo.snapshot()["components"]["processor"]["process"]["alive"]:
                raise RuntimeError("Exited processor reported as alive")
            results["processor_exit"] = {"alive": False}
        finally:
            demo.stop()
        return {"status": "passed", "model_called": False, "checks": results}
