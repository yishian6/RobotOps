"""
    端到端评测管线的离线自检：用固定流程替身验证场景、评分与汇总。

    这些用例启动真实子进程并注入真实故障，但不调用模型；scripted 结果只证明评测
    管线可以运行，不作为诊断质量或成本收益证据。
"""

import json
from unittest.mock import Mock

import pytest
from robotops_benchmarks import e2e
from robotops_benchmarks.e2e import SCENARIOS, evaluate, format_markdown


def test_scripted_harness_scores_real_faults():
    """
        验证固定流程替身能在两个真实故障上完成调查并通过全部检查项。

        Args:
            无显式参数。

        Returns:
            None: 状态、报告、检查项、工具错误和聚合表均符合预期。
    """
    result = evaluate("scripted", ["config_mismatch", "processor_exit"], ["evidence"], 1)
    assert result["provider"] == "scripted"
    assert result["real_model_calls"] == 0
    assert result["diagnosis_quality_evaluated"] is False
    assert len(result["results"]) == 2
    for row in result["results"]:
        assert row["state"] == "COMPLETED", row
        assert row["report_status"] == "proposed", row
        assert row["passed"], row
        assert row["tool_errors"] == 0
        assert row["tool_calls"] >= 3
        assert row["snapshots"] == row["model_calls"]
    assert len(result["summary"]) == 2
    assert all(row["pass_rate"] == 1 for row in result["summary"])
    assert all(row["checks"]["evidence"] == 1 for row in result["summary"])
    markdown = format_markdown(result)
    assert "config_mismatch" in markdown and "pass_rate" not in markdown


def test_scenarios_declare_reviewable_checks():
    """
        验证每个场景都给出原因关键词、必需观测来源和动作边界，避免空评分。

        Args:
            无显式参数。

        Returns:
            None: 四个故障场景的评分条件完整且可区分。
    """
    assert set(SCENARIOS) == {
        "input_stopped",
        "processing_stalled",
        "config_mismatch",
        "processor_exit",
    }
    for scenario in SCENARIOS.values():
        assert scenario["symptom"]
        assert scenario["cause_terms"]
        assert scenario["required_tools"]
        assert isinstance(scenario["forbid_restart"], bool)


def test_cli_defaults_to_offline_and_previews_paid_matrix(monkeypatch, capsys):
    """
        核对评测入口的默认路径、付费范围校验及无密钥预览。

        Args:
            monkeypatch: 替换模型配置和评测调用，记录实际传参。
            capsys: 捕获命令的预览和错误提示。

        Returns:
            None: 默认使用替身，缺少付费范围时报错，预览不初始化模型或执行调查。
    """
    config_load = Mock(side_effect=AssertionError("离线调用不应加载模型配置"))
    evaluate_mock = Mock(return_value={"provider": "scripted", "real_model_calls": 0, "summary": []})
    monkeypatch.setattr(e2e.Config, "load", config_load)
    monkeypatch.setattr(e2e, "evaluate", evaluate_mock)

    with pytest.raises(SystemExit) as error:
        e2e.main(["--provider", "deepseek"])
    assert error.value.code == 2
    assert "paid runs require" in capsys.readouterr().err
    assert e2e.main([
        "--provider", "deepseek", "--scenarios", "processing_stalled",
        "--strategies", "evidence", "--modes", "single,adaptive", "--repeats", "1", "--plan",
    ]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["planned_investigations"] == 2 and plan["real_model_calls"] == 0
    assert plan["scenarios"] == ["processing_stalled"] and plan["modes"] == ["single", "adaptive"]
    evaluate_mock.assert_not_called()
    assert e2e.main([]) == 0
    evaluate_mock.assert_called_once_with("scripted", list(SCENARIOS), ["evidence", "full"], 1, ("single",))
    config_load.assert_not_called()
