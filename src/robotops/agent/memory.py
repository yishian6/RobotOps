"""
    调查内结构化工作记忆。记忆是带引用的模型注记，不能替代原始观测。
"""

import json
from typing import Annotated, Literal

from pydantic import Field, model_validator
from pydantic_core import PydanticCustomError

from ..models import Identifier, Model
from ..storage import Store

# 匹配 ev- 加 32 位小写十六进制 UUID，拒绝路径或自定义引用；存在性与归属仍另查数据库。
EvidenceID = Annotated[str, Field(pattern=r"^ev-[0-9a-f]{32}$")]


class Fact(Model):
    """
        一条待复核的事实表述；运行/时间/实例语义仍以引用证据为准。
    """

    # 自然语言是模型的归纳，至少带一条可回取引用；字段校验不判断归纳是否属实。
    statement: str = Field(min_length=1, max_length=2000)
    evidence_ids: list[EvidenceID] = Field(min_length=1)


class MemoryHypothesis(Model):
    """
        分别记录支持和反对同一假设的证据，防止摘要只保留单方面结论。
    """

    claim: str = Field(min_length=1, max_length=2000)
    assessment: Literal["supported", "refuted", "unverified"] = Field(
        default="unverified", description="supported 需要 support_ids，refuted 需要 contradict_ids。"
    )
    # 支持与反对引用分开保存，并共同进入保护集合，不能因结论倾向而丢掉另一侧。
    support_ids: list[EvidenceID] = Field(default_factory=list)
    contradict_ids: list[EvidenceID] = Field(default_factory=list)

    @model_validator(mode="after")
    def require_support(self):
        """
            避免在工作记忆中记录无对应证据的确定性判断。

            Args:
                无显式参数。

            Returns:
                MemoryHypothesis: 校验通过的当前记忆假设实例。
        """
        if self.assessment == "supported" and not self.support_ids:
            raise PydanticCustomError(
                "hypothesis_evidence_required",
                "Supported hypotheses need support_ids. Use unverified if evidence is missing.",
                {"field": "support_ids"},
            )
        if self.assessment == "refuted" and not self.contradict_ids:
            raise PydanticCustomError(
                "hypothesis_evidence_required",
                "Refuted hypotheses need contradict_ids. Use unverified if evidence is missing.",
                {"field": "contradict_ids"},
            )
        return self


class WorkingMemory(Model):
    """
        每次提交完整的当前记忆；历史版本保留，关键/反向引用只增不减。

        facts 和 hypotheses 的自然语言内容由模型生成，后端只校验结构与引用归属，
        不保证其语义判断正确。不得把历史案例描述为当前实例的观测事实。
    """

    # 阶段由调查进展决定，装配器据此折叠已有记忆的普通结果，不靠固定轮数截断历史。
    phase: Literal["triage", "investigation", "verification", "reporting"] = "triage"
    # 焦点帮助计算依赖范围；空列表保留全应用视野，不等于忽略所有组件。
    focus_components: list[Identifier] = Field(default_factory=list)
    facts: list[Fact] = Field(default_factory=list)
    hypotheses: list[MemoryHypothesis] = Field(default_factory=list)
    # 模型显式标记的关键证据与假设两侧引用共同保护；普通事实引用只参与记忆覆盖判断。
    critical_ids: list[EvidenceID] = Field(default_factory=list)
    pending_questions: list[str] = Field(default_factory=list)

    def references(self):
        """
            汇集所有引用，供写入前一次性检查运行归属，而非信任模型提供的 ID。

            Args:
                无显式参数。

            Returns:
                set[str]: 事实、假设两侧及显式关键字段引用的证据 ID 并集。
        """
        return set(self.critical_ids).union(
            *(set(f.evidence_ids) for f in self.facts),
            *(set(h.support_ids + h.contradict_ids) for h in self.hypotheses),
        )


class MemoryStore:
    """
        复用 SQLite 保留版本；同一运行的更新在写事务中串行，避免保护集合丢失。
    """

    def __init__(self, store: Store, run_id: str):
        """
            为单次调查构造持久化工作记忆入口。

            Args:
                store (Store): 业务存储实例，提供当前应用、运行记录、证据及 SQLite 事务连接。
                run_id (str): 调查运行 ID；查询和写入均限定在这个运行的归属范围内。

            Returns:
                None: 绑定已存在运行的记忆访问器，不立即创建记忆版本。
        """
        self.store, self.run_id = store, run_id
        self._incident_id = store.run_incident_id(run_id)

    def current(self):
        """
            返回最新持久化版本；尚未提交记忆时返回明确的版本 0，不伪造历史事实。

            Args:
                无显式参数。

            Returns:
                dict: 含 version、memory、protected_ids 的最新快照；未更新过时为版本 0。
        """
        with self.store.connection() as db:
            row = db.execute(
                "SELECT body FROM memories WHERE run_id=? ORDER BY version DESC LIMIT 1",
                (self.run_id,),
            ).fetchone()
        return (
            json.loads(row[0])
            if row
            else {"version": 0, "memory": WorkingMemory().model_dump(), "protected_ids": []}
        )

    def update(self, memory: WorkingMemory, *, visible_evidence: set[str] | None = None):
        """
            校验当前应用和运行引用；任一非法引用使整次更新失败，不保存半份记忆。

            Args:
                memory (WorkingMemory): 已经通过 Schema 校验的完整工作记忆，不是对旧版本的局部补丁。
                visible_evidence: 调用角色可见的证据范围，为 None 时使用本次运行的完整范围。

            Returns:
                dict: 新记忆版本、完整结构化记忆及累计保护引用。
        """
        references = memory.references()
        if visible_evidence is not None and not references <= visible_evidence:
            raise ValueError("Memory cites evidence outside this agent's scope")
        incident = self.store.incident(self._incident_id)
        contract = self.store.contract(incident["app_id"])
        if not set(memory.focus_components) <= set(contract["components"]):
            raise ValueError("Memory focus outside application contract")
        if not references <= self.store.evidence_ids(self.run_id):
            raise ValueError("Memory cites evidence outside this investigation")
        # 先验归属再保护，避免把其他运行的引用带入后续上下文或污染历史版本。
        protected = set(memory.critical_ids)
        for hypothesis in memory.hypotheses:
            protected.update(hypothesis.support_ids + hypothesis.contradict_ids)
        with self.store.connection() as db:
            # 在读取旧版本和保护集合前取得写事务，避免并发更新各自基于旧快照而丢失引用。
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT body FROM memories WHERE run_id=? ORDER BY version DESC LIMIT 1",
                (self.run_id,),
            ).fetchone()
            previous = json.loads(row[0]) if row else {"version": 0, "protected_ids": []}
            # 保护集合跨记忆版本只增不减；模型后续删去某个假设也不能顺带抹掉已有反证。
            protected.update(previous["protected_ids"])
            # 正文是完整替换快照，保护集合则继承历史；排序使同一集合的序列化结果稳定。
            result = {
                "version": previous["version"] + 1,
                "memory": memory.model_dump(mode="json"),
                "protected_ids": sorted(protected),
            }
            db.execute(
                "INSERT INTO memories VALUES (?, ?, ?)",
                (self.run_id, result["version"], json.dumps(result, ensure_ascii=False)),
            )
        return result
