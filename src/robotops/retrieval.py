"""本地历史案例库：先校验适用条件，再通过 SQLite FTS5 排序。

案例是人工/测试流程确认的经验，不由模型的诊断报告自动晋升。中文使用相邻双字词，
英文使用单词；这是一种可解释的词法检索，不具备语义向量检索或中文专业分词能力。
"""

import json
import re
from datetime import datetime, timezone
from uuid import uuid4

from pydantic import AwareDatetime, Field, model_validator

from .models import Identifier, Model, utc_now
from .storage import Store


def search_terms(text: str) -> list[str]:
    """生成去重的 FTS 词；查询表达式只由这些词构建，原始输入不作为 FTS 语法执行。"""
    terms = re.findall(r"[a-z0-9_]+", text.lower())
    for span in re.findall(r"[\u3400-\u9fff]+", text):
        terms.extend(span[i : i + 2] for i in range(max(1, len(span) - 1)))
    return sorted(set(terms))


class CaseCreate(Model):
    """案例的适用范围与可审查来源。verified_at 代表外部验证声明，并非平台自证。

    精确匹配应用及版本；components 必须全部存在于当前契约；过期案例不能检出。
    source_uri 应指向验证记录/工单，不能填密钥或含凭证 URL；平台不主动访问该地址。
    """

    app_id: Identifier
    app_version: str = Field(min_length=1, max_length=64)
    components: list[Identifier] = Field(min_length=1)
    title: str = Field(min_length=1, max_length=300)
    symptoms: str = Field(min_length=1, max_length=4000)
    root_cause: str = Field(min_length=1, max_length=4000)
    resolution: str = Field(min_length=1, max_length=4000)
    source_uri: str = Field(min_length=1, max_length=1000)
    verified_at: AwareDatetime | None = None
    valid_until: AwareDatetime | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def validity(self):
        if len(set(self.components)) != len(self.components):
            raise ValueError("Case components must be unique")
        if self.verified_at and self.valid_until and self.valid_until <= self.verified_at:
            raise ValueError("Case expiry must follow verification")
        return self


class CaseLibrary:
    """复用 Store 的事务连接；正文和 FTS 索引同事务写入，不需要额外向量服务。"""

    def __init__(self, store: Store):
        self.store = store

    def add(self, case: CaseCreate):
        """保存新的案例版本；返回来源与验证信息，不覆盖已有案例。"""
        body = {
            "id": f"case-{uuid4().hex}",
            "created_at": utc_now(),
            **case.model_dump(mode="json"),
        }
        terms = " ".join(search_terms(" ".join([case.title, case.symptoms, case.root_cause])))
        with self.store.connection() as db:
            db.execute(
                "INSERT INTO cases VALUES (?, ?, ?, ?)",
                (body["id"], case.app_id, case.app_version, json.dumps(body, ensure_ascii=False)),
            )
            db.execute("INSERT INTO cases_fts VALUES (?, ?)", (body["id"], terms))
        return body

    def get(self, case_id: str):
        """按 ID 返回完整案例，包括草稿/已撤回状态；找不到时抛 KeyError。"""
        with self.store.connection() as db:
            row = db.execute("SELECT body FROM cases WHERE id=?", (case_id,)).fetchone()
        if row is None:
            raise KeyError("Case not found")
        return json.loads(row[0])

    def disable(self, case_id: str):
        """撤回过时经验而不删除来源；先前调查已保存的历史检索证据保持原样。"""
        with self.store.connection() as db:
            row = db.execute("SELECT body FROM cases WHERE id=?", (case_id,)).fetchone()
            if row is None:
                raise KeyError("Case not found")
            body = json.loads(row[0])
            body["enabled"] = False
            db.execute(
                "UPDATE cases SET body=? WHERE id=?",
                (json.dumps(body, ensure_ascii=False), case_id),
            )
        return body

    def search(self, contract: dict, query: str, components: list[str] | None = None, limit=5):
        """只返回当前契约可用的已验证案例；空查询返回空，不退回全库灌入上下文。

        应用/版本/组件/有效期过滤发生在 LIMIT 之前。额外组件范围只能缩小匹配，
        不能扩大到其他应用。SQL 值全部参数绑定，FTS 词加双引号以隔离查询运算符。
        """
        focus = components or []
        if not set(focus) <= set(contract["components"]):
            raise ValueError("Case focus outside application contract")
        if not 1 <= limit <= 20 or len(query) > 2000:
            raise ValueError("Invalid case search range")
        terms = search_terms(query)
        if not terms:
            return []
        match = " OR ".join('"' + term + '"' for term in terms)
        now = datetime.now(timezone.utc).timestamp()
        # 时间使用 julianday 比较，兼容外部输入中的不同时区偏移；未来验证也不生效。
        with self.store.connection() as db:
            rows = db.execute(
                """SELECT c.body FROM cases_fts f JOIN cases c ON c.id=f.case_id
                WHERE cases_fts MATCH ? AND c.app_id=? AND c.app_version=?
                AND json_extract(c.body, '$.enabled')=1
                AND julianday(json_extract(c.body, '$.verified_at'))<=julianday(?, 'unixepoch')
                AND (json_extract(c.body, '$.valid_until') IS NULL OR
                    julianday(json_extract(c.body, '$.valid_until'))>julianday(?, 'unixepoch'))
                AND NOT EXISTS (SELECT 1 FROM json_each(c.body, '$.components') x
                    WHERE x.value NOT IN (SELECT value FROM json_each(?)))
                AND (?='[]' OR EXISTS (SELECT 1 FROM json_each(c.body, '$.components') x
                    WHERE x.value IN (SELECT value FROM json_each(?))))
                ORDER BY bm25(cases_fts), c.id LIMIT ?""",
                (
                    match,
                    contract["app_id"],
                    contract["version"],
                    now,
                    now,
                    json.dumps(contract["components"]),
                    json.dumps(focus),
                    json.dumps(focus),
                    limit,
                ),
            ).fetchall()
        return [json.loads(row[0]) for row in rows]
