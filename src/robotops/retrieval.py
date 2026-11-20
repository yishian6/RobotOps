"""
    本地历史案例库：先校验适用条件，再通过 SQLite FTS5 排序。

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
    """
        生成去重的 FTS 词；查询表达式只由这些词构建，原始输入不作为 FTS 语法执行。

        Args:
            text (str): 原始查询或案例描述字符串；保留英文标识中的数字/下划线，并生成中文双字词。

        Returns:
            list[str]: 按字典序排序的去重词集合；无可识别字符时为空列表。
    """
    # 英文按标识整词索引，保留数字/下划线；input_stopped 不会自动拆成 input 和 stopped。
    terms = re.findall(r"[a-z0-9_]+", text.lower())
    # U+3400–U+9FFF 近似覆盖扩展 A 与基本 CJK，也含区间内少量符号，并非完整中文分词。
    for span in re.findall(r"[\u3400-\u9fff]+", text):
        # 单字段保留自身，长段产生 n-1 个相邻双字片段，避免末尾只剩一个字的伪双字词。
        terms.extend(span[i : i + 2] for i in range(max(1, len(span) - 1)))
    return sorted(set(terms))


class CaseCreate(Model):
    """
        案例的适用范围与可审查来源。verified_at 代表外部验证声明，并非平台自证。

        精确匹配应用及版本；components 必须全部存在于当前契约；过期案例不能检出。
        source_uri 应指向验证记录/工单，不能填密钥或含凭证 URL；平台不主动访问该地址。
    """

    app_id: Identifier
    # 应用版本精确匹配，避免将旧版处置经验无条件套用到新程序。
    app_version: str = Field(min_length=1, max_length=64)
    components: list[Identifier] = Field(min_length=1)
    title: str = Field(min_length=1, max_length=300)
    symptoms: str = Field(min_length=1, max_length=4000)
    root_cause: str = Field(min_length=1, max_length=4000)
    resolution: str = Field(min_length=1, max_length=4000)
    # 来源只用于追溯；有来源不等于已验证，未提供 verified_at 的记录仍是草稿。
    source_uri: str = Field(min_length=1, max_length=1000)
    verified_at: AwareDatetime | None = None
    valid_until: AwareDatetime | None = None
    # 撤回采用软禁用，保留旧调查中引用的来源；valid_until 则限制经验的时间适用范围。
    enabled: bool = True

    @model_validator(mode="after")
    def validity(self):
        """
            校验案例的基本适用条件，避免无法解释的重复组件与倒置时间范围。

            Args:
                无显式参数。

            Returns:
                CaseCreate: 适用组件和验证时间关系通过校验的当前模型。
        """
        if len(set(self.components)) != len(self.components):
            raise ValueError("Case components must be unique")
        if self.verified_at and self.valid_until and self.valid_until <= self.verified_at:
            raise ValueError("Case expiry must follow verification")
        return self


class CaseLibrary:
    """
        复用 Store 的事务连接；正文和 FTS 索引同事务写入，不需要额外向量服务。
    """

    def __init__(self, store: Store):
        """
            创建使用同一本地 SQLite 数据库的案例库入口。

            Args:
                store (Store): 业务存储实例，提供当前应用、运行记录、证据及 SQLite 事务连接。

            Returns:
                None: 复用存储依赖，不新建服务、索引连接池或案例。
        """
        self.store = store

    def add(self, case: CaseCreate):
        """
            保存新的案例版本；返回来源与验证信息，不覆盖已有案例。

            Args:
                case (CaseCreate): 已校验的案例接入模型；未填 verified_at 的草稿只保存、不参与有效检索。

            Returns:
                dict: 新案例的 ID、创建时间和全部来源/适用条件字段。
        """
        body = {
            "id": f"case-{uuid4().hex}",
            "created_at": utc_now(),
            **case.model_dump(mode="json"),
        }
        # 按现象与根因建立检索词；解决步骤不进入该索引，避免通用操作词主导相似度。
        terms = " ".join(search_terms(" ".join([case.title, case.symptoms, case.root_cause])))
        with self.store.connection() as db:
            db.execute(
                "INSERT INTO cases VALUES (?, ?, ?, ?)",
                (body["id"], case.app_id, case.app_version, json.dumps(body, ensure_ascii=False)),
            )
            # 正文与 FTS 索引同事务写入，避免检索命中一条尚未提交正文的案例。
            db.execute("INSERT INTO cases_fts VALUES (?, ?)", (body["id"], terms))
        return body

    def get(self, case_id: str):
        """
            按 ID 返回完整案例，包括草稿/已撤回状态；找不到时抛 KeyError。

            Args:
                case_id (str): 案例库生成的唯一 ID，可指向启用、草稿或已撤回的记录。

            Returns:
                dict: 完整案例正文，含验证时间、来源及启用状态。
        """
        with self.store.connection() as db:
            row = db.execute("SELECT body FROM cases WHERE id=?", (case_id,)).fetchone()
        if row is None:
            raise KeyError("Case not found")
        return json.loads(row[0])

    def disable(self, case_id: str):
        """
            撤回过时经验而不删除来源；先前调查已保存的历史检索证据保持原样。

            Args:
                case_id (str): 案例库生成的唯一 ID，可指向启用、草稿或已撤回的记录。

            Returns:
                dict: enabled 已置为 False 的案例正文；重复撤回保持该状态。
        """
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
        """
            只返回当前契约可用的已验证案例；空查询返回空，不退回全库灌入上下文。

            应用/版本/组件/有效期过滤发生在 LIMIT 之前。额外组件范围只能缩小匹配，
            不能扩大到其他应用。SQL 值全部参数绑定，FTS 词加双引号以隔离查询运算符。

            Args:
                contract (dict): 服务端提供的应用契约，必须含 app_id、version、components。
                query (str): 按词法解析的检索文本；不是可执行的 FTS 查询表达式。
                components (list[str] | None): 可选焦点组件，必须属于契约；上游闭包由调用方按需要计算。
                limit: 最多返回的案例条数，范围为 1–20。

            Returns:
                list[dict]: 在限制条数内按相关性排列的适用案例；无词项或无命中时为空。
        """
        focus = components or []
        if not set(focus) <= set(contract["components"]):
            raise ValueError("Case focus outside application contract")
        if not 1 <= limit <= 20 or len(query) > 2000:
            raise ValueError("Invalid case search range")
        terms = search_terms(query)
        if not terms:
            return []
        # 只拼接正则提取的词项并逐词加引号；原始引号、OR/NOT 或括号不能成为 FTS 操作语法。
        match = " OR ".join('"' + term + '"' for term in terms)
        # 两个有效期谓词共用一个当前时刻，避免恰好跨过边界时得到自相矛盾的筛选条件。
        now = datetime.now(timezone.utc).timestamp()
        # 时间使用 julianday 比较，兼容外部输入中的不同时区偏移；未来验证也不生效。
        # 先做版本、验证时间、有效期和组件适用过滤，再 LIMIT，避免无效案例占满候选名额。
        # 组件须全部属于当前契约；可选焦点只需与案例组件相交，两者约束不能互相替代。
        # NOT EXISTS 排除含任何未知组件的案例；EXISTS 仅要求与可选焦点相交。
        # bm25 分值越小越相关，ID 作为同分次序，确保限制条数下的返回顺序稳定。
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
