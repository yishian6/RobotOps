# RobotOps Agent

面向应用运行保障的故障调查 Agent。先验证本地后端和 Agent，再接 ROS2 / 实车。

当前版本 **0.2.0 / G1**：已在 G0 的真实本地进程、FastAPI、LangGraph 调查与原始证据基础上，接入 **FTS5 历史检索、结构化工作记忆、证据感知上下文装配、逐轮请求快照及用量汇总**。

当前调查只读，工作记忆工具仅修改调查元数据。G2 的自动恢复/持久续跑、G3 的周期报告、G4 的 ROS2 / C++ Collector / 实车验证尚未实现。未配置密钥，真实模型诊断质量及总 Token/费用对照仍待验收；离线字节数结果不作为实际降本成绩。

## 环境

要求 Linux、Python 3.11+、带 FTS5 和 JSON 函数的 SQLite（本地已验证）；当前使用 conda 提供的 Python 3.13.5 建立独立 `.venv`，依赖固定在 `uv.lock`。没有修改 conda base 包或系统 ROS1。

已有本工作区环境可直接使用：

```bash
.venv/bin/python -m robotops_examples.pipeline smoke
.venv/bin/robotops serve
```

新环境可以选择 uv：

```bash
uv sync --frozen --extra dev
uv run python -m robotops_examples.pipeline smoke
uv run robotops serve
```

也可以使用 conda + pip：

```bash
conda create -n robotops python=3.13 pip
conda activate robotops
python -m pip install -e '.[dev]'
python -m robotops_examples.pipeline smoke
robotops serve
```

uv 使用锁定版本；pip 路径根据 `pyproject.toml` 的版本范围解析依赖。

示例入口的 `smoke` 启动两个真实子进程，通过本机回环 UDP 传递样本，注入故障、检查观测并清理子进程。**不调用模型，也不模拟模型诊断。** 数据保存在临时目录，退出后清理。

`serve` 默认绑定 `127.0.0.1:8000`，接口文档位于 `http://127.0.0.1:8000/docs`。这是本地开发接口，尚未提供远程鉴权与持久任务调度。显式启用 pipeline 示例后才包含演示故障控制。

## 项目目录与配置

```text
src/robotops/                     # 可复用的核心代码
  config.py                      # Config.load：配置读取、覆盖、路径与参数校验
  api.py                         # 应用/案例/事件/调查/证据/上下文 API
  cli.py                         # 核心服务与 Prompt 检查入口
  agent/
    client.py                    # 模型 HTTP 协议及响应模型
    workflow.py                  # LangGraph 调查状态图
    prompt_loader.py             # Prompt 读取、清单校验和消息装配
    memory.py                    # 事实/假设/反证、单调保护集合与记忆版本
    context.py                   # 上下文装配、精确日志聚合、请求快照
  retrieval.py                   # SQLite FTS5 案例入库、适用过滤和撤回
  tools.py                       # 只读工具注册、参数校验、分派和证据记录
  observations.py                # 核心需要的只读观测协议
  models.py                      # 业务数据契约和诊断报告
  storage.py                     # SQLite、原始证据和运行轨迹
config/                          # 非敏感、可编辑且随安装包分发的资源
  __init__.py                    # 资源包标识，不放配置加载逻辑
  robotops.toml                  # 运行参数与 Prompt 版本选择
  prompts/investigation/v1/       # 保留 G0 基线
  prompts/investigation/v2/       # 默认 G1 指令
    system.txt                   # 固定规则
    task.txt                     # 固定任务指令
    manifest.json                # 版本、内容指纹与变更说明
examples/                        # 独立示例，核心默认启动不导入
  pipeline/
    __main__.py                  # 独立的示例服务/烟测命令入口
    app.py                       # 组装核心 API、观测适配及示例控制路由
    runtime.py                   # 子进程启动/清理与故障驱动
    worker.py                    # 真实 UDP 生产/处理程序
    observer.py                  # 将场景观测适配为核心只读协议
    smoke.py                     # 实际故障与观测烟测
    contract.json                # 本示例应用的运行契约与组件依赖
    cases.json                   # 标明 fixture 来源的示例检查手册
    cases.py                     # 显式导出案例，不自动灌入调查
benchmarks/
  context.py                     # 独立离线上下文对照入口，不调用模型
tests/                           # 核心、检索、上下文、配置、Prompt、示例与边界测试
doc/                             # 本地设计与开发记录，Git 忽略
data/                            # 本地运行产物，Git 忽略
```

`src/robotops/config.py` 负责加载配置，`config/` 负责保存配置资源。
启动时默认读取工作目录下的 `config/robotops.toml`，也可用 `ROBOTOPS_CONFIG` 指定文件。
优先级为**环境变量 > TOML > Config 默认值**；TOML 路径相对配置文件，环境路径相对启动目录。
未找到默认文件时允许使用默认值；显式指定的文件缺失、键拼错或值无效时直接报错。
密钥只从环境变量读取，所有 `.env*` 文件都被 Git 忽略，程序不自动加载 `.env`。

`prompts_dir = "prompts"` 指向配置目录下的模板，`ROBOTOPS_PROMPTS_DIR` 可覆盖。
未指定模板目录时读取随包安装的内置资源；显式指定的目录错误时不回退。
`prompt_loader.py` 只负责资源读取和消息装配，不保存模板正文。

`examples/pipeline/app.py` 在 `/demo/start` 首次调用时注册同目录 `contract.json`；
已有契约以数据库为准，不随文件变更自动覆盖。核心 API 和 CLI 不导入或选择示例，不注册示例应用或挂载故障控制路由。
示例通过 `ObservationSource` 提供只读数据，进程控制和故障注入留在示例中。
应用契约必须显式提供非空且不重复的组件列表，可使用 planner/controller 等应用自定义标识。可选 `dependencies` 使用“组件 → 上游组件列表”，拒绝未声明组件、重复依赖和循环；焦点组件的传递上游会进入调查上下文及案例检索范围。动作建议的目标仍须属于本次契约。观测工具需要对应适配器，注册契约本身不会自动监控程序，尚未完成 ROS 适配。

安装时通过 setuptools 显式映射 `robotops` → `src/robotops/`、
`robotops.resources` → `config/`、`robotops_examples` → `examples/`、`robotops_benchmarks` → `benchmarks/`，
使配置模板和示例在 editable / wheel / sdist 安装后保持同样的读取方式。

目录设计参考了 [Codex 的配置模块](https://github.com/openai/codex/tree/main/codex-rs/config/src)
和 [Prompt 模块](https://github.com/openai/codex/tree/main/codex-rs/prompts)、
[Claude Code 公开仓库的 examples](https://github.com/anthropics/claude-code/tree/main/examples)、
[DSH（DeepSeek Harness）的核心职责划分](https://github.com/deepseek-ai/deepseek-harness/tree/master/packages/core)。
Claude Code 的公开仓库仅用于参考公开扩展和示例组织，不视为完整内部运行架构的源码依据。

除 README 外的设计、实施规划和开发记录均放在本地 `doc/` 并排除提交；
README 独立提供安装、运行、配置和边界说明。

## Prompt 管理

当前采用仓库管理的版本化 Prompt，未接入远程 Prompt 平台。参考 [Langfuse 版本与发布](https://langfuse.com/docs/prompt-management/features/prompt-version-control)、[LangSmith 环境管理](https://docs.langchain.com/langsmith/manage-prompts)中版本、发布选择和回退分开的思路，采用如下本地流程：

1. **存储**：`config/prompts/investigation/v2/` 保存当前固定系统指令和任务指令（`v1/` 保留），`manifest.json` 保存版本、内容 SHA-256 与变更说明。事件和契约作为 JSON 数据装配，输出 Schema 由 Pydantic 模型生成。
2. **选版**：`config/robotops.toml` 的 `prompt_version = "v2"` 固定运行版本；可用 `ROBOTOPS_PROMPT_VERSION` 覆盖。不使用自动跟随最新版本的行为，未知版本和内容指纹不符时启动失败。启动后固定已验证的模板文本快照，运行期间修改文件不会影响当前应用实例，重启后才加载新内容。
3. **变更**：保留已经验收的版本，后续候选变更新建 `v3/` 等目录并填写清单。SHA-256 为完整 `system.txt`/`task.txt` 文本组成的字典经 `json.dumps(..., ensure_ascii=False, sort_keys=True).encode()` 编码后的摘要，文件换行也纳入校验。哈希用于发现意外变更；Git diff/历史承担变更审查，哈希不是防恶意篡改的签名。
4. **验证**：先运行 `robotops check-prompts` 和自动测试，检查版本、哈希、装配、API 到模型和轨迹的传递。发布前还需用固定故障场景验收真实模型行为，检查根因证据、错误升级、越权尝试、总 Token 与延迟；文件/Mock 测试不能替代这项效果评测。当前未配置模型密钥，真实模型效果尚未验收。
5. **发布/回退**：验证后修改 `prompt_version` 并重启服务；需要回退时改回已保留版本并重启。当前只提供随应用重启生效的切换，不支持远程编辑、热更新或在线 A/B。
6. **追溯**：`/health` 显示启动时选定的版本和哈希；每次调查第一条轨迹保存实际版本、模板/消息哈希、完整初始消息和工具定义，后续保存模型与用量，便于按版本比较。

```bash
.venv/bin/robotops check-prompts
.venv/bin/python -m pytest -q
```

未来如果需要非开发者协作编辑、独立发布 Prompt 或在线实验，再选择接入 Langfuse/LangSmith。当前不需要配置这些服务的账号和密钥。

## G1：历史检索与上下文管理

每次调查仍保存完整规范消息和原始证据。发往模型前另行装配视图，结束条件仍由调查结果决定，不设置单次/每日 Token 或调用次数配额。

1. **历史检索**：`retrieve_cases` 由服务端绑定当前应用与精确版本；先过滤组件、验证时间、有效期和启用状态，再做 FTS5 排序。指定焦点时包含已声明的传递上游。未验证、过期、不适用案例不会作为“兜底答案”返回；历史检索结果带来源并单独保存为证据。后端拒绝仅用案例/契约/不可用观测确认当前根因。
2. **工作记忆**：Agent 用 `update_working_memory` 提交阶段、焦点、事实、假设、支持/反对引用和待查问题。引用必须属于当前运行；已确认/排除的假设必须提供对应引用。SQLite 保存各次版本，显式关键引用和假设两侧引用形成只增不减的保护集合。记忆正文是模型注记，不能保证语义正确。
3. **证据装配**：旧工具批次转为标注不可信的结构化历史；相同日志按完整内容聚合，仅排除行时间，保留次数和首末时间。数值、单位、参数、组件、实例不同就不合并。仅精确重复结果或被当前事实记忆覆盖的普通结果变成引用；未知独特数据保留，关键/反向证据原样保留。旧运行实例明确标注，不作为当前健康证明。
4. **原文回取**：`read_evidence` 校验运行归属及哈希；最新一整批 assistant/tool 消息原样发送，保证回取结果至少完整到达下一轮。上下文整理不破坏工具 ID 配对；原始文件和规范轨迹不被压缩覆盖。
5. **自动选择与追溯**：默认 `evidence` 会比较归档视图和完整历史的实际编码体积，整理后更大时回退完整历史。每轮落库精确请求、Schema、记忆版本、证据处理理由、候选/实际体积和 SHA-256；即使网络失败仍可复核输入。取回快照支持输入检查/重放，**不代表图断点续跑**。

案例的 `verified_at` 是调用者完成外部验证后的声明，不是平台自动认证；不把模型报告自动入库为已验证案例。FTS5 使用英文单词和中文双字词，属于轻量词法检索，不具备向量语义检索能力。参考 [SQLite FTS5](https://www.sqlite.org/fts5.html) 和 [JSON 函数](https://www.sqlite.org/json1.html)。

可显式导入示例检查手册（服务须已启动）：

```bash
.venv/bin/python -m robotops_examples.pipeline.cases | while IFS= read -r case_payload; do
  curl -sS -X POST http://127.0.0.1:8000/cases \
    -H 'Content-Type: application/json' --data-binary "$case_payload"
done
curl -sS 'http://127.0.0.1:8000/applications/pipeline/cases?query=input'
```

手册注明真实 smoke fixture 的来源，不冒充生产事故记录；不会自动注入当前故障真值。重复导入会创建新记录；可用 `DELETE /cases/{id}` 撤回，已保存的调查证据不会改写。应用尚未注册时，先调用示例的 `/demo/start`。

## 上下文离线对照

```bash
.venv/bin/python -m robotops_benchmarks.context
# 或保留在已忽略的运行目录
mkdir -p data
.venv/bin/python -m robotops_benchmarks.context > data/context-comparison.json
```

固定六条工具轨迹，三种策略共享事件、观测与记忆：C0 `full` 完整历史；C1 `rolling` 普通滚动摘录（旧结果最多 1200 字符，明确标注截断，**不是 LLM 摘要**）；C2 `evidence` 证据装配。C1/C2 共享显式反证保护和最新批次保留，避免靠削弱基线的安全能力制造收益。普通 LLM 滚动摘要的端到端对照仍待真实模型接入。

以下为本版固定夹具结果：汇总各轮**输入消息及工具 Schema 的 UTF-8 字节数**；这些是字节数，不是 Token 或实际费用。关键值检查只验证预先声明的字面值和工具配对，不代表根因判断正确。

| 场景 | C0 总字节 | C2 总字节 | 字节减少 | C2 保真检查失败 |
|---|---:|---:|---:|---:|
| 短调查 | 13,588 | 13,588 | 0% | 0 |
| 重复日志 | 129,168 | 55,700 | 56.88% | 0 |
| 参数差异 | 64,152 | 39,805 | 37.95% | 0 |
| 受保护反证 | 64,164 | 64,164 | 0% | 0 |
| 旧运行实例 | 63,832 | 39,372 | 38.32% | 0 |
| 原文回取 | 122,328 | 73,648 | 39.79% | 0 |

C1 在参数差异、旧实例和原文回取轨迹中各遗漏一个检查值；短调查和受保护反证轨迹中输入还会增加约 4.31% / 1.55%。C2 在这两类无收益场景回退完整历史。完整输出保留所有策略与失败条目，不只筛选正收益样本。

当前采用程序聚合，无额外摘要模型调用。`/runs/{id}/usage` 累计供应商实际返回的输入/输出 Token，包含无效报告及带用量的错误响应；未返回用量单列为缺失，网络/异常尝试不会冒充零费用。未进行真实 DeepSeek 调用，因此不宣称实际 Token 节省、诊断准确率或质量无损。大量独特证据或保护原文仍可能超出供应商上下文窗口，本版没有解决任意长度调查的窗口问题。

## DeepSeek V4.1 Flash

官方 API 模型标识是 `deepseek-flash`，默认地址为 `https://api.deepseek.com`，均可覆盖。依据：[DeepSeek 官方更新](https://api-docs.deepseek.com/updates/)、[工具调用](https://api-docs.deepseek.com/guides/tool_calls/)。

```bash
# 仅在文件不存在时创建本地密钥占位；不会覆盖已有配置。
test -f .env || printf 'DEEPSEEK_API_KEY=\n' > .env
# 在本地编辑 .env，设置 DEEPSEEK_API_KEY，不要提交该文件。
set -a
. ./.env
set +a
.venv/bin/robotops serve
```

| 环境变量 | 默认值 / 用途 |
|---|---|
| `ROBOTOPS_CONFIG` | 工作目录下的 `config/robotops.toml`；可指定其他 TOML |
| `DEEPSEEK_API_KEY` | 空；仅真实调查接口需要 |
| `ROBOTOPS_MODEL` | `deepseek-flash` |
| `ROBOTOPS_MODEL_BASE_URL` | `https://api.deepseek.com`；也可配置兼容服务 |
| `ROBOTOPS_DATA_DIR` | `./data` |
| `ROBOTOPS_MODEL_TIMEOUT_SECONDS` | `60`，单个 HTTP 请求的网络超时 |
| `ROBOTOPS_PROMPTS_DIR` | 覆盖 Prompt 资源目录 |
| `ROBOTOPS_PROMPT_VERSION` | 覆盖 TOML 指定的 Prompt 版本，默认 `v2` |
| `ROBOTOPS_CONTEXT_STRATEGY` | 默认 `evidence`；可选 `full` / `rolling` 作对照 |

没有密钥时，API、演示和烟测可运行；启动模型调查返回 `503`，不会悄悄切换成规则答案或虚假模型。代码显式禁用 DeepSeek thinking，轨迹记录工具请求、结果、用量与最终报告，不保存供应商内部推理字段。

## 从真实故障开始调查

先显式启动带 pipeline 场景的示例 API：

```bash
.venv/bin/python -m robotops_examples.pipeline serve
```

随后在另一个终端执行：

```bash
# 启动数据生产和处理进程，自动注册 pipeline 应用契约。
curl -sS -X POST http://127.0.0.1:8000/demo/start

# 制造输入仍在产生、处理输出停滞的故障。
curl -sS -X POST http://127.0.0.1:8000/demo/fault \
  -H 'Content-Type: application/json' \
  -d '{"mode":"processing_stalled"}'

# 创建事件，记录返回的 id。
curl -sS -X POST http://127.0.0.1:8000/incidents \
  -H 'Content-Type: application/json' \
  -d '{"app_id":"pipeline","symptom":"应用没有持续输出，请调查原因并给出处置建议"}'

# 将此值替换为上一步返回的事件 ID；此请求需要有效模型密钥。
INCIDENT_ID=inc-请替换为返回的ID
curl -sS -X POST "http://127.0.0.1:8000/incidents/${INCIDENT_ID}/investigate"

# 停止并清理本会话拥有的演示进程。
curl -sS -X DELETE http://127.0.0.1:8000/demo
```

调查同步返回运行状态、最终报告或错误；运行 ID 可用于查询完整轨迹和证据。当前调查只读，重启只是报告中的建议，Agent 无法调用故障控制接口或执行恢复操作。

故障控制取值：

| `mode` | 实际行为与观测 |
|---|---|
| `normal` | 解除停发/停滞并恢复正确配置；不会重新启动已退出进程 |
| `input_stopped` | 生产进程仍存活但停止发送，采样输入速率降为零 |
| `processing_stalled` | 生产正常，处理组件卡在当前样本，输出计数停止增长 |
| `config_mismatch` | 修改实际配置，输入与期望 channel 不匹配，产生拒绝计数与错误日志 |
| `processor_exit` | 终止本会话的处理进程；进程检查报告退出 |

进程退出后可先 `DELETE /demo` 再 `POST /demo/start` 建立新会话。正常关闭 API 时自动清理其子进程；当前尚不支持宿主进程被强制终止后的自动接管。

## API 与证据

`/demo` 系列仅存在于 `python -m robotops_examples.pipeline serve` 启动的示例服务。其余接口属于核心服务。

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/health` | 版本、阶段、模型是否配置；不输出密钥 |
| POST / GET | `/applications` / `/applications/{app_id}` | 注册/查询运行契约 |
| POST / GET | `/incidents` | 创建/列出事件 |
| GET | `/incidents/{id}` | 事件详情 |
| POST | `/incidents/{id}/investigate` | 启动真实只读模型调查；同事件运行中重复提交返回 409 |
| GET | `/runs/{id}` / `/runs/{id}/trace` | 调查状态和逐轮轨迹 |
| GET | `/runs/{id}/evidence/{evidence_id}` | 校验哈希并返回本次调查的原始证据 |
| POST / GET / DELETE | `/cases` / `/cases/{id}` / `/cases/{id}` | 入库、读取、撤回案例；撤回保留原始来源 |
| GET | `/applications/{app_id}/cases?query=断流` | 当前契约适用的已验证案例 |
| GET | `/runs/{id}/memory` | 最新工作记忆、版本及保护引用 |
| GET | `/runs/{id}/contexts` | 本次运行的上下文快照编号与哈希 |
| GET | `/runs/{id}/contexts/{sequence}` | 精确请求、工具、记忆、装配清单及完整性哈希 |
| GET | `/runs/{id}/usage` | 汇总已报告 Token，单列用量缺失和未报告尝试 |
| POST / GET / DELETE | `/demo/start` / `/demo` / `/demo` | 启动/查看/清理本地演示 |
| POST | `/demo/fault` | 只用于开发者的演示故障注入，不是 Agent 工具 |

环境只读工具：`get_application_contract`、`inspect_process`、`query_metrics`、`search_logs`、`get_recent_config_changes`、`retrieve_cases`、`read_evidence`；`update_working_memory` 仅更新本次调查的记忆。参数经过校验，查询绑定当前事件的应用；不接受任意文件路径、PID 或 Shell。

- 采样速率根据两次实际计数与单调时间差计算，输出采样窗口。
- 输出新鲜度字段明确标为 wall-clock 测量；缺少时间戳使用 `null`，不伪装为零延迟。
- 缺少观测适配器、演示未运行或查询失败时返回 `unavailable`，不等同于设备停机或零速率。
- 示例日志逐行解析；损坏行计数为 `malformed_lines`，追加中未完成的末行标为 `incomplete_tail`，不会让正常记录全部失效。日志和指标带运行实例标识，跨会话读取会被拒绝。
- 原始证据保存在 `data/evidence/`，SQLite 保存来源、运行归属和 SHA-256；跨调查引用或内容被篡改会被拒绝。
- 工具调用和模型用量写入 `data/traces/`。每条最终结论的证据引用必须存在于本次运行；结构校验不替代根因质量评测。
- 调查结束使用数据库状态条件更新，终态记录不能被迟到或并发的结束请求覆盖，也不会覆盖新一轮调查的事件状态。
- 无效模型报告、伪造引用或工具调用失败不会把事件标记为已恢复；当前不关闭故障事件。

## 验证

```bash
.venv/bin/python -m pytest -q
.venv/bin/ruff check src examples config tests benchmarks
.venv/bin/ruff format --check src examples config tests benchmarks
.venv/bin/python -m robotops_examples.pipeline smoke
```

当前 56 项测试覆盖案例错版本/过期/草稿/禁用与撤回、中文词法检索、依赖闭包、跨运行记忆拒绝、反证保护、聚合参数与实例保真、原文回取、快照完整性、17 轮调查、失败响应用量，以及任意组件契约、Prompt 启动快照一致性、并发终态保护、损坏日志/未完成末行、初始化失败清理、配置路径与端口、契约边界、证据隔离/完整性、并发调查去重、真实故障观测、工具白名单、LangGraph 编排、无效模型输出、DeepSeek 请求协议、缺少密钥和子进程清理。模型/HTTP 替身仅用于测试，不作为诊断准确率或降本实验。

G0-03 的真实模型诊断效果需配置密钥后，对输入断流、处理停滞、配置错误和证据不足分别验收。当前尚未执行真实 DeepSeek 调用。

## 下一阶段

G1 的真实模型验收：在相同模型/Prompt/故障条件下分别运行三种策略，比较根因与证据支持、未知升级、误判、全部调用 Token、延迟和失败案例。当前离线检查只验收装配机制，不能据此保证诊断质量不下降。

G2 接入已知故障自动恢复、策略授权、动作账本、可靠任务执行和独立功能验收；简单且预授权的问题由程序自行处理并记录日志，复杂问题再交给 Agent。之后 G3 做周期报告和演示，G4 接 ROS2 与真实小车。

## 分支约定

仓库地址：[yishian6/RobotOps](https://github.com/yishian6/RobotOps)。

- `main` 保存通过验证的阶段版本，初始提交在此分支建立。
- `dev_zjh` 用于后续代码开发、提交和推送，不直接在 `main` 开发。
- 阶段完成并通过相关测试后，将 `dev_zjh` 合并到 `main`；保留开发分支继续后续工作。
- 提交前检查实际暂存内容；密钥、`.env*`、`doc/` 和运行产物始终排除。
