# RobotOps

面向机器人应用的本地故障监测、证据调查与受控恢复工具。C++ 在 ROS 1 侧记录 Topic 异常，Python 负责派发调查、执行只读工具和保存报告。操作员可以为特定 launch 启用一次异常退出重启，随后用原 Topic 的持续新消息验收。

**开发版本** `dev_zjh` 0.5.1　·　**已发布版本** `v0.5.0`

## 项目架构

RobotOps 在机器人主机上运行。**C++ 负责 ROS 1 感知和 Topic 验收，Python 负责事件、调查和受控重启。** DeepSeek 根据当前证据选择下一项只读检查，工具执行、证据保存和恢复状态都由本地程序处理。

### 故障数据如何流转

1. **ROS 1 自动调查。** C++ 检测首条消息超时或发布后停发，先将来源、序号、时间和阈值写入 `ros-facts.db`，再通过 Unix Socket 通知 Python。Worker 按来源和序号将实时消息与数据库回放去重，把触发事实交给 Agent，并按需加载 Topic 排查 Skill。
2. **异常退出调查。** Runner 记录普通命令或 ROS launch 节点的退出码和故障前控制台位置。systemd 监测负责已登记服务的退出事件。可以从 API 发起调查，Agent 按事件回查已有日志或其他可用观测。
3. **证据与报告。** LangGraph 负责模型与工具往返。网关校验工具参数、应用范围与证据归属。长日志先给预览，再按原证据分页。上下文装配保留任务和关键证据，超过阈值按优先级裁剪。长调查还可用工作记忆记录假设、支持证据和反证。最终报告通过引用和原文校验后保存，逐轮请求可从快照回查。

### 恢复如何验收

操作员通过 `--restart-on-exit` 授权 Runner 在指定 launch 异常退出后按原参数重启一次。新实例先过进程稳定检查，启用 `--verify-topic` 时再由 C++ 验收同一发布节点在原 Topic 上的连续新消息。业务验收 `PASSED` 后，Python 保存回执并关闭关联退出事件。

### 主要模块与存储

| 模块 | 职责 |
|---|---|
| [C++ 采集器](cpp/ros1_facts/) | ROS 1 消息到达观测、事实持久化、实时通知及重启后的 Topic 验收 |
| [Python 平台](src/robotops/) | FastAPI、应用契约、事件与任务、Runner、工具网关和证据校验 |
| [Agent 层](src/robotops/agent/) | DeepSeek 原生工具调用、LangGraph 循环、上下文、工作记忆和报告校验。默认单 Agent，手动调查可分派应用和系统子任务 |
| SQLite、证据文件与 Redis | `ros-facts.db` 保存 C++ 事实，`runtime.db` 保存事件、任务、案例、证据索引、快照与报告。文件保留证据原文和 Trace，Redis 可缓存已校验的日志页 |
| [配置](config/)、[示例](examples/)、[评测](benchmarks/)、[测试](tests/) | 运行配置、版本化 Prompt、Skill、可运行故障场景、对照实验和回归用例 |

[项目技术文档](doc/RobotOps_技术文档.md) · [阶段汇报与实测](doc/RobotOps_当前版本汇报.md) · [上下文管理](doc/RobotOps_上下文管理.md)

## 快速开始

需要 Linux、Python 3.11+，以及支持 FTS5 和 JSON 函数的 SQLite。用 uv 从锁文件安装。

```bash
git clone --branch dev_zjh https://github.com/yishian6/RobotOps.git
cd RobotOps
uv sync --locked --extra dev
```

先跑隔离的 UDP 示例。它会创建真实子进程并验证故障观测，无需 ROS 或模型密钥。

```bash
uv run python -m robotops_examples.pipeline smoke
```

### 跑一次真实模型调查

在本地 `.env` 中提供 `DEEPSEEK_API_KEY`，启动示例 API。服务默认监听本机回环地址。

```bash
uv run --env-file .env python -m robotops_examples.pipeline serve
```

在另一个终端启动示例进程，注入处理停滞并创建事件。

```bash
curl -fsS -X POST http://127.0.0.1:8000/demo/start
curl -fsS -X POST http://127.0.0.1:8000/demo/fault \
  -H 'Content-Type: application/json' -d '{"mode":"processing_stalled"}'
curl -fsS -X POST http://127.0.0.1:8000/incidents \
  -H 'Content-Type: application/json' \
  -d '{"app_id":"pipeline","symptom":"生产端继续发送，处理端停止输出"}'
```

将上一步返回的 `id` 填入 `INCIDENT_ID`。调查请求会调用模型，清理命令会停止示例子进程。

```bash
INCIDENT_ID='<事件 ID>'
curl -fsS -X POST "http://127.0.0.1:8000/incidents/${INCIDENT_ID}/investigate"
curl -fsS -X DELETE http://127.0.0.1:8000/demo
```

只启动核心 API 时用 `uv run robotops serve`。示例和核心服务都可从本机打开交互式接口。 [API 文档](http://127.0.0.1:8000/docs)

## ROS 1 故障检测与恢复

### 构建采集器

构建需要 C++17、CMake 3.16+、Noetic 的 `roscpp` 和 `topic_tools`，以及 SQLite3、nlohmann/json 开发文件。

```bash
source /opt/ros/noetic/setup.bash
cmake -S cpp/ros1_facts -B build/ros1_facts -DPYTHON_EXECUTABLE=/usr/bin/python3
cmake --build build/ros1_facts --parallel 2
mkdir -p data
```

### 用 ROS 1 Topic 触发自动调查

各终端使用同一个 ROS Master 和数据目录。示例应用 `camera` 需先登记，可通过核心 API 的 `POST /applications` 提交 `{"app_id":"camera","components":["camera"]}`，也可使用之前已接入的应用。测首条消息时先启动 ROS Master、Worker 和采集器，再启动被测发布节点。

```bash
# 终端一，启动接收事实和调用模型的 Worker。
uv run --env-file .env robotops watch-ros-facts camera \
  --socket data/camera-ros-facts.sock --facts-db data/camera-ros-facts.db

# 终端二，加载 Noetic 后启动采集器。
build/ros1_facts/robotops_ros1_facts camera /camera/image_raw 2000 \
  data/camera-ros-facts.sock data/camera-ros-facts.db --startup-timeout-ms 10000
```

示例中，订阅后等不到首条消息的阈值是 **10 秒**，开始收到消息后的停发阈值是 **2 秒**。每个事实库绑定一个应用和一个 Topic，Worker 的 `--facts-db` 指向采集器写入的库。Python 将实时通知和只读回放按来源、序号去重。

自动调查默认最多 6 次模型请求。只收到完整的触发事实、缺少现场适配器且没有适用案例时，首轮直接提交报告。适用的 Topic Skill 随系统消息下发，名称和指纹可在 Trace 查到。 [首次请求与工具流转](doc/RobotOps_上下文管理.md#ros-1-自动调查的首次请求)

### 为一个独立的 ROS 1 launch 启用一次重启

在加载 Noetic 和应用工作空间的终端，用原 launch 命令接入。

```bash
uv run robotops run --restart-on-exit camera -- roslaunch camera_driver camera.launch
```

launch 异常结束时，Runner 按原参数重启一次。故障前的控制台片段、退出码和新旧启动编号可随原事件回查。只记录普通命令退出时，可用 `uv run robotops run image-worker -- python worker.py`。

### 验收重启后的原 Topic

在同一条命令上指定实际的输出 Topic 和已构建的采集器。

```bash
uv run robotops run --restart-on-exit --verify-topic /camera/image_raw \
  --ros-facts-bin build/ros1_facts/robotops_ros1_facts camera -- \
  roslaunch camera_driver camera.launch
```

重启的 launch 稳定后，C++ 检查同一发布节点的连续新消息，跳过首条锁存回放。允许的消息间隔取契约的 `health.max_output_silence_seconds`，默认 1 秒。连续窗口至少 5 秒，且至少是允许间隔的三倍。可在事件详情的 `launch_restart.topic_acceptance` 查看验收结果。只有 `PASSED` 会把关联退出事件转为 `RESOLVED`。 [真实 Noetic 验收](doc/RobotOps_当前版本汇报.md#m4-重启后的-topic-业务验收)

### 监测已有 systemd 服务

先启动核心 API，再登记已有服务。下面以用户级服务为例，替换为自己的单元名。

```bash
curl -fsS -X POST http://127.0.0.1:8000/services \
  -H 'Content-Type: application/json' -d '{"unit":"my-worker.service","scope":"user"}'
uv run robotops monitor
```

系统服务使用 `scope=system`。监测器记录状态和异常退出，可用 `--once` 查询一轮。

## 数据和证据怎么查

默认数据目录为 `data/`。服务、Runner 和 Worker 若从不同目录启动，请统一设置绝对路径 `ROBOTOPS_DATA_DIR`。

| 数据 | 位置 |
|---|---|
| ROS 1 来源和触发事实 | C++ 写入独立的 `ros-facts.db`，路径由采集器参数指定 |
| 事件、任务、验收回执、报告、工作记忆和快照 | Python 的 `runtime.db` |
| 普通观测原文 | `evidence/`，SQLite 索引保留运行归属和 SHA-256 |
| 模型与工具轨迹、受管进程控制台 | `traces/` 和 `executions/` |

从 `GET /incidents/{id}` 查触发事实和恢复回执，自动调查的运行 ID 在 `automatic_run_id`。用 `GET /runs/{run_id}` 看报告，用该运行下的 `/evidence/{evidence_id}` 回查原始证据。查看 `/trace` 中的 `context_sequence`，再查 `/contexts/{sequence}` 可还原模型当轮实际输入。运行 `COMPLETED` 表示报告已交付，事件 `RESOLVED` 表示原 Topic 的恢复验收通过。

常用配置包括 `DEEPSEEK_API_KEY`、`ROBOTOPS_MODEL`、`ROBOTOPS_DATA_DIR` 和 `ROBOTOPS_CONTEXT_COMPACT_AFTER_TOKENS`。配置优先级为环境变量、TOML、默认值。默认输入估算裁剪阈值为 131,072 Token，模型总窗口单独配置。 [运行配置](config/robotops.toml) · [窗口与快照细节](doc/RobotOps_上下文管理.md#6-m5-请求前的窗口裁剪)

### 可选 Redis 分页缓存

SQLite 核对证据归属，文件保存原文。Redis 暂存已校验的日志分页。命中仍核对归属和文件身份，缓存不可用时回读原文。

```bash
uv sync --locked --extra dev --extra redis
export ROBOTOPS_REDIS_CACHE_URL=redis://127.0.0.1:6379/0
uv run --no-sync robotops serve
```

## 测试与实验

```bash
uv run pytest -q -rs
uv run robotops check-prompts
uv run python -m robotops_benchmarks.context
uv run python -m robotops_benchmarks.e2e --provider scripted \
  --scenarios processing_stalled --strategies evidence --modes single --repeats 1

# 构建采集器并加载 Noetic 后跑全套。
ROBOTOPS_ROS1_FACTS_BIN="$PWD/build/ros1_facts/robotops_ros1_facts" \
  uv run pytest -q -rs
```
