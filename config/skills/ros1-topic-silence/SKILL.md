---
name: ros1-topic-silence
description: 调查本次运行中由 ROS 1 采集器记录的 Topic 首条消息超时或停发。
---

## ROS 1 Topic 静默排查

1. 核对触发观测的应用、Topic、时间和证据 ID。`topic_startup_timeout` 的 `silent_ms` 从采集器订阅开始算起，期间未收到首条消息，`last_message_at_ms` 为 `null`。`topic_silent` 的 `silent_ms` 从最近一条消息算起。用 `threshold_ms` 解释采集触发。若契约还有输出静默上限，换算单位后分别说明。
2. 已给出的触发事实可以直接引用。现场工具可用时，先查能区分发布进程、上游输入和接收路径的实测。案例工具可用时，按症状查询一次；空结果可以直接记录，无需回读或重查。手头只有触发观测时，直接提交 `needs_human`。
3. 当前观测是输出 Topic 的到达情况，和契约的输出静默上限比较。报告结论限于采集窗口内的消息到达，窗口之后是否开始发布留待观测。
4. 待查项使用 ROS 1 Noetic 的观测。优先用 `rostopic info` 查看同一 ROS Master 上目标 Topic 的发布节点登记；用 `rosnode info` 核查实际连接，用 `rostopic hz` 核查实时消息到达。登记、连接和消息到达是三项独立观测。ROS 2 / DDS QoS 不适用于该链路，不写入报告或候选原因。根因依据不足时提出一项最能区分原因的检查。
5. 最终报告引用原始触发证据，说明已确认现象、证据支持的判断和待查项。
