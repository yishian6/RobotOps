---
name: ros1-topic-silence
description: 调查本次运行中由 ROS 1 采集器记录的 Topic 首条消息超时或停发。
---

## ROS 1 Topic 静默排查

1. 核对触发观测的应用、Topic、时间和证据 ID。`topic_startup_timeout` 的 `silent_ms` 从采集器订阅开始算起，期间未收到首条消息，`last_message_at_ms` 为 `null`。`topic_silent` 的 `silent_ms` 从最近一条消息算起。用 `threshold_ms` 解释采集触发。若契约还有输出静默上限，换算单位后分别说明。
2. 静默表示采集端在该时段没有收到新消息。用已开放的现场工具区分发布进程退出、上游输入中断、处理停滞和接收路径异常。先查能区分现有假设的一项；已给出的触发字段可以直接引用。
3. 结合当前实例和时间核对证据。如果只能确认静默现象，报告 `needs_human`，写出最能区分原因的一项待补观测。
4. 最终报告引用原始触发证据，说明已确认现象、证据支持的判断和待查项。
