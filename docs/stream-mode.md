# Stream mode

TUN 的 `stream` 模式使用 turntf core 的 dedicated point-to-point stream path。帧不会进入 transient acceptance。每个 peer 先发送 `Open`，收到 `OpenAck` 后才发送 `Data`。`Ack` 使用累计 offset 和 window credit；路径切换使用新的 epoch 与 `Resume`，旧 epoch 帧会被忽略。

stream transport 或握手失败时，peer 会按 `dial_retry_interval` 持续重试 stream。只有 `dial_policy` 选中的一侧会建立 Relay fallback，另一侧只接受入站 Relay，确保每个 peer 最多存在一个 fallback 建连循环。

Relay 与 stream 是互斥的当前出口。stream 握手成功后会取消 fallback、关闭已有 Relay 并立即接管 TUN 出口。后续路径中断会先使用原 stream ID 和未确认数据执行 `Resume`；如果远端已丢失 stream 状态且 `Resume` 超时，则创建新的 stream。
