# Stream mode

TUN 的 `stream` 模式使用 turntf core 的 dedicated point-to-point stream path。帧不会进入 transient acceptance。每个 peer 先发送 `Open`，收到 `OpenAck` 后才发送 `Data`。`Ack` 使用累计 offset 和 window credit；路径切换使用新的 epoch 与 `Resume`，旧 epoch 帧会被忽略。

传输模式按 peer 解析。`peers[].transport_mode` 可设置为 `stream`、`relay` 或 `auto`；省略时继承全局 `transport.mode`。只有 effective mode 为 `stream` 的 peer 才运行 stream lifecycle 和发送 `Open`。`relay` 与 `auto` peer 保持原有 `shouldDial` / Relay dial loop，且不会因其他 peer 使用 stream 而发送 `Open`。

stream transport 或握手失败时，仅该 stream peer 会按 `dial_retry_interval` 持续重试 stream。只有 `dial_policy` 选中的一侧会为该 peer 建立 Relay fallback，另一侧只接受入站 Relay，确保每个 peer 最多存在一个 fallback 建连循环。

Relay 与 stream 是单个 peer 上互斥的当前出口。stream 握手成功后会取消该 peer 的 fallback、关闭其已有 Relay 并立即接管该 peer 的 TUN 出口；其他 peer 的 Relay 不受影响。后续路径中断会先使用原 stream ID 和未确认数据执行 `Resume`；如果远端已丢失 stream 状态且 `Resume` 超时，则创建新的 stream。
