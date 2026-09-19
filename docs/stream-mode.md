# Stream mode

TUN 的 `stream` 模式使用 turntf core 的 dedicated point-to-point stream path。帧不会进入 transient acceptance。每个 peer 先发送 `Open`，收到 `OpenAck` 后才发送 `Data`。`Ack` 使用累计 offset 和 window credit；路径切换使用新的 epoch 与 `Resume`，旧 epoch 帧会被忽略。

stream transport 或握手失败时，peer 回退到配置已有的 Relay path。Relay 与 stream 是互斥的当前出口，避免同一 IP packet 同时从两条路径交付。
