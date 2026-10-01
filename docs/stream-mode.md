# Stream mode

TUN 的 `stream` 模式使用 turntf core 的 dedicated point-to-point stream path。帧不会进入 transient acceptance。每个 peer 先发送 `Open`，收到 `OpenAck` 后才发送 `Data`。`Ack` 使用累计 offset 和 window credit；路径切换使用新的 epoch 与 `Resume`，旧 epoch 帧会被忽略。

`ResolveUserSessions` 返回多个 transient session 时，`Open` 和 `Resume` 会按返回顺序逐个尝试。发送失败、异步发送错误或握手超时只淘汰当前候选。`Open` 候选耗尽后按 `dial_retry_interval` 退避并重新解析；`Resume` 候选耗尽或会话解析失败后结束本轮恢复，创建新的 stream ID 并重新 `Open`，新建失败再按同一间隔退避。远端进程重启后可能已丢失旧 stream 状态，反复 `Resume` 同一个 ID 无法恢复该状态。`OpenAck` 和 `Ack` 同时按 stream ID、epoch 与 `TargetSession` 匹配，旧 session 的迟到响应不能激活当前候选。

传输模式按 peer 解析。`peers[].transport_mode` 可设置为 `stream`、`relay` 或 `auto`；省略时继承全局 `transport.mode`。只有 effective mode 为 `stream` 的 peer 才运行 stream lifecycle 和发送 `Open`。`relay` 与 `auto` peer 保持原有 `shouldDial` / Relay dial loop，且不会因其他 peer 使用 stream 而发送 `Open`。

stream transport 或握手失败时，仅该 stream peer 会按 `dial_retry_interval` 持续重试 stream。只有 `dial_policy` 选中的一侧会为该 peer 建立 Relay fallback，另一侧只接受入站 Relay，确保每个 peer 最多存在一个 fallback 建连循环。

会话解析和 Relay 建连尝试使用 `turntf.request_timeout` 限制响应等待，默认 `10s`；超时只取消当前尝试，后续按现有生命周期继续重试。stream 未确认数据的 ACK 停滞检测也覆盖队列空闲时的少量流量，避免小请求一直停留在坏 stream 而无法触发满窗口检测。默认停滞期限为 `5s`，空闲检测每四分之一期限采样一次；完全已确认的空闲 stream 保持连接，不会因没有业务流量被切换。

Relay 与 stream 是单个 peer 上互斥的当前出口。stream 握手成功后立即接管该 peer 的 TUN 出口，但 Relay fallback 保持 warm；同一用户的多条入站 Relay 连接也都保持接收能力。stream 路径中断时先切回 Relay 出口，并使用原 stream ID 和未确认数据执行 `Resume`；如果远端已丢失 stream 状态且候选均不可恢复，则创建新的 stream。重建时旧 stream 队列与未确认 batch 会被释放，内层 TCP 通过自身重传恢复，UDP packet 可能丢失；重建只影响该 peer，保持 warm Relay 和其他 peer 的传输模式。

## 吞吐

TUN packet 以 2 字节长度前缀聚合成 batch，单帧不超过 SDK 的 128 KiB 上限，旧版本接收端可直接解码。发送窗口占满时，发送循环等待 ACK 或路径中断唤醒（单次等待最长 50ms，以保持 ACK 停滞采样），醒来后先把等待期间入队的 packet 并入同一 batch，负载越高帧越大，核心逐帧路由与 ACK 开销随之下降。

接收侧收到 DATA 后只在共享读取协程中写 TUN，并把累计 ACK 交给该 receiver 的写协程；同一 epoch 内只保留最新的未发 ACK。`Resume` 的 ACK 仍同步发送并丢弃旧 epoch 的未发 ACK，receiver 被新 `Open` 替换或收到 `Close` 时停止写协程。
