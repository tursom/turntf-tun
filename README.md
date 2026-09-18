# turntf-tun

`turntf-tun` 是基于 turntf Relay 的 Linux 三层 overlay。它创建 TUN 虚拟网卡，读取 IP packet，按照目标 IP 的最长前缀路由到指定 turntf peer，再把对端 packet 写回本地 TUN。

## 数据面设计

- 只传输 IP packet，不传 Ethernet header、MAC、ARP 或广播帧。
- 每个 peer 配置明确的 CIDR 路由，支持 IPv4 和 IPv6。
- Relay 固定使用 `best_effort`：无 ACK、无 Relay 重传、无 Relay 排序，避免与 IP 层上面的 TCP 重传叠加。
- 每个 peer 有界发送队列；队列满时丢包，TUN 读循环不会等待慢 Relay。
- 每个 peer 使用独立的 Relay 连接和收发循环，单个出口拥塞不会阻塞其他 peer。
- TUN MTU 应按实际出口和 turntf 封装开销选择。默认 `1400`，部署后可通过吞吐、丢包和长尾测试调整。

这不是二层交换机，不提供 ARP、广播、组播泛洪、NAT 或真实 LAN 网段转发。需要二层互通时使用 `turntf-tap-switch`。

## 构建和配置校验

```bash
go build -o turntf-tun ./cmd/turntf-tun
./turntf-tun example-config > config.yaml
./turntf-tun check-config -c config.yaml
sudo ./turntf-tun run -c config.yaml
```

需要 Linux、root 或 `CAP_NET_ADMIN`，以及每个节点独立的 turntf 登录账号。

## 路由示例

kr、cc、kiwi 可以使用同一虚拟网段，但每个节点只把对端地址路由给对应 peer：

```yaml
# kr
tun:
  name: "turntf0"
  mtu: 1400
  addresses: ["10.250.0.1/24"]
peers:
  - name: "cc"
    user: {node_id: 1, user_id: 2}
    routes: ["10.250.0.2/32"]
  - name: "kiwi"
    user: {node_id: 1, user_id: 3}
    routes: ["10.250.0.3/32"]
```

cc 和 kiwi 使用对应的本地地址和 peer 路由。三台节点应互相列入 `peers`，并为三台节点分别配置唯一的 turntf 用户。

## 性能边界

TUN 进程会在队列满时丢弃 packet，因此它不会用无界内存换吞吐。最终性能仍受 turntf WebSocket、核心节点调度、Relay 窗口和公网路径影响。部署前应同时观察吞吐、丢包、首包延迟、并发空窗、内存和长尾；不能只用单次下载速度选择 MTU。
