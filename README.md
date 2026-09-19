# turntf-tun

`turntf-tun` 是基于 turntf Relay 的 Linux 三层 overlay。它创建 TUN 虚拟网卡，读取 IP packet，按照目标 IP 的最长前缀路由到指定 turntf peer，再把对端 packet 写回本地 TUN。

## 数据面设计

- 只传输 IP packet，不传 Ethernet header、MAC、ARP 或广播帧。
- 每个 peer 配置明确的 CIDR 路由，支持 IPv4 和 IPv6。
- Relay 使用 `at_least_once`，并在每个 peer 上把 IP packet 聚合成最多 128 KiB 的 batch；acceptance RPC 有界并行，避免高 RTT 链路上逐包等待，同时不引入可靠有序重排。IP 层上面的 TCP/UDP 仍负责自己的语义。
- 每个 peer 有界发送队列；队列满时丢包，TUN 读循环不会等待慢 Relay。
- 每个 peer 使用独立的数据面状态；可通过 `peers[].transport_mode` 在同一进程内混用 Relay 和 stream，单个 peer 的 stream 建连、重试或拥塞不会切换其他 peer 的传输模式。
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

## 容器镜像

`master`、`v*` tag 和手动触发的 GitHub Actions 会在测试通过后把单平台 Linux 镜像推送到：

```text
ghcr.io/tursom/turntf-tun
```

每次发布都有 `sha-<commit>` 标签；`master` 同时更新 `latest`。本地构建：

```bash
docker build --platform linux/amd64 -t turntf-tun:local .
```

容器必须显式映射 TUN 设备并授予 `NET_ADMIN`，配置文件应只读挂载：

```bash
docker run --rm \
  --device /dev/net/tun:/dev/net/tun \
  --cap-add NET_ADMIN \
  -v "$PWD/config.yaml:/etc/turntf/config.yaml:ro" \
  ghcr.io/tursom/turntf-tun:latest \
  run -c /etc/turntf/config.yaml
```

不要把 turntf 密码写入镜像或提交到仓库。生产配置应使用宿主机权限受限的文件或容器编排系统的 secret 挂载。镜像不需要 `--privileged`；只有部署环境无法单独映射 TUN 设备或 capability 时，才应评估更宽权限及其风险。

## Compose 部署

`deploy/docker-compose.yml` 是三台节点共用的 Compose 定义，生产镜像固定为已核实的 digest：

```text
ghcr.io/tursom/turntf-tun@sha256:ab581c485bc8aecaf324f4b71e3ae8765e30c35fc1956fcae381f9fa0dea2d19
```

三份节点模板位于 `deploy/kr/config.example.yaml`、`deploy/cc/config.example.yaml` 和 `deploy/kiwi/config.example.yaml`。模板不包含真实密码和 peer ID，填充后应保存为节点本地 `/opt/turntf/tun/config.yaml`，权限设置为 `0600`；部署目录设置为 `0700`。

示例部署步骤：

```bash
mkdir -p /opt/turntf/tun
chmod 700 /opt/turntf/tun
install -m 600 config.yaml /opt/turntf/tun/config.yaml
install -m 600 docker-compose.yml /opt/turntf/tun/docker-compose.yml
cd /opt/turntf/tun
export IMAGE_REF='ghcr.io/tursom/turntf-tun@sha256:ab581c485bc8aecaf324f4b71e3ae8765e30c35fc1956fcae381f9fa0dea2d19'
docker compose config
docker compose pull
docker compose up -d
docker compose ps
```

容器只使用 host network、`/dev/net/tun` 和 `NET_ADMIN`，没有使用 `privileged`。启动前必须把模板中的 `REPLACE_*`、`node_id` 和 `user_id` 替换为真实值；`node_id: 0` 或占位密码不能用于生产启动。

三台节点的建议虚拟地址为（本地使用 `/32`，对端路由由程序按 `peers.routes` 安装）：

```text
kr    10.250.0.1/32
cc    10.250.0.2/32
kiwi  10.250.0.3/32
```

停止或回滚时只操作 `/opt/turntf/tun` 这个 Compose 项目，不要停止现有核心、Web、forward、newt 或 udp2raw 服务。生产变更前先备份该目录和配置；部署后验证三节点之间的 ICMP、TCP、UDP 以及容器 restart count。



kr、cc、kiwi 可以使用同一虚拟网段，但每个节点只把对端地址路由给对应 peer：

```yaml
# kr
tun:
  name: "turntf0"
  mtu: 1400
  addresses: ["10.250.0.1/32"]
peers:
  - name: "cc"
    user: {node_id: 1, user_id: 2}
    routes: ["10.250.0.2/32"]
  - name: "kiwi"
    user: {node_id: 1, user_id: 3}
    routes: ["10.250.0.3/32"]
```

cc 和 kiwi 使用对应的本地地址和 peer 路由。三台节点应互相列入 `peers`，并为三台节点分别配置唯一的 turntf 用户。

## 逐 peer 传输模式

全局 `transport.mode` 和可选的 `peers[].transport_mode` 都接受 `auto`、`relay`、`stream`。peer 未设置 `transport_mode` 时继承全局值；显式设置时只覆盖该 peer。`auto` 保持原有 Relay 建连规则，由 `dial_policy` 和双方 user ref 决定拨号侧；`relay` 同样使用该 Relay 路径；只有 effective mode 为 `stream` 的 peer 才启动 stream lifecycle，并在 stream 不可用时按 `dial_retry_interval` 重试及按 `dial_policy` 建立 Relay fallback。

```yaml
transport:
  mode: "relay"
peers:
  - name: "cc"
    user: {node_id: 1, user_id: 2}
    routes: ["10.250.0.2/32"]
    transport_mode: "stream"
    dial_policy: "auto"
  - name: "kiwi"
    user: {node_id: 1, user_id: 3}
    routes: ["10.250.0.3/32"]
    # transport_mode 省略，继承全局 relay
    dial_policy: "auto"
```

stream 激活后的出口优先级也按 peer 隔离：只有目标 peer 已激活 stream 时才走 stream，其他 peer 继续使用各自的 Relay 连接。详细协议与 fallback 行为见 [`docs/stream-mode.md`](docs/stream-mode.md)。

## 性能边界

TUN 进程会在队列满时丢弃 packet，因此它不会用无界内存换吞吐。最终性能仍受 turntf WebSocket、核心节点调度、Relay 窗口和公网路径影响。部署前应同时观察吞吐、丢包、首包延迟、并发空窗、内存和长尾；不能只用单次下载速度选择 MTU。
