# xtunnel

x-tunnel 多通道 WebSocket 隧道的 **Go 客户端核心库**（module `github.com/v2up-32mb/xtunnel`）。

从 [`x-tunnel`](https://github.com/v2up-32mb/x-tunnel)（CLI 仓库，现 `xtunnel-cli`）中提取的协议实现层，
供 Android 客户端（[`x-client`](https://github.com/v2up-32mb/x-client) 的 gomobile AAR）与 CLI 共同引用。
协议实现以 x-tunnel 上游 commit `5b10fc1` 为基线。

## 能力

- **8 字节二进制协议**（`protocol/`）：connID + msgType 帧编解码，多路复用 WebSocket 通道
- **连接池**（`pool.go`）：多通道 WebSocket、下行通道选择（selectDownlink 竞争）、
  背压控制（写队列字节上限 + 聚合写 worker）、快重试、统计
- **代理适配器**（`proxy_dialer.go`）：池 → `xshared/dialer` 的桥接
  - `streamDialer()`：TCP 流拨号（注册 → MsgTCPConnect（Hot Pair/广播）→ 等待建立 → 内存管道）
  - `udpDialer()`：UDP 通道拨号（UDP ASSOCIATE 语义、端口黑名单、IPStrategy 过滤、懒启动竞速）
  - `ParseSocks5Auth`/`AuthEqual`：本地代理监听地址与鉴权工具
- **反向模式**（`reverse.go`）：服务端按客户端传入的监听值开监听，流量由客户端出网
  （`cfg.EnableReverse` + `cfg.ReverseListeners`，客户端处理服务端 `MsgTCPConnect` 拨号目标；
  支持服务端主动拨号单播直达）
- **中继节点管理**（`relay.go`）：节点评分、加权负载均衡（负载因子降权）、测速循环、健康检查
- **Hot Pair 双端热表**（`pair_warmer.go`）：客户端预热通道对并经 `MsgHotPairNotify` 批量通知
  服务端建表；拨号期零选路消息（connID = 预热键.唯一后缀），Pair 共享复用，正反双模式通用
- **ECH/DoH 共享栈**（`ech_bridge.go`）：复用 [`xshared`](https://github.com/v2up-32mb/xshared) 的
  `ech`/`dns`/`config`（DoH 多服务器 fallback → UDP DNS → 标准 TLS 降级）

## 用法

```go
cfg := xtunnel.DefaultConfig()
cfg.ServerAddr = "wss://worker.example.com/xxx"
cfg.Token = "..."
cfg.Connections = 4

p, err := xtunnel.NewClientPool(cfg, ctx, cancel)
// ... Start 后通过适配器接入 xshared SOCKS5/HTTP 代理服务器：
// client := xtunnel.NewClient(p)  // 或直接经 clientPool 适配器
// client.StreamDialer() / client.UDPDialer()
```

反向模式（`-l` 值改为由服务端开监听，流量由客户端出网）：

```go
cfg.EnableReverse = true
cfg.ReverseListeners = []string{"socks5://user:pass@0.0.0.0:30000"} // 绑定地址由服务端解释
```

完整接入示例见 `xtunnel-cli`（CLI 壳仓库）与 `x-client/golib`（gomobile 入口）。

## 依赖

- `github.com/v2up-32mb/xshared`（ECH/DoH/配置/拨号接口）
- `github.com/gorilla/websocket`、`github.com/google/uuid`

## 测试

```bash
go test ./... -race
```

## 许可

与上游 x-tunnel 一致。
