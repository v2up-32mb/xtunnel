# AGENTS.md — xtunnel 核心库协作指引

面向在该仓库工作的开发者与 AI agent。本文件用于取代 Claude 专有命名
（`CLAUDE.md`），采用通用的 `AGENTS.md`，任何 agent / 编辑器 / 工具都按此约定读取。

## 项目定位

`github.com/v2up-32mb/xtunnel` 是多通道 WebSocket 隧道的 **Go 客户端核心库**，
被下游壳（`xtunnel-cli` 的 CLI、`x-client` 的 gomobile AAR）共同引用。
协议实现以 x-tunnel 上游 commit `5b10fc1` 为基线。

## 硬性约束（不得违反）

1. **核心库零直接日志输出**。任何需要展示的信息必须经 `LogEvent` 回调
   （`SetLogf`，默认静默）交给壳；壳负责等级过滤、模块路由与输出。
   - 等级约定：诊断/竞速→`LevelDebug`，常规流程/访问→`LevelInfo`，
     非致命异常→`LevelWarn`，硬失败→`LevelError`。
   - 永远不要 `log.Printf` / `fmt.Println` 直接打到 stdout。
   - 把 `log_hook.go` 视作日志的**唯一出口**（atomic.Value，可运行时切换，
     注入的 hook 必须并发安全）。
2. **协议消息集自 v0.2.0 起保持稳定**（`MsgHotPairNotify` 0x22 是最近一次新增）。
   任何协议改动必须：
   - 在 `protocol/` 中同步新消息常量与编解码、单元测试；
   - 在 `CHANGELOG.md` 该版本明确标注 **⚠️ 协议变更** 与"服务端需跟随"；
   - 更新 `README.md` 的版本升级速查表。
3. **connID 前缀约定**：`prebind-` 前缀的 connID 是 HotPair 预热键（服务端解析建表）。
   `protocol.ShortID` 对 `prebind-` 形态保留前缀+8 位 UUID 展示（勿改回纯前 8 位截断）。
4. **行为改动不得悄悄发生**。重赛制、日志签名、选路策略这类影响下游行为的变更，
   一律进 CHANGELOG 的"升级指引"，标注是可选还是破坏性。

## 发版流程（每个 tag 必修）

1. 代码 + 测试完成：`go test ./... -race` 全绿。
2. `CHANGELOG.md` 记入该版本：变更分类（Added/Changed/Fixed）+ 升级指引。
3. `README.md`"版本与升级"速查表同步该行。
4. 打语义化 tag 并推送（`git push origin main --tags`）。
   - 文档类修订若无代码变更，随下一个版本 tag 发布，不重打旧 tag。

## 结构速览

| 文件/目录 | 职责 |
|---|---|
| `protocol/` | 8 字节二进制协议、消息常量、错误判定、`ShortID`、IP 策略 |
| `pool.go` | 多通道连接池、下行竞争、背压、快重试、统计 |
| `pair_warmer.go` | HotPair 双端热表、周期健康检查（重赛制）、预绑定竞速 |
| `reverse.go` | 反向模式：服务端开监听、客户端出网 |
| `relay.go` | 中继节点评分、加权负载均衡、测速 |
| `ech_bridge.go` | ECH/DoH 共享栈（依赖 `xshared`） |
| `proxy_dialer.go` | 池 → `xshared/dialer` 桥接（stream/udp） |
| `log_hook.go` | 日志事件钩子（唯一日志出口） |

## 兼容性立场

- 升级红线（README 速查表同步维护）：v0.2.3 `SetLogf` 签名破坏性（壳需适配），
  v0.2.0 协议变更（服务端需跟随）；其余版本无破坏性，直接 `go get` 升版。
- 下游 bump 决策依据 = `CHANGELOG.md` 的"升级指引" + README 速查表，勿绕过文档改行为。

## 测试

```bash
go test ./... -race
```

并发相关改动（日志钩子、选路竞争、连接池）必须经 `-race` 验证。