# AGENTS.md — xtunnel 核心库协作指引

面向在该仓库工作的开发者与 AI agent。本文件用于取代 Claude 专有命名
（`CLAUDE.md`），采用通用的 `AGENTS.md`，任何 agent / 编辑器 / 工具都按此约定读取。

## 项目定位

`github.com/v2up-32mb/xtunnel` 是多通道 WebSocket 隧道的 **Go 核心库，客户端与服务端能力一体**：

- **客户端能力**（库根包）：多通道连接池、代理适配、反向出网、中继、ECH/DoH、HotPair 预热
- **服务端能力**（`server/` 子包）：隧道服务、连接池与背压、热表、反向监听

核心库**不区分“客户端核心库 / 服务端核心库”**——整个库只有**一套**日志钩子
（`xtunnel.SetLogf`/`xtunnel.LogEvent`/`xtunnel.CoreLog`），服务端与其他子包均复用同一体系
（module 命名空间区分，如 `server.pool`），协议与配置共享。
下游壳（`xtunnel-cli` 的 CLI、`x-client` 的 gomobile AAR）共同引用。
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
5. **能力分层**：本库只保留核心协议（编解码/池/热表/背压/反向隧道/中继/配置）。
   通用能力（ECH/DoH、SOCKS5/HTTP、pipe、解析工具）一律在 `xshared`，此处只调用不实现；
   新发现的通用重复实现应上移 xshared。
6. **发版铁律（最高优先级）**：**绝不未经人工确认就自行打 tag 并推送**。
   任何发版动作（打 tag、`push --tags`、创建 release）必须先向用户明确汇报版本号与发布内容并获得批准，
   批准后方可执行；提交/推送日常分支不在此限。

## 发版流程（每个 tag 必修）

1. 代码 + 测试完成：`go test ./... -race` 全绿。
2. `CHANGELOG.md` 记入该版本：变更分类（Added/Changed/Fixed）+ 升级指引。
3. `README.md`"版本与升级"速查表同步该行。
4. **先获取人工批准再打 tag**：将版本号与发布内容（CHANGELOG 摘要 + 破坏性变更 + 下游动作）
   汇报给用户并获批后，才执行 `git tag` 与 `git push origin main --tags`。
   绝不未经人工确认就自行打 tag 并推送。
   - 文档类修订若无代码变更，随下一个版本 tag 发布，不重打旧 tag。

## 结构速览

| 文件 | 职责 |
|---|---|
| `protocol/` | 8 字节二进制协议、消息常量、错误判定、`ShortID`、IP 策略 |
| `pool.go` | 客户端多通道连接池、下行竞争、背压、快重试、统计 |
| `pair_warmer.go` | HotPair 双端热表、周期健康检查（重赛制）、预绑定竞速 |
| `reverse.go` / `reverse_server.go` | 客户端反向出网 / 服务端反向隧道 |
| `server_pool.go` / `handler.go` / `connection.go` / `hotpair_table.go` / `reverse_listener.go` | 服务端能力 |
| `relay.go` | 中继节点评分、加权负载均衡、测速 |
| `proxy_dialer.go` | 池 → `xshared/dialer` 接口实现（通用能力在 xshared） |
| `log_hook.go` | 统一日志事件钩子（唯一日志出口，含 `srvLog`） |

**能力分层铁律**：本库仅核心协议。一切通用能力（ECH/DoH、SOCKS5/HTTP 服务器与解析、
内存管道 `pipe`）都属于 `xshared`；在本库发现通用实现应立即上移 xshared 并改调。

## 兼容性立场

- 升级红线（README 速查表同步维护）：v0.2.3 `SetLogf` 签名破坏性（壳需适配），
  v0.2.0 协议变更（服务端需跟随）；其余版本无破坏性，直接 `go get` 升版。
- 下游 bump 决策依据 = `CHANGELOG.md` 的"升级指引" + README 速查表，勿绕过文档改行为。

## v0.3.0 规划（新会话从此处开始）

> 本规划是「当前进行中的演进批次」的权威来源。开工前先读本小节 + CHANGELOG + README 速查表。
> CLI 仓库侧的落地细节见 `xtunnel-cli/AGENTS.md`「演进目标」段落。

批次 = 三个工作项一次版本。推荐顺序随依赖而定：**先 C（服务端入库，做完再动协议）→ 再 B（协议）→ 方向 2（事件）可与 B 并行或随 v0.3.1**。

### ① C 方案：服务端核心化 —— ✅ 已完成（超越原目标）

- **目标**：把 `xtunnel-cli` 的 `server/pkg` 并入本库。
- **结果**：不止并入 `server/` 子包——随后按架构要求**直接合并进根包**（无子目录），
  统一为一套日志钩子（`xtunnel.SetLogf`/`LogEvent`/`CoreLog`，`srvLog` 走 `server.` 模块前缀），
  服务端 `Config`→`ServerConfig`、`DefaultConfig`→`DefaultServerConfig`。
- **验收达成**：CLI 壳本地 replace 验证编译/测试/静态二进制全通过；正式切换（删 CLI `server/pkg`、
  go.mod 升版后壳改 import `xtunnel`）随 v0.3.0 发布执行。
- **额外符合架构**：通用能力（socks5 解析/鉴权、ECH 装配、内存管道）已上移 `xshared` v0.1.1，
  本库保持纯协议核心。

### ② B 方案：显式 begin 协议 —— ✅ 已随 v0.3.1 落地（`MsgHotPairBegin` 0x23）

- **目标**：新增 `MsgHotPairBegin`（客户端→服务端），用显式「轮次 armed」替代 `prebindStateTTL` 5s 定时窗口；轮次边界显式化、广播确定性每轮 1 次、低延迟链路不再依赖定时窗口。
- **设计（草案）**：
  - `protocol/`：新增消息常量 `MsgHotPairBegin`（建议 0x23）与编解码；payload 建议 { 轮次 ID + 目标通道数 }（可再议）。
  - 客户端（`pair_warmer.go`）：广播前先发 `MsgHotPairBegin` 通知服务端 armed；轮次结束发 done（或复用 connID 生命周期/超时）。
  - 服务端（`handlePrebindRequest`）：由「每帧预绑定状态 + TTL」改为「显式 armed 后首帧定上行并广播 `MsgSelectUplink`，同轮次后续帧忽略，轮次结束即注销」；旧客户端无 Begin 时保留 5s 窗口兜底（兼容）。
- **涉及**：`protocol/protocol.go`、`pair_warmer.go`、迁入后的 `server/`、`win7-compat` 客户端同步、CLI 服务端。
- **验证**：服务端日志「空 target 行每轮 1 条」；重复帧回归测试；双端集成。
- **发布**：属协议变更，随 v0.3.0 发版；CHANGELOG 标注 ⚠️ 协议变更 + 服务端跟随指引。

### ③ 方向 2：结构化领域事件 —— ✅ 已随 v0.3.1 落地（LogEvent.Domain 可选结构化负载）

- **现状（方向 1）**：`LogEvent{Level, Module, Time, Format, Args}`，壳自行格式化字符串。
- **目标**：在既有日志事件之外提供结构化领域事件字段——连接事件（ID/Target/通道/State/流量）、HotPair 事件（键/上下行/promoted/回退）、池事件（通道上下线/背压档位）；壳可统计/过滤而无需解析字符串；保留字符串渲染兼容（事件可派生 Message）。
- **步骤**：定义事件类型与 `emitXxxEvent` 出口（建议与日志共用单出口、事件带 Type + 可选结构化负载）；改造 `pool.go`/`pair_warmer.go`/`reverse.go` 的访问与状态日志点；壳渲染层可选使用。
- **验收**：壳能按 State 聚合统计；既有 LogEvent 契约不破坏。
- **排期**：推荐 v0.3.1（控制批次），用户要求可并入 v0.3.0。

### v0.3.0 发布红线

- CHANGELOG 记 B 的协议变更 + C 的服务端入库；README 速查表同步；
- `win7-compat` 客户端先同步发版或明确兼容声明；
- 服务端部署跟随 C/B 合入后重建。

## 测试

```bash
go test ./... -race
```

并发相关改动（日志钩子、选路竞争、连接池）必须经 `-race` 验证。