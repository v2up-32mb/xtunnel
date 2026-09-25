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

## v0.3.0 规划（新会话从此处开始）

> 本规划是「当前进行中的演进批次」的权威来源。开工前先读本小节 + CHANGELOG + README 速查表。
> CLI 仓库侧的落地细节见 `xtunnel-cli/AGENTS.md`「演进目标」段落。

批次 = 三个工作项一次版本。推荐顺序随依赖而定：**先 C（服务端入库，做完再动协议）→ 再 B（协议）→ 方向 2（事件）可与 B 并行或随 v0.3.1**。

### ① C 方案：服务端核心库化（先做，无依赖）

- **目标**：把 `xtunnel-cli` 的 `server/pkg` 迁入本库 `server/` 子包（`package server`）。
- **前置状态**：✅ 服务端核心日志已收敛为注入钩子（`server.SetLogf`，默认静默，50 处 `srvLog` 带等级/模块），壳已统一 `[等级][模块]`；迁移只差改 import 与包名。
- **进度**（本批次已推进）：
  - ✅ 步骤 1-2：`server/` 子包已迁入（commit `6c9d78c`），`go test ./... -race` 全绿；
  - ✅ 步骤 3 验证：CLI 壳本地 replace 切到本库 `server`，编译/测试/静态二进制构建全通过（`SetLogf` 已就位，零其他改动）；实验后已回退，保持 CLI 服务端原状可运行；
  - ⏳ 正式切换（步骤 3-4 提交）随 v0.3.0 发布时执行：CLI 删本地 `server/pkg`、go.mod 升版后壳改 import。
- **步骤**：
  1. 复制 `xtunnel-cli/server/pkg/*.go`（handler/pool/connection/server/reverse/reverse_listener/hotpair_table/config/cert + 测试）→ 本库 `server/`；
  2. import 从 `x-tunnel/server/pkg` 改为 `github.com/v2up-32mb/xtunnel/{protocol,server}`；
  3. CLI 壳 `server/cmd/x-tunnel-server` 改依赖本库，启动时 `server.SetLogf(renderServerLog)`；
  4. 回归：本库 `go test ./... -race`、CLI 服务端功能测试、重部署验证日志格式不变。
- **验收**：CLI 服务端与核心库同版本同步发版；双方日志行为不变。
- **代价**：CLI 壳迁移 + 三方回归；协议与服务端同库导致发版耦合（可接受）。

### ② B 方案：显式 begin 协议（协议变更，随 C 之后做）

- **目标**：新增 `MsgHotPairBegin`（客户端→服务端），用显式「轮次 armed」替代 `prebindStateTTL` 5s 定时窗口；轮次边界显式化、广播确定性每轮 1 次、低延迟链路不再依赖定时窗口。
- **设计（草案）**：
  - `protocol/`：新增消息常量 `MsgHotPairBegin`（建议 0x23）与编解码；payload 建议 { 轮次 ID + 目标通道数 }（可再议）。
  - 客户端（`pair_warmer.go`）：广播前先发 `MsgHotPairBegin` 通知服务端 armed；轮次结束发 done（或复用 connID 生命周期/超时）。
  - 服务端（`handlePrebindRequest`）：由「每帧预绑定状态 + TTL」改为「显式 armed 后首帧定上行并广播 `MsgSelectUplink`，同轮次后续帧忽略，轮次结束即注销」；旧客户端无 Begin 时保留 5s 窗口兜底（兼容）。
- **涉及**：`protocol/protocol.go`、`pair_warmer.go`、迁入后的 `server/`、`win7-compat` 客户端同步、CLI 服务端。
- **验证**：服务端日志「空 target 行每轮 1 条」；重复帧回归测试；双端集成。
- **发布**：属协议变更，随 v0.3.0 发版；CHANGELOG 标注 ⚠️ 协议变更 + 服务端跟随指引。

### ③ 方向 2：结构化领域事件（可与 B 并行，或随 v0.3.1）

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