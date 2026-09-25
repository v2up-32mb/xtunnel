# Changelog

本文件记录 `github.com/v2up-32mb/xtunnel` 各版本的变更与**下游升级指引**。
版本格式遵循 [语义化版本](https://semver.org/lang/zh-CN/)；`protocol/` 子包的任何变更都单独标注，方便下游评估是否需要同步升级服务端。

> 升级速查：**协议变更于 v0.3.1 新增 `MsgHotPairBegin`(0x23)**（显式预绑定轮次开始）。
> 消息集其余部分自 v0.2.0 起未变。协议升级为**向后兼容**：旧服务端忽略 Begin 帧、旧客户端不发送
> Begin 则由服务端 5s 窗口兜底，无需两端同步升级即可共存。

---

## v0.3.1 — 2026-09-26

**Added（B 方案：显式预绑定轮次）**

- 协议新增 `MsgHotPairBegin`(0x23)（客户端→服务端）：预绑定竞速广播前先发送，服务端置 armed；
  预绑定状态保留窗口从 Begin 起算，**不再依赖帧到达后的 5s 定时窗口**。
- 服务端 `handleHotPairBegin` 置 armed；`handlePrebindRequest` 在 armed 未过期时**直接采用
  剩余窗口**（unregister 时刻 = Begin + TTL，即使剩余短于 5s），仅当 armed 已过期或
  旧客户端无 Begin 时回退 5s。**旧客户端无 Begin → 原 5s 兜底；旧服务端忽略 Begin → 兼容**。
- 客户端 `BuildPair` 广播前先广播 Begin；win7-compat 同步。
- 新增回归测试：armed 窗口单次广播、旧端兜底。

**Added（方向 2：结构化领域事件）**

- `LogEvent` 新增可选 `Domain *DomainEvent` 字段（`Type` + 结构化 `Payload`），
  **既有纯字符串日志契约完全兼容**（无 Domain 时 Domain=nil）。
- 类型：`ConnEvent`（established/closed：Client/Target/通道）、`HotPairEvent`
  （promoted/fallback/bound：键/双通道）、`PoolEvent`（channel_up/channel_down/backpressure）。
- 发射入口：`coreLogD`/`srvLogD`（壳用 `SetLogf` 收到 LogEvent 后按 `ev.Domain` 聚合统计，
  无需解析字符串）。
- 接入点：服务端连接建立/关闭、HotPair 提升、通道上下线、背压档位；客户端 HotPair 单播/回退。

**升级指引**

- 协议向后兼容，服务端无需两端同步升级；`MsgHotPairBegin` 建议新端先发、旧端自然忽略。
- 壳可选择性消费 `ev.Domain` 做结构化统计；不消费则照旧按 Format/Args 渲染。

---

## v0.3.0 — 2026-09-26

**统一核心库（客户端 + 服务端能力一体，纯协议核心）**

**Added / Changed**

- **服务端能力并入根包**：`xtunnel-cli` 的 `server/pkg` 全部代码（handler/pool/connection/
  reverse/reverse_listener/hotpair_table/config/cert + 测试）合并进根包 `xtunnel`，
  不再有 `server/` 子包。下游导入同一包、选能力即用：
  - 客户端：`xtunnel.NewClient`/`NewClientPool`/`NewPairWarmer`/`NewRelayNodeManager`
  - 服务端：`xtunnel.NewServer`/`DefaultServerConfig`/`NewHotPairTable`/`NewReverseListenerManager`
- **统一一套日志体系**：`xtunnel.SetLogf`/`LogEvent`/`LevelDebug|Info|Warn|Error`/`CoreLog`。
  服务端日志 `srvLog` 走同一钩子，module 加 `server.` 前缀（`server.pool`/`server.handler`/...）。
  不再存在第二套服务端日志类型或 `SetLogf`。
- **通用能力上移 `xshared`（纯协议核心）**：
  - `ParseSocks5Auth`/`AuthEqual` → `xshared/socks5`
  - ECH/DoH 装配 → `xshared/ech.NewEchManagerFromDoH`（`ech_bridge.go` 删除）
  - `NewBufferedPipe` → `xshared/pipe`
  - 本库仅保留：8 字节二进制协议、通道池、下行竞争、背压、心跳、HotPair 热表/预绑定、
    反向隧道、中继、配置、dialer 接口实现（`poolStreamDialer`/`poolUDPDialer` 因依赖内部池
    必须留在本库）。

**Breaking（下游升级动作）**

- ⚠️ `xtunnel.ParseSocks5Auth`/`AuthEqual` **已删除** → 改用 `xshared/socks5.ParseSocks5Auth`/`AuthEqual`。
- ⚠️ `xtunnel.NewBufferedPipe` **已删除** → 改用 `xshared/pipe.NewBufferedPipe`。
- ⚠️ 服务端配置：本库内 `ServerConfig`/`DefaultServerConfig`（合并前 `server.Config`/`server.DefaultConfig`）。
- 日志：服务端事件 module 前缀由 `pool`/`handler` 变为 `server.pool`/`server.handler`（壳按需适配）。
- 依赖：`github.com/v2up-32mb/xshared` 最低 **v0.1.1**。

**升级指引**

- 协议消息集未变，服务端无需跟随升级。
- 客户端壳：升 `xshared` 至 v0.1.1，`ParseSocks5Auth`/`AuthEqual`/`NewBufferedPipe` 改调 xshared。
- 服务端壳：删本地 `server/pkg`，导入 `xtunnel` 用 `NewServer`/`ServerConfig`，`xtunnel.SetLogf` 接管渲染。

---

## v0.2.4 — 2026-09-25

**Fixed**

- `protocol.ShortID` 识别 `prebind-` 前缀：此类 connID 保留前缀 + 后续 8 位 UUID（共 16 位），
  修复日志 `ID:prebind-` 裁切盲区（此前前缀恰好 8 字符把截断窗口占满，UUID 完全不可见）。
  其他 ID 仍截前 8 位，日志长度不变。新增 `protocol/errors_test.go` 覆盖。

**升级指引**

- 纯展示层调整：**无 API 变更、无协议变更**。
- 下游只需 `go get github.com/v2up-32mb/xtunnel@v0.2.4` 重新构建即可；服务端无需跟随。

---

## v0.2.3 — 2026-09-25

**Changed(破坏性)**

- `SetLogf` 签名升级：`func(format string, args ...any)` → `func(ev LogEvent)`。
  `LogEvent{Level, Module, Time, Format, Args}`，等级 `LevelDebug/LevelInfo/LevelWarn/LevelError`。
  **旧壳必须适配新签名**，详见 README「日志契约」。
- `SetLogf` 实现改为 `atomic.Value` 存储，可在运行中安全切换；注入的 hook 必须并发安全。

**Added**

- 日志等级化：信息统一经 `LogEvent` 交壳处理，核心库零直接日志（默认静默）。
  等级约定：诊断/竞速→Debug，常规流程→Info，非致命异常→Warn，硬失败→Error。

**升级指引**

- 协议不变、服务端无需升级。
- 下游壳：改 `SetLogf` 回调签名并按等级/模块渲染；不接则保持静默（可选功能）。

---

## v0.2.2 — 2026-09-25

**Added**

- **周期健康检查重赛制**：队首连任 / 备用Pair提升 / 新最优顶替 + 数量对齐，
  周期检查更稳健（针对"新建 pair 恒等于旧 pair"问题）。
- **预绑定竞速诊断**：池级 `prebindRacers` 汇总（按通道去重、3s 窗口、汇总后清理），
  便于定位广播风暴/重复帧。
- **核心库日志钩子**（初版）：`SetLogf(func(format string, args ...any))`，默认静默（v0.2.3 起签名变更）。

**Changed**

- 客户端周期健康维护逻辑整体替换为重赛制算法（与 v0.2.0 引入的"客户端负责健康维护"延续）。

**升级指引**

- 协议不变、服务端无需升级。
- 新增可选 `SetLogf`（v0.2.3 起为 `func(LogEvent)`）；不调用则静默，行为不变。

---

## v0.2.1 — 2026-09-25

**Fixed**

- 反向注册无回执的报错补充可诊断提示（区分超时/被拒）。

**Docs**

- README 补充反向模式与 HotPair v2 双端热表说明。

**升级指引**：无破坏性变更；直接升级。

---

## v0.2.0 — 2026-09-25

**Added（HotPair v2，协议变更）**

- **协议新增 `MsgHotPairNotify`（0x22）**：客户端→服务端批量预热通道对通知，payload 为 `HotPairInfo` 记录序列。
- **connID 前缀约定**：`prebind-` 前缀的 connID 作为预热键，服务端解析建表。
- **HotPair 双端热表**：客户端预热通道对并批量通知；拨号期零选路消息；Pair 共享复用，正反双模式通用。
- **周期健康维护统一由客户端负责**（`periodicRefresh`）。
- 反向预绑定（`MsgPrebindRequest` 处理）与反向热路径拨号。

**升级指引**

- ⚠️ **协议变更**：新增消息 `MsgHotPairNotify`(0x22)。使用本库的客户端如需 HotPair 热路径，
  服务端必须支持 `MsgHotPairNotify`（xtunnel-cli 服务端自对应版本起已支持）。
  不使用 HotPair（不启用预热）则与旧服务端兼容。
- 无 Go API 破坏性变更。

---

## v0.1.0 — 2026-09-25

**初始版本**

- 从 `xtunnel-cli` 提取协议实现层（基线：x-tunnel 上游 commit `5b10fc1`）。
- 能力：8 字节二进制协议、多通道连接池、下行通道竞争选择、背压控制、快重试、
  SOCKS5/UDP 适配、反向模式、ECH/DoH 共享栈。
- 库间依赖固定为 tag 版本引用。