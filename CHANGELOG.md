# Changelog

本文件记录 `github.com/v2up-32mb/xtunnel` 各版本的变更与**下游升级指引**。
版本格式遵循 [语义化版本](https://semver.org/lang/zh-CN/)；`protocol/` 子包的任何变更都单独标注，方便下游评估是否需要同步升级服务端。

> 升级速查：**协议版本 = 0x22（MsgHotPairNotify）**。协议消息集自 v0.2.0 起未再变化；
> 后续版本均为客户端侧行为/展示/日志改动，服务端无需跟随升级（除非另行标注）。

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