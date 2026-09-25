package xtunnel

import (
	"sync/atomic"
	"time"
)

// 核心库约定：不直接向 stdout/stderr 输出日志。
// 所有需要展示的信息统一经 coreLogHook 以 LogEvent 形式交给下游壳处理，
// 壳负责按等级过滤、按模块路由并决定输出目标与格式；核心库默认静默。

// LogLevel 日志等级
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

// DomainEventType 结构化领域事件类型（方向 2）：事件自带结构化负载，
// 壳可据此做统计/过滤/路由，无需解析字符串。
type DomainEventType string

const (
	DomainConn    DomainEventType = "conn"    // 连接事件（建立/关闭）
	DomainHotPair DomainEventType = "hotpair" // Hot Pair 事件（提升/回退/建表）
	DomainPool    DomainEventType = "pool"    // 池事件（通道上下线/背压档位）
)

// DomainEvent 结构化领域事件负载（可选）：挂载在 LogEvent.Domain 上。
type DomainEvent struct {
	Type    DomainEventType
	Payload any
}

// 领域事件结构化负载

// ConnEvent 连接事件负载：Event 为 "established"/"closed"
type ConnEvent struct {
	Event      string // established / closed
	Client     string // 客户端标识
	Target     string // 目标地址
	UplinkCh   int    // 上行通道（closed 时可能为 0）
	DownlinkCh int    // 下行通道（正向建立后才有）
}

// HotPairEvent 事件负载：Event 为 "promoted"/"fallback"/"bound"
type HotPairEvent struct {
	Event    string // promoted / fallback / bound
	Key      string // 预热键（prebind-...）
	ChA, ChB int    // 双端通道
}

// PoolEvent 池事件负载：Event 为 "channel_up"/"channel_down"/"backpressure"
type PoolEvent struct {
	Event    string // channel_up / channel_down / backpressure
	ClientID string
	ChID     int
	Detail   string // 背压档位描述（backpressure 时用）
}

// LogEvent 一条库内日志事件
type LogEvent struct {
	Level  LogLevel
	Module string // 来源模块：pool / pair_warmer / relay / reverse / client ...
	Time   time.Time
	Format string
	Args   []any

	// Domain 可选结构化领域负载（方向 2）：nil 表示纯字符串日志。
	// 壳优先消费 Domain（结构化），退路是按 Format/Args 渲染。
	Domain *DomainEvent
}

type logHookFunc func(LogEvent)

// coreLogHook 用 atomic.Value 存储，允许运行时安全切换（不影响并发日志调用）。
var coreLogHook atomic.Value // stores logHookFunc

func init() {
	coreLogHook.Store(logHookFunc(func(LogEvent) {}))
}

// SetLogf 由下游壳注入日志事件处理函数；传入 nil 恢复静默。
// 壳可按 ev.Level / ev.Module 过滤，自行决定目标与格式（控制台/文件/JSON/远端等）。
// 注意：
//  1. 可随时调用（原子切换）；建议在启动前注入，以免丢失早期日志；
//  2. 注入的 hook 实现必须并发安全（coreLog 会从 pool/pair_warmer/relay/reverse 等
//     多个 goroutine 同时触发）。
func SetLogf(fn func(ev LogEvent)) {
	if fn == nil {
		coreLogHook.Store(logHookFunc(func(LogEvent) {}))
		return
	}
	coreLogHook.Store(logHookFunc(fn))
}

// coreLogD 带结构化领域负载的统一日志入口；domain 为 nil 等价于纯日志。
func coreLogD(level LogLevel, module, format string, args []any, domain *DomainEvent) {
	coreLogHook.Load().(logHookFunc)(LogEvent{Level: level, Module: module, Time: time.Now(), Format: format, Args: args, Domain: domain})
}

// coreLog 库内统一日志入口（带等级与模块元数据）
func coreLog(level LogLevel, module, format string, args ...any) {
	coreLogD(level, module, format, args, nil)
}

// CoreLog 统一的公开日志入口（与 SetLogf 同一套事件体系）。
// 供库内子包（如 server/）与第三方接入方使用；module 建议使用带命名空间的名称（如 "server.pool"）。
func CoreLog(level LogLevel, module, format string, args ...any) {
	coreLog(level, module, format, args...)
}

// srvLog 服务端组件统一日志入口（合并自原 server 子包）：
// module 加 "server." 命名空间前缀，与客户端模块（"pool"/"pair_warmer"/...）区分，
// 便于壳按模块路由过滤；底层走同一套 CoreLog 钩子。
func srvLog(level LogLevel, module, format string, args ...any) {
	coreLog(level, "server."+module, format, args...)
}

// srvLogD 服务端带结构化领域负载的日志入口（方向 2）。
func srvLogD(level LogLevel, module, format string, args []any, domain *DomainEvent) {
	coreLogD(level, "server."+module, format, args, domain)
}
