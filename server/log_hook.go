package server

import (
	"sync/atomic"
	"time"
)

// 服务端核心约定：不直接向 stdout/stderr 输出日志（为进入上游核心库 xtunnel/server 做准备）。
// 所有需要展示的信息统一经 serverLogHook 以 LogEvent 形式交给壳处理，
// 壳负责按等级过滤、按模块路由并决定输出目标与格式；核心默认静默。

// LogLevel 日志等级（与客户端核心 xtunnel.LogLevel 对齐，便于后续统一合并）
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
)

// LogEvent 一条服务端核心日志事件
type LogEvent struct {
	Level  LogLevel
	Module string // 来源模块：handler / pool / server / connection / reverse / reverse_listener / hotpair_table
	Time   time.Time
	Format string
	Args   []any
}

type serverLogHookFunc func(LogEvent)

// serverLogHook 用 atomic.Value 存储，允许运行时安全切换。
var serverLogHook atomic.Value // stores serverLogHookFunc

func init() {
	serverLogHook.Store(serverLogHookFunc(func(LogEvent) {}))
}

// SetLogf 由壳注入日志事件处理函数；传入 nil 恢复静默。
// 可随时调用（原子切换）；注入的 hook 必须并发安全。
func SetLogf(fn func(ev LogEvent)) {
	if fn == nil {
		serverLogHook.Store(serverLogHookFunc(func(LogEvent) {}))
		return
	}
	serverLogHook.Store(serverLogHookFunc(fn))
}

// srvLog 服务端核心统一日志入口（带等级与模块元数据）
func srvLog(level LogLevel, module, format string, args ...any) {
	serverLogHook.Load().(serverLogHookFunc)(LogEvent{Level: level, Module: module, Time: time.Now(), Format: format, Args: args})
}
