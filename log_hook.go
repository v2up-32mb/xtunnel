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

// LogEvent 一条库内日志事件
type LogEvent struct {
	Level  LogLevel
	Module string // 来源模块：pool / pair_warmer / relay / reverse / client ...
	Time   time.Time
	Format string
	Args   []any
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

// coreLog 库内统一日志入口（带等级与模块元数据）
func coreLog(level LogLevel, module, format string, args ...any) {
	coreLogHook.Load().(logHookFunc)(LogEvent{Level: level, Module: module, Time: time.Now(), Format: format, Args: args})
}
