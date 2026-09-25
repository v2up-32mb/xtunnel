// server 子包日志接入：复用 xtunnel 顶层“统一日志钩子”（单一 LogEvent/SetLogf 体系）。
//
// 核心库定位为统一库（客户端 + 服务端能力一体），不设第二套日志事件。
// 服务端核心不直接向 stdout 输出日志；所有信息经 srvLog 转发到 xtunnel.CoreLog，
// 由壳（xtunnel.SetLogf 注入的钩子）统一等级过滤、模块路由与输出；默认静默。
//
// module 命名空间：服务端事件统一带 "server." 前缀（如 "server.pool" / "server.handler"），
// 与客户端模块（"pool" / "pair_warmer" / ...）区分，便于壳按模块路由。
package server

import "github.com/v2up-32mb/xtunnel"

// LogLevel 类型别名：与 xtunnel.LogLevel 为同一类型（不另设等级体系）。
type LogLevel = xtunnel.LogLevel

const (
	LevelDebug = xtunnel.LevelDebug
	LevelInfo  = xtunnel.LevelInfo
	LevelWarn  = xtunnel.LevelWarn
	LevelError = xtunnel.LevelError
)

// srvLog 服务端核心统一日志入口：转发到 xtunnel 统一钩子（module 加 server. 命名空间前缀）
func srvLog(level LogLevel, module, format string, args ...any) {
	xtunnel.CoreLog(level, "server."+module, format, args...)
}