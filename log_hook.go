package xtunnel

// 核心库约定：不直接向 stdout/stderr 输出日志。
// 所有需要展示的信息统一经 coreLogf 交给下游壳处理，由壳决定
// 输出目标、格式、级别与轮转；核心库默认静默。
// 下游壳在启动时调用 SetLogf 注入自己的日志函数即可接管全部客户端日志。

var coreLogf = func(string, ...any) {}

// SetLogf 由下游壳注入日志输出函数（如 log.Printf）。
// 传入 nil 或空实现可恢复为静默。
func SetLogf(fn func(format string, args ...any)) {
	if fn == nil {
		coreLogf = func(string, ...any) {}
		return
	}
	coreLogf = fn
}
