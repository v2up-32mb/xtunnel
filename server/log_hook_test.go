package server

import (
	"sync/atomic"
	"testing"

	"github.com/v2up-32mb/xtunnel"
)

// TestSrvLogRoutesToUnifiedHook 验证服务端日志走 xtunnel 顶层统一钩子：
// 不存在独立的服务端日志体系，srvLog 转发到 xtunnel.CoreLog（module 带 server. 前缀）。
func TestSrvLogRoutesToUnifiedHook(t *testing.T) {
	var got atomic.Value
	xtunnel.SetLogf(func(ev xtunnel.LogEvent) {
		got.Store(ev.Module + "|" + ev.Format)
	})
	defer xtunnel.SetLogf(nil)

	srvLog(LevelInfo, "pool", "hello %s", "world")

	g := got.Load()
	if g == nil {
		t.Fatal("统一日志钩子未被调用（srvLog 未转发到 xtunnel.CoreLog）")
	}
	if s := g.(string); s != "server.pool|hello %s" {
		t.Errorf("srvLog 事件 = %q, want %q", s, "server.pool|hello %s")
	}
}
