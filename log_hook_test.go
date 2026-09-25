package xtunnel

import (
	"sync"
	"testing"
	"time"
)

// TestSetLogfDeliversEvent 注入 hook 后 coreLog 事件字段应完整
func TestSetLogfDeliversEvent(t *testing.T) {
	var got LogEvent
	var wg sync.WaitGroup
	wg.Add(1)
	SetLogf(func(ev LogEvent) {
		got = ev
		wg.Done()
	})
	defer SetLogf(nil)

	coreLog(LevelWarn, "pool", "测试 %d %s", 7, "x")
	wg.Wait()

	if got.Level != LevelWarn {
		t.Fatalf("Level = %v, want LevelWarn", got.Level)
	}
	if got.Module != "pool" {
		t.Fatalf("Module = %q, want pool", got.Module)
	}
	if got.Format != "测试 %d %s" || len(got.Args) != 2 ||
		got.Args[0].(int) != 7 || got.Args[1].(string) != "x" {
		t.Fatalf("Format/Args mismatch: %q %v", got.Format, got.Args)
	}
	if time.Since(got.Time) > time.Second {
		t.Fatalf("Time not set: %v", got.Time)
	}
}

// TestSetLogfNilRestoresSilent SetLogf(nil) 后 coreLog 不再触发 hook
func TestSetLogfNilRestoresSilent(t *testing.T) {
	SetLogf(func(LogEvent) { t.Error("hook 不应在恢复静默后被调用") })
	SetLogf(nil)
	// 若未静默,上面 hook 会触发 t.Error
	coreLog(LevelDebug, "pool", "静默测试")
}

// TestLogLevelOrder 等级枚举顺序
func TestLogLevelOrder(t *testing.T) {
	if !(LevelDebug < LevelInfo && LevelInfo < LevelWarn && LevelWarn < LevelError) {
		t.Fatalf("LogLevel 顺序异常: %d %d %d %d", LevelDebug, LevelInfo, LevelWarn, LevelError)
	}
}
