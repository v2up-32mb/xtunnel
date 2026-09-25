package xtunnel

import (
	"sync/atomic"
	"testing"
)

// TestDomainEventStructured 方向 2：壳能收到结构化领域负载并按键聚合（无需解析字符串）。
func TestDomainEventStructured(t *testing.T) {
	var got atomic.Value // stores ConnEvent
	SetLogf(func(ev LogEvent) {
		if ev.Domain != nil && ev.Domain.Type == DomainConn {
			if c, ok := ev.Domain.Payload.(ConnEvent); ok {
				got.Store(c)
			}
		}
	})
	defer SetLogf(nil)

	coreLogD(LevelInfo, "pool", "[服务端] %s 访问: %s, ID:%s",
		[]any{"1.2.3.4:5", "example.com:443", "prebind-abc"},
		&DomainEvent{Type: DomainConn, Payload: ConnEvent{Event: "established", Client: "1.2.3.4:5", Target: "example.com:443", UplinkCh: 3}})

	g := got.Load()
	if g == nil {
		t.Fatal("壳未收到结构化 conn 事件")
	}
	c := g.(ConnEvent)
	if c.Event != "established" || c.Target != "example.com:443" || c.UplinkCh != 3 {
		t.Errorf("结构化负载不符: %+v", c)
	}
}

// TestDomainEventNilKeepsPlainLog 兼容：无 Domain 的日志仍按纯字符串事件传递。
func TestDomainEventNilKeepsPlainLog(t *testing.T) {
	var got atomic.Value
	SetLogf(func(ev LogEvent) { got.Store(ev) })
	defer SetLogf(nil)

	coreLog(LevelInfo, "pool", "hello %s", "world")
	ev := got.Load().(LogEvent)
	if ev.Domain != nil {
		t.Fatalf("纯日志 Domain 应为 nil, got %+v", ev.Domain)
	}
	if ev.Format != "hello %s" {
		t.Errorf("Format = %q", ev.Format)
	}
}
