package xtunnel

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/v2up-32mb/xtunnel/protocol"
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

// TestConnClosedEventCarriesChannels P2-6: 关闭事件携带通道字段(壳可按通道聚合)。
func TestConnClosedEventCarriesChannels(t *testing.T) {
	var got atomic.Value
	SetLogf(func(ev LogEvent) {
		if ev.Domain != nil && ev.Domain.Type == DomainConn {
			if c, ok := ev.Domain.Payload.(ConnEvent); ok && c.Event == "closed" {
				got.Store(c)
			}
		}
	})
	defer SetLogf(nil)

	p := newTestServerPool()
	connID := "tcp-closed-1"
	p.mu.Lock()
	p.conns[connID] = &ServerConnState{
		connID:       connID,
		target:       "example.com:443",
		clientAddr:   "1.2.3.4:5",
		connected:    true,
		uplinkChID:   8,
		downlinkChID: 43,
	}
	p.mu.Unlock()

	p.unregisterConn(connID)

	g := got.Load()
	if g == nil {
		t.Fatal("壳未收到 closed 结构化事件")
	}
	c := g.(ConnEvent)
	if c.UplinkCh != 8 || c.DownlinkCh != 43 {
		t.Errorf("closed 事件应携带通道字段(壳可按通道聚合): %+v", c)
	}
}

// TestHotPairBoundEventP2 方向2: hotpair_table.HandleNotify 在锁外发射 bound 事件(壳可按键聚合建表)。
func TestHotPairBoundEvent(t *testing.T) {
	var mu sync.Mutex
	var got []HotPairEvent
	SetLogf(func(ev LogEvent) {
		if ev.Domain != nil && ev.Domain.Type == DomainHotPair {
			if h, ok := ev.Domain.Payload.(HotPairEvent); ok && h.Event == "bound" {
				mu.Lock()
				got = append(got, h)
				mu.Unlock()
			}
		}
	})
	defer SetLogf(nil)

	tbl := NewHotPairTable()
	payload := protocol.EncodeHotPairNotify([]protocol.HotPairInfo{
		{Key: "prebind-b1", ChA: 3, ChB: 7},
		{Key: "prebind-b2", ChA: 5, ChB: 9},
	})
	tbl.HandleNotify("client-a", payload)

	// 壳侧验证非结构性回归: 表确实建起来了
	if e := tbl.Lookup("client-a", "prebind-b1"); e == nil || e.ChA != 3 || e.ChB != 7 {
		t.Fatalf("表项未建立: %+v", e)
	}

	mu.Lock()
	bounds := append([]HotPairEvent(nil), got...)
	mu.Unlock()
	if len(bounds) != 2 {
		t.Fatalf("应发射 2 条 bound 事件, got %d: %+v", len(bounds), bounds)
	}
	want := map[string]HotPairEvent{
		"prebind-b1": {Event: "bound", Key: "prebind-b1", ChA: 3, ChB: 7},
		"prebind-b2": {Event: "bound", Key: "prebind-b2", ChA: 5, ChB: 9},
	}
	for _, h := range bounds {
		w, ok := want[h.Key]
		if !ok || w.ChA != h.ChA || w.ChB != h.ChB {
			t.Errorf("bound 事件负载不符: %+v (want %+v)", h, w)
		}
	}
}
