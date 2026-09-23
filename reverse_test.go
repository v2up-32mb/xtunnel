package xtunnel

import (
	"testing"
	"time"

	"github.com/v2up-32mb/xtunnel/protocol"
)

func TestReverseConnLifecycle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.ConnectTimeout = time.Second
	p, err := newClientPool(cfg, nil, nil)
	if err != nil {
		t.Fatalf("newClientPool: %v", err)
	}
	// add reverse conn
	rc, ok := p.addReverseConn("id1", "1.2.3.4:80", 1)
	if !ok || rc == nil {
		t.Fatalf("add failed")
	}
	if !p.hasReverseConn("id1") {
		t.Fatalf("hasReverseConn false")
	}
	p.removeReverseConn("id1")
	if p.hasReverseConn("id1") {
		t.Fatalf("still present")
	}
}

func TestReverseListenerID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableReverse = true
	p, _ := newClientPool(cfg, nil, nil)
	id1 := p.getOrCreateListenerID("socks5://0.0.0.0:30000")
	id2 := p.getOrCreateListenerID("socks5://0.0.0.0:30000")
	if id1 != id2 {
		t.Fatalf("ids differ")
	}
}

func TestReverseHandleListenResult(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableReverse = true
	p, _ := newClientPool(cfg, nil, nil)
	// simulate
	id := p.getOrCreateListenerID("socks5://0.0.0.0:30000")
	meta := []byte{byte(protocol.StatusOK)}
	p.handleReverseListenResult(id, meta)
	// no panic
}
