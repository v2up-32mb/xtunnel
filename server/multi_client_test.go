package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// dialServerWS 以指定 clientID/chID 与服务端建立 WebSocket 连接
func dialServerWS(t *testing.T, server *httptest.Server, clientID string, chID int) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(server.URL, "http") +
		"?client_id=" + clientID + "&ch_id=" + strconv.Itoa(chID)
	conn, resp, err := (&websocket.Dialer{Subprotocols: []string{"token"}}).Dial(url, nil)
	if err != nil {
		if resp != nil {
			t.Fatalf("dial client=%s ch_id=%d failed: %v (HTTP %d)", clientID, chID, err, resp.StatusCode)
		}
		t.Fatalf("dial client=%s ch_id=%d failed: %v", clientID, chID, err)
	}
	return conn
}

// TestMultipleClientsCanShareSameChID 回归测试：
// 不同客户端可以使用相同的 ch_id 同时在线，互不拒绝、互不干扰。
func TestMultipleClientsCanShareSameChID(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Token = "token"
	p := newServerPool(cfg.Token, cfg)

	server := httptest.NewServer(http.HandlerFunc(p.handleWebSocket))
	defer server.Close()

	connA := dialServerWS(t, server, "client-a", 1)
	defer connA.Close()
	connB := dialServerWS(t, server, "client-b", 1)
	defer connB.Close()

	// Dial 返回时服务端 handler 可能尚未完成注册，轮询等待两个客户端都注册就绪，
	// 避免与升级握手产生竞态（未注册的客户端收 prebind 广播会走全量广播）。
	var chA, chB *ServerWSConn
	var namespaces, active int
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.RLock()
		chA = p.clientChConns["client-a"][1]
		chB = p.clientChConns["client-b"][1]
		namespaces = len(p.clientChConns)
		active = 0
		for _, wsConn := range p.wsConns {
			if wsConn == nil {
				continue
			}
			// closed 由 wsConn.mu 保护（不能用 p.mu 读，避免与 close() 竞争）
			wsConn.mu.Lock()
			c := wsConn.closed
			wsConn.mu.Unlock()
			if !c {
				active++
			}
		}
		p.mu.RUnlock()

		if chA != nil && chB != nil && namespaces == 2 && active == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("clients registration not ready: a=%v b=%v ns=%d active=%d",
				chA != nil, chB != nil, namespaces, active)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if chA == nil || chB == nil {
		t.Fatalf("expected both clients registered (a=%v b=%v)", chA != nil, chB != nil)
	}
	if chA == chB {
		t.Fatal("expected distinct ServerWSConn for different clients")
	}
	if namespaces != 2 {
		t.Fatalf("expected 2 client namespaces, got %d", namespaces)
	}
	if active != 2 {
		t.Fatalf("expected 2 active ws connections, got %d", active)
	}

	// 客户端 B 发起预绑定，应只收到自己的 MsgSelectUplink 广播，A 不应收到
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, protocol.PrebindTarget...)
	msg := protocol.EncodeMessage(protocol.MsgPrebindRequest, "regress-conn", meta, nil)
	if err := connB.WriteMessage(websocket.BinaryMessage, msg); err != nil {
		t.Fatalf("client-b write prebind failed: %v", err)
	}

	connB.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := connB.ReadMessage()
	if err != nil {
		t.Fatalf("client-b failed to receive MsgSelectUplink: %v", err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("client-b unexpected message type %d", mt)
	}
	tp, _, _, _, err := protocol.DecodeMessage(data)
	if err != nil {
		t.Fatalf("client-b decode failed: %v", err)
	}
	if tp != protocol.MsgSelectUplink {
		t.Fatalf("client-b expected MsgSelectUplink, got %v", tp)
	}

	// 客户端 A 不应收到属于 B 的下行消息
	connA.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, _, err := connA.ReadMessage(); err == nil {
		t.Fatal("client-a unexpectedly received message routed for client-b")
	}
}

// TestCleanupChannelDoesNotCloseOtherClientSameChID 回归测试：
// 一个客户端的 ch_id 关闭清理时，其他客户端相同 ch_id 的连接与连接状态不受影响。
func TestCleanupChannelDoesNotCloseOtherClientSameChID(t *testing.T) {
	p := newTestServerPool()

	wsA := &ServerWSConn{chID: 1, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	wsB := &ServerWSConn{chID: 1, clientID: "client-b", pool: p, writeChan: make(chan writeTask, 1)}
	p.wsConns = append(p.wsConns, wsA, wsB)
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: wsA},
		"client-b": {1: wsB},
	}
	stA := &ServerConnState{connID: "conn-a", clientID: "client-a", uplinkChID: 1}
	stB := &ServerConnState{connID: "conn-b", clientID: "client-b", uplinkChID: 1}
	p.conns["conn-a"] = stA
	p.conns["conn-b"] = stB

	// 清理 client-a 的 ch_id=1
	p.cleanupChannel("client-a", 1)

	p.mu.RLock()
	_, aExists := p.clientChConns["client-a"]
	bConn := p.clientChConns["client-b"][1]
	_, stAExists := p.conns["conn-a"]
	_, stBExists := p.conns["conn-b"]
	// wsConns 中 A 已清空、B 保留
	wsAGone := p.wsConns[0] == nil
	wsBAlive := p.wsConns[1] == wsB
	p.mu.RUnlock()

	if aExists {
		t.Fatal("expected client-a namespace to be removed after cleanup")
	}
	if bConn == nil {
		t.Fatal("expected client-b same ch_id connection to survive cleanup")
	}
	if stAExists {
		t.Fatal("expected client-a connection state to be closed")
	}
	if !stBExists {
		t.Fatal("expected client-b connection state to survive cleanup")
	}
	if !wsAGone || !wsBAlive {
		t.Fatalf("wsConns cleanup mismatch: A gone=%v B alive=%v", wsAGone, wsBAlive)
	}
}
