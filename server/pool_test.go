package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

func newTestServerPool() *serverPool {
	return &serverPool{
		config:            DefaultConfig(),
		conns:             make(map[string]*ServerConnState),
		wsConns:           make([]*ServerWSConn, 0),
		clientChConns:     make(map[string]map[int]*ServerWSConn),
		prebindTTL:        100 * time.Millisecond,
		globalQueueLimit:  1024,
		backpressureState: int32(protocol.BackpressureNormal),
	}
}

func TestHandlePrebindRequestStateSurvivesThenCleansUp(t *testing.T) {
	p := newTestServerPool()
	connID := "prebind-test-1"
	meta := []byte{0}
	meta = append(meta, protocol.PrebindTarget...)

	p.handleMessage("", 1, 10, protocol.MsgPrebindRequest, connID, meta, nil)

	// 短存活窗口内状态应存在（同轮重复预绑定帧据此被忽略，避免广播风暴）
	p.mu.RLock()
	_, exists := p.conns[connID]
	p.mu.RUnlock()
	if !exists {
		t.Fatal("prebind connID should survive within TTL window")
	}

	// 窗口结束后应清理，不泄漏
	time.Sleep(p.prebindTTL + 200*time.Millisecond)
	p.mu.RLock()
	_, exists = p.conns[connID]
	p.mu.RUnlock()
	if exists {
		t.Fatal("prebind connID should be cleaned up after TTL")
	}
}

func TestPrebindDoesNotLeakConns(t *testing.T) {
	p := newTestServerPool()
	meta := []byte{0}
	meta = append(meta, protocol.PrebindTarget...)
	for i := 0; i < 1000; i++ {
		connID := fmt.Sprintf("prebind-%d", i)
		p.handleMessage("", 1, 10, protocol.MsgPrebindRequest, connID, meta, nil)
	}
	// 每个预绑定状态短存活 TTL，所有 AfterFunc 注销后应无残留
	time.Sleep(p.prebindTTL + 300*time.Millisecond)
	p.mu.RLock()
	n := len(p.conns)
	p.mu.RUnlock()
	if n != 0 {
		t.Fatalf("expected 0 conns after prebind TTL, got %d", n)
	}
}

func TestCheckOriginDefaultAllow(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.com/ws", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Origin", "https://evil.example")

	if !checkOrigin(req) {
		t.Fatal("expected default origin check to allow request")
	}
}

func TestServerPoolStatsCountsActiveChannels(t *testing.T) {
	p := &serverPool{
		conns:         make(map[string]*ServerConnState),
		wsConns:       []*ServerWSConn{{closed: false}, {closed: true}, nil},
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	p.conns["a"] = &ServerConnState{}
	p.conns["b"] = &ServerConnState{}

	stats := p.Stats()
	if stats.ActiveConnections != 1 {
		t.Fatalf("expected 1 active connection, got %d", stats.ActiveConnections)
	}
	if stats.ActiveChannels != 1 {
		t.Fatalf("expected 1 active channel, got %d", stats.ActiveChannels)
	}
	if stats.TotalConnections != 2 {
		t.Fatalf("expected 2 total connections, got %d", stats.TotalConnections)
	}
}

func TestServerWSConnCloseClosesBufferedWriteChan(t *testing.T) {
	conn, cleanup := newServerTestWebSocketConn(t)
	defer cleanup()

	p := &serverPool{
		config:        &Config{WriteTimeout: time.Second},
		conns:         make(map[string]*ServerConnState),
		wsConns:       make([]*ServerWSConn, 1),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	wsConn := &ServerWSConn{
		ws:        conn,
		chID:      1,
		pool:      p,
		writeChan: make(chan writeTask, 2),
	}
	queue := wsConn.writeChan
	queue <- writeTask{msgType: websocket.BinaryMessage, data: []byte("x"), size: 1}
	p.wsConns[0] = wsConn
	if p.clientChConns[""] == nil {
		p.clientChConns[""] = make(map[int]*ServerWSConn)
	}
	p.clientChConns[""][1] = wsConn

	done := make(chan struct{})
	go func() {
		wsConn.close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected close to return promptly")
	}

	if _, ok := <-queue; !ok {
		t.Fatal("expected buffered write task to remain readable before channel drains")
	}

	select {
	case _, ok := <-queue:
		if ok {
			t.Fatal("expected writeChan to be closed after buffered task drains")
		}
	default:
		t.Fatal("expected drained writeChan to be immediately readable as closed")
	}
}

func TestAsyncWriteQueueFullDoesNotTriggerBackpressureState(t *testing.T) {
	p := &serverPool{
		config:            &Config{ReadBufferSize: 8},
		conns:             make(map[string]*ServerConnState),
		clientChConns:     make(map[string]map[int]*ServerWSConn),
		globalQueueLimit:  8,
		backpressureState: int32(protocol.BackpressureNormal),
	}
	wsConn := &ServerWSConn{
		chID:      1,
		pool:      p,
		writeChan: make(chan writeTask, 1),
	}
	wsConn.writeChan <- writeTask{msgType: websocket.BinaryMessage, data: []byte("busy"), size: 4}
	atomic.StoreInt64(&p.globalQueueBytes, 4)

	if err := wsConn.asyncWrite(websocket.BinaryMessage, []byte("1234567")); err == nil {
		t.Fatal("expected asyncWrite to fail when queue is full")
	}
	if got := protocol.BackpressureState(atomic.LoadInt32(&p.backpressureState)); got != protocol.BackpressureNormal {
		t.Fatalf("expected backpressure state to stay normal after failed enqueue, got %v", got)
	}
	if got := atomic.LoadInt64(&p.globalQueueBytes); got != 4 {
		t.Fatalf("expected queue bytes to remain 4 for buffered task only, got %d", got)
	}
}

func TestAsyncWriteHighWaterReturnsPromptly(t *testing.T) {
	p := &serverPool{
		config:            &Config{ReadBufferSize: 8},
		conns:             make(map[string]*ServerConnState),
		wsConns:           make([]*ServerWSConn, 1),
		clientChConns:     make(map[string]map[int]*ServerWSConn),
		globalQueueLimit:  8,
		backpressureState: int32(protocol.BackpressureNormal),
	}
	wsConn := &ServerWSConn{
		chID:      1,
		pool:      p,
		writeChan: make(chan writeTask, 1),
	}
	p.wsConns[0] = wsConn
	if p.clientChConns[""] == nil {
		p.clientChConns[""] = make(map[int]*ServerWSConn)
	}
	p.clientChConns[""][1] = wsConn

	done := make(chan error, 1)
	go func() {
		done <- wsConn.asyncWrite(websocket.BinaryMessage, []byte("1234567"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected enqueue to succeed, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected asyncWrite to return promptly when entering high-water backpressure")
	}
}

func TestBroadcastBackpressureSkipsSaturatedChannel(t *testing.T) {
	p := &serverPool{
		config:            &Config{ReadBufferSize: 8},
		conns:             make(map[string]*ServerConnState),
		wsConns:           make([]*ServerWSConn, 1),
		clientChConns:     make(map[string]map[int]*ServerWSConn),
		globalQueueLimit:  64,
		backpressureState: int32(protocol.BackpressureNormal),
	}
	wsConn := &ServerWSConn{
		chID:      1,
		pool:      p,
		writeChan: make(chan writeTask, 1),
	}
	wsConn.writeChan <- writeTask{msgType: websocket.BinaryMessage, data: []byte("busy"), size: 4}
	p.wsConns[0] = wsConn
	if p.clientChConns[""] == nil {
		p.clientChConns[""] = make(map[int]*ServerWSConn)
	}
	p.clientChConns[""][1] = wsConn

	done := make(chan struct{})
	go func() {
		p.broadcastBackpressure(protocol.BackpressurePause)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected broadcastBackpressure to return promptly even with saturated channel")
	}

	if got := len(wsConn.writeChan); got != 1 {
		t.Fatalf("expected saturated queue length to remain 1, got %d", got)
	}
}

func TestSendDownlinkBeforeSelectionOnlyTargetsOwningClient(t *testing.T) {
	p := &serverPool{
		conns:         make(map[string]*ServerConnState),
		wsConns:       make([]*ServerWSConn, 3),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	p.wsConns[0] = &ServerWSConn{chID: 1, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	p.wsConns[1] = &ServerWSConn{chID: 2, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	p.wsConns[2] = &ServerWSConn{chID: 3, clientID: "client-b", pool: p, writeChan: make(chan writeTask, 1)}
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: p.wsConns[0], 2: p.wsConns[1]},
		"client-b": {3: p.wsConns[2]},
	}
	p.conns["tcp-conn"] = &ServerConnState{connID: "tcp-conn", clientID: "client-a"}

	if err := p.sendDownlink("tcp-conn", protocol.MsgConnStatus, []byte{byte(protocol.StatusOK)}, nil); err != nil {
		t.Fatalf("sendDownlink failed: %v", err)
	}
	if got := len(p.wsConns[0].writeChan); got != 1 {
		t.Fatalf("expected client-a channel 1 to receive message, got %d", got)
	}
	if got := len(p.wsConns[1].writeChan); got != 1 {
		t.Fatalf("expected client-a channel 2 to receive message, got %d", got)
	}
	if got := len(p.wsConns[2].writeChan); got != 0 {
		t.Fatalf("expected foreign client channel to receive no message, got %d", got)
	}
}

func TestBroadcastWriteRemainsGlobalAcrossClients(t *testing.T) {
	p := &serverPool{
		wsConns:       make([]*ServerWSConn, 3),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	p.wsConns[0] = &ServerWSConn{chID: 1, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	p.wsConns[1] = &ServerWSConn{chID: 2, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	p.wsConns[2] = &ServerWSConn{chID: 3, clientID: "client-b", pool: p, writeChan: make(chan writeTask, 1)}
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: p.wsConns[0], 2: p.wsConns[1]},
		"client-b": {3: p.wsConns[2]},
	}

	if err := p.broadcastWrite(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("broadcastWrite failed: %v", err)
	}

	for i, wsConn := range p.wsConns {
		if got := len(wsConn.writeChan); got != 1 {
			t.Fatalf("expected channel %d to receive global broadcast, got %d", i+1, got)
		}
	}
}

func TestHandleWebSocketRejectsWhenMaxTotalChannelsReached(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxTotalChannels = 1
	p := newServerPool("token", cfg)
	p.wsConns = []*ServerWSConn{{chID: 1, clientID: "client-a", pool: p}}
	p.clientChConns = map[string]map[int]*ServerWSConn{"client-a": {1: p.wsConns[0]}}

	server := httptest.NewServer(http.HandlerFunc(p.handleWebSocket))
	defer server.Close()

	dialURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?client_id=client-b"
	conn, resp, err := (&websocket.Dialer{Subprotocols: []string{"token"}}).Dial(dialURL, nil)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected websocket dial to be rejected when max total channels reached")
	}
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		if resp == nil {
			t.Fatalf("expected HTTP 429 response, got nil response and err %v", err)
		}
		t.Fatalf("expected HTTP 429 response, got %d", resp.StatusCode)
	}
}

func TestHandleWebSocketRejectsWhenMaxChannelsPerClientReached(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxChannelsPerClient = 1
	p := newServerPool("token", cfg)
	p.wsConns = []*ServerWSConn{{chID: 1, clientID: "client-a", pool: p}}
	p.clientChConns = map[string]map[int]*ServerWSConn{"client-a": {1: p.wsConns[0]}}

	server := httptest.NewServer(http.HandlerFunc(p.handleWebSocket))
	defer server.Close()

	dialURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?client_id=client-a"
	conn, resp, err := (&websocket.Dialer{Subprotocols: []string{"token"}}).Dial(dialURL, nil)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected websocket dial to be rejected when max channels per client reached")
	}
	if resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		if resp == nil {
			t.Fatalf("expected HTTP 429 response, got nil response and err %v", err)
		}
		t.Fatalf("expected HTTP 429 response, got %d", resp.StatusCode)
	}
}

func TestSendDownlinkAfterSelectionOnlyTargetsChosenChannel(t *testing.T) {
	p := &serverPool{
		conns:         make(map[string]*ServerConnState),
		wsConns:       make([]*ServerWSConn, 3),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	p.wsConns[0] = &ServerWSConn{chID: 1, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	p.wsConns[1] = &ServerWSConn{chID: 2, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	p.wsConns[2] = &ServerWSConn{chID: 3, clientID: "client-a", pool: p, writeChan: make(chan writeTask, 1)}
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: p.wsConns[0], 2: p.wsConns[1], 3: p.wsConns[2]},
	}
	p.conns["tcp-conn"] = &ServerConnState{connID: "tcp-conn", clientID: "client-a", downlinkChID: 2}

	if err := p.sendDownlink("tcp-conn", protocol.MsgConnStatus, []byte{byte(protocol.StatusOK)}, nil); err != nil {
		t.Fatalf("sendDownlink failed: %v", err)
	}
	if got := len(p.wsConns[0].writeChan); got != 0 {
		t.Fatalf("expected channel 1 not to receive unicast downlink, got %d", got)
	}
	if got := len(p.wsConns[1].writeChan); got != 1 {
		t.Fatalf("expected chosen channel 2 to receive unicast downlink, got %d", got)
	}
	if got := len(p.wsConns[2].writeChan); got != 0 {
		t.Fatalf("expected channel 3 not to receive unicast downlink, got %d", got)
	}
}

func TestHandleUDPConnectFirstChannelWins(t *testing.T) {
	p := &serverPool{
		config:            DefaultConfig(),
		conns:             make(map[string]*ServerConnState),
		wsConns:           make([]*ServerWSConn, 2),
		clientChConns:     make(map[string]map[int]*ServerWSConn),
		globalQueueLimit:  1024,
		backpressureState: int32(protocol.BackpressureNormal),
	}
	p.wsConns[0] = &ServerWSConn{chID: 1, clientID: "client-a", pool: p}
	p.wsConns[1] = &ServerWSConn{chID: 2, clientID: "client-a", pool: p}
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: p.wsConns[0], 2: p.wsConns[1]},
	}

	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte("127.0.0.1:53")...)
	p.handleUDPConnect("client-a", 1, "udp-conn", meta)

	p.mu.RLock()
	first := p.conns["udp-conn"]
	p.mu.RUnlock()
	if first == nil {
		t.Fatal("expected first UDP connect to register connection")
	}
	firstUDP := first.targetUDP
	defer func() {
		if firstUDP != nil {
			_ = firstUDP.Close()
		}
	}()

	p.handleUDPConnect("client-a", 2, "udp-conn", meta)

	p.mu.RLock()
	current := p.conns["udp-conn"]
	p.mu.RUnlock()
	if current != first {
		t.Fatal("expected duplicate UDP connect to keep first connection state")
	}
	if current.uplinkChID != 1 {
		t.Fatalf("expected uplink channel to remain 1, got %d", current.uplinkChID)
	}
	if current.targetUDP != firstUDP {
		t.Fatal("expected duplicate UDP connect not to replace target UDP socket")
	}

	p.unregisterConn("udp-conn")
}

func TestServerStatsCountSentBytesFromAsyncWrite(t *testing.T) {
	p := &serverPool{
		config:        &Config{WriteTimeout: time.Second},
		conns:         make(map[string]*ServerConnState),
		wsConns:       make([]*ServerWSConn, 1),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	conn, cleanup := newServerTestWebSocketConn(t)
	defer cleanup()

	wsConn := &ServerWSConn{
		ws:        conn,
		chID:      1,
		pool:      p,
		writeChan: make(chan writeTask, 1),
	}
	p.wsConns[0] = wsConn
	if p.clientChConns[""] == nil {
		p.clientChConns[""] = make(map[int]*ServerWSConn)
	}
	p.clientChConns[""][1] = wsConn

	msg := []byte("hello")
	if err := wsConn.asyncWrite(websocket.BinaryMessage, msg); err != nil {
		t.Fatalf("asyncWrite failed: %v", err)
	}

	select {
	case task := <-wsConn.writeChan:
		if task.size > 0 {
			p.removeQueueBytes(task.size)
		}
		if err := wsConn.writeDirect(task.msgType, task.data); err != nil {
			t.Fatalf("writeDirect failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued write task")
	}

	if got := p.Stats().BytesSent; got != uint64(len(msg)) {
		t.Fatalf("expected bytes sent %d, got %d", len(msg), got)
	}
}

func TestServerStatsCountReceivedBytesFromHandleMessage(t *testing.T) {
	p := &serverPool{conns: make(map[string]*ServerConnState)}
	msg := protocol.EncodeMessage(protocol.MsgConnStatus, "cid", []byte{byte(protocol.StatusOK)}, nil)
	msgType, connID, meta, payload, err := protocol.DecodeMessage(msg)
	if err != nil {
		t.Fatalf("decode message failed: %v", err)
	}

	p.handleMessage("", 1, len(msg), msgType, connID, meta, payload)

	if got := p.Stats().BytesReceived; got != uint64(len(msg)) {
		t.Fatalf("expected bytes received %d, got %d", len(msg), got)
	}
}

func TestHandleTCPDataBuffersMultipleUplinkPayloadsBeforeTargetConnect(t *testing.T) {
	p := &serverPool{conns: make(map[string]*ServerConnState)}
	st := &ServerConnState{connID: "tcp-conn", uplinkChID: 1}
	p.conns[st.connID] = st

	p.handleTCPData(1, st.connID, []byte("hello"))
	p.handleTCPData(1, st.connID, []byte("world"))

	st.mu.RLock()
	defer st.mu.RUnlock()
	if len(st.pendingData) != 2 {
		t.Fatalf("expected 2 pending payloads, got %d", len(st.pendingData))
	}
	if string(st.pendingData[0]) != "hello" {
		t.Fatalf("expected first payload hello, got %q", string(st.pendingData[0]))
	}
	if string(st.pendingData[1]) != "world" {
		t.Fatalf("expected second payload world, got %q", string(st.pendingData[1]))
	}
}

func TestHandleTCPDataIgnoresNonUplinkBeforeTargetConnect(t *testing.T) {
	p := &serverPool{conns: make(map[string]*ServerConnState)}
	st := &ServerConnState{connID: "tcp-conn", uplinkChID: 1}
	p.conns[st.connID] = st

	p.handleTCPData(2, st.connID, []byte("ignored"))

	st.mu.RLock()
	defer st.mu.RUnlock()
	if len(st.pendingData) != 0 {
		t.Fatalf("expected no pending payload from non-uplink channel, got %d", len(st.pendingData))
	}
}

func TestHandleSelectDownlinkRejectsForeignClientChannel(t *testing.T) {
	p := &serverPool{
		conns:         make(map[string]*ServerConnState),
		wsConns:       make([]*ServerWSConn, 2),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	p.wsConns[0] = &ServerWSConn{chID: 1, clientID: "client-a", pool: p}
	p.wsConns[1] = &ServerWSConn{chID: 2, clientID: "client-b", pool: p}
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: p.wsConns[0]},
		"client-b": {2: p.wsConns[1]},
	}

	st := &ServerConnState{connID: "tcp-conn", uplinkChID: 1, clientID: "client-a", target: "example.com:443"}
	p.conns[st.connID] = st

	meta := make([]byte, 4)
	binary.BigEndian.PutUint32(meta, 2)
	p.handleSelectDownlink("client-a", 1, st.connID, meta)

	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.downlinkChID != 0 {
		t.Fatalf("expected foreign client channel to be rejected, got downlink %d", st.downlinkChID)
	}
}

func TestHandleSelectDownlinkAcceptsSameClientChannel(t *testing.T) {
	p := &serverPool{
		conns:         make(map[string]*ServerConnState),
		wsConns:       make([]*ServerWSConn, 2),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	p.wsConns[0] = &ServerWSConn{chID: 1, clientID: "client-a", pool: p}
	p.wsConns[1] = &ServerWSConn{chID: 2, clientID: "client-a", pool: p}
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: p.wsConns[0], 2: p.wsConns[1]},
	}

	st := &ServerConnState{connID: "tcp-conn", uplinkChID: 1, clientID: "client-a", target: "example.com:443"}
	p.conns[st.connID] = st

	meta := make([]byte, 4)
	binary.BigEndian.PutUint32(meta, 2)
	p.handleSelectDownlink("client-a", 1, st.connID, meta)

	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.downlinkChID != 2 {
		t.Fatalf("expected same-client channel 2 to become downlink, got %d", st.downlinkChID)
	}
}

func TestHandleSelectDownlinkKeepsFirstWinner(t *testing.T) {
	p := &serverPool{
		conns:         make(map[string]*ServerConnState),
		wsConns:       make([]*ServerWSConn, 3),
		clientChConns: make(map[string]map[int]*ServerWSConn),
	}
	p.wsConns[0] = &ServerWSConn{chID: 1, clientID: "client-a", pool: p}
	p.wsConns[1] = &ServerWSConn{chID: 2, clientID: "client-a", pool: p}
	p.wsConns[2] = &ServerWSConn{chID: 3, clientID: "client-a", pool: p}
	p.clientChConns = map[string]map[int]*ServerWSConn{
		"client-a": {1: p.wsConns[0], 2: p.wsConns[1], 3: p.wsConns[2]},
	}

	st := &ServerConnState{connID: "tcp-conn", uplinkChID: 1, clientID: "client-a", target: "example.com:443"}
	p.conns[st.connID] = st

	meta := make([]byte, 4)
	binary.BigEndian.PutUint32(meta, 2)
	p.handleSelectDownlink("client-a", 1, st.connID, meta)
	binary.BigEndian.PutUint32(meta, 3)
	p.handleSelectDownlink("client-a", 1, st.connID, meta)

	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.downlinkChID != 2 {
		t.Fatalf("expected first selected downlink 2 to remain winner, got %d", st.downlinkChID)
	}
}

func TestConnectTargetDoesNotReviveClosedPendingOverflowConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	p := &serverPool{conns: make(map[string]*ServerConnState)}
	st := &ServerConnState{connID: "tcp-conn", uplinkChID: 1, target: listener.Addr().String()}
	p.conns[st.connID] = st

	p.handleTCPData(1, st.connID, []byte(strings.Repeat("x", pendingDataMaxSize+1)))

	p.mu.RLock()
	_, exists := p.conns[st.connID]
	p.mu.RUnlock()
	if exists {
		t.Fatal("expected overflowing pending data to unregister connection")
	}

	p.connectTarget(st)

	st.mu.RLock()
	targetConn := st.targetConn
	connected := st.connected
	closed := st.closed
	st.mu.RUnlock()

	if targetConn != nil {
		_ = targetConn.Close()
	}

	if !closed {
		t.Fatal("expected overflowing pending data to mark connection closed")
	}
	if connected {
		t.Fatal("expected closed connection not to be marked connected by connectTarget")
	}
	if targetConn != nil {
		t.Fatal("expected connectTarget not to attach targetConn after connection was closed")
	}
}

func newServerTestWebSocketConn(t *testing.T) (*websocket.Conn, func()) {
	t.Helper()

	serverDone := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			close(serverDone)
			return
		}
		defer ws.Close()
		defer close(serverDone)

		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))

	dialURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	if err != nil {
		server.Close()
		t.Fatalf("dial websocket failed: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		server.Close()
		select {
		case <-serverDone:
		case <-time.After(time.Second):
		}
	}

	return conn, cleanup
}

func TestHandleTCPConnectUsesRemoteAddrForClientAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Token = "token"
	p := newServerPool(cfg.Token, cfg)

	p.wsConns = []*ServerWSConn{{
		clientID:   "client-a",
		remoteAddr: "203.0.113.10:4567",
		closed:     true,
	}}
	p.clientChConns = map[string]map[int]*ServerWSConn{"client-a": {1: p.wsConns[0]}}

	meta := append([]byte{0}, []byte("203.0.113.1:65000")...)
	p.handleTCPConnect("client-a", 1, "conn-1", meta)

	p.mu.RLock()
	st := p.conns["conn-1"]
	p.mu.RUnlock()
	if st == nil {
		t.Fatalf("connection state was not created")
	}
	if st.clientID != "client-a" {
		t.Fatalf("clientID = %q, want %q", st.clientID, "client-a")
	}
	if st.clientAddr != "203.0.113.10:4567" {
		t.Fatalf("clientAddr = %q, want remote address", st.clientAddr)
	}
}

// TestPrebindDuplicatesIgnoredFirstArrivalWins 同 connID 重复帧应被忽略、
// 首到者定上行，且不重建状态（一轮仅一广播的核心保证）
func TestPrebindDuplicatesIgnoredFirstArrivalWins(t *testing.T) {
	p := newTestServerPool()
	meta := []byte{0}
	meta = append(meta, protocol.PrebindTarget...)
	connID := "prebind-dup-1"

	// 第一条：通道 1 先到，成为上行
	p.handleMessage("", 1, 10, protocol.MsgPrebindRequest, connID, meta, nil)
	// 同 connID 重复帧：通道 20、30 随后到达，应被忽略（不重建状态、不改上行）
	p.handleMessage("", 20, 10, protocol.MsgPrebindRequest, connID, meta, nil)
	p.handleMessage("", 30, 10, protocol.MsgPrebindRequest, connID, meta, nil)

	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()
	if st == nil {
		t.Fatal("prebind state should exist within TTL")
	}
	if st.uplinkChID != 1 {
		t.Fatalf("first arrival should win uplink, got %d", st.uplinkChID)
	}

	// TTL 后清理
	time.Sleep(p.prebindTTL + 200*time.Millisecond)
	p.mu.RLock()
	_, exists := p.conns[connID]
	p.mu.RUnlock()
	if exists {
		t.Fatal("prebind state should be cleaned after TTL")
	}
}
