package xtunnel

import (
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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
	rc, ok := p.addReverseConn("id1", "1.2.3.4:80", "1.2.3.4:80", 1)
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

// ---- 测试辅助：按帧读取并解码 ----

func readTestFrame(t *testing.T, conn *websocket.Conn, timeout time.Duration) (protocol.MessageType, string, []byte, []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	mtype, connID, meta, payload, err := protocol.DecodeMessage(raw)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return mtype, connID, meta, payload
}

func be32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// newReverseTestPool 构建带单通道的测试池（含反向字段初始化与写 worker）
func newReverseTestPool(t *testing.T, cfg *Config, clientConn *websocket.Conn) *clientPool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool: %v", err)
	}
	p.wsConns[0] = clientConn
	go p.writeWorker(0, clientConn, p.writeQueues[0])
	return p
}

// TestReverseDialFullChain 反向拨号全链路：
// MsgTCPConnect → 广播 MsgSelectUplink(首达通道) → 本地拨号 → MsgConnStatus OK
// → MsgSelectDownlink 定发送通道 → 上行数据泵按通道单播 → 下行数据写入本地 conn
func TestReverseDialFullChain(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	cfg.ConnectTimeout = 2 * time.Second
	cfg.WriteTimeout = 2 * time.Second
	p := newReverseTestPool(t, cfg, clientConn)

	// 假目标服务
	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := targetLn.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	target := targetLn.Addr().String()

	done := make(chan struct{})
	go func() {
		p.handleChannel(1, clientConn)
		close(done)
	}()
	t.Cleanup(func() { _ = clientConn.Close() })

	// 1. 服务端下发反向拨号请求
	connID := "rev-test-1"
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte(target)...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgTCPConnect: %v", err)
	}

	// 2. 期望广播 MsgSelectUplink 携带首达通道 1
	mtype, gotID, gotMeta, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgSelectUplink || gotID != connID {
		t.Fatalf("expected MsgSelectUplink for %s, got type=%d id=%s", connID, mtype, gotID)
	}
	if binary.BigEndian.Uint32(gotMeta) != 1 {
		t.Fatalf("expected uplink channel 1, got %d", binary.BigEndian.Uint32(gotMeta))
	}

	// 3. 期望 MsgConnStatus OK
	mtype, gotID, _, _ = readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgConnStatus || gotID != connID {
		t.Fatalf("expected MsgConnStatus OK for %s, got type=%d id=%s", connID, mtype, gotID)
	}

	// 4. 目标被拨通
	var dialed net.Conn
	select {
	case dialed = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("target was never dialed")
	}

	// 5. MsgSelectDownlink 定发送通道
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, be32(1), nil)); err != nil {
		t.Fatalf("write MsgSelectDownlink: %v", err)
	}

	// 6. 下行数据 → 写入本地 conn
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, []byte("REQ-DATA"))); err != nil {
		t.Fatalf("write MsgTCPData: %v", err)
	}
	_ = dialed.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 32)
	n, err := dialed.Read(buf)
	if err != nil || string(buf[:n]) != "REQ-DATA" {
		t.Fatalf("dialed conn got %q err=%v, want REQ-DATA", string(buf[:max(n, 0)]), err)
	}

	// 7. 上行数据 → 经 sendCh=1 单播
	if _, err := dialed.Write([]byte("RESP-DATA")); err != nil {
		t.Fatalf("write to dialed: %v", err)
	}
	mtype, gotID, _, payload := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgTCPData || gotID != connID || string(payload) != "RESP-DATA" {
		t.Fatalf("expected MsgTCPData RESP-DATA, got type=%d id=%s payload=%q", mtype, gotID, string(payload))
	}

	// 8. 服务端关闭 → MsgTCPClose
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPClose, connID, nil, nil)); err != nil {
		t.Fatalf("write MsgTCPClose: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for p.hasReverseConn(connID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.hasReverseConn(connID) {
		t.Fatal("reverse conn not cleaned after MsgTCPClose")
	}
}

// TestReverseDialFailure 拨号失败 → MsgConnStatus ERR + 状态清理
func TestReverseDialFailure(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	cfg.ConnectTimeout = 2 * time.Second
	p := newReverseTestPool(t, cfg, clientConn)

	done := make(chan struct{})
	go func() {
		p.handleChannel(1, clientConn)
		close(done)
	}()
	t.Cleanup(func() { _ = clientConn.Close() })

	// 不可达目标（保留端口 1，通常无监听）
	connID := "rev-test-err"
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte("127.0.0.1:1")...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgTCPConnect: %v", err)
	}

	// 先到 MsgSelectUplink，再到 MsgConnStatus ERR
	mtype, _, _, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgSelectUplink {
		t.Fatalf("expected MsgSelectUplink, got %d", mtype)
	}
	mtype, gotID, gotMeta, _ := readTestFrame(t, serverConn, 5*time.Second)
	if mtype != protocol.MsgConnStatus || gotID != connID || len(gotMeta) < 1 || gotMeta[0] != byte(protocol.StatusERR) {
		t.Fatalf("expected MsgConnStatus ERR, got type=%d id=%s", mtype, gotID)
	}
	if p.hasReverseConn(connID) {
		t.Fatal("reverse conn should be removed after dial failure")
	}
}

// TestReverseDisabledIgnoresTCPConnect 未启用反向时忽略 MsgTCPConnect
func TestReverseDisabledIgnoresTCPConnect(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = false
	cfg.Connections = 1
	p := newReverseTestPool(t, cfg, clientConn)

	done := make(chan struct{})
	go func() {
		p.handleChannel(1, clientConn)
		close(done)
	}()
	t.Cleanup(func() { _ = clientConn.Close() })

	connID := "rev-disabled"
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte("127.0.0.1:1")...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgTCPConnect: %v", err)
	}

	_ = serverConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, _, err := serverConn.ReadMessage(); err == nil {
		t.Fatal("expected no response when reverse disabled")
	}
}

// TestReverseChannelCleanup 通道断开 → 反向连接清理
func TestReverseChannelCleanup(t *testing.T) {
	clientConn, _, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	p := newReverseTestPool(t, cfg, clientConn)

	if _, ok := p.addReverseConn("rev-cleanup", "t", "t", 1); !ok {
		t.Fatal("add failed")
	}
	p.cleanupChannel(1)
	if p.hasReverseConn("rev-cleanup") {
		t.Fatal("reverse conn should be cleaned on channel cleanup")
	}
}

// TestReverseListenerRegistration 通道就绪触发监听注册，spec 原文透传
func TestReverseListenerRegistration(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	cfg.ReverseListeners = []string{"socks5://127.0.0.1:30000"}
	p := newReverseTestPool(t, cfg, clientConn)

	p.reverseOnChannelReady(1)

	mtype, _, gotMeta, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgReverseListen {
		t.Fatalf("expected MsgReverseListen, got %d", mtype)
	}
	if string(gotMeta) != "socks5://127.0.0.1:30000" {
		t.Fatalf("spec mismatch: %q", string(gotMeta))
	}
}

// TestReverseListenAllFailTriggersCallback 全部注册失败 → OnReverseError 回调
func TestReverseListenAllFailTriggersCallback(t *testing.T) {
	clientConn, _, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	errCh := make(chan error, 1)
	cfg.OnReverseError = func(err error) { errCh <- err }
	p := newReverseTestPool(t, cfg, clientConn)

	p.getOrCreateListenerID("socks5://127.0.0.1:30000")
	p.checkReverseRegFatal()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected non-nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnReverseError not called")
	}
}

// TestReverseListenOKSuppressesCallback 任一监听成功 → 不触发全败回调
func TestReverseListenOKSuppressesCallback(t *testing.T) {
	clientConn, _, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	errCh := make(chan error, 1)
	cfg.OnReverseError = func(err error) { errCh <- err }
	p := newReverseTestPool(t, cfg, clientConn)

	id := p.getOrCreateListenerID("socks5://127.0.0.1:30000")
	p.handleReverseListenResult(id, []byte{byte(protocol.StatusOK)})
	p.checkReverseRegFatal()

	select {
	case err := <-errCh:
		t.Fatalf("OnReverseError should not fire when one listener is OK: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestReversePrebind 预绑定：不拨号、回 MsgSelectUplink、选路完成即清理
func TestReversePrebind(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	p := newReverseTestPool(t, cfg, clientConn)

	done := make(chan struct{})
	go func() {
		p.handleChannel(1, clientConn)
		close(done)
	}()
	t.Cleanup(func() { _ = clientConn.Close() })

	connID := "prebind-test-1"
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte(protocol.PrebindTarget)...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgPrebindRequest, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgPrebindRequest: %v", err)
	}

	mtype, gotID, gotMeta, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgSelectUplink || gotID != connID {
		t.Fatalf("expected MsgSelectUplink for prebind, got type=%d id=%s", mtype, gotID)
	}
	if binary.BigEndian.Uint32(gotMeta) != 1 {
		t.Fatalf("expected uplink channel 1, got %d", binary.BigEndian.Uint32(gotMeta))
	}

	// 服务端回 MsgSelectDownlink([P2]) → 选路全部完成，warm 状态保留等待提升
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, be32(1), nil)); err != nil {
		t.Fatalf("write MsgSelectDownlink: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		rc := p.getReverseConn(connID)
		if rc != nil && atomic.LoadInt32(&rc.sendCh) == 1 {
			if atomic.LoadInt32(&rc.prebind) != 1 {
				t.Fatal("state should still be warm (prebind=1)")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("warm state not settled: rc=%v sendCh=%d", rc != nil, func() int {
				if rc == nil {
					return -1
				}
				return int(atomic.LoadInt32(&rc.sendCh))
			}())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestReverseWarmPromotion 预热 Pair 消费：拨号期零选路消息——
// MsgTCPConnect（复用 prebind connID）直接提升 warm 状态并拨号，不广播 MsgSelectUplink
func TestReverseWarmPromotion(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	cfg.ConnectTimeout = 2 * time.Second
	cfg.WriteTimeout = 2 * time.Second
	p := newReverseTestPool(t, cfg, clientConn)

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen target: %v", err)
	}
	defer targetLn.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := targetLn.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	target := targetLn.Addr().String()

	done := make(chan struct{})
	go func() {
		p.handleChannel(1, clientConn)
		close(done)
	}()
	t.Cleanup(func() { _ = clientConn.Close() })

	// 1. 预热握手三步：PrebindRequest → SelectUplink → SelectDownlink([P2])
	connID := "prebind-warm-1"
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte(protocol.PrebindTarget)...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgPrebindRequest, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgPrebindRequest: %v", err)
	}
	mtype, gotID, gotMeta, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgSelectUplink || gotID != connID || binary.BigEndian.Uint32(gotMeta) != 1 {
		t.Fatalf("expected MsgSelectUplink ch=1, got type=%d id=%s meta=%v", mtype, gotID, gotMeta)
	}
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, be32(2), nil)); err != nil {
		t.Fatalf("write warmup MsgSelectDownlink: %v", err)
	}

	// 2. 拨号期：服务端复用 prebind connID 单播 MsgTCPConnect 到 P1
	meta = append([]byte{byte(protocol.IPStrategyDefault)}, []byte(target)...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgTCPConnect: %v", err)
	}

	// 3. 期望：目标被拨通 + MsgConnStatus OK；此后不再出现任何 MsgSelectUplink
	mtype, gotID, _, _ = readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgConnStatus || gotID != connID {
		t.Fatalf("expected MsgConnStatus for warm promotion, got type=%d id=%s", mtype, gotID)
	}
	var dialed net.Conn
	select {
	case dialed = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("target was never dialed")
	}

	// 4. 数据往返：上行走 sendCh=P2? —— 此测试只有通道 1，
	//    P2=2 是虚拟通道号，客户端 sendCh=2 单播会失败 → 读泵回退广播仍可达。
	//    下行经 recvCh=1 正常。
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, []byte("REQ"))); err != nil {
		t.Fatalf("write MsgTCPData: %v", err)
	}
	_ = dialed.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	n, err := dialed.Read(buf)
	if err != nil || string(buf[:n]) != "REQ" {
		t.Fatalf("dialed conn got %q err=%v, want REQ", string(buf[:max(n, 0)]), err)
	}

	// 5. 服务端关闭连接
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPClose, connID, nil, nil)); err != nil {
		t.Fatalf("write MsgTCPClose: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for p.hasReverseConn(connID) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.hasReverseConn(connID) {
		t.Fatal("reverse conn not cleaned after MsgTCPClose")
	}
}

// TestReverseHotPairSignal 客户端 -hotpair：通道就绪时向服务端发送 MsgReverseHotPair 授权
func TestReverseHotPairSignal(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = true
	cfg.Connections = 1
	p := newReverseTestPool(t, cfg, clientConn)

	p.reverseOnChannelReady(1)

	mtype, gotID, gotMeta, gotPayload := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgReverseHotPair {
		t.Fatalf("expected MsgReverseHotPair, got %d", mtype)
	}
	if gotID != "" || len(gotMeta) != 0 || len(gotPayload) != 0 {
		t.Fatalf("expected empty connID/meta/payload, got id=%q meta=%v payload=%v", gotID, gotMeta, gotPayload)
	}
}

// TestReverseHotPairSignalDisabled 未开 -hotpair 时不发送授权
func TestReverseHotPairSignalDisabled(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = false
	cfg.Connections = 1
	p := newReverseTestPool(t, cfg, clientConn)

	p.reverseOnChannelReady(1)

	_ = serverConn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, _, err := serverConn.ReadMessage(); err == nil {
		t.Fatal("no MsgReverseHotPair expected when hotpair disabled")
	}
}

// TestReverseHotPairNoForwardWarmer 反向模式下不创建正向 PairWarmer
func TestReverseHotPairNoForwardWarmer(t *testing.T) {
	clientConn, _, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = true
	cfg.Connections = 1
	p := newReverseTestPool(t, cfg, clientConn)

	if p.pairWarmer != nil {
		t.Fatal("forward PairWarmer should not be created in reverse mode")
	}
}
