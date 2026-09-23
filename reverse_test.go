package xtunnel

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
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

// TestReverseListenNoResponseHintMessage 无回执场景：报错应提示服务端可能不支持反向模式
func TestReverseListenNoResponseHintMessage(t *testing.T) {
	clientConn, _, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	errCh := make(chan error, 1)
	cfg.OnReverseError = func(err error) { errCh <- err }
	p := newReverseTestPool(t, cfg, clientConn)

	// 仅发送注册（getOrCreateListenerID 填充状态），模拟旧服务端永不回执
	p.getOrCreateListenerID("socks5://127.0.0.1:30000")
	p.checkReverseRegFatal()

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "不支持反向模式") {
			t.Fatalf("expected hint about unsupported server, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnReverseError not called")
	}
}

// TestReverseListenERRNoReason ERR 回执但无原因文本：不应误报“不支持反向模式”
func TestReverseListenERRNoReason(t *testing.T) {
	clientConn, _, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.Connections = 1
	errCh := make(chan error, 1)
	cfg.OnReverseError = func(err error) { errCh <- err }
	p := newReverseTestPool(t, cfg, clientConn)

	id := p.getOrCreateListenerID("socks5://127.0.0.1:30000")
	p.handleReverseListenResult(id, []byte{byte(protocol.StatusERR)})
	p.checkReverseRegFatal()

	select {
	case err := <-errCh:
		if strings.Contains(err.Error(), "不支持反向模式") {
			t.Fatalf("ERR received: should not hint unsupported server, got: %v", err)
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

// TestReverseHotPromotion 预热热路径：connID 带 Pair 键前缀 + 到达通道==表项收包通道
// → 直接提升为真实连接，零选路消息（不广播 MsgSelectUplink）
func TestReverseHotPromotion(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = true
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

	// 本地热表登记预热 Pair：键 prebind-key-1，收包通道(Downlink)=1、发包通道(Uplink)=1
	// （单通道测试环境收发同通道；真实环境上下行分属不同通道）
	pair := &HotChannelPair{
		ID:           "01",
		PrebindID:    "prebind-key-1",
		UplinkChID:   1,
		DownlinkChID: 1,
		state:        int32(PairStateReady),
	}
	p.pairWarmer.mu.Lock()
	p.pairWarmer.pairs = append(p.pairWarmer.pairs, pair)
	p.pairWarmer.primary = pair
	p.pairWarmer.mu.Unlock()

	// 服务端（拨号方）按约定组合 connID 并单播 MsgTCPConnect 到 ChB
	connID := protocol.HotPairConnID(pair.PrebindID, "conn-a")
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte(target)...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgTCPConnect: %v", err)
	}

	// 期望：直接 MsgConnStatus OK（无 MsgSelectUplink），目标被拨通，sendCh 已按表预置
	mtype, gotID, gotMeta, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgConnStatus || gotID != connID || gotMeta[0] != byte(protocol.StatusOK) {
		t.Fatalf("expected MsgConnStatus OK for hot promotion, got type=%d id=%s", mtype, gotID)
	}
	var dialed net.Conn
	select {
	case dialed = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("target was never dialed")
	}
	if rc := p.getReverseConn(connID); rc == nil || atomic.LoadInt32(&rc.sendCh) != 1 {
		t.Fatalf("sendCh not preset from pair table: rc=%v", rc != nil)
	}

	// 数据往返：下行写入本地 conn
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, []byte("REQ"))); err != nil {
		t.Fatalf("write MsgTCPData: %v", err)
	}
	_ = dialed.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 16)
	n, err := dialed.Read(buf)
	if err != nil || string(buf[:n]) != "REQ" {
		t.Fatalf("dialed conn got %q err=%v, want REQ", string(buf[:max(n, 0)]), err)
	}

	// 服务端关闭连接
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

// TestReverseHotPromotionFallback 预热热路径回退：connID 带前缀但到达通道与表项
// 收包通道不一致（如 Pair 已失效后的广播副本）→ 落回经典竞争路径（广播 MsgSelectUplink）
func TestReverseHotPromotionFallback(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = true
	cfg.Connections = 1
	cfg.ConnectTimeout = 2 * time.Second
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

	// 表项登记收包通道 2（与本测试唯一通道 1 不符 → 校验失败）
	pair := &HotChannelPair{
		ID:           "01",
		PrebindID:    "prebind-key-2",
		UplinkChID:   1,
		DownlinkChID: 2,
		state:        int32(PairStateReady),
	}
	p.pairWarmer.mu.Lock()
	p.pairWarmer.pairs = append(p.pairWarmer.pairs, pair)
	p.pairWarmer.primary = pair
	p.pairWarmer.mu.Unlock()

	connID := protocol.HotPairConnID(pair.PrebindID, "conn-b")
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte(target)...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgTCPConnect: %v", err)
	}

	// 期望：经典路径——广播 MsgSelectUplink，随后 ConnStatus OK
	mtype, gotID, gotMeta, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgSelectUplink || gotID != connID || binary.BigEndian.Uint32(gotMeta) != 1 {
		t.Fatalf("expected fallback MsgSelectUplink ch=1, got type=%d id=%s meta=%v", mtype, gotID, gotMeta)
	}
	mtype, gotID, _, _ = readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgConnStatus || gotID != connID {
		t.Fatalf("expected MsgConnStatus, got type=%d id=%s", mtype, gotID)
	}
	select {
	case <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("target was never dialed")
	}
}

// TestReverseHotPromotionUnknownKey connID 前缀在本地热表中无对应表项 → 经典路径
func TestReverseHotPromotionUnknownKey(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = true
	cfg.Connections = 1
	cfg.ConnectTimeout = 2 * time.Second
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

	connID := "prebind-unknown.conn-c"
	meta := append([]byte{byte(protocol.IPStrategyDefault)}, []byte(target)...)
	if err := serverConn.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)); err != nil {
		t.Fatalf("write MsgTCPConnect: %v", err)
	}

	mtype, gotID, _, _ := readTestFrame(t, serverConn, 3*time.Second)
	if mtype != protocol.MsgSelectUplink || gotID != connID {
		t.Fatalf("expected classic MsgSelectUplink for unknown key, got type=%d id=%s", mtype, gotID)
	}
	select {
	case <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("target was never dialed")
	}
}

// TestReverseWarmerCreatedInReverseMode 反向模式下同样创建 PairWarmer：
// 健康维护统一由客户端负责，预热通道对通知服务端建双端热表
func TestReverseWarmerCreatedInReverseMode(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = true
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	p, _ := newClientPool(cfg, ctx, cancel)
	if p.pairWarmer == nil {
		t.Fatal("PairWarmer should be created when EnableHotPair (reverse mode included)")
	}

	cfgNoHP := DefaultConfig()
	cfgNoHP.EnableReverse = true
	cfgNoHP.EnableHotPair = false
	p2, _ := newClientPool(cfgNoHP, nil, nil)
	if p2.pairWarmer != nil {
		t.Fatal("PairWarmer should not be created when hotpair disabled")
	}
}

// TestReverseOnChannelReadyNoExtraSignal 反向通道就绪不再发送预热授权帧（0x22 已移除）
func TestReverseOnChannelReadyNoExtraSignal(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	cfg := DefaultConfig()
	cfg.EnableReverse = true
	cfg.EnableHotPair = true
	cfg.Connections = 1
	p := newReverseTestPool(t, cfg, clientConn)

	p.reverseOnChannelReady(1)

	// 无监听参数：不应有任何出帧
	_ = serverConn.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, _, err := serverConn.ReadMessage(); err == nil {
		t.Fatal("no frame expected from reverseOnChannelReady without listeners")
	}
}
