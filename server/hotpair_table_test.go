package server

import (
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// newWarmTestServer 构建启用 Hot Pair 的测试服务端 + 双通道假客户端
func newWarmTestServer(t *testing.T) (*serverPool, *httptest.Server, *websocket.Conn, *websocket.Conn, func()) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Token = "token"
	p := newServerPool(cfg.Token, cfg)

	server := httptest.NewServer(http.HandlerFunc(p.handleWebSocket))
	conn1 := dialServerWS(t, server, "client-a", 1)
	conn2 := dialServerWS(t, server, "client-a", 2)

	// 等待两个通道注册就绪
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.RLock()
		ch1 := p.clientChConns["client-a"][1]
		ch2 := p.clientChConns["client-a"][2]
		p.mu.RUnlock()
		if ch1 != nil && ch2 != nil {
			// closed 由 wsConn.mu 保护（不能用 p.mu 读，避免与 close() 竞争）
			var c1, c2 bool
			ch1.mu.Lock()
			c1 = ch1.closed
			ch1.mu.Unlock()
			ch2.mu.Lock()
			c2 = ch2.closed
			ch2.mu.Unlock()
			if !c1 && !c2 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("channels not registered in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cleanup := func() {
		_ = conn1.Close()
		_ = conn2.Close()
		server.Close()
	}
	return p, server, conn1, conn2, cleanup
}

func readFrameType(t *testing.T, conn *websocket.Conn, timeout time.Duration) (protocol.MessageType, string, []byte, []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	mtype, connID, meta, payload, err := protocol.DecodeMessage(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return mtype, connID, meta, payload
}

func be32v(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return v2b(v)
}

func v2b(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// notifyHotPairs 模拟客户端批量预热通道对通知（经 handleMessage 路由）
func notifyHotPairs(t *testing.T, p *serverPool, entries ...protocol.HotPairInfo) {
	t.Helper()
	payload := protocol.EncodeHotPairNotify(entries)
	p.handleMessage("client-a", 1, len(payload), protocol.MsgHotPairNotify, "", nil, payload)
}

// TestHotPairTableNotifyUpsertLookup 通知解析 → 按键 upsert → Lookup/Newest
func TestHotPairTableNotifyUpsertLookup(t *testing.T) {
	p, _, _, _, cleanup := newWarmTestServer(t)
	defer cleanup()

	key1 := "prebind-aaaa"
	key2 := "prebind-bbbb"
	notifyHotPairs(t, p,
		protocol.HotPairInfo{Key: key1, ChA: 1, ChB: 2},
		protocol.HotPairInfo{Key: key2, ChA: 2, ChB: 1},
	)

	e := p.hotPairs.Lookup("client-a", key1)
	if e == nil || e.ChA != 1 || e.ChB != 2 {
		t.Fatalf("Lookup(key1) = %+v", e)
	}
	// 同键 upsert：新值覆盖旧值
	notifyHotPairs(t, p, protocol.HotPairInfo{Key: key1, ChA: 2, ChB: 1})
	e = p.hotPairs.Lookup("client-a", key1)
	if e == nil || e.ChA != 2 || e.ChB != 1 {
		t.Fatalf("Lookup after upsert = %+v", e)
	}

	// Newest 返回最新通知的表项（key1 刚被刷新）
	if got := p.hotPairs.Newest("client-a"); got == nil || got.Key != key1 {
		t.Fatalf("Newest = %+v, want key1", got)
	}

	// 其他客户端隔离
	if got := p.hotPairs.Lookup("client-b", key1); got != nil {
		t.Fatalf("cross-client lookup should be empty, got %+v", got)
	}
	if got := p.hotPairs.Newest("client-b"); got != nil {
		t.Fatalf("cross-client Newest should be empty, got %+v", got)
	}

	clients, entries := p.hotPairs.SizeForTest()
	if clients != 1 || entries != 2 {
		t.Fatalf("size = (%d,%d), want (1,2)", clients, entries)
	}
}

// TestHotPairTableInvalidate 通道断开清扫引用表项；客户端全断清空
func TestHotPairTableInvalidate(t *testing.T) {
	p, _, conn1, conn2, cleanup := newWarmTestServer(t)
	defer cleanup()

	key := "prebind-cccc"
	notifyHotPairs(t, p, protocol.HotPairInfo{Key: key, ChA: 1, ChB: 2})

	// 通道 2 断开：引用 ChB=2 的表项被清扫
	p.hotPairs.InvalidateChannel(2)
	if got := p.hotPairs.Lookup("client-a", key); got != nil {
		t.Fatalf("entry should be swept after channel 2 dies, got %+v", got)
	}

	// 重新登记后客户端全部掉线 → 清空
	notifyHotPairs(t, p, protocol.HotPairInfo{Key: key, ChA: 1, ChB: 2})
	_ = conn1.Close()
	_ = conn2.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.hotPairs.mu.Lock()
		_, alive := p.hotPairs.table["client-a"]
		p.hotPairs.mu.Unlock()
		if !alive {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("client entries should be cleared when client is gone")
}

// TestReverseDialStreamHotPairUnicast 预热表就绪时 DialStream 单播 ChB 直达，零选路消息。
// 数据面：上行 stream→隧道→ChB 单播；下行 ChA→隧道→stream。
func TestReverseDialStreamHotPairUnicast(t *testing.T) {
	p, _, conn1, conn2, cleanup := newWarmTestServer(t)
	defer cleanup()

	p.reverseManager.HandleReverseListen("client-a", 1, "lid-1", []byte("socks5://127.0.0.1:0"))
	readFrameType(t, conn1, 3*time.Second) // 消费监听回执

	// 假目标服务（客户端侧出口）
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

	// 模拟客户端预热完成：通知 {键, ChA=1(client→server), ChB=2(server→client)}
	key := "prebind-uni-1"
	notifyHotPairs(t, p, protocol.HotPairInfo{Key: key, ChA: 1, ChB: 2})

	// DialStream：单播 MsgTCPConnect 应只出现在通道 2（ChB），connID 带键前缀
	dialer := &reverseDialer{pool: p, clientID: "client-a"}
	streamCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := dialer.DialStream(context.Background(), targetLn.Addr().String())
		if err != nil {
			errCh <- err
			return
		}
		streamCh <- conn
	}()

	mtype1, id1, _, _ := readFrameType(t, conn2, 3*time.Second)
	if mtype1 != protocol.MsgTCPConnect {
		t.Fatalf("expected MsgTCPConnect on ChB, got type=%d", mtype1)
	}
	gotKey, _, ok := protocol.SplitHotPairConnID(id1)
	if !ok || gotKey != key {
		t.Fatalf("connID %q not prefixed with key %q", id1, key)
	}

	// 通道 1 不应收到任何拨号帧（单播而非广播）
	_ = conn1.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, _, err := conn1.ReadMessage(); err == nil {
		t.Fatal("channel 1 should not receive unicast dial request")
	}

	// 假客户端（提升路径）回 OK → DialStream 返回
	if err := conn2.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, id1, []byte{byte(protocol.StatusOK)}, nil)); err != nil {
		t.Fatalf("write ConnStatus: %v", err)
	}
	var stream net.Conn
	select {
	case stream = <-streamCh:
	case err := <-errCh:
		t.Fatalf("DialStream failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("DialStream timeout")
	}

	// 上行：app 写入 → rc.sendCh=ChB=2 → 通道 2 单播
	if _, err := stream.Write([]byte("PING")); err != nil {
		t.Fatalf("stream write: %v", err)
	}
	mtypeU, idU, _, payloadU := readFrameType(t, conn2, 3*time.Second)
	if mtypeU != protocol.MsgTCPData || idU != id1 || string(payloadU) != "PING" {
		t.Fatalf("expected upstream MsgTCPData PING on ch2, got type=%d id=%s payload=%q", mtypeU, idU, string(payloadU))
	}

	// 下行：客户端经 ChA=1 发数据 → 服务端收包通道校验通过 → stream 可读
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, id1, nil, []byte("PONG"))); err != nil {
		t.Fatalf("write downstream: %v", err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf2 := make([]byte, 16)
	n2, err := stream.Read(buf2)
	if err != nil || string(buf2[:n2]) != "PONG" {
		t.Fatalf("stream got %q err=%v, want PONG", string(buf2[:max(n2, 0)]), err)
	}
}

// TestReverseDialStreamFallback 无预热表项时回退广播竞争
func TestReverseDialStreamFallback(t *testing.T) {
	p, _, conn1, conn2, cleanup := newWarmTestServer(t)
	defer cleanup()

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

	dialer := &reverseDialer{pool: p, clientID: "client-a"}
	streamCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := dialer.DialStream(context.Background(), targetLn.Addr().String())
		if err != nil {
			errCh <- err
			return
		}
		streamCh <- conn
	}()

	// 广播：两个通道都收到 MsgTCPConnect，connID 为裸 uuid
	mtype1, id1, _, _ := readFrameType(t, conn1, 3*time.Second)
	if mtype1 != protocol.MsgTCPConnect {
		t.Fatalf("expected broadcast MsgTCPConnect on ch1, got type=%d", mtype1)
	}
	if _, _, ok := protocol.SplitHotPairConnID(id1); ok {
		t.Fatalf("fallback connID should be bare uuid, got %q", id1)
	}
	mtype2, id2, _, _ := readFrameType(t, conn2, 3*time.Second)
	if mtype2 != protocol.MsgTCPConnect || id2 != id1 {
		t.Fatalf("expected same MsgTCPConnect on ch2, got type=%d id=%s", mtype2, id2)
	}

	// 通道 1 竞争获胜：回 MsgSelectUplink；服务端应回 MsgSelectDownlink
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, id1, be32v(1), nil)); err != nil {
		t.Fatalf("write MsgSelectUplink: %v", err)
	}
	mtypeS, _, metaS, _ := readFrameType(t, conn1, 3*time.Second)
	if mtypeS != protocol.MsgSelectDownlink || binary.BigEndian.Uint32(metaS) != 1 {
		t.Fatalf("expected MsgSelectDownlink, got type=%d", mtypeS)
	}
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, id1, []byte{byte(protocol.StatusOK)}, nil)); err != nil {
		t.Fatalf("write ConnStatus: %v", err)
	}

	select {
	case <-streamCh:
	case err := <-errCh:
		t.Fatalf("DialStream failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("DialStream timeout")
	}
	_ = accepted
}

// TestReverseDialStreamFallbackRepair 预热热路径下客户端丢失热表：
// 客户端按经典路径回 MsgSelectUplink，服务端一次性兜底修复（采用客户端实际
// 收发通道并幂等补发 MsgSelectDownlink）；经典路径的重复帧不再触发修复。
func TestReverseDialStreamFallbackRepair(t *testing.T) {
	p, _, conn1, conn2, cleanup := newWarmTestServer(t)
	defer cleanup()

	p.reverseManager.HandleReverseListen("client-a", 1, "lid-1", []byte("socks5://127.0.0.1:0"))
	readFrameType(t, conn1, 3*time.Second) // 消费监听回执

	key := "prebind-fix-1"
	notifyHotPairs(t, p, protocol.HotPairInfo{Key: key, ChA: 1, ChB: 2})

	dialer := &reverseDialer{pool: p, clientID: "client-a"}
	streamCh := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := dialer.DialStream(context.Background(), "example.test:443")
		if err != nil {
			errCh <- err
			return
		}
		streamCh <- conn
	}()

	// 拨号期：单播 MsgTCPConnect 到 ChB=2（键前缀 connID），零选路消息
	mtype1, id1, _, _ := readFrameType(t, conn2, 3*time.Second)
	if mtype1 != protocol.MsgTCPConnect {
		t.Fatalf("expected MsgTCPConnect on ch2, got type=%d", mtype1)
	}
	if gotKey, _, ok := protocol.SplitHotPairConnID(id1); !ok || gotKey != key {
		t.Fatalf("connID %q not prefixed with %q", id1, key)
	}

	// 模拟客户端丢失热表：按经典路径广播 MsgSelectUplink([2])（经通道 2 到达）
	if err := conn2.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, id1, be32v(2), nil)); err != nil {
		t.Fatalf("write late MsgSelectUplink: %v", err)
	}
	// 服务端兜底修复：采用客户端实际通道（sendCh=2 收包=2），并经 sendCh
	// 补发一次 MsgSelectDownlink([2])——客户端由此拿到服务端收包通道
	mtypeR, idR, metaR, _ := readFrameType(t, conn2, 3*time.Second)
	if mtypeR != protocol.MsgSelectDownlink || idR != id1 || binary.BigEndian.Uint32(metaR) != 2 {
		t.Fatalf("expected repair MsgSelectDownlink [2], got type=%d id=%s meta=%v", mtypeR, idR, metaR)
	}

	// 经典路径重复帧（通道 1 到达的副本）不应再触发修复/补发
	if err := conn1.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, id1, be32v(1), nil)); err != nil {
		t.Fatalf("write duplicate MsgSelectUplink: %v", err)
	}
	_ = conn2.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, _, err := conn2.ReadMessage(); err == nil {
		t.Fatal("duplicate SelectUplink must not trigger a second repair")
	}

	// 拨号成功收尾
	if err := conn2.WriteMessage(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, id1, []byte{byte(protocol.StatusOK)}, nil)); err != nil {
		t.Fatalf("write ConnStatus: %v", err)
	}
	select {
	case <-streamCh:
	case err := <-errCh:
		t.Fatalf("DialStream failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("DialStream timeout")
	}
}
