package server

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestAsyncWriteQueueFullSendsChannelReset(t *testing.T) {
	p := newTestServerPool()
	wsConn := &ServerWSConn{
		pool:      p,
		chID:      1,
		writeChan: make(chan writeTask, 0),
	}

	// 使用一个真实的测试 WebSocket 连接
	conn, cleanup := newServerTestWebSocketConn(t)
	defer cleanup()
	wsConn.ws = conn

	// 连续触发 5 次队列满（阈值已提高至 5）
	for i := 0; i < 5; i++ {
		_ = wsConn.asyncWrite(websocket.BinaryMessage, make([]byte, 10))
	}

	// 检查计数器是否已重置（达到阈值后 queueFullCount 会被清零）
	wsConn.mu.Lock()
	count := wsConn.queueFullCount
	wsConn.mu.Unlock()

	if count != 0 {
		t.Fatalf("expected queueFullCount to be reset to 0 after threshold, got %d", count)
	}

	// 验证 lastQueueFull 被设置
	wsConn.mu.Lock()
	last := wsConn.lastQueueFull
	wsConn.mu.Unlock()
	if last.IsZero() {
		t.Fatal("expected lastQueueFull to be set")
	}
}

func TestAsyncWriteQueueFullDoesNotResetBeforeThreshold(t *testing.T) {
	p := newTestServerPool()
	wsConn := &ServerWSConn{
		pool:      p,
		chID:      1,
		writeChan: make(chan writeTask, 0),
	}

	conn, cleanup := newServerTestWebSocketConn(t)
	defer cleanup()
	wsConn.ws = conn

	// 只触发 2 次队列满
	for i := 0; i < 2; i++ {
		_ = wsConn.asyncWrite(websocket.BinaryMessage, make([]byte, 10))
	}

	wsConn.mu.Lock()
	count := wsConn.queueFullCount
	wsConn.mu.Unlock()

	if count != 2 {
		t.Fatalf("expected queueFullCount to be 2 before threshold, got %d", count)
	}
}

func TestAsyncWriteQueueFullCountResetsAfterOneSecond(t *testing.T) {
	p := newTestServerPool()
	wsConn := &ServerWSConn{
		pool:      p,
		chID:      1,
		writeChan: make(chan writeTask, 0),
	}

	conn, cleanup := newServerTestWebSocketConn(t)
	defer cleanup()
	wsConn.ws = conn

	// 触发 2 次队列满
	for i := 0; i < 2; i++ {
		_ = wsConn.asyncWrite(websocket.BinaryMessage, make([]byte, 10))
	}

	// 等待超过 1 秒
	time.Sleep(1100 * time.Millisecond)

	// 再次触发，应该重置计数器
	_ = wsConn.asyncWrite(websocket.BinaryMessage, make([]byte, 10))

	wsConn.mu.Lock()
	count := wsConn.queueFullCount
	wsConn.mu.Unlock()

	if count != 1 {
		t.Fatalf("expected queueFullCount to reset after 1s interval, got %d", count)
	}
}
