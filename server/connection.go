package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// writeTask 写入任务
type writeTask struct {
	msgType int
	data    []byte
	size    int
}

// ServerConnState 服务端连接状态
type ServerConnState struct {
	connID       string
	clientID     string
	target       string
	targetConn   net.Conn
	targetUDP    *net.UDPConn
	uplinkChID   int
	downlinkChID int
	ipStrategy   protocol.IPStrategy
	isUDP        bool
	clientAddr   string
	connected    bool
	closed       bool // 防止重复注销
	mu           sync.RWMutex
	pendingData  [][]byte // 连接建立前到达的数据缓存
}

// pendingDataMaxSize 缓存数据最大字节数（防止恶意客户端耗尽内存）
const pendingDataMaxSize = 1024 * 1024 // 1MB

// ServerWSConn WebSocket 连接
type ServerWSConn struct {
	ws             *websocket.Conn
	chID           int
	clientID       string
	remoteAddr     string
	pool           *serverPool
	mu             sync.Mutex
	closed         bool
	writeChan      chan writeTask
	queueFullCount int
	lastQueueFull  time.Time
	lastReset      time.Time // 上次触发 MsgChannelReset 的时间，用于冷却
}

// start 启动写入协程
func (wsConn *ServerWSConn) start() {
	go wsConn.writeLoop()
}

// readLoop 读取循环
func (wsConn *ServerWSConn) readLoop() {
	defer func() {
		// closed 受 mu 保护，必须持锁读取（-race 检出点）
		wsConn.mu.Lock()
		alreadyClosed := wsConn.closed
		wsConn.mu.Unlock()
		if !alreadyClosed {
			wsConn.close()
		}
	}()

	wsConn.ws.SetPongHandler(func(string) error {
		return wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))
	})
	wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))
	wsConn.ws.SetPingHandler(func(m string) error {
		wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))
		// Pong 用 WriteControl 直接发送，并发安全且不走写队列，避免队列满触发 MsgChannelReset
		_ = wsConn.ws.WriteControl(websocket.PongMessage, []byte(m), time.Now().Add(wsConn.pool.config.WriteTimeout))
		return nil
	})

	for {
		mt, msg, err := wsConn.ws.ReadMessage()
		if err != nil {
			if !protocol.IsNormalCloseError(err) {
				srvLog(LevelWarn, "connection", "[服务端] 通道 %d 读取消息失败: %v", wsConn.chID, err)
			} else {
				srvLog(LevelInfo, "connection", "[服务端] 通道 %d 正常关闭: %v", wsConn.chID, err)
			}
			return
		}
		// 每次成功读取消息后重置读超时
		wsConn.ws.SetReadDeadline(time.Now().Add(wsConn.pool.config.ReadTimeout))

		if mt != websocket.BinaryMessage {
			continue
		}

		msgType, connID, meta, payload, err := protocol.DecodeMessage(msg)
		if err != nil {
			continue
		}

		wsConn.pool.handleMessage(wsConn.clientID, wsConn.chID, len(msg), msgType, connID, meta, payload)
	}
}

// writeLoop 写入循环
func (wsConn *ServerWSConn) writeLoop() {
	ticker := time.NewTicker(wsConn.pool.config.PingInterval)
	defer ticker.Stop()

	// 通道快照：close() 会将字段置 nil 并 close 通道，写循环统一使用快照避免字段竞态
	wsConn.mu.Lock()
	ch := wsConn.writeChan
	wsConn.mu.Unlock()
	if ch == nil {
		return
	}

	// 退出时回收队列字节
	defer func() {
		for {
			select {
			case task, ok := <-ch:
				if !ok {
					return
				}
				if task.size > 0 {
					wsConn.pool.removeQueueBytes(task.size)
				}
			default:
				return
			}
		}
	}()

	for {
		select {
		case task, ok := <-ch:
			if !ok {
				return
			}
			// 写入前减少队列字节计数
			if task.size > 0 {
				wsConn.pool.removeQueueBytes(task.size)
			}
			if err := wsConn.writeDirect(task.msgType, task.data); err != nil {
				srvLog(LevelWarn, "connection", "[服务端] 通道 %d 写消息失败: %v", wsConn.chID, err)
				wsConn.close()
				return
			}
		case <-ticker.C:
			if err := wsConn.writeDirect(websocket.PingMessage, []byte{}); err != nil {
				srvLog(LevelWarn, "connection", "[服务端] 通道 %d ping发送失败: %v", wsConn.chID, err)
				wsConn.close()
				return
			}
		}
	}
}

// asyncWrite 异步写入
func (wsConn *ServerWSConn) asyncWrite(msgType int, data []byte) error {
	size := len(data)

	wsConn.mu.Lock()
	if wsConn.closed || wsConn.writeChan == nil {
		wsConn.mu.Unlock()
		return nil
	}

	newSize := wsConn.pool.addQueueBytes(size)
	queue := wsConn.writeChan

	select {
	case queue <- writeTask{msgType: msgType, data: data, size: size}:
		wsConn.mu.Unlock()
		wsConn.pool.updateBackpressureState(newSize)
		return nil
	default:
		wsConn.pool.rollbackQueueBytes(size)

		now := time.Now()
		if wsConn.lastQueueFull.IsZero() || now.Sub(wsConn.lastQueueFull) > time.Second {
			wsConn.queueFullCount = 0
		}
		wsConn.lastQueueFull = now
		wsConn.queueFullCount++
		// 提高阈值并加冷却：1 秒内连续 5 次队列满且距上次重置超过 5 秒才重置，
		// 避免正常突发流量误触发通道重建。
		shouldReset := wsConn.queueFullCount >= 5 && (wsConn.lastReset.IsZero() || now.Sub(wsConn.lastReset) > 5*time.Second)
		if shouldReset {
			wsConn.queueFullCount = 0
			wsConn.lastReset = now
		}
		wsConn.mu.Unlock()

		if shouldReset {
			_ = wsConn.notifyChannelReset()
		}
		return fmt.Errorf("写队列满")
	}
}

// writeDirect 直接写入
func (wsConn *ServerWSConn) writeDirect(msgType int, data []byte) error {
	wsConn.mu.Lock()
	defer wsConn.mu.Unlock()

	if wsConn.closed {
		return nil
	}

	if msgType != websocket.BinaryMessage {
		_ = wsConn.ws.SetWriteDeadline(time.Now().Add(wsConn.pool.config.WriteTimeout))
		if err := wsConn.ws.WriteMessage(msgType, data); err != nil {
			return err
		}
		wsConn.pool.addSentBytes(len(data))
		_ = wsConn.ws.SetWriteDeadline(time.Time{})
		return nil
	}

	_ = wsConn.ws.SetWriteDeadline(time.Now().Add(wsConn.pool.config.WriteTimeout))
	if err := wsConn.ws.WriteMessage(msgType, data); err != nil {
		return err
	}
	wsConn.pool.addSentBytes(len(data))
	_ = wsConn.ws.SetWriteDeadline(time.Time{})
	return nil
}

// notifyChannelReset 通知客户端该通道需要重置
func (wsConn *ServerWSConn) notifyChannelReset() error {
	wsConn.mu.Lock()
	defer wsConn.mu.Unlock()
	if wsConn.closed {
		return nil
	}
	meta := make([]byte, 4)
	binary.BigEndian.PutUint32(meta, uint32(wsConn.chID))
	data := protocol.EncodeMessage(protocol.MsgChannelReset, "", meta, nil)
	_ = wsConn.ws.SetWriteDeadline(time.Now().Add(wsConn.pool.config.WriteTimeout))
	err := wsConn.ws.WriteMessage(websocket.BinaryMessage, data)
	_ = wsConn.ws.SetWriteDeadline(time.Time{})
	return err
}
func (wsConn *ServerWSConn) close() {
	wsConn.mu.Lock()
	if wsConn.closed {
		wsConn.mu.Unlock()
		return
	}
	wsConn.closed = true
	writeChan := wsConn.writeChan
	wsConn.writeChan = nil
	ws := wsConn.ws
	wsConn.mu.Unlock()

	if ws != nil {
		_ = ws.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		_ = ws.Close()
	}

	if writeChan != nil {
		close(writeChan)
	}

	wsConn.pool.cleanupChannel(wsConn.clientID, wsConn.chID)
}
