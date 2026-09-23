package xtunnel

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

type reverseConn struct {
	id        string
	target    string
	resolved  string
	recvCh    int
	sendCh    int32 // atomic
	connMu    sync.Mutex
	conn      net.Conn
	closeOnce sync.Once
}

func (rc *reverseConn) setConn(c net.Conn) {
	rc.connMu.Lock()
	rc.conn = c
	rc.connMu.Unlock()
}

func (rc *reverseConn) getConn() net.Conn {
	rc.connMu.Lock()
	c := rc.conn
	rc.connMu.Unlock()
	return c
}

type reverseListenerState struct {
	spec       string
	listenerID string
	ok         bool
	reason     string
}

func (p *clientPool) hasReverseConn(connID string) bool {
	p.revMu.RLock()
	_, ok := p.reverseConns[connID]
	p.revMu.RUnlock()
	return ok
}

func (p *clientPool) getReverseConn(connID string) *reverseConn {
	p.revMu.RLock()
	rc := p.reverseConns[connID]
	p.revMu.RUnlock()
	return rc
}

func (p *clientPool) addReverseConn(connID, target, resolved string, recvCh int) (*reverseConn, bool) {
	p.revMu.Lock()
	if _, exists := p.reverseConns[connID]; exists {
		p.revMu.Unlock()
		return nil, false
	}
	rc := &reverseConn{
		id:       connID,
		target:   target,
		resolved: resolved,
		recvCh:   recvCh,
	}
	p.reverseConns[connID] = rc
	p.revMu.Unlock()
	return rc, true
}

func (p *clientPool) removeReverseConn(connID string) {
	p.revMu.Lock()
	delete(p.reverseConns, connID)
	p.revMu.Unlock()
}

func (p *clientPool) reverseCleanupChannel(chID int) {
	p.revMu.Lock()
	for id, rc := range p.reverseConns {
		if rc.recvCh == chID || int(atomic.LoadInt32(&rc.sendCh)) == chID {
			rc.closeOnce.Do(func() {
				if c := rc.getConn(); c != nil {
					c.Close()
				}
			})
			delete(p.reverseConns, id)
		}
	}
	p.revMu.Unlock()
}

func (p *clientPool) handleReverseTCPConnect(chID int, connID string, meta []byte) {
	if !p.config.EnableReverse {
		return
	}
	if len(meta) < 1 {
		return
	}
	target := string(meta[1:])
	// 遵循设计：忽略 meta[0] 策略字节，客户端用自身 IPStrategy 解析（解析一次，拨号用 resolved，日志用原目标）
	resolved := target
	if p.config.IPStrategy != protocol.IPStrategyDefault {
		resolved = protocol.ResolveWithStrategy(target, p.config.IPStrategy)
	}

	rc, ok := p.addReverseConn(connID, target, resolved, chID)
	if !ok {
		// 已有连接占用
		return
	}

	// 广播 MsgSelectUplink
	upBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(upBytes, uint32(chID))
	_ = p.broadcastWrite(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, connID, upBytes, nil))
	log.Printf("[客户端] 反向拨号请求: %s (来自通道 %d), ID:%s", target, chID, protocol.ShortID(connID))

	// 异步拨号
	go func() {
		conn, err := net.DialTimeout("tcp", rc.resolved, p.config.ConnectTimeout)
		if err != nil {
			log.Printf("[客户端] 反向拨号失败 %s -> %s: %v", protocol.ShortID(connID), target, err)
			_ = p.broadcastWrite(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, connID, []byte{byte(protocol.StatusERR)}, nil))
			p.removeReverseConn(connID)
			return
		}
		rc.setConn(conn)
		_ = p.broadcastWrite(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgConnStatus, connID, []byte{byte(protocol.StatusOK)}, nil))
		// 读泵：与正向模式服务端 forwardTargetToClient 对齐，不设读超时，避免误杀空闲连接
		buf := make([]byte, 64*1024)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				// 正常的关闭处理
				if !protocol.IsNormalCloseError(err) {
					log.Printf("[客户端] 反向连接读错误 %s: %v", protocol.ShortID(connID), err)
				}
				p.sendReverseClose(rc, connID)
				return
			}
			data := make([]byte, n)
			copy(data, buf[:n])
			sendCh := int(atomic.LoadInt32(&rc.sendCh))
			if sendCh > 0 {
				_ = p.asyncWriteDirect(sendCh, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, data))
			} else {
				_ = p.broadcastWrite(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, data))
			}
		}
	}()
}

func (p *clientPool) sendReverseClose(rc *reverseConn, connID string) {
	rc.closeOnce.Do(func() {
		if c := rc.getConn(); c != nil {
			c.Close()
		}
	})
	sendCh := int(atomic.LoadInt32(&rc.sendCh))
	if sendCh > 0 {
		_ = p.asyncWriteDirect(sendCh, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPClose, connID, nil, nil))
	} else {
		_ = p.broadcastWrite(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPClose, connID, nil, nil))
	}
	p.removeReverseConn(connID)
}

func (p *clientPool) handleReverseServerMsg(chID int, mtype protocol.MessageType, connID string, meta, payload []byte) {
	rc := p.getReverseConn(connID)
	if rc == nil {
		return
	}
	switch mtype {
	case protocol.MsgSelectDownlink:
		if rc.recvCh != chID {
			return
		}
		if len(meta) >= 4 {
			newSend := int(binary.BigEndian.Uint32(meta[0:4]))
			// CAS from 0
			for {
				cur := atomic.LoadInt32(&rc.sendCh)
				if cur != 0 {
					break
				}
				if atomic.CompareAndSwapInt32(&rc.sendCh, 0, int32(newSend)) {
					break
				}
			}
		}
	case protocol.MsgTCPData:
		if rc.recvCh != chID {
			return
		}
		c := rc.getConn()
		if c == nil {
			return
		}
		_ = c.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
		_, err := c.Write(payload)
		_ = c.SetWriteDeadline(time.Time{})
		if err != nil {
			// 写入失败,关闭
			p.sendReverseClose(rc, connID)
		}
	case protocol.MsgTCPClose:
		p.sendReverseClose(rc, connID)
	}
}

// 反向监听注册
func (p *clientPool) reverseOnChannelReady(chID int) {
	if !p.config.EnableReverse || len(p.config.ReverseListeners) == 0 {
		return
	}
	// 确保反向状态已初始化
	p.revMu.Lock()
	if p.reverseListenerState == nil {
		p.reverseListenerState = make(map[string]*reverseListenerState)
		p.reverseListenerMap = make(map[string]string) // spec -> listenerID
	}
	if !p.reverseRegOnceDone {
		// 触发一次定时器
		p.reverseRegOnceDone = true
		timeout := 2 * p.config.ConnectTimeout
		if timeout < 10*time.Second {
			timeout = 10 * time.Second
		}
		time.AfterFunc(timeout, func() {
			p.checkReverseRegFatal()
		})
	}
	p.revMu.Unlock()

	// 对每个 spec 发送注册
	p.revMu.RLock()
	specs := make([]string, 0, len(p.config.ReverseListeners))
	for _, s := range p.config.ReverseListeners {
		// 去重
		found := false
		for _, ss := range specs {
			if ss == s {
				found = true
				break
			}
		}
		if !found {
			specs = append(specs, s)
		}
	}
	p.revMu.RUnlock()

	for _, spec := range specs {
		listenerID := p.getOrCreateListenerID(spec)
		_ = p.asyncWriteDirect(chID, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgReverseListen, listenerID, []byte(spec), nil))
	}
}

func (p *clientPool) getOrCreateListenerID(spec string) string {
	// 双检锁在反向锁外已经有一次保护，这里保守起见直接加锁
	p.revMu.Lock()
	defer p.revMu.Unlock()
	if id, ok := p.reverseListenerMap[spec]; ok {
		return id
	}
	id := uuid.NewString()
	p.reverseListenerMap[spec] = id
	if p.reverseListenerState == nil {
		p.reverseListenerState = make(map[string]*reverseListenerState)
	}
	// 记录状态
	if _, exists := p.reverseListenerState[id]; !exists {
		p.reverseListenerState[id] = &reverseListenerState{spec: spec, listenerID: id}
	}
	return id
}

func (p *clientPool) handleReverseListenResult(connID string, meta []byte) {
	if len(meta) == 0 {
		return
	}
	status := protocol.ConnStatus(meta[0])
	reason := ""
	if len(meta) > 1 {
		reason = string(meta[1:])
	}
	p.revMu.Lock()
	state, ok := p.reverseListenerState[connID]
	if !ok {
		// 可能来得晚,仍记录
		// 尝试通过 listenerID 找到 spec
		// 先不记录,避免无限增长
		p.revMu.Unlock()
		return
	}
	state.ok = status == protocol.StatusOK
	state.reason = reason
	p.revMu.Unlock()

	spec := ""
	if state != nil {
		spec = state.spec
	}
	if status == protocol.StatusOK {
		log.Printf("[客户端] 反向监听已注册: %s", spec)
	} else {
		log.Printf("[客户端] 反向监听注册失败: %s: %s", spec, reason)
	}
}

// 检查启动阶段全败
func (p *clientPool) checkReverseRegFatal() {
	p.revMu.Lock()
	anyOK := false
	var failures []string
	for _, st := range p.reverseListenerState {
		if st.ok {
			anyOK = true
			break
		}
		if st.reason != "" {
			failures = append(failures, st.spec+": "+st.reason)
		} else {
			failures = append(failures, st.spec+": unknown")
		}
	}
	p.revMu.Unlock()

	if anyOK {
		return
	}
	if len(p.reverseListenerState) == 0 {
		// 没有任何结果,视为全部失败
		if p.config.OnReverseError != nil {
			p.config.OnReverseError(fmt.Errorf("%s", "反向监听全部未响应"))
		} else {
			log.Printf("[客户端] 反向监听全部未响应")
		}
		return
	}
	msg := "反向监听全部注册失败"
	if len(failures) > 0 {
		msg += ": " + strings.Join(failures, "; ")
	}
	if p.config.OnReverseError != nil {
		p.config.OnReverseError(fmt.Errorf("%s", msg))
	} else {
		log.Printf("[客户端] %s", msg)
	}
}
