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
	id       string
	target   string
	resolved string
	recvCh   int
	sendCh   int32 // atomic
	// prebind: 1=warm 预绑定状态（预热期已定收发通道，等待 MsgTCPConnect 提升）；
	// 0=真实连接。提升用 CAS 1→0 去重。
	prebind   int32 // atomic
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

	// 广播 MsgSelectUplink（仅广播竞争路径；预热 Pair 消费路径见 handleReverseServerMsg）
	upBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(upBytes, uint32(chID))
	_ = p.broadcastWrite(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, connID, upBytes, nil))
	log.Printf("[客户端] 反向拨号请求: %s (来自通道 %d), ID:%s", target, chID, protocol.ShortID(connID))

	go p.dialReverseTarget(rc, connID, target, resolved)
}

// dialReverseTarget 异步拨号 + 读泵（广播竞争路径与预热 Pair 提升路径共用）。
// 数据发送通道取 rc.sendCh（预热路径在预热期已定；广播路径由 MsgSelectDownlink 确定）。
func (p *clientPool) dialReverseTarget(rc *reverseConn, connID, target, resolved string) {
	conn, err := net.DialTimeout("tcp", resolved, p.config.ConnectTimeout)
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
			// warm 预绑定状态：预热期选路至此全部完成（客户端已同时持有 P1/P2），
			// 保留状态等待 MsgTCPConnect 提升（TTL 兜底清理防泄漏）
		}
	case protocol.MsgTCPConnect:
		// 预热 Pair 消费：服务端复用 prebind connID 单播拨号请求。
		// 收发通道在预热期已协商完毕，直接提升为真实连接，零选路消息。
		if rc.recvCh != chID || len(meta) < 1 {
			return
		}
		if !atomic.CompareAndSwapInt32(&rc.prebind, 1, 0) {
			return // 已提升或非 warm 状态
		}
		target := string(meta[1:])
		resolved := target
		if p.config.IPStrategy != protocol.IPStrategyDefault {
			resolved = protocol.ResolveWithStrategy(target, p.config.IPStrategy)
		}
		rc.target = target
		rc.resolved = resolved
		log.Printf("[客户端] 反向拨号请求: %s (预热 Pair 通道 %d), ID:%s", target, chID, protocol.ShortID(connID))
		go p.dialReverseTarget(rc, connID, target, resolved)
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
	if !p.config.EnableReverse {
		return
	}
	// 反向预热开关由客户端 -hotpair 决定（镜像正向由客户端 PairWarmer 决定）：
	// 通道就绪时通知服务端为本客户端启用反向 Pair 预热，服务端零配置。
	if p.config.EnableHotPair {
		_ = p.asyncWriteDirect(chID, websocket.BinaryMessage,
			protocol.EncodeMessage(protocol.MsgReverseHotPair, "", nil, nil))
	}
	if len(p.config.ReverseListeners) == 0 {
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

// handleReversePrebind 处理服务端反向预绑定请求（-hotpair 预热路径）。
// 首达占用 → 广播 MsgSelectUplink([首达通道]) → 不拨号；服务端竞争出 P2 完成 Pair
// 后经 P1 回 MsgSelectDownlink([P2])，客户端补全 sendCh 并保留 warm 状态——
// 上下行选路在预热期全部完成，拨号期仅消费（见 handleReverseServerMsg 的 MsgTCPConnect 分支）。
func (p *clientPool) handleReversePrebind(chID int, connID string, meta []byte) {
	if !p.config.EnableReverse {
		return
	}
	if len(meta) < 1 {
		return
	}
	// 仅处理预绑定目标（与正向 PrebindTarget 一致）
	if string(meta[1:]) != protocol.PrebindTarget {
		return
	}
	rc, ok := p.addReverseConn(connID, protocol.PrebindTarget, protocol.PrebindTarget, chID)
	if !ok {
		// 已有通道占用（广播副本），忽略
		return
	}
	atomic.StoreInt32(&rc.prebind, 1)
	upBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(upBytes, uint32(chID))
	_ = p.broadcastWrite(websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectUplink, connID, upBytes, nil))
	// 兑底：若 warm 状态一直未被 MsgTCPConnect 消费（服务端 Pair 未被使用/旧版服务端），定时清理防泄漏
	time.AfterFunc(reversePrebindTTL, func() {
		if cur := p.getReverseConn(connID); cur != nil && atomic.LoadInt32(&cur.prebind) == 1 {
			p.removeReverseConn(connID)
		}
	})
}

// reversePrebindTTL 反向预绑定状态的兜底存活期（预热刷新间隔 30s 的零头）
const reversePrebindTTL = 5 * time.Second

// checkReverseRegFatal 检查启动阶段反向监听是否全部注册失败
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
