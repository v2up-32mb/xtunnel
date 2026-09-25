package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// ServerReverseConn 服务端反向连接状态（请求方）。
// sendCh = 服务端发包通道（ChB，server→client；热路径来自预热表，经典路径来自 MsgSelectUplink meta）；
// recvCh = 服务端收包通道（ChA，client→server；热路径来自预热表，经典路径取 MsgSelectUplink 首达通道）。
type ServerReverseConn struct {
	connID        string
	target        string
	ownerClientID string
	sendCh        int32 // atomic
	recvCh        int32 // atomic

	pipe net.Conn // 隧道侧端：MsgTCPData 写入此端（缓冲管道，慢读不阻塞读循环）
	app  net.Conn // 应用侧端：DialStream 成功后返回给 xshared 隧道泵

	settled int32 // atomic：result 是否已定型（防止 close 后再次 close panic）
	result  chan struct{}
	ok      bool
	errMsg  string

	// pairPreset：DialStream 从预热表取到 Pair 并预置收发通道。
	// 客户端丢失热表回退经典选路时，允许一次性的兜底修复（repaired）。
	pairPreset bool
	repaired   int32 // atomic：兜底修复只补发一次 SelectDownlink
}

func newServerReverseConn(connID, clientID, target string) *ServerReverseConn {
	appEnd, tunEnd := xtunnel.NewBufferedPipe()
	return &ServerReverseConn{
		connID:        connID,
		target:        target,
		ownerClientID: clientID,
		pipe:          tunEnd,
		app:           appEnd,
		result:        make(chan struct{}),
	}
}

// settle 定型结果（幂等）：首个调用方生效并唤醒等待者。
func (rc *ServerReverseConn) settle(ok bool, msg string) {
	if atomic.CompareAndSwapInt32(&rc.settled, 0, 1) {
		rc.ok = ok
		rc.errMsg = msg
		close(rc.result)
	}
}

func (rc *ServerReverseConn) isSettled() bool {
	return atomic.LoadInt32(&rc.settled) == 1
}

type reverseDialer struct {
	pool     *serverPool
	clientID string
}

// DialStream 实现 dialer.Dialer（反向模式请求方）：
// 预热热路径：从预热表取最新 Pair，connID = 键 + '.' + 唯一后缀，单播 MsgTCPConnect
// 到 ChB（服务端发包通道），客户端按前缀查表直接获得完整收发通道，拨号期零选路消息；
// 无就绪 Pair / 单播失败 → 广播 MsgTCPConnect → 竞争 MsgSelectUplink 定 P1/P2
// → 经 P1 回 MsgSelectDownlink → 等 MsgConnStatus → 返回应用侧管道端。
// Pair 共享复用：后缀保证并发连接 connID 互不冲突。
func (d *reverseDialer) DialStream(ctx context.Context, target string) (net.Conn, error) {
	if d.pool.countActiveChannelsForClient(d.clientID) == 0 {
		return nil, fmt.Errorf("无可用客户端通道")
	}
	// 预热热路径：最新表项 + 双通道活性校验
	var entry *HotPairEntry
	if d.pool.hotPairs != nil {
		e := d.pool.hotPairs.Newest(d.clientID)
		if e != nil && d.pool.channelAliveForClient(d.clientID, e.ChA) && d.pool.channelAliveForClient(d.clientID, e.ChB) {
			entry = e
		}
	}
	connID := uuid.NewString()
	if entry != nil {
		connID = protocol.HotPairConnID(entry.Key, connID)
	}
	rc := newServerReverseConn(connID, d.clientID, target)

	d.pool.revMu.Lock()
	d.pool.revConns[connID] = rc
	d.pool.revMu.Unlock()

	meta := make([]byte, 1+len(target))
	meta[0] = byte(protocol.IPStrategyDefault)
	copy(meta[1:], []byte(target))
	msg := protocol.EncodeMessage(protocol.MsgTCPConnect, connID, meta, nil)

	if entry != nil {
		atomic.StoreInt32(&rc.sendCh, int32(entry.ChB)) // 服务端发包通道（server→client）
		atomic.StoreInt32(&rc.recvCh, int32(entry.ChA)) // 服务端收包通道（client→server）
		rc.pairPreset = true
		srvLog(LevelDebug, "reverse", "[服务端] 反向拨号走预热 Pair (键:%s ChA:%d ChB:%d)，客户端 %s，ID:%s",
			protocol.ShortID(entry.Key), entry.ChA, entry.ChB, protocol.ShortID(d.clientID), protocol.ShortID(connID))
		if err := d.pool.sendToChannel(d.clientID, entry.ChB, websocket.BinaryMessage, msg); err != nil {
			// Pair 通道已失效：废弃该通道全部表项并回退广播竞争
			srvLog(LevelWarn, "reverse", "[服务端] Hot Pair 通道 %d 发送失败，回退广播: %v", entry.ChB, err)
			d.pool.hotPairs.InvalidateChannel(entry.ChB)
			entry = nil
		}
	}
	if entry == nil {
		if err := d.pool.broadcastWriteToClient(d.clientID, websocket.BinaryMessage, msg); err != nil {
			d.pool.removeReverseConn(connID)
			return nil, fmt.Errorf("无可用客户端通道")
		}
	}

	// 上行泵：应用侧 → 隧道（P1 已知则单播，否则广播）
	go d.pool.reverseUpstreamPump(rc)

	// 等待拨号结果
	timer := time.NewTimer(d.pool.connectTimeout())
	defer timer.Stop()
	select {
	case <-rc.result:
	case <-timer.C:
		rc.settle(false, "拨号超时")
		// 尽力通知客户端清理，避免客户端侧连接泄漏
		d.pool.reverseNotifyClose(rc)
		d.pool.removeReverseConn(connID)
		return nil, fmt.Errorf("连接 %s 超时", target)
	case <-ctx.Done():
		rc.settle(false, ctx.Err().Error())
		d.pool.reverseNotifyClose(rc)
		d.pool.removeReverseConn(connID)
		return nil, ctx.Err()
	}

	if !rc.ok {
		d.pool.removeReverseConn(connID)
		return nil, fmt.Errorf("拨号失败: %s", rc.errMsg)
	}

	return rc.app, nil
}

// reverseUpstreamPump 应用侧数据 → 隧道。
func (p *serverPool) reverseUpstreamPump(rc *ServerReverseConn) {
	connID := rc.connID
	buf := make([]byte, 64*1024)
	for {
		n, err := rc.pipe.Read(buf)
		if err != nil {
			// 应用侧关闭（SOCKS5 隧道拆除属正常路径）：通知客户端关闭并清理
			p.reverseNotifyClose(rc)
			p.removeReverseConn(connID)
			return
		}
		if n > 0 {
			sendCh := int(atomic.LoadInt32(&rc.sendCh))
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendCh > 0 {
				if err := p.sendToChannel(rc.ownerClientID, sendCh, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, data)); err != nil {
					// 单播通道失效：回退广播，避免数据静默丢失
					_ = p.broadcastWriteToClient(rc.ownerClientID, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, data))
				}
			} else {
				_ = p.broadcastWriteToClient(rc.ownerClientID, websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgTCPData, connID, nil, data))
			}
		}
	}
}

// reverseNotifyClose 尽力通知客户端关闭反向连接（P1 已知则单播，否则广播）。
func (p *serverPool) reverseNotifyClose(rc *ServerReverseConn) {
	msg := protocol.EncodeMessage(protocol.MsgTCPClose, rc.connID, nil, nil)
	sendCh := int(atomic.LoadInt32(&rc.sendCh))
	if sendCh > 0 {
		_ = p.sendToChannel(rc.ownerClientID, sendCh, websocket.BinaryMessage, msg)
	} else {
		_ = p.broadcastWriteToClient(rc.ownerClientID, websocket.BinaryMessage, msg)
	}
}

// handleReverseMessage 处理来自客户端的反向连接消息（handleMessage 前置拦截后进入）。
func (p *serverPool) handleReverseMessage(clientID string, chID int, msgType protocol.MessageType, connID string, meta, payload []byte) {
	rc := p.getReverseConn(connID)
	if rc == nil {
		return
	}
	switch msgType {
	case protocol.MsgSelectUplink:
		if len(meta) < 4 {
			return
		}
		clientRecv := int32(binary.BigEndian.Uint32(meta[:4]))
		if atomic.CompareAndSwapInt32(&rc.sendCh, 0, clientRecv) {
			// 首达竞争：sendCh 来自 meta（客户端收包通道），recvCh 取到达通道（客户端发包通道），
			// 并经 sendCh 回 MsgSelectDownlink 让客户端拿到服务端收包通道
			atomic.StoreInt32(&rc.recvCh, int32(chID))
			downMeta := make([]byte, 4)
			binary.BigEndian.PutUint32(downMeta, uint32(chID))
			_ = p.sendToChannel(clientID, int(clientRecv), websocket.BinaryMessage, protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, downMeta, nil))
			return
		}
		// sendCh 已定型：仅预热热路径允许一次兜底修复——客户端丢失热表后
		// 重新选路（广播副本到达），采用客户端实际收发通道替换预置值，并
		// 幂等补发一次 MsgSelectDownlink（经典路径的重复帧在此被忽略）
		if rc.pairPreset && atomic.CompareAndSwapInt32(&rc.repaired, 0, 1) {
			atomic.StoreInt32(&rc.sendCh, clientRecv)
			atomic.StoreInt32(&rc.recvCh, int32(chID))
			downMeta := make([]byte, 4)
			binary.BigEndian.PutUint32(downMeta, uint32(chID))
			_ = p.sendToChannel(clientID, int(clientRecv), websocket.BinaryMessage,
				protocol.EncodeMessage(protocol.MsgSelectDownlink, connID, downMeta, nil))
			srvLog(LevelDebug, "reverse", "[服务端] 反向连接 %s 兜底修复：采用客户端重新选路通道 (发包:%d 收包:%d)",
				protocol.ShortID(connID), clientRecv, chID)
		}

	case protocol.MsgConnStatus:
		if len(meta) < 1 {
			return
		}
		if protocol.ConnStatus(meta[0]) == protocol.StatusOK {
			rc.settle(true, "")
		} else {
			reason := ""
			if len(meta) > 1 {
				reason = string(meta[1:])
			}
			rc.settle(false, reason)
		}

	case protocol.MsgTCPData:
		// 只接受来自 P2（服务端选定收包通道）的数据
		if int(atomic.LoadInt32(&rc.recvCh)) != chID {
			return
		}
		_ = rc.pipe.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout))
		_, err := rc.pipe.Write(payload)
		_ = rc.pipe.SetWriteDeadline(time.Time{})
		if err != nil {
			p.reverseNotifyClose(rc)
			p.removeReverseConn(connID)
		}

	case protocol.MsgTCPClose:
		p.reverseNotifyClose(rc)
		p.removeReverseConn(connID)
	}
}

// removeReverseConn 移除反向连接：关管道、唤醒等待者。
func (p *serverPool) removeReverseConn(connID string) {
	p.revMu.Lock()
	rc, ok := p.revConns[connID]
	if ok {
		delete(p.revConns, connID)
	}
	p.revMu.Unlock()
	if !ok {
		return
	}
	rc.settle(false, "connection closed")
	_ = rc.pipe.Close()
	_ = rc.app.Close()
}

func (p *serverPool) getReverseConn(connID string) *ServerReverseConn {
	p.revMu.RLock()
	rc := p.revConns[connID]
	p.revMu.RUnlock()
	return rc
}
