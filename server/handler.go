package server

import (
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/v2up-32mb/xtunnel/protocol"
)

// ======================== TCP 处理 ========================

// prebindStateTTL 预绑定状态短存活窗口（默认值）：健康检查时客户端并行广播 ~N 条
// 预绑定帧，首帧建状态并广播 MsgSelectUplink；窗口内其余帧因状态已存在被忽略，
// 避免每帧重建状态重新广播（广播风暴）。实测一轮预绑定在同秒内到齐，5s 窗口
// 对任意慢链路的一轮都足够，窗口结束后注销状态释放。
// 可通过 serverPool.prebindTTL 覆盖（测试用）。
// 演进目标（B 方案）：后续用显式 begin 消息替代定时窗口，见设计讨论。
const prebindStateTTL = 5 * time.Second

// handleTCPConnect 处理 TCP 连接请求
func (p *serverPool) handleTCPConnect(clientID string, chID int, connID string, meta []byte) {
	if len(meta) < 1 {
		p.sendDownlink(connID, protocol.MsgConnStatus, []byte{byte(protocol.StatusERR)}, nil)
		return
	}

	ipStrategy := protocol.IPStrategy(meta[0])
	target := string(meta[1:])

	// 发布状态前先解析归属客户端，避免对已发布状态无锁写 clientID/clientAddr（数据竞争）
	p.mu.RLock()
	wsConn := p.clientChConns[clientID][chID]
	p.mu.RUnlock()
	var inboundClientID, inboundAddr string
	if wsConn != nil {
		inboundClientID = wsConn.clientID
		inboundAddr = wsConn.remoteAddr
	}

	// 第一个到达的通道占用连接,后续的丢弃
	p.mu.Lock()
	st, exists := p.conns[connID]
	if !exists {
		// 第一个到达的通道:创建状态并占用
		st = &ServerConnState{
			connID:     connID,
			target:     target,
			uplinkChID: chID,
			ipStrategy: ipStrategy,
			isUDP:      false,
			connected:  false,
			clientID:   inboundClientID,
			clientAddr: inboundAddr,
		}
		p.conns[connID] = st
		p.mu.Unlock()

		// 发送 MsgSelectUplink（广播）携带上行通道 ID；预热热路径（connID 携带
		// Pair 键前缀且到达通道与表项发包方向一致）预置下行通道，跳过选路帧，
		// 客户端按前缀查表直接获得完整收发通道，拨号期零选路消息
		uplinkChIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(uplinkChIDBytes, uint32(chID))
		promoted := false
		if p.hotPairs != nil {
			if key, _, ok := protocol.SplitHotPairConnID(connID); ok {
				if e := p.hotPairs.Lookup(clientID, key); e != nil && e.ChA == chID && p.channelAliveForClient(clientID, e.ChB) {
					st.mu.Lock()
					st.downlinkChID = e.ChB
					st.mu.Unlock()
					promoted = true
					srvLog(LevelInfo, "handler", "[服务端] %s 访问: %s, 通道: TX %d RX %d (预热 Pair 提升, 键:%s), ID:%s",
						st.clientAddr, target, chID, e.ChB, protocol.ShortID(e.Key), protocol.ShortID(connID))
				}
			}
		}
		if !promoted {
			_ = p.sendDownlink(connID, protocol.MsgSelectUplink, uplinkChIDBytes, nil)
			srvLog(LevelInfo, "handler", "[服务端] %s 访问: %s, 通道: TX %d, ID:%s", st.clientAddr, target, chID, protocol.ShortID(connID))
		}

		// 异步连接目标服务器
		go p.connectTarget(st)

	} else {
		// 后续通道:丢弃
		p.mu.Unlock()
		// 已有其他通道处理此连接,静默丢弃
	}
}

// connectTarget 连接目标服务器
func (p *serverPool) connectTarget(st *ServerConnState) {
	st.mu.RLock()
	if st.closed {
		st.mu.RUnlock()
		return
	}
	resolvedTarget := protocol.ResolveWithStrategy(st.target, st.ipStrategy)
	st.mu.RUnlock()

	// 连接到目标
	conn, err := net.DialTimeout("tcp", resolvedTarget, p.connectTimeout())
	if err != nil {
		srvLog(LevelWarn, "handler", "[服务端] 连接目标失败 %s: %v", st.target, err)
		p.sendDownlink(st.connID, protocol.MsgConnStatus, []byte{byte(protocol.StatusERR)}, nil)
		// 补发 MsgTCPClose 通知客户端清理，避免半开连接
		p.sendDownlink(st.connID, protocol.MsgTCPClose, nil, nil)
		p.mu.Lock()
		delete(p.conns, st.connID)
		p.mu.Unlock()
		return
	}

	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		_ = conn.Close()
		return
	}
	st.targetConn = conn
	st.connected = true
	// 获取并清空缓存数据
	pending := st.pendingData
	st.pendingData = nil
	st.mu.Unlock()

	srvLog(LevelInfo, "handler", "[服务端] %s 连接目标成功 %s, ID:%s", st.clientAddr, st.target, protocol.ShortID(st.connID))

	// 发送连接成功（广播）
	_ = p.sendDownlink(st.connID, protocol.MsgConnStatus, []byte{byte(protocol.StatusOK)}, nil)

	// 发送缓存的数据
	if len(pending) > 0 {
		for _, data := range pending {
			if _, err := conn.Write(data); err != nil {
				srvLog(LevelWarn, "handler", "[服务端] 写入缓存数据失败 %s: %v", st.target, err)
				p.unregisterConn(st.connID)
				return
			}
		}
	}

	// 启动目标→客户端转发
	go p.forwardTargetToClient(st)
}

// handleTCPData 处理 TCP 数据
func (p *serverPool) handleTCPData(chID int, connID string, payload []byte) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()

	if st == nil {
		return
	}

	st.mu.RLock()
	targetConn := st.targetConn
	uplinkChID := st.uplinkChID
	st.mu.RUnlock()

	if uplinkChID > 0 && chID != uplinkChID {
		// 调试日志:需要时可解除注释
		// log.Printf("[服务端] 警告: 收到来自通道 %d 的数据,但上行通道是 %d, ID:%s,忽略",
		// 	chID, uplinkChID, protocol.ShortID(connID))
		return
	}

	if targetConn == nil {
		// 连接还未建立,缓存数据
		st.mu.Lock()
		if st.targetConn == nil {
			// 检查缓存大小限制,防止恶意客户端耗尽内存
			var currentSize int
			for _, d := range st.pendingData {
				currentSize += len(d)
			}
			if currentSize+len(payload) > pendingDataMaxSize {
				st.mu.Unlock()
				srvLog(LevelWarn, "handler", "[服务端] pendingData 超出限制 %d bytes, 拒绝连接 ID:%s", pendingDataMaxSize, protocol.ShortID(connID))
				p.unregisterConn(connID)
				return
			}
			st.pendingData = append(st.pendingData, payload)
			st.mu.Unlock()
			return
		}
		targetConn = st.targetConn
		st.mu.Unlock()
	}

	_, err := targetConn.Write(payload)
	if err != nil {
		srvLog(LevelWarn, "handler", "[服务端] 写入目标失败 %s: %v", st.target, err)
		p.unregisterConn(connID)
	}
}

// handleSelectDownlink 处理选择下行通道
func (p *serverPool) handleSelectDownlink(clientID string, chID int, connID string, meta []byte) {
	// meta 包含客户端选择的下行通道号（4字节,大端序）
	var downlinkChID int
	if len(meta) >= 4 {
		downlinkChID = int(binary.BigEndian.Uint32(meta[0:4]))
	} else {
		// 兼容旧版本:使用当前发送消息的通道
		downlinkChID = chID
	}

	p.mu.RLock()
	st := p.conns[connID]
	// 下行通道从消息来源客户端自己的编号空间中查找
	wsConn := p.clientChConns[clientID][downlinkChID]
	p.mu.RUnlock()

	if st == nil {
		return
	}

	// 验证下行通道是否仍然活跃
	if wsConn == nil || wsConn.closed {
		srvLog(LevelWarn, "handler", "[服务端] 警告: 客户端尝试选择已关闭的通道 %d 作为下行通道, connID:%s", downlinkChID, protocol.ShortID(connID))
		return
	}

	// 验证消息是否从上行通道发送,且下行通道仍属于同一客户端
	st.mu.RLock()
	uplinkChID := st.uplinkChID
	ownerID := st.clientID
	st.mu.RUnlock()

	if uplinkChID > 0 && chID != uplinkChID {
		srvLog(LevelWarn, "handler", "[服务端] 警告: MsgSelectDownlink 来自通道 %d,但上行通道是 %d, ID:%s,忽略",
			chID, uplinkChID, protocol.ShortID(connID))
		return
	}
	if ownerID != "" && wsConn.clientID != "" && wsConn.clientID != ownerID {
		srvLog(LevelWarn, "handler", "[服务端] 警告: 客户端 %s 试图选择其他客户端 %s 的通道 %d 作为下行通道, connID:%s",
			ownerID, wsConn.clientID, downlinkChID, protocol.ShortID(connID))
		return
	}

	st.mu.Lock()
	if st.downlinkChID == 0 {
		st.downlinkChID = downlinkChID
	} else {
		// 已经选择过下行通道,但收到另一个选择请求
		srvLog(LevelWarn, "handler", "[服务端] 警告: %s 访问: %s, 当前下行通道 %d, 试图改为 %d, ID:%s,忽略",
			st.clientAddr, st.target, st.downlinkChID, downlinkChID, protocol.ShortID(connID))
	}
	st.mu.Unlock()
}

// handleTCPClose 处理 TCP 连接关闭
func (p *serverPool) handleTCPClose(chID int, connID string) {
	p.unregisterConn(connID)
}

// handlePrebindRequest 处理预绑定请求
func (p *serverPool) handlePrebindRequest(clientID string, chID int, connID string, meta []byte) {
	if len(meta) < 1 {
		return
	}

	ipStrategy := protocol.IPStrategy(meta[0])

	// 发布状态前先解析归属客户端，避免把字段写进已发布的状态（数据竞争）
	p.mu.RLock()
	wsConn := p.clientChConns[clientID][chID]
	p.mu.RUnlock()

	var inboundClientID, inboundAddr string
	if wsConn != nil {
		inboundClientID = wsConn.clientID
		inboundAddr = wsConn.remoteAddr
	}

	p.mu.Lock()
	if _, exists := p.conns[connID]; exists {
		p.mu.Unlock()
		return
	}

	st := &ServerConnState{
		connID:     connID,
		uplinkChID: chID,
		ipStrategy: ipStrategy,
		connected:  true,
		clientID:   inboundClientID,
		clientAddr: inboundAddr,
	}
	p.conns[connID] = st
	p.mu.Unlock()

	uplinkChIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(uplinkChIDBytes, uint32(chID))
	_ = p.sendDownlink(connID, protocol.MsgSelectUplink, uplinkChIDBytes, nil)

	// 预绑定只完成上行选择。状态保留一个短存活窗口，期间并行到达的重复预绑定帧
	// 会因上方 p.conns[connID] 已存在而直接忽略，避免每帧都重建状态并再次广播
	// MsgSelectUplink（原为立即注销，导致一次健康检查 ~N 条并行预绑定触发 ~N 轮
	// 广播风暴）。窗口结束后再清理状态，避免泄漏。
	connIDCopy := connID
	ttl := p.prebindTTL
	if ttl <= 0 {
		ttl = prebindStateTTL
	}
	time.AfterFunc(ttl, func() {
		p.unregisterConn(connIDCopy)
	})
}

// forwardTargetToClient 转发目标→客户端数据
func (p *serverPool) forwardTargetToClient(st *ServerConnState) {
	// 主动关闭时通知客户端发 MsgTCPClose，避免半开连接。
	// 利用 defer LIFO：先于 unregisterConn 执行，此时 st 仍在 p.conns 中可发送。
	// 若连接是因客户端先发 MsgTCPClose 而关闭，unregisterConn 已删除 st，此处 sendDownlink 会安全返回错误，不产生回声。
	defer p.sendDownlink(st.connID, protocol.MsgTCPClose, nil, nil)
	defer p.unregisterConn(st.connID)

	buf := make([]byte, 32*1024)
	for {
		n, err := st.targetConn.Read(buf)
		if err != nil {
			// 检查是否是连接被其他 goroutine 关闭（正常情况）
			if err != io.EOF && !protocol.IsNormalCloseError(err) {
				srvLog(LevelWarn, "handler", "[服务端] 读取目标错误 %s: %v", st.target, err)
			}
			return
		}

		if n > 0 {
			_ = p.sendDownlink(st.connID, protocol.MsgTCPData, nil, buf[:n])
		}
	}
}

// ======================== UDP 处理 ========================

// handleUDPConnect 处理 UDP 连接请求
func (p *serverPool) handleUDPConnect(clientID string, chID int, connID string, meta []byte) {
	if len(meta) < 1 {
		p.sendDownlink(connID, protocol.MsgConnStatus, []byte{byte(protocol.StatusERR)}, nil)
		return
	}

	ipStrategy := protocol.IPStrategy(meta[0])
	target := string(meta[1:])

	// 发布状态前先解析归属客户端，避免对已发布状态无锁写 clientID/clientAddr（数据竞争）
	p.mu.RLock()
	wsConn := p.clientChConns[clientID][chID]
	p.mu.RUnlock()
	var inboundClientID, inboundAddr string
	if wsConn != nil {
		inboundClientID = wsConn.clientID
		inboundAddr = wsConn.remoteAddr
	}

	p.mu.Lock()
	if _, exists := p.conns[connID]; exists {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	// 创建 UDP socket
	udpConn, err := net.ListenUDP("udp", nil)
	if err != nil {
		srvLog(LevelError, "handler", "[服务端] 创建 UDP 失败: %v", err)
		p.sendDownlink(connID, protocol.MsgConnStatus, []byte{byte(protocol.StatusERR)}, nil)
		return
	}

	// 创建连接状态
	st := &ServerConnState{
		connID:     connID,
		target:     target,
		targetUDP:  udpConn,
		uplinkChID: chID,
		ipStrategy: ipStrategy,
		isUDP:      true,
		connected:  true,
		clientID:   inboundClientID,
		clientAddr: inboundAddr,
	}

	p.mu.Lock()
	if _, exists := p.conns[connID]; exists {
		p.mu.Unlock()
		_ = udpConn.Close()
		return
	}
	p.conns[connID] = st
	p.mu.Unlock()

	// 发送 MsgSelectUplink（广播）,携带上行通道ID
	uplinkChIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(uplinkChIDBytes, uint32(chID))
	_ = p.sendDownlink(connID, protocol.MsgSelectUplink, uplinkChIDBytes, nil)

	srvLog(LevelInfo, "handler", "[服务端] %s UDP 访问: %s, 通道: TX %d, ID:%s", st.clientAddr, target, chID, protocol.ShortID(connID))

	// 启动 UDP 接收
	go p.forwardUDPToClient(st)
}

// handleUDPData 处理 UDP 数据
func (p *serverPool) handleUDPData(chID int, connID string, meta, payload []byte) {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()

	if st == nil || st.targetUDP == nil {
		return
	}

	// 只接受来自上行通道的数据,其余通道丢弃
	st.mu.RLock()
	uplinkChID := st.uplinkChID
	st.mu.RUnlock()

	if uplinkChID > 0 && chID != uplinkChID {
		// 调试日志:需要时可解除注释
		// log.Printf("[服务端] 警告: 收到来自通道 %d 的 UDP 数据,但上行通道是 %d, ID:%s,忽略",
		// 	chID, uplinkChID, protocol.ShortID(connID))
		return
	}

	// meta 是目标地址字符串
	targetAddr := string(meta)
	if targetAddr == "" {
		targetAddr = st.target
	}

	// IP 策略解析
	resolvedTarget := protocol.ResolveWithStrategy(targetAddr, st.ipStrategy)

	// 解析 UDP 地址
	udpAddr, err := net.ResolveUDPAddr("udp", resolvedTarget)
	if err != nil {
		srvLog(LevelWarn, "handler", "[服务端] 解析 UDP 地址失败 %s: %v", targetAddr, err)
		return
	}

	// 发送 UDP 包
	_, err = st.targetUDP.WriteToUDP(payload, udpAddr)
	if err != nil {
		srvLog(LevelWarn, "handler", "[服务端] 发送 UDP 失败: %v", err)
	}
}

// handleUDPClose 处理 UDP 连接关闭
func (p *serverPool) handleUDPClose(chID int, connID string) {
	p.unregisterConn(connID)
}

// forwardUDPToClient 转发 UDP→客户端数据
func (p *serverPool) forwardUDPToClient(st *ServerConnState) {
	buf := make([]byte, 64*1024)
	// natMap := make(map[string]string) // remoteAddr -> connID for responses

	defer p.unregisterConn(st.connID)

	for {
		st.mu.RLock()
		udpConn := st.targetUDP
		connected := st.connected
		st.mu.RUnlock()

		if !connected || udpConn == nil {
			return
		}

		udpConn.SetReadDeadline(time.Now().Add(p.udpReadTimeout()))
		n, addr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		if n > 0 {
			// 构造返回地址
			replyAddr := addr.String()
			// natMap[replyAddr] = st.connID

			_ = p.sendDownlink(st.connID, protocol.MsgUDPData, []byte(replyAddr), buf[:n])
		}
	}
}
