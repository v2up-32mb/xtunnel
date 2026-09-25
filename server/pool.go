package server

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

// maxAllowedChID 客户端可指定的通道 ID 上限。
// 防止恶意客户端传入巨大 ch_id 导致内部映射无限增长（OOM）。
const maxAllowedChID = 65535

// allocClientChIDLocked 在指定客户端的通道编号空间内分配最小未占用 chID。
// 返回 0 表示该客户端编号已用尽。调用方需持有 p.mu。
func (p *serverPool) allocClientChIDLocked(clientID string) int {
	for i := 1; i <= maxAllowedChID; i++ {
		if existing := p.clientChConns[clientID][i]; existing == nil || existing.closed {
			return i
		}
	}
	return 0
}

// serverPool 服务端连接池
type serverPool struct {
	config        *Config
	token         string
	mu            sync.RWMutex
	bytesSent     uint64
	bytesReceived uint64

	// 连接状态映射
	conns map[string]*ServerConnState

	// WebSocket 连接
	wsConns []*ServerWSConn

	// clientID -> chID -> wsConn
	// 每个客户端拥有独立的通道编号空间：不同客户端的 ch_id 可以相同，
	// 不会互相占用/拒绝（协议路由始终能通过 conn 状态或来源连接定位客户端）。
	clientChConns map[string]map[int]*ServerWSConn

	// 反向连接状态
	revMu    sync.RWMutex
	revConns map[string]*ServerReverseConn

	// 反向监听管理器
	reverseManager *ReverseListenerManager

	// 预热通道对表（被动）：健康维护由客户端负责，经 MsgHotPairNotify 通知存储
	hotPairs *HotPairTable

	// prebindTTL 预绑定状态短存活窗口（默认 prebindStateTTL，测试可覆盖）
	prebindTTL time.Duration

	// 背压控制
	globalQueueBytes     int64 // 全局队列字节数
	globalQueueLimit     int64 // 全局队列字节限制
	backpressureState    int32 // 当前背压状态 (atomic)
	backpressureCooldown int32 // 背压通知冷却 (atomic)
}

// newServerPool 创建新的服务端连接池
func newServerPool(token string, config *Config) *serverPool {
	limit := int64(config.BackpressureLimitBytes)
	if limit <= 0 {
		limit = 32 << 20 // 默认 32MB
	}
	p := &serverPool{
		config:            config,
		token:             token,
		conns:             make(map[string]*ServerConnState),
		wsConns:           make([]*ServerWSConn, 0),
		clientChConns:     make(map[string]map[int]*ServerWSConn),
		revConns:          make(map[string]*ServerReverseConn),
		prebindTTL:        prebindStateTTL,
		globalQueueLimit:  limit,
		backpressureState: int32(protocol.BackpressureNormal),
	}
	p.reverseManager = NewReverseListenerManager(config.MaxReverseListeners)
	p.reverseManager.pool = p
	// 预热通道对表：被动接收客户端 MsgHotPairNotify，正向提升与反向热拨号共用
	p.hotPairs = NewHotPairTable()
	return p
}

// initReverseManager 已废弃：Manager 在 newServerPool 中随池创建，避免懒初始化数据竞争

// checkOrigin 是 WebSocket Origin 校验扩展点。
// 当前默认保持兼容行为：允许所有来源。
func checkOrigin(r *http.Request) bool {
	return true
}

// newUpgrader 根据配置创建 WebSocket 升级器
func (p *serverPool) newUpgrader() *websocket.Upgrader {
	return &websocket.Upgrader{
		ReadBufferSize:  p.config.ReadBufferSize,
		WriteBufferSize: p.config.WriteBufferSize,
		CheckOrigin:     checkOrigin,
		Subprotocols:    []string{p.token},
	}
}

// handleWebSocket 处理 WebSocket 连接
func (p *serverPool) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 验证 Token
	requestedProtocols := websocket.Subprotocols(r)
	if len(requestedProtocols) == 0 || requestedProtocols[0] != p.token {
		srvLog(LevelWarn, "pool", "[服务端] 认证失败: 来自 %s", r.RemoteAddr)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// 解析 client_id
	queryParams, _ := url.ParseQuery(r.URL.RawQuery)
	clientID := queryParams.Get("client_id")
	if clientID == "" {
		clientID = uuid.New().String()
	}

	// 解析客户端期望的通道 ID
	var chID int
	if chIDStr := queryParams.Get("ch_id"); chIDStr != "" {
		_, err := fmt.Sscanf(chIDStr, "%d", &chID)
		if err != nil || chID <= 0 || chID > maxAllowedChID {
			srvLog(LevelWarn, "pool", "[服务端] 无效的 ch_id 参数: %s", chIDStr)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	} else {
		// 如果客户端没有提供 ch_id,服务端在该客户端的编号空间内自动分配
		p.mu.Lock()
		chID = p.allocClientChIDLocked(clientID)
		p.mu.Unlock()
		if chID == 0 {
			srvLog(LevelWarn, "pool", "[服务端] 拒绝客户端 %s: 该客户端通道编号已用尽", clientID)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
	}

	totalActive, clientActive := p.countActiveChannels(clientID)
	if p.config.MaxTotalChannels > 0 && totalActive >= p.config.MaxTotalChannels {
		srvLog(LevelWarn, "pool", "[服务端] 拒绝客户端 %s:总通道数已达上限 %d", clientID, p.config.MaxTotalChannels)
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	if p.config.MaxChannelsPerClient > 0 && clientActive >= p.config.MaxChannelsPerClient {
		srvLog(LevelWarn, "pool", "[服务端] 拒绝客户端 %s:客户端通道数已达上限 %d", clientID, p.config.MaxChannelsPerClient)
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}

	// 升级为 WebSocket
	upgrader := p.newUpgrader()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		srvLog(LevelWarn, "pool", "[服务端] WebSocket 升级失败: %v", err)
		return
	}

	wsConn := &ServerWSConn{
		ws:         ws,
		chID:       chID,
		clientID:   clientID,
		remoteAddr: ws.RemoteAddr().String(),
		pool:       p,
		writeChan:  make(chan writeTask, 4096),
	}

	// 存储 WebSocket 连接（再次校验上限，避免并发窗口超限）
	p.mu.Lock()
	totalActive, clientActive = p.countActiveChannelsLocked(clientID)
	if (p.config.MaxTotalChannels > 0 && totalActive >= p.config.MaxTotalChannels) ||
		(p.config.MaxChannelsPerClient > 0 && clientActive >= p.config.MaxChannelsPerClient) {
		p.mu.Unlock()
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "channel limit reached"), time.Now().Add(p.config.WriteTimeout))
		_ = ws.Close()
		srvLog(LevelWarn, "pool", "[服务端] 客户端 %s 在升级后命中通道上限，已关闭新通道", clientID)
		return
	}
	// 检查 ch_id 是否已被该客户端自身的活跃连接占用，防止通道劫持。
	// 不同客户端的 ch_id 相互独立，互不拒绝；仅同客户端内已占用且未断开时拒绝。
	// 仅当旧连接已断开（closed）时才允许新连接接管，兼容正常断线重连。
	if existing := p.clientChConns[clientID][chID]; existing != nil && !existing.closed {
		p.mu.Unlock()
		_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "channel id in use"), time.Now().Add(p.config.WriteTimeout))
		_ = ws.Close()
		srvLog(LevelWarn, "pool", "[服务端] 拒绝客户端 %s: ch_id %d 已被该客户端占用", clientID, chID)
		return
	}
	if p.clientChConns[clientID] == nil {
		p.clientChConns[clientID] = make(map[int]*ServerWSConn)
	}
	p.clientChConns[clientID][chID] = wsConn
	p.wsConns = append(p.wsConns, wsConn)
	p.mu.Unlock()

	srvLog(LevelInfo, "pool", "[服务端] 通道 %d 已连接, 客户端: %s", chID, clientID)

	// 启动写入协程
	wsConn.start()

	// 启动读取循环
	wsConn.readLoop()

	srvLog(LevelInfo, "pool", "[服务端] 通道 %d 已断开", chID)
}

func (p *serverPool) countActiveChannels(clientID string) (total int, clientTotal int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.countActiveChannelsLocked(clientID)
}

func (p *serverPool) countActiveChannelsLocked(clientID string) (total int, clientTotal int) {
	for _, wsConn := range p.wsConns {
		if wsConn == nil || wsConn.closed {
			continue
		}
		total++
		if wsConn.clientID == clientID {
			clientTotal++
		}
	}
	return
}

func (p *serverPool) countActiveChannelsForClient(clientID string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cnt := 0
	for _, wsConn := range p.wsConns {
		if wsConn != nil && !wsConn.closed && wsConn.clientID == clientID {
			cnt++
		}
	}
	return cnt
}

// channelAliveForClient 指定客户端的指定通道是否活跃（预热表项使用前校验）
func (p *serverPool) channelAliveForClient(clientID string, chID int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	wsConn := p.clientChConns[clientID][chID]
	return wsConn != nil && !wsConn.closed
}

func (p *serverPool) connectTimeout() time.Duration {
	if p == nil || p.config == nil || p.config.HandshakeTimeout <= 0 {
		return 10 * time.Second
	}
	return p.config.HandshakeTimeout
}

func (p *serverPool) udpReadTimeout() time.Duration {
	if p == nil || p.config == nil || p.config.ReadTimeout <= 0 {
		return 30 * time.Second
	}
	return p.config.ReadTimeout * 2
}

func (p *serverPool) addSentBytes(n int) {
	if n > 0 {
		atomic.AddUint64(&p.bytesSent, uint64(n))
	}
}

func (p *serverPool) addReceivedBytes(n int) {
	if n > 0 {
		atomic.AddUint64(&p.bytesReceived, uint64(n))
	}
}

// handleMessage 处理消息
// clientID 是消息来源 WebSocket 连接所属的客户端，用于在客户端各自的通道编号空间内路由。
func (p *serverPool) handleMessage(clientID string, chID int, rawLen int, msgType protocol.MessageType, connID string, meta, payload []byte) {
	p.addReceivedBytes(rawLen)
	// 反向连接拦截：优先处理反向连接表中的 connID（属于其他客户端的同名 connID 直接丢弃，不落入正向逻辑）
	if rc := p.getReverseConn(connID); rc != nil {
		if rc.ownerClientID != clientID {
			srvLog(LevelWarn, "pool", "[服务端] 警告: 客户端 %s 试图操作其他客户端的反向连接 %s, 忽略", protocol.ShortID(clientID), protocol.ShortID(connID))
			return
		}
		p.handleReverseMessage(clientID, chID, msgType, connID, meta, payload)
		return
	}
	// 预热通道对：收到客户端批量预热通知（健康维护由客户端负责）
	if p.hotPairs != nil && msgType == protocol.MsgHotPairNotify {
		p.hotPairs.HandleNotify(clientID, payload)
		return
	}
	switch msgType {
	case protocol.MsgReverseListen:
		p.reverseManager.HandleReverseListen(clientID, chID, connID, meta)
		return
	case protocol.MsgTCPConnect:
		p.handleTCPConnect(clientID, chID, connID, meta)

	case protocol.MsgTCPData:
		p.handleTCPData(chID, connID, payload)

	case protocol.MsgSelectDownlink:
		p.handleSelectDownlink(clientID, chID, connID, meta)

	case protocol.MsgTCPClose:
		p.handleTCPClose(chID, connID)

	case protocol.MsgPrebindRequest:
		p.handlePrebindRequest(clientID, chID, connID, meta)

	case protocol.MsgUDPConnect:
		p.handleUDPConnect(clientID, chID, connID, meta)

	case protocol.MsgUDPData:
		p.handleUDPData(chID, connID, meta, payload)

	case protocol.MsgUDPClose:
		p.handleUDPClose(chID, connID)
	}
}

// sendDownlink 发送下行数据
func (p *serverPool) sendDownlink(connID string, msgType protocol.MessageType, meta, payload []byte) error {
	p.mu.RLock()
	st := p.conns[connID]
	p.mu.RUnlock()

	if st == nil {
		return fmt.Errorf("连接不存在")
	}

	st.mu.RLock()
	downlink := st.downlinkChID
	clientID := st.clientID
	st.mu.RUnlock()

	if downlink > 0 {
		// 已选择下行通道:单播（按该连接所属客户端定位通道）
		return p.sendToChannel(clientID, downlink, websocket.BinaryMessage, protocol.EncodeMessage(msgType, connID, meta, payload))
	}

	// 未选择:只广播给同一客户端的活跃通道
	return p.broadcastWriteToClient(clientID, websocket.BinaryMessage, protocol.EncodeMessage(msgType, connID, meta, payload))
}

// broadcastWrite 广播写入所有通道
func (p *serverPool) broadcastWrite(msgType int, data []byte) error {
	return p.broadcastWriteToClient("", msgType, data)
}

// broadcastWriteToClient 广播写入指定客户端的活跃通道；clientID 为空时表示全量广播
func (p *serverPool) broadcastWriteToClient(clientID string, msgType int, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var activeConns []*ServerWSConn
	for _, wsConn := range p.wsConns {
		if wsConn == nil || wsConn.closed {
			continue
		}
		if clientID != "" && wsConn.clientID != clientID {
			continue
		}
		activeConns = append(activeConns, wsConn)
	}

	if len(activeConns) == 0 {
		return fmt.Errorf("无可用通道")
	}

	// 发送到所有活跃通道,忽略写队列满错误
	for _, wsConn := range activeConns {
		_ = wsConn.asyncWrite(msgType, data)
	}
	return nil
}

// sendToChannel 发送到指定客户端的指定通道
func (p *serverPool) sendToChannel(clientID string, chID int, msgType int, data []byte) error {
	p.mu.RLock()
	wsConn := p.clientChConns[clientID][chID]
	p.mu.RUnlock()

	if wsConn == nil || wsConn.closed {
		return fmt.Errorf("客户端 %s 通道 %d 不可用", protocol.ShortID(clientID), chID)
	}

	_ = wsConn.asyncWrite(msgType, data)
	return nil
}

// cleanupChannel 清理指定客户端的通道
func (p *serverPool) cleanupChannel(clientID string, chID int) {
	var clientGone bool
	p.mu.Lock()

	var wsConn *ServerWSConn
	if m := p.clientChConns[clientID]; m != nil {
		wsConn = m[chID]
		delete(m, chID)
		if len(m) == 0 {
			delete(p.clientChConns, clientID)
			clientGone = true
		}
	}
	for i, wc := range p.wsConns {
		if wc == wsConn && wc != nil {
			p.wsConns[i] = nil
			break
		}
	}

	// 清理使用此通道的连接（只清理属于同一客户端的连接，
	// 避免多客户端使用相同 ch_id 时误关其他客户端的连接）
	var toClose []string
	for connID, st := range p.conns {
		st.mu.RLock()
		useCh := st.clientID == clientID && (st.uplinkChID == chID || st.downlinkChID == chID)
		st.mu.RUnlock()

		if useCh {
			toClose = append(toClose, connID)
		}
	}
	p.mu.Unlock()

	for _, connID := range toClose {
		p.unregisterConn(connID)
	}

	// 清理反向连接（移出锁外调用 removeReverseConn，内部含 settle 幂等与管道关闭）
	p.revMu.RLock()
	var revToClose []string
	for connID, rc := range p.revConns {
		if rc.ownerClientID == clientID && (atomic.LoadInt32(&rc.sendCh) == int32(chID) || atomic.LoadInt32(&rc.recvCh) == int32(chID)) {
			revToClose = append(revToClose, connID)
		}
	}
	p.revMu.RUnlock()
	for _, connID := range revToClose {
		p.removeReverseConn(connID)
	}

	if p.hotPairs != nil {
		if clientGone {
			p.hotPairs.InvalidateClient(clientID)
		} else {
			p.hotPairs.InvalidateChannel(chID)
		}
	}
	if clientGone && p.reverseManager != nil {
		p.reverseManager.ShutdownClient(clientID)
	}

	if wsConn != nil {
		wsConn.mu.Lock()
		if !wsConn.closed {
			wsConn.closed = true
			writeChan := wsConn.writeChan
			wsConn.writeChan = nil
			ws := wsConn.ws
			wsConn.mu.Unlock()
			if writeChan != nil {
				close(writeChan)
			}
			if ws != nil {
				_ = ws.Close()
			}
			return
		}
		wsConn.mu.Unlock()
	}
}

// unregisterConn 注销连接
func (p *serverPool) unregisterConn(connID string) {
	p.mu.Lock()
	st := p.conns[connID]
	delete(p.conns, connID)
	p.mu.Unlock()

	if st == nil {
		return
	}

	st.mu.Lock()
	// 防止重复注销
	if st.closed {
		st.mu.Unlock()
		return
	}
	st.closed = true

	if !st.connected {
		st.mu.Unlock()
		return
	}
	st.connected = false

	target := st.target
	up := st.uplinkChID
	down := st.downlinkChID
	clientAddr := st.clientAddr

	if st.targetConn != nil {
		st.targetConn.Close()
	}
	if st.targetUDP != nil {
		st.targetUDP.Close()
	}
	st.mu.Unlock()

	u := "-"
	d := "-"
	if up > 0 {
		u = fmt.Sprintf("%d", up)
	}
	if down > 0 {
		d = fmt.Sprintf("%d", down)
	}

	srvLog(LevelInfo, "pool", "[服务端] %s 访问: %s, 通道: TX %s RX %s, ID:%s, 已关闭",
		clientAddr, target, u, d, protocol.ShortID(connID))
}

// Shutdown 主动关闭所有活跃 WebSocket 通道，向客户端发送 Close Frame。
// 用于服务端优雅关闭时让客户端及时感知断开，避免半开连接。
func (p *serverPool) Shutdown() {
	// 先关闭反向监听器，释放端口并失败化等待中的反向拨号
	if p.reverseManager != nil {
		p.reverseManager.Shutdown()
	}
	p.mu.RLock()
	conns := make([]*ServerWSConn, 0, len(p.wsConns))
	for _, wsConn := range p.wsConns {
		if wsConn != nil {
			conns = append(conns, wsConn)
		}
	}
	p.mu.RUnlock()
	for _, wsConn := range conns {
		wsConn.close()
	}
}

// Stats 返回统计信息
func (p *serverPool) Stats() *ServerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	activeConns := 0
	for _, wsConn := range p.wsConns {
		if wsConn != nil && !wsConn.closed {
			activeConns++
		}
	}

	return &ServerStats{
		ActiveConnections: activeConns,
		ActiveChannels:    activeConns,
		TotalConnections:  int64(len(p.conns)),
		BytesSent:         atomic.LoadUint64(&p.bytesSent),
		BytesReceived:     atomic.LoadUint64(&p.bytesReceived),
	}
}

// addQueueBytes 增加全局队列字节数
func (p *serverPool) addQueueBytes(size int) int64 {
	return atomic.AddInt64(&p.globalQueueBytes, int64(size))
}

// rollbackQueueBytes 回退尚未成功入队的字节数
func (p *serverPool) rollbackQueueBytes(size int) int64 {
	newSize := atomic.AddInt64(&p.globalQueueBytes, -int64(size))
	if newSize < 0 {
		atomic.StoreInt64(&p.globalQueueBytes, 0)
		return 0
	}
	return newSize
}

// updateBackpressureState 根据当前队列水位更新背压状态
func (p *serverPool) updateBackpressureState(newSize int64) {
	limit := p.globalQueueLimit
	if limit <= 0 {
		return
	}

	if newSize > limit*95/100 {
		if protocol.BackpressureState(atomic.LoadInt32(&p.backpressureState)) != protocol.BackpressurePause {
			atomic.StoreInt32(&p.backpressureState, int32(protocol.BackpressurePause))
			p.broadcastBackpressure(protocol.BackpressurePause)
			srvLog(LevelWarn, "pool", "[服务端] 背压通知: 暂停 (队列: %d/%d bytes, %.1f%%)", newSize, limit, float64(newSize)*100/float64(limit))
		}
		return
	}

	if newSize > limit*8/10 {
		currentState := protocol.BackpressureState(atomic.LoadInt32(&p.backpressureState))
		if currentState == protocol.BackpressureNormal {
			if atomic.CompareAndSwapInt32(&p.backpressureCooldown, 0, 1) {
				atomic.StoreInt32(&p.backpressureState, int32(protocol.BackpressureSlowDown))
				p.broadcastBackpressure(protocol.BackpressureSlowDown)
				srvLog(LevelWarn, "pool", "[服务端] 背压通知: 减速 (队列: %d/%d bytes, %.1f%%)", newSize, limit, float64(newSize)*100/float64(limit))
				go func() {
					time.Sleep(1 * time.Second)
					atomic.StoreInt32(&p.backpressureCooldown, 0)
				}()
			}
		}
	}
}

// removeQueueBytes 减少全局队列字节数并检查恢复
func (p *serverPool) removeQueueBytes(size int) {
	newSize := atomic.AddInt64(&p.globalQueueBytes, -int64(size))
	if newSize < 0 {
		atomic.StoreInt64(&p.globalQueueBytes, 0)
		newSize = 0
	}
	limit := p.globalQueueLimit

	if limit <= 0 {
		return
	}

	currentState := protocol.BackpressureState(atomic.LoadInt32(&p.backpressureState))

	// 分级恢复机制：
	// 暂停(95%) -> 减速(90%) -> 正常(70%)
	// 特殊情况：队列快速降到30%以下，直接恢复正常
	// 恢复阈值略低于触发阈值，避免频繁切换状态

	// 直接从暂停恢复到正常（极低水位：30%，用于队列快速清空的情况）
	if currentState == protocol.BackpressurePause && newSize < limit*3/10 {
		atomic.StoreInt32(&p.backpressureState, int32(protocol.BackpressureNormal))
		p.broadcastBackpressure(protocol.BackpressureNormal)
		srvLog(LevelInfo, "pool", "[服务端] 背压通知: 直接恢复正常 (队列: %d/%d bytes, %.1f%%)", newSize, limit, float64(newSize)*100/float64(limit))
		return
	}

	// 从暂停恢复到减速（90%，略低于暂停触发阈值95%）
	if currentState == protocol.BackpressurePause && newSize < limit*9/10 {
		atomic.StoreInt32(&p.backpressureState, int32(protocol.BackpressureSlowDown))
		p.broadcastBackpressure(protocol.BackpressureSlowDown)
		srvLog(LevelInfo, "pool", "[服务端] 背压通知: 从暂停恢复到减速 (队列: %d/%d bytes, %.1f%%)", newSize, limit, float64(newSize)*100/float64(limit))
		return
	}

	// 从减速恢复到正常（70%，略低于减速触发阈值80%）
	if currentState == protocol.BackpressureSlowDown && newSize < limit*7/10 {
		atomic.StoreInt32(&p.backpressureState, int32(protocol.BackpressureNormal))
		p.broadcastBackpressure(protocol.BackpressureNormal)
		srvLog(LevelInfo, "pool", "[服务端] 背压通知: 恢复正常 (队列: %d/%d bytes, %.1f%%)", newSize, limit, float64(newSize)*100/float64(limit))
		return
	}
}

// broadcastBackpressure 广播背压状态到所有通道
func (p *serverPool) broadcastBackpressure(state protocol.BackpressureState) {
	meta := []byte{byte(state)}
	msg := protocol.EncodeMessage(protocol.MsgBackpressure, "", meta, nil)
	_ = p.broadcastWrite(websocket.BinaryMessage, msg)
}
