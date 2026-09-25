package xtunnel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

const (
	PairStateReady = iota
	PairStateDraining
	PairStateClosed
)

// HotChannelPair 表示一个热通道对
// 通道语义与 protocol.HotPairInfo 对齐：
// UplinkChID = ChA（client→server 方向，客户端发包）；
// DownlinkChID = ChB（server→client 方向，客户端收包）。
type HotChannelPair struct {
	ID           string // 槽位 ID（两位十进制，仅日志展示用）
	PrebindID    string // 预热 Pair 键（预热期双方已知，用于通知与拨号 connID 前缀）
	UplinkChID   int
	DownlinkChID int
	state        int32
	createdAt    time.Time
	refs         int32
}

// State 返回当前状态
func (p *HotChannelPair) State() int { return int(atomic.LoadInt32(&p.state)) }

func (p *HotChannelPair) setState(s int) { atomic.StoreInt32(&p.state, int32(s)) }

// SetStateForTest 仅用于测试设置状态
func (p *HotChannelPair) SetStateForTest(s int) { p.setState(s) }

// PairWarmerConfig PairWarmer 配置
type PairWarmerConfig struct {
	PairCount       int
	RefreshInterval time.Duration
	PrebindTimeout  time.Duration
}

// PairWarmer 热通道对管理器
type PairWarmer struct {
	pool    *clientPool
	mu      sync.RWMutex
	pairs   []*HotChannelPair
	primary *HotChannelPair
	config  PairWarmerConfig
	ctx     context.Context
	cancel  context.CancelFunc

	prebindResultCh chan prebindResult

	// 预热通道对通知（client→server 批量帧）：
	// 健康维护由客户端负责，预热完成后通知服务端建双端热表；
	// 新 Pair 500ms 防抖合批，周期刷新时对存量 Ready Pair 心跳重发刷新服务端表项时间戳
	notifyMu      sync.Mutex
	notifyPending map[string]protocol.HotPairInfo // key → 待通知记录（按键去重）
	notifyTimer   *time.Timer
}

// prebindResult 预绑定结果
type prebindResult struct {
	connID       string
	uplinkChID   int
	downlinkChID int
	err          error
}

// NewPairWarmer 创建新的 PairWarmer
func NewPairWarmer(pool *clientPool, cfg *Config) *PairWarmer {
	ctx, cancel := context.WithCancel(pool.ctx)
	return &PairWarmer{
		pool: pool,
		config: PairWarmerConfig{
			PairCount:       cfg.HotPairCount,
			RefreshInterval: cfg.HotPairRefreshInterval,
			PrebindTimeout:  3 * time.Second,
		},
		ctx:             ctx,
		cancel:          cancel,
		prebindResultCh: make(chan prebindResult, 8),
		notifyPending:   make(map[string]protocol.HotPairInfo),
	}
}

// AcquirePrimary 获取一个 Ready 状态的 Pair 并增加引用计数。
// 优先返回当前 primary；若 primary 不可用，则扫描 pairs 列表。
// 引用计数自增在读锁内完成，确保 InvalidateChannel（需要写锁）无法在自增与状态检查之间移除 Pair。
func (w *PairWarmer) AcquirePrimary() *HotChannelPair {
	w.mu.RLock()
	defer w.mu.RUnlock()

	tryAcquire := func(pair *HotChannelPair) bool {
		if pair == nil || pair.State() != PairStateReady {
			return false
		}
		atomic.AddInt32(&pair.refs, 1)
		// 二次检查：自增后状态可能已被置为 Draining/Closed，此时放弃
		if pair.State() == PairStateReady {
			return true
		}
		atomic.AddInt32(&pair.refs, -1)
		return false
	}

	if tryAcquire(w.primary) {
		return w.primary
	}
	for _, pair := range w.pairs {
		if pair == w.primary {
			continue
		}
		if tryAcquire(pair) {
			return pair
		}
	}
	return nil
}

// ReleasePair 减少 Pair 引用计数。
// Pair 本身保持 Ready 供后续请求复用；只有处于 Draining 状态且 refs 归零时才会移除。
func (w *PairWarmer) ReleasePair(pair *HotChannelPair) {
	if pair == nil {
		return
	}

	refs := atomic.AddInt32(&pair.refs, -1)
	if refs <= 0 && pair.State() == PairStateDraining && pair.State() != PairStateClosed {
		pair.setState(PairStateClosed)
		w.mu.Lock()
		w.removePair(pair)
		w.ensurePrimaryLocked()
		w.mu.Unlock()
	}
}

// removePair 从池中移除 Pair（调用者需持有 mu.Lock）
func (w *PairWarmer) removePair(pair *HotChannelPair) {
	removedID := pair.ID
	for i := 0; i < len(w.pairs); i++ {
		if w.pairs[i] == pair {
			w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
			i--
		}
	}
	if w.primary == pair {
		w.primary = nil
	}
	coreLog(LevelInfo, "pair_warmer", "[PairWarmer] 移除 Pair %s (状态=%s, refs=%d)", removedID, pairStateString(pair.State()), atomic.LoadInt32(&pair.refs))
}

// InvalidateChannel 废弃包含指定通道的所有 Pair。
// 将 Pair 标记为 Draining；若当前无请求使用（refs<=0）则立即移除，否则等待使用方释放。
func (w *PairWarmer) InvalidateChannel(chID int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := 0; i < len(w.pairs); i++ {
		pair := w.pairs[i]
		if pair.State() == PairStateClosed {
			continue
		}
		if pair.UplinkChID == chID || pair.DownlinkChID == chID {
			if pair.State() != PairStateDraining {
				pair.setState(PairStateDraining)
				coreLog(LevelWarn, "pair_warmer", "[PairWarmer] Pair %s 因通道 %d 失效进入 Draining (refs=%d)", pair.ID, chID, atomic.LoadInt32(&pair.refs))
			}
			if atomic.LoadInt32(&pair.refs) <= 0 {
				pair.setState(PairStateClosed)
				coreLog(LevelInfo, "pair_warmer", "[PairWarmer] Pair %s 立即移除 (无活跃引用)", pair.ID)
				w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
				i--
				if w.primary == pair {
					w.primary = nil
				}
			}
		}
	}
	w.ensurePrimaryLocked()
}

// ensurePrimaryLocked 在 primary 为 nil 或不可用时，从 Ready 的 Pair 中选举新的 primary。
// 调用者需持有 mu.Lock。
func (w *PairWarmer) ensurePrimaryLocked() {
	if w.primary != nil && w.primary.State() == PairStateReady {
		return
	}
	oldPrimary := ""
	if w.primary != nil {
		oldPrimary = w.primary.ID
	}
	for _, pair := range w.pairs {
		if pair.State() == PairStateReady {
			w.primary = pair
			if oldPrimary != "" {
				coreLog(LevelWarn, "pair_warmer", "[PairWarmer] primary 重新选举: %s -> %s", oldPrimary, pair.ID)
			}
			return
		}
	}
	w.primary = nil
}

// SetPrimaryForTest 仅用于测试设置主 Pair
func (w *PairWarmer) SetPrimaryForTest(pair *HotChannelPair) {
	w.mu.Lock()
	w.primary = pair
	w.mu.Unlock()
}

// PairCountForTest 仅用于测试返回 Pair 数量
func (w *PairWarmer) PairCountForTest() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.pairs)
}

// pairStateString 返回 Pair 状态的可读字符串
func pairStateString(state int) string {
	switch state {
	case PairStateReady:
		return "Ready"
	case PairStateDraining:
		return "Draining"
	case PairStateClosed:
		return "Closed"
	default:
		return fmt.Sprintf("Unknown(%d)", state)
	}
}

// deletePrebindState 删除预绑定临时状态（不调用 Unregister，避免重复加锁和额外日志）
func (w *PairWarmer) deletePrebindState(connID string) {
	w.pool.mu.Lock()
	st := w.pool.conns[connID]
	if st != nil {
		st.closed = true
		delete(w.pool.conns, connID)
	}
	w.pool.mu.Unlock()
}

// BuildPair 使用可用通道列表构建一个 Hot Pair，同步等待预绑定结果
func (w *PairWarmer) BuildPair(available []int) (*HotChannelPair, error) {
	if w.ctx.Err() != nil {
		return nil, fmt.Errorf("PairWarmer 已关闭")
	}
	if len(available) < 2 {
		return nil, fmt.Errorf("可用通道不足，需要至少 2 个通道")
	}

	connID := "prebind-" + uuid.New().String()

	// 在客户端连接池中注册临时状态，使 handleChannel 的 selectDownlink 能正常竞争下行通道。
	w.pool.mu.Lock()
	w.pool.conns[connID] = &clientConnState{
		id:        connID,
		target:    protocol.PrebindTarget,
		start:     time.Now(),
		connected: make(chan bool, 1),
		closed:    false,
	}
	w.pool.mu.Unlock()

	meta := make([]byte, 1+len(protocol.PrebindTarget))
	meta[0] = byte(w.pool.config.IPStrategy)
	copy(meta[1:], protocol.PrebindTarget)

	msg := protocol.EncodeMessage(protocol.MsgPrebindRequest, connID, meta, nil)

	// B 方案：广播前先发显式 MsgHotPairBegin，服务端 armed（窗口从 Begin 起算，
	// 不依赖帧到达后的 5s）；旧服务端忽略该消息，兼容。
	beginMsg := protocol.EncodeMessage(protocol.MsgHotPairBegin, "", nil, nil)
	for _, chID := range available {
		if err := w.pool.asyncWriteDirect(chID, websocket.BinaryMessage, beginMsg); err != nil {
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 预绑定 Begin 发送到通道 %d 失败: %v", chID, err)
		}
	}

	// 广播到可用通道
	sent := 0
	for _, chID := range available {
		if err := w.pool.asyncWriteDirect(chID, websocket.BinaryMessage, msg); err != nil {
			// 记录日志但继续其他通道
			coreLog(LevelWarn, "pair_warmer", "[PairWarmer] 预绑定请求发送到通道 %d 失败: %v", chID, err)
		} else {
			sent++
		}
	}
	if sent == 0 {
		w.deletePrebindState(connID)
		return nil, fmt.Errorf("无法发送预绑定请求到任何可用通道")
	}
	coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 预绑定竞速广播: 可用通道 %d 个, 成功入队 %d 个, 失败 %d 个", len(available), sent, len(available)-sent)

	// 等待预绑定结果
	timer := time.NewTimer(w.config.PrebindTimeout)
	defer timer.Stop()

	for {
		select {
		case <-w.ctx.Done():
			w.deletePrebindState(connID)
			return nil, fmt.Errorf("PairWarmer 已关闭")
		case <-timer.C:
			w.deletePrebindState(connID)
			return nil, fmt.Errorf("预绑定超时")
		case res := <-w.prebindResultCh:
			if res.connID == connID {
				if res.err != nil {
					w.deletePrebindState(connID)
					return nil, res.err
				}
				if w.ctx.Err() != nil {
					w.deletePrebindState(connID)
					return nil, fmt.Errorf("PairWarmer 已关闭")
				}
				pair := &HotChannelPair{
					ID:           "",
					PrebindID:    connID,
					UplinkChID:   res.uplinkChID,
					DownlinkChID: res.downlinkChID,
					state:        int32(PairStateReady),
					createdAt:    time.Now(),
				}
				w.mu.Lock()
				w.pairs = append(w.pairs, pair)
				wasPrimary := w.primary
				if w.primary == nil || w.primary.State() != PairStateReady {
					w.primary = pair
				}
				w.mu.Unlock()
				// 预热完成后把通道对通知给服务端（双端热表），再清理临时连接状态
				w.queueHotPairNotify(pair)
				w.deletePrebindState(connID)
				if wasPrimary == nil {
					coreLog(LevelInfo, "pair_warmer", "[PairWarmer] 首次构建 Pair (上行: %d, 下行: %d)，设为 primary（ID 待分配）", pair.UplinkChID, pair.DownlinkChID)
				} else {
					primaryLabel := wasPrimary.ID
					if primaryLabel == "" {
						primaryLabel = "未分配"
					}
					coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 构建候选 Pair (上行: %d, 下行: %d)，当前 primary=%s", pair.UplinkChID, pair.DownlinkChID, primaryLabel)
				}
				return pair, nil
			}
			// 不匹配的 connID 忽略，继续等待
		}
	}
}

// HandlePrebindResult 由 clientPool.handleChannel 在收到 MsgSelectUplink 时调用
func (w *PairWarmer) HandlePrebindResult(connID string, uplinkChID, downlinkChID int, err error) {
	res := prebindResult{
		connID:       connID,
		uplinkChID:   uplinkChID,
		downlinkChID: downlinkChID,
		err:          err,
	}
	select {
	case w.prebindResultCh <- res:
	default:
	}
}

// pairChannelsEqual 判断两个 Hot Pair 的通道是否完全一致
func pairChannelsEqual(a, b *HotChannelPair) bool {
	if a == nil || b == nil {
		return false
	}
	return a.UplinkChID == b.UplinkChID && a.DownlinkChID == b.DownlinkChID
}

// assignPairSlot 为新建 Pair 分配 1..8 中未占用的最小槽位 ID（两位十进制）。
// 同一组 hot-pair 的 ID 稳定复用（如 01、02），不随构建次数递增；
// 替换场景由调用方直接继承旧 Pair 的 ID。
func (w *PairWarmer) assignPairSlot(pair *HotChannelPair) {
	w.mu.Lock()
	defer w.mu.Unlock()
	used := make(map[string]bool, len(w.pairs))
	for _, p := range w.pairs {
		if p != pair && p.ID != "" {
			used[p.ID] = true
		}
	}
	for i := 1; i <= 8; i++ {
		id := fmt.Sprintf("%02d", i)
		if !used[id] {
			pair.ID = id
			return
		}
	}
	pair.ID = "ff" // 理论不可达：Pair 上限 8
}

// discardCandidatePair 移除重建时通道与旧 Pair 一致的冗余候选，保留旧 Pair 继续服务。
// 若候选已被会话引用，则标记 Draining，等引用归零后由 ReleasePair 移除。
func (w *PairWarmer) discardCandidatePair(pair *HotChannelPair) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if atomic.LoadInt32(&pair.refs) <= 0 {
		pair.setState(PairStateClosed)
		w.removePair(pair)
		w.ensurePrimaryLocked()
		return
	}
	pair.setState(PairStateDraining)
}

// invalidatePair 将指定 Pair 标记为 Draining；若当前无会话引用（refs==0）则立即移除并重新选举 primary，
// 避免周期刷新替换路径积累大量无人使用的 Draining Pair。
func (w *PairWarmer) invalidatePair(pair *HotChannelPair) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if pair == nil || pair.State() != PairStateReady {
		return
	}
	pair.setState(PairStateDraining)
	if atomic.LoadInt32(&pair.refs) <= 0 {
		pair.setState(PairStateClosed)
		w.removePair(pair)
		w.ensurePrimaryLocked()
	}
}

// pruneIdleDrainingPairs 清理 refs 为 0 的 Draining Pair（修复历史版本积累的存量）。
func (w *PairWarmer) pruneIdleDrainingPairs() {
	w.mu.Lock()
	defer w.mu.Unlock()
	pruned := false
	for _, pair := range w.pairs {
		if pair.State() == PairStateDraining && atomic.LoadInt32(&pair.refs) <= 0 {
			pair.setState(PairStateClosed)
			w.removePair(pair)
			pruned = true
		}
	}
	if pruned {
		w.ensurePrimaryLocked()
	}
}

// validatePrimaryChannels 验证 primary 的通道是否仍可用；失效时清理并触发重建。
// 返回 true 表示通道全部有效。mode 用于日志显示（单 Pair / 多 Pair）。
func (w *PairWarmer) validatePrimaryChannels(primary *HotChannelPair, mode string) bool {
	if primary == nil || primary.State() != PairStateReady {
		return true
	}
	available := w.pool.availableChannels()
	uplinkValid := false
	downlinkValid := false
	for _, chID := range available {
		if chID == primary.UplinkChID {
			uplinkValid = true
		}
		if chID == primary.DownlinkChID {
			downlinkValid = true
		}
	}
	if !uplinkValid || !downlinkValid {
		coreLog(LevelWarn, "pair_warmer", "[PairWarmer] %s模式下 primary %s 的通道已失效 (上行:%d 有效:%v, 下行:%d 有效:%v)，触发重建",
			mode, primary.ID, primary.UplinkChID, uplinkValid, primary.DownlinkChID, downlinkValid)
		if !uplinkValid {
			w.InvalidateChannel(primary.UplinkChID)
		}
		if !downlinkValid {
			w.InvalidateChannel(primary.DownlinkChID)
		}
		return false
	}
	return true
}

// Run 启动 PairWarmer 主循环，监听通道就绪/失效通知并构建/刷新 Pair
func (w *PairWarmer) Run() {
	coreLog(LevelInfo, "pair_warmer", "[PairWarmer] 启动运行循环")
	defer coreLog(LevelInfo, "pair_warmer", "[PairWarmer] 运行循环已退出")

	var refreshTicker *time.Ticker
	if w.config.RefreshInterval > 0 {
		refreshTicker = time.NewTicker(w.config.RefreshInterval)
		defer refreshTicker.Stop()
	}

	for {
		select {
		case <-w.ctx.Done():
			return
		case chID := <-w.pool.chReadyCh:
			w.tryBuildPairs()
			_ = chID // 日志中可记录，但当前版本不依赖具体 chID
		case chID := <-w.pool.chInvalidCh:
			w.tryRefresh()
			_ = chID
		case <-func() <-chan time.Time {
			if refreshTicker == nil {
				return nil
			}
			return refreshTicker.C
		}():
			w.periodicRefresh()
			// 心跳重发存量 Ready Pair 通知，刷新服务端表项时间戳
			w.heartbeatHotPairNotify()
		}
	}
}

// tryBuildPairs 尝试构建 Hot Pair，直到 Ready 的 Pair 数量达到 PairCount
func (w *PairWarmer) tryBuildPairs() {
	readyCount := 0
	w.mu.RLock()
	for _, pair := range w.pairs {
		if pair.State() == PairStateReady {
			readyCount++
		}
	}
	w.mu.RUnlock()

	if readyCount >= w.config.PairCount {
		return
	}

	available := w.pool.availableChannels()
	if len(available) < 2 {
		coreLog(LevelWarn, "pair_warmer", "[PairWarmer] 可用通道不足 (%d)，无法构建 Pair", len(available))
		return
	}

	for readyCount < w.config.PairCount {
		coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 尝试构建 Pair (%d/%d)，可用通道: %v", readyCount+1, w.config.PairCount, available)
		pair, err := w.BuildPair(available)
		if err != nil {
			coreLog(LevelWarn, "pair_warmer", "[PairWarmer] 构建 Pair 失败: %v", err)
			return
		}
		w.assignPairSlot(pair)
		coreLog(LevelInfo, "pair_warmer", "[PairWarmer] 成功构建 Pair %s (上行: %d, 下行: %d)", pair.ID, pair.UplinkChID, pair.DownlinkChID)
		readyCount++
	}
}

// tryRefresh 尝试刷新 Pair，当 primary 不可用时触发重建
func (w *PairWarmer) tryRefresh() {
	w.mu.Lock()
	if w.primary == nil || w.primary.State() != PairStateReady {
		w.ensurePrimaryLocked()
	}
	w.mu.Unlock()

	// 验证 primary 的通道是否真实可用
	w.mu.RLock()
	primary := w.primary
	w.mu.RUnlock()

	if primary != nil && primary.State() == PairStateReady {
		available := w.pool.availableChannels()
		uplinkValid := false
		downlinkValid := false
		for _, chID := range available {
			if chID == primary.UplinkChID {
				uplinkValid = true
			}
			if chID == primary.DownlinkChID {
				downlinkValid = true
			}
		}
		if !uplinkValid || !downlinkValid {
			coreLog(LevelWarn, "pair_warmer", "[PairWarmer] primary %s 的通道已失效 (上行:%d 有效:%v, 下行:%d 有效:%v)，标记为 Draining",
				primary.ID, primary.UplinkChID, uplinkValid, primary.DownlinkChID, downlinkValid)
			if !uplinkValid {
				w.InvalidateChannel(primary.UplinkChID)
			}
			if !downlinkValid {
				w.InvalidateChannel(primary.DownlinkChID)
			}
		}
	}

	w.tryBuildPairs()
}

// findReadyMatchLocked 在队列中查找与候选通道一致的 Ready Pair（调用方需持有 mu 读锁）。
// 返回（队首匹配, 备用匹配）：与队首一致时返回队首；否则返回首个通道一致的备用。
// 候选自身与 Draining/Closed Pair 均不参与比对（命中非 Ready 视为不匹配，走"全都不匹配"分支）。
func (w *PairWarmer) findReadyMatchLocked(candidate *HotChannelPair) (*HotChannelPair, *HotChannelPair) {
	if candidate == nil {
		return nil, nil
	}
	var spareMatch *HotChannelPair
	for _, pair := range w.pairs {
		if pair == candidate || pair.State() != PairStateReady {
			continue
		}
		if !pairChannelsEqual(candidate, pair) {
			continue
		}
		if pair == w.primary {
			return pair, nil
		}
		if spareMatch == nil {
			spareMatch = pair
		}
	}
	return nil, spareMatch
}

// promoteToHead 将队列中已有的 pair 提升为队首（位置 0）并设为 primary。
// 用于"最优解已在队列备用位"时直接上位，避免新建重复 pair。
func (w *PairWarmer) promoteToHead(pair *HotChannelPair) {
	if pair == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := -1
	for i, p := range w.pairs {
		if p == pair {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	if idx > 0 {
		w.pairs = append(w.pairs[:idx], w.pairs[idx+1:]...)
		w.pairs = append([]*HotChannelPair{pair}, w.pairs...)
	}
	w.primary = pair
}

// replaceHeadWithCandidate 候选顶替队首：候选入队首（位置 0）并设为 primary；
// 老队首健康则降级为第二位备用（位置 1），不销毁。
func (w *PairWarmer) replaceHeadWithCandidate(candidate, oldHead *HotChannelPair) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if candidate == nil {
		return
	}
	remove := func(p *HotChannelPair) bool {
		for i, q := range w.pairs {
			if q == p {
				w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
				return true
			}
		}
		return false
	}
	if !remove(candidate) {
		// 候选已被并发失效移除（通道断开），放弃顶替，避免二次插回
		return
	}
	var demote *HotChannelPair
	if oldHead != nil && oldHead != candidate && oldHead.State() == PairStateReady {
		demote = oldHead
		remove(oldHead)
	}
	w.pairs = append([]*HotChannelPair{candidate}, w.pairs...)
	w.primary = candidate
	if demote != nil {
		w.pairs = append(w.pairs, nil)
		copy(w.pairs[2:], w.pairs[1:])
		w.pairs[1] = demote
	}
}

// deduplicatePairs 队列去重：相同通道的 Ready Pair 只保留靠前的，剔除位于队列后边的重复项。
// 重复项若在服务（refs>0，多为历史残留）则先标 Draining，引用归零后由 ReleasePair 移除。
func (w *PairWarmer) deduplicatePairs() {
	w.mu.Lock()
	defer w.mu.Unlock()
	seen := make(map[string]*HotChannelPair, len(w.pairs))
	for i := 0; i < len(w.pairs); i++ {
		pair := w.pairs[i]
		if pair.State() != PairStateReady {
			continue
		}
		key := fmt.Sprintf("%d/%d", pair.UplinkChID, pair.DownlinkChID)
		if prev, ok := seen[key]; ok {
			// 后边重复项：剔除（保留靠前的 prev）
			pair.setState(PairStateDraining)
			if atomic.LoadInt32(&pair.refs) > 0 {
				coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 去重: 位于队列后边的 Pair %s 与 %s 通道重复且在服务，标记 Draining", pair.ID, prev.ID)
				continue
			}
			pair.setState(PairStateClosed)
			w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
			i--
			if w.primary == pair {
				w.primary = nil
			}
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 去重: 剔除位于队列后边、与 %s 通道重复的 Pair %s", prev.ID, pair.ID)
			continue
		}
		seen[key] = pair
	}
	w.ensurePrimaryLocked()
}

// healthCheckSpares 备用体检：非队首的 Ready Pair 若任一通道已失效则删除（有引用则排干），
// 健康则保留。队首（primary）的通道健康由重赛决策天然覆盖。
func (w *PairWarmer) healthCheckSpares() {
	available := w.pool.availableChannels()
	isAlive := func(chID int) bool {
		for _, c := range available {
			if c == chID {
				return true
			}
		}
		return false
	}
	w.mu.RLock()
	spares := make([]*HotChannelPair, 0, len(w.pairs))
	for _, pair := range w.pairs {
		if pair == w.primary || pair.State() != PairStateReady {
			continue
		}
		spares = append(spares, pair)
	}
	w.mu.RUnlock()
	for _, pair := range spares {
		switch {
		case !isAlive(pair.UplinkChID):
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 备用 %s 上行通道 %d 已失效，删除", pair.ID, pair.UplinkChID)
			w.InvalidateChannel(pair.UplinkChID)
		case !isAlive(pair.DownlinkChID):
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 备用 %s 下行通道 %d 已失效，删除", pair.ID, pair.DownlinkChID)
			w.InvalidateChannel(pair.DownlinkChID)
		}
	}
}

// trimExcessPairs 数量对齐：Ready 数量超过 PairCount 时，从队列末尾（队尾）逐个淘汰；
// 队尾有活跃引用则标记 Draining 等待释放。数量不足由调用方 tryBuildPairs 补足。
func (w *PairWarmer) trimExcessPairs() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.config.PairCount <= 0 {
		return
	}
	for {
		readyCount := 0
		for _, pair := range w.pairs {
			if pair.State() == PairStateReady {
				readyCount++
			}
		}
		if readyCount <= w.config.PairCount {
			break
		}
		var tail *HotChannelPair
		for i := len(w.pairs) - 1; i >= 0; i-- {
			if w.pairs[i].State() == PairStateReady {
				tail = w.pairs[i]
				break
			}
		}
		if tail == nil {
			break
		}
		tail.setState(PairStateDraining)
		if atomic.LoadInt32(&tail.refs) > 0 {
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 数量对齐: 队尾 Pair %s 有活跃引用，标记 Draining（引用归零后淘汰）", tail.ID)
			break
		}
		tail.setState(PairStateClosed)
		coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 数量对齐: Ready=%d/%d 超出，从队尾淘汰 Pair %s", readyCount, w.config.PairCount, tail.ID)
		w.removePair(tail)
	}
	w.ensurePrimaryLocked()
}

// periodicRefresh 周期性刷新：每次体检都重赛一场，找出当前最优通道对并置顶。
// 规则（单 Pair 与多 Pair 通用）：
//  1. 重赛：向所有可用通道广播预绑定竞速，构建候选 Pair；
//  2. 候选与队列内 Ready Pair 比对：
//     - 与队首一致 → 队首连任，候选丢弃（"保持不变"仅在真比过之后出现）；
//     - 与某备用一致 → 该备用直接提升为队首（不新建），候选丢弃，避免重复建档；
//     - 与任何 Ready Pair 都不一致（含命中 Draining 的情形）→ 视为真·新最优：
//     候选顶替队首，老队首降级为备用；
//  3. 队列去重：相同通道的 Ready Pair 只保留靠前的，剔除位于队列后边的；
//  4. 备用体检：失效则删、健康则留；
//  5. 数量对齐：Ready 数超出 PairCount 从队尾淘汰，不足则补足。
func (w *PairWarmer) periodicRefresh() {
	// 周期心跳：对存量 Ready Pair 重发通知，刷新服务端表项时间戳（防 TTL 过期误清健康 Pair）
	w.heartbeatHotPairNotify()

	// 清理历史版本遗留的 refs=0 的 Draining Pair，避免列表无限膨胀
	w.pruneIdleDrainingPairs()

	w.mu.RLock()
	var primaryID string
	if w.primary != nil {
		primaryID = w.primary.ID
	}
	readyCount := 0
	stateList := make([]string, 0, len(w.pairs))
	for _, pair := range w.pairs {
		stateList = append(stateList, fmt.Sprintf("%s[%s,refs=%d]", pair.ID, pairStateString(pair.State()), atomic.LoadInt32(&pair.refs)))
		if pair.State() == PairStateReady {
			readyCount++
		}
	}
	w.mu.RUnlock()

	mode := "单 Pair"
	if w.config.PairCount > 1 {
		mode = "多 Pair"
	}
	coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 周期性刷新触发: Ready=%d/%d, primary=%s, allPairs=%v (%s)", readyCount, w.config.PairCount, primaryID, stateList, mode)

	// ============ 1. 重赛一场：构建候选 ============
	available := w.pool.availableChannels()
	var candidate *HotChannelPair
	if len(available) >= 2 {
		var err error
		candidate, err = w.BuildPair(available)
		if err != nil {
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 周期性刷新: 构建候选失败: %v，保留现有 Pair", err)
		}
	} else if readyCount > 0 {
		coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 周期性刷新: 可用通道不足 (%d)，无法重赛，保留现有 Pair", len(available))
	}

	// ============ 2. 候选与全队比对决策 ============
	if candidate != nil {
		w.mu.RLock()
		head := w.primary
		headMatch, spareMatch := w.findReadyMatchLocked(candidate)
		w.mu.RUnlock()

		switch {
		case headMatch != nil:
			// 候选与队首一致：队首连任，冗余候选丢弃（真比过之后才"保持不变"）
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 周期性刷新: 候选 (%d/%d) 与队首 %s 通道一致，队首连任，保持不变",
				candidate.UplinkChID, candidate.DownlinkChID, headMatch.ID)
			w.discardCandidatePair(candidate)
		case spareMatch != nil:
			// 最优解已在队列中（备用位）：直接提升为队首，候选丢弃，避免重复建档
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 周期性刷新: 候选 (%d/%d) 与备用 %s 通道一致，提升 %s 为队首，新建候选丢弃",
				candidate.UplinkChID, candidate.DownlinkChID, spareMatch.ID, spareMatch.ID)
			w.promoteToHead(spareMatch)
			w.discardCandidatePair(candidate)
		default:
			// 与任何 Ready Pair 都不一致（含命中 Draining）：视为真·新最优
			if head == candidate {
				// BuildPair 已在空队列场景把候选立为 primary，补分配槽位 ID
				w.assignPairSlot(candidate)
				coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 周期性刷新: 新 Pair (%d/%d) 已是队首（原队首缺失/不可用）",
					candidate.UplinkChID, candidate.DownlinkChID)
				break
			}
			w.assignPairSlot(candidate)
			headLabel := "无"
			if head != nil {
				headLabel = head.ID
			}
			coreLog(LevelDebug, "pair_warmer", "[PairWarmer] 周期性刷新: 候选 (%d/%d) 为新最优，分配槽位 %s，顶替队首 %s",
				candidate.UplinkChID, candidate.DownlinkChID, candidate.ID, headLabel)
			w.replaceHeadWithCandidate(candidate, head)
		}
	}

	// ============ 3. 队列去重：相同通道只留靠前，剔除后边 ============
	w.deduplicatePairs()

	// ============ 4. 备用体检：失效删、健康留 ============
	w.healthCheckSpares()

	// ============ 5. 数量对齐：超出从队尾淘汰、不足补足 ============
	w.trimExcessPairs()
	w.tryBuildPairs()
	// 补足过程可能再次撞出同通道 pair，兜底去重
	w.deduplicatePairs()

	// 刷新后检查 primary 是否变化
	w.mu.RLock()
	var newPrimaryID string
	if w.primary != nil {
		newPrimaryID = w.primary.ID
	}
	w.mu.RUnlock()
	if newPrimaryID != "" && newPrimaryID != primaryID {
		coreLog(LevelDebug, "pair_warmer", "[PairWarmer] primary 已切换: %s -> %s", primaryID, newPrimaryID)
	}
}

// queueHotPairNotify 将 Ready Pair 加入待通知队列（按键去重，500ms 合批后发送）。

// hotPairNotifyDebounce 新 Pair 通知的合批窗口
const hotPairNotifyDebounce = 500 * time.Millisecond

// queueHotPairNotify 将 Pair 键与收发通道加入待通知队列（按键去重），防抖合批发送。
// 预热是后台路径，500ms 延迟不影响数据面；合批避免启动期多条 Pair 产生多条通知帧。
func (w *PairWarmer) queueHotPairNotify(pair *HotChannelPair) {
	if pair == nil || pair.PrebindID == "" {
		return
	}
	w.notifyMu.Lock()
	w.notifyPending[pair.PrebindID] = protocol.HotPairInfo{
		Key: pair.PrebindID,
		ChA: pair.UplinkChID,
		ChB: pair.DownlinkChID,
	}
	if w.notifyTimer == nil {
		w.notifyTimer = time.AfterFunc(hotPairNotifyDebounce, w.flushHotPairNotify)
	}
	w.notifyMu.Unlock()
}

// flushHotPairNotify 把待通知记录编码为一帧 MsgHotPairNotify 发给服务端。
// 发送前过滤已失效的 Pair；全部通道发送失败时保留队列等待下次重试。
func (w *PairWarmer) flushHotPairNotify() {
	w.notifyMu.Lock()
	w.notifyTimer = nil
	if len(w.notifyPending) == 0 {
		w.notifyMu.Unlock()
		return
	}
	pending := make([]protocol.HotPairInfo, 0, len(w.notifyPending))
	for key, info := range w.notifyPending {
		if w.lookupReady(key) == nil {
			// Pair 已失效：丢弃通知
			delete(w.notifyPending, key)
			continue
		}
		pending = append(pending, info)
	}
	if len(pending) == 0 {
		w.notifyMu.Unlock()
		return
	}
	w.notifyMu.Unlock()

	msg := protocol.EncodeMessage(protocol.MsgHotPairNotify, "", nil, protocol.EncodeHotPairNotify(pending))
	for _, chID := range w.pool.availableChannels() {
		if err := w.pool.asyncWriteDirect(chID, websocket.BinaryMessage, msg); err == nil {
			// 发送成功：清除已通知记录
			w.notifyMu.Lock()
			for _, info := range pending {
				delete(w.notifyPending, info.Key)
			}
			w.notifyMu.Unlock()
			coreLog(LevelInfo, "pair_warmer", "[PairWarmer] 预热通道对已通知服务端 (%d 条)", len(pending))
			return
		}
	}
	coreLog(LevelWarn, "pair_warmer", "[PairWarmer] 预热通道对通知发送失败，等待重试 (%d 条)", len(pending))
}

// heartbeatHotPairNotify 周期心跳：对存量 Ready Pair 重发通知，
// 刷新服务端表项时间戳（防 TTL 过期误清健康 Pair）。
func (w *PairWarmer) heartbeatHotPairNotify() {
	w.mu.RLock()
	ready := make([]*HotChannelPair, 0, len(w.pairs))
	for _, pair := range w.pairs {
		if pair.State() == PairStateReady {
			ready = append(ready, pair)
		}
	}
	w.mu.RUnlock()
	for _, pair := range ready {
		w.queueHotPairNotify(pair)
	}
}

// LookupReady 按 Pair 键查找 Ready 状态的 Pair（反向拨号提升路径使用）。
// 找不到或已失效返回 nil，调用方回退广播竞争路径。
func (w *PairWarmer) LookupReady(prebindKey string) *HotChannelPair {
	if prebindKey == "" {
		return nil
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lookupReady(prebindKey)
}

// lookupReady 内部查找（调用方需持有 mu 读锁或写锁）
func (w *PairWarmer) lookupReady(prebindKey string) *HotChannelPair {
	for _, pair := range w.pairs {
		if pair.PrebindID == prebindKey && pair.State() == PairStateReady {
			return pair
		}
	}
	return nil
}
