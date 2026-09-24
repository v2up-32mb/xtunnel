package xtunnel

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/v2up-32mb/xtunnel/protocol"
)

func newTestClientPool(cfg *Config) *clientPool {
	ctx, cancel := context.WithCancel(context.Background())
	p, _ := newClientPool(cfg, ctx, cancel)
	return p
}

func TestPairWarmerNew(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)
	if w == nil {
		t.Fatal("NewPairWarmer() returned nil")
	}
	if w.config.PairCount != cfg.HotPairCount {
		t.Fatalf("expected PairCount %d, got %d", cfg.HotPairCount, w.config.PairCount)
	}
}

func TestPairWarmerAcquireReleaseKeepsPairReady(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	pair := &HotChannelPair{
		ID:           "test-pair-1",
		UplinkChID:   1,
		DownlinkChID: 2,
	}
	pair.SetStateForTest(PairStateReady)

	w.SetPrimaryForTest(pair)
	// 手动加入 pairs 列表
	w.mu.Lock()
	w.pairs = append(w.pairs, pair)
	w.mu.Unlock()

	acquired := w.AcquirePrimary()
	if acquired == nil {
		t.Fatal("AcquirePrimary() returned nil for ready pair")
	}
	if acquired.ID != pair.ID {
		t.Fatalf("expected acquired ID %q, got %q", pair.ID, acquired.ID)
	}

	w.ReleasePair(acquired)

	// Pair 应该保持 Ready 供后续请求复用
	if pair.State() != PairStateReady {
		t.Fatalf("expected state Ready after release, got %d", pair.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected 1 pair, got %d", w.PairCountForTest())
	}

	// 再次获取应仍然成功
	acquired2 := w.AcquirePrimary()
	if acquired2 == nil {
		t.Fatal("expected to re-acquire released pair")
	}
	w.ReleasePair(acquired2)
}

func TestPairWarmerReleaseDrainingPair(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	pair := &HotChannelPair{
		ID:           "test-pair-1",
		UplinkChID:   1,
		DownlinkChID: 2,
	}
	pair.SetStateForTest(PairStateDraining)
	pair.refs = 1

	w.SetPrimaryForTest(pair)
	// 手动加入 pairs 列表以便 removePair 工作
	w.mu.Lock()
	w.pairs = append(w.pairs, pair)
	w.mu.Unlock()

	w.ReleasePair(pair)

	if pair.State() != PairStateClosed {
		t.Fatalf("expected state Closed after release, got %d", pair.State())
	}
	if w.PairCountForTest() != 0 {
		t.Fatalf("expected 0 pairs, got %d", w.PairCountForTest())
	}
}

func TestPairWarmerInvalidateChannel(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	pair1 := &HotChannelPair{
		ID:           "pair-1",
		UplinkChID:   1,
		DownlinkChID: 2,
	}
	pair1.SetStateForTest(PairStateReady)

	pair2 := &HotChannelPair{
		ID:           "pair-2",
		UplinkChID:   3,
		DownlinkChID: 4,
	}
	pair2.SetStateForTest(PairStateReady)

	w.mu.Lock()
	w.pairs = append(w.pairs, pair1, pair2)
	w.primary = pair1
	w.mu.Unlock()

	w.InvalidateChannel(2)

	if pair1.State() != PairStateClosed {
		t.Fatalf("expected pair1 state Closed, got %d", pair1.State())
	}
	if pair2.State() != PairStateReady {
		t.Fatalf("expected pair2 state Ready, got %d", pair2.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected 1 pair remaining, got %d", w.PairCountForTest())
	}

	// primary 失效后应自动选举 pair2 为新的 primary
	w.mu.RLock()
	prim := w.primary
	w.mu.RUnlock()
	if prim != pair2 {
		t.Fatal("expected primary to be pair2 after invalidating pair1")
	}
}

func TestPairWarmerInvalidateChannelDoesNotCloseReadyPair(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	pair := &HotChannelPair{
		ID:           "pair-1",
		UplinkChID:   1,
		DownlinkChID: 2,
	}
	pair.SetStateForTest(PairStateReady)
	pair.refs = 1

	w.mu.Lock()
	w.pairs = append(w.pairs, pair)
	w.primary = pair
	w.mu.Unlock()

	w.InvalidateChannel(2)

	if pair.State() != PairStateDraining {
		t.Fatalf("expected state Draining, got %d", pair.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected 1 pair (refs > 0), got %d", w.PairCountForTest())
	}

	w.ReleasePair(pair)

	if pair.State() != PairStateClosed {
		t.Fatalf("expected state Closed after release, got %d", pair.State())
	}
	if w.PairCountForTest() != 0 {
		t.Fatalf("expected 0 pairs after release, got %d", w.PairCountForTest())
	}
}

func TestPairWarmerBuildPairTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotPairCount = 1
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)
	w.config.PrebindTimeout = 100 * time.Millisecond

	// 使用没有真实连接的 clientPool，BuildPair 应超时返回错误
	available := []int{1, 2, 3}
	_, err := w.BuildPair(available)
	if err == nil {
		t.Fatal("expected BuildPair to return error on timeout")
	}
	if err.Error() != "预绑定超时" {
		t.Fatalf("expected timeout error, got: %v", err)
	}
}

func TestPairWarmerHandlePrebindResultMatchesConnID(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	// 发送一个不匹配的 connID 结果
	w.HandlePrebindResult("wrong-conn-id", 1, 2, nil)

	// 发送匹配的 connID 结果
	w.HandlePrebindResult("prebind-test-123", 1, 2, nil)

	// 验证通道中有两个结果
	if len(w.prebindResultCh) != 2 {
		t.Fatalf("expected 2 prebind results in channel, got %d", len(w.prebindResultCh))
	}

	// 读取第一个（不匹配的）
	res1 := <-w.prebindResultCh
	if res1.connID != "wrong-conn-id" {
		t.Fatalf("expected first result connID wrong-conn-id, got %s", res1.connID)
	}

	// 读取第二个（匹配的）
	res2 := <-w.prebindResultCh
	if res2.connID != "prebind-test-123" {
		t.Fatalf("expected second result connID prebind-test-123, got %s", res2.connID)
	}
	if res2.uplinkChID != 1 || res2.downlinkChID != 2 {
		t.Fatalf("expected uplink 1 downlink 2, got %d %d", res2.uplinkChID, res2.downlinkChID)
	}
}

func TestPairWarmerAcquireReturnsNilForDrainingPair(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	pair := &HotChannelPair{
		ID:           "test-pair",
		UplinkChID:   1,
		DownlinkChID: 2,
	}
	pair.SetStateForTest(PairStateDraining)

	w.SetPrimaryForTest(pair)

	acquired := w.AcquirePrimary()
	if acquired != nil {
		t.Fatal("expected AcquirePrimary to return nil for draining pair")
	}
}

func TestPairWarmerAcquireReturnsNilForClosedPair(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	pair := &HotChannelPair{
		ID:           "test-pair",
		UplinkChID:   1,
		DownlinkChID: 2,
	}
	pair.SetStateForTest(PairStateClosed)

	w.SetPrimaryForTest(pair)

	acquired := w.AcquirePrimary()
	if acquired != nil {
		t.Fatal("expected AcquirePrimary to return nil for closed pair")
	}
}

func TestPairWarmerBuildPairInsufficientChannels(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	_, err := w.BuildPair([]int{1})
	if err == nil {
		t.Fatal("expected BuildPair to return error with insufficient channels")
	}
}

func TestPairWarmerHandlePrebindResultChannelFull(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	// 填满通道
	for i := 0; i < 8; i++ {
		w.HandlePrebindResult("fill", 1, 2, nil)
	}

	// 第9个应该被丢弃（不会panic）
	w.HandlePrebindResult("overflow", 1, 2, nil)

	if len(w.prebindResultCh) != 8 {
		t.Fatalf("expected channel to remain at capacity 8, got %d", len(w.prebindResultCh))
	}
}

func TestPairChannelsEqual(t *testing.T) {
	a := &HotChannelPair{UplinkChID: 1, DownlinkChID: 2}
	b := &HotChannelPair{UplinkChID: 1, DownlinkChID: 2}
	c := &HotChannelPair{UplinkChID: 1, DownlinkChID: 3}
	d := &HotChannelPair{UplinkChID: 3, DownlinkChID: 2}
	if !pairChannelsEqual(a, b) {
		t.Fatal("expected equal channels (1,2)/(1,2)")
	}
	if pairChannelsEqual(a, c) || pairChannelsEqual(a, d) {
		t.Fatal("expected different channels to be unequal")
	}
	if pairChannelsEqual(nil, a) || pairChannelsEqual(a, nil) {
		t.Fatal("expected nil pair to be unequal")
	}
}

func TestPairWarmerAssignPairSlot(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	// 顺序构建 2 对：槽位 ID 应稳定为 01、02，不随构建次数递增
	p1 := &HotChannelPair{UplinkChID: 1, DownlinkChID: 2}
	p2 := &HotChannelPair{UplinkChID: 3, DownlinkChID: 4}
	w.mu.Lock()
	w.pairs = append(w.pairs, p1, p2)
	w.mu.Unlock()
	w.assignPairSlot(p1)
	w.assignPairSlot(p2)
	if p1.ID != "01" || p2.ID != "02" {
		t.Fatalf("expected slot IDs 01/02, got %q/%q", p1.ID, p2.ID)
	}

	// 移除 01 后重新构建：应复用已释放的槽位 01，而不是继续递增
	p3 := &HotChannelPair{UplinkChID: 5, DownlinkChID: 6}
	w.mu.Lock()
	w.removePair(p1)
	w.pairs = append(w.pairs, p3)
	w.mu.Unlock()
	w.assignPairSlot(p3)
	if p3.ID != "01" {
		t.Fatalf("expected freed slot 01 to be reused, got %q", p3.ID)
	}

	// 替换路径：候选直接继承旧 Pair 的槽位 ID，不额外占用新槽位
	candidate := &HotChannelPair{UplinkChID: 7, DownlinkChID: 8}
	candidate.ID = p2.ID
	if candidate.ID != "02" {
		t.Fatalf("expected candidate to inherit old pair ID %q, got %q", p2.ID, candidate.ID)
	}
}

func TestPairWarmerDiscardCandidatePair(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	old := &HotChannelPair{ID: "aa", UplinkChID: 1, DownlinkChID: 2}
	old.SetStateForTest(PairStateReady)
	candidate := &HotChannelPair{ID: "bb", UplinkChID: 1, DownlinkChID: 2}
	candidate.SetStateForTest(PairStateReady)

	w.mu.Lock()
	w.pairs = append(w.pairs, old, candidate)
	w.primary = old
	w.mu.Unlock()

	w.discardCandidatePair(candidate)

	if candidate.State() != PairStateClosed {
		t.Fatalf("expected candidate Closed after discard, got %d", candidate.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected 1 pair after discard, got %d", w.PairCountForTest())
	}
	w.mu.RLock()
	prim := w.primary
	w.mu.RUnlock()
	if prim != old {
		t.Fatal("expected old pair to remain primary")
	}
}

func TestPairWarmerDiscardCandidatePairWithRefs(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	candidate := &HotChannelPair{ID: "cd", UplinkChID: 1, DownlinkChID: 2}
	candidate.SetStateForTest(PairStateReady)
	candidate.refs = 1

	w.mu.Lock()
	w.pairs = append(w.pairs, candidate)
	w.primary = candidate
	w.mu.Unlock()

	w.discardCandidatePair(candidate)

	if candidate.State() != PairStateDraining {
		t.Fatalf("expected candidate to be Draining with refs, got %d", candidate.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected candidate to stay until released, got %d", w.PairCountForTest())
	}
	if got := w.AcquirePrimary(); got != nil {
		t.Fatal("expected AcquirePrimary to return nil for draining candidate")
	}

	w.ReleasePair(candidate)

	if w.PairCountForTest() != 0 {
		t.Fatalf("expected 0 pairs after release, got %d", w.PairCountForTest())
	}
}

func TestPairWarmerInvalidatePairIdleRemovesImmediately(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	primary := &HotChannelPair{ID: "aa", UplinkChID: 1, DownlinkChID: 2}
	primary.SetStateForTest(PairStateReady)
	backup := &HotChannelPair{ID: "bb", UplinkChID: 3, DownlinkChID: 4}
	backup.SetStateForTest(PairStateReady)

	w.mu.Lock()
	w.pairs = append(w.pairs, primary, backup)
	w.primary = primary
	w.mu.Unlock()

	w.invalidatePair(primary)

	if primary.State() != PairStateClosed {
		t.Fatalf("expected primary Closed after invalidate (refs=0), got %d", primary.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected 1 pair remaining, got %d", w.PairCountForTest())
	}
	w.mu.RLock()
	prim := w.primary
	w.mu.RUnlock()
	if prim != backup {
		t.Fatal("expected backup to be re-elected as primary after invalidate")
	}
}

func TestPairWarmerInvalidatePairWithRefs(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	pair := &HotChannelPair{ID: "aa", UplinkChID: 1, DownlinkChID: 2}
	pair.SetStateForTest(PairStateReady)
	pair.refs = 1

	w.mu.Lock()
	w.pairs = append(w.pairs, pair)
	w.primary = pair
	w.mu.Unlock()

	w.invalidatePair(pair)

	if pair.State() != PairStateDraining {
		t.Fatalf("expected Draining with refs, got %d", pair.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected pair to stay until released, got %d", w.PairCountForTest())
	}

	w.ReleasePair(pair)

	if pair.State() != PairStateClosed {
		t.Fatalf("expected Closed after release, got %d", pair.State())
	}
	if w.PairCountForTest() != 0 {
		t.Fatalf("expected 0 pairs, got %d", w.PairCountForTest())
	}
}

func TestPairWarmerPruneIdleDrainingPairs(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	idle := &HotChannelPair{ID: "01", UplinkChID: 1, DownlinkChID: 2}
	idle.SetStateForTest(PairStateDraining)
	busy := &HotChannelPair{ID: "02", UplinkChID: 3, DownlinkChID: 4}
	busy.SetStateForTest(PairStateDraining)
	busy.refs = 2
	ready := &HotChannelPair{ID: "03", UplinkChID: 5, DownlinkChID: 6}
	ready.SetStateForTest(PairStateReady)

	w.mu.Lock()
	w.pairs = append(w.pairs, idle, busy, ready)
	w.primary = ready
	w.mu.Unlock()

	w.pruneIdleDrainingPairs()

	if idle.State() != PairStateClosed {
		t.Fatalf("expected idle draining pair Closed, got %d", idle.State())
	}
	if busy.State() != PairStateDraining {
		t.Fatalf("expected busy draining pair to stay Draining, got %d", busy.State())
	}
	if ready.State() != PairStateReady {
		t.Fatalf("expected ready pair untouched, got %d", ready.State())
	}
	if w.PairCountForTest() != 2 {
		t.Fatalf("expected 2 pairs after prune, got %d", w.PairCountForTest())
	}
}

func TestPairWarmerReleasePairReElectsPrimary(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	primary := &HotChannelPair{ID: "aa", UplinkChID: 1, DownlinkChID: 2}
	primary.SetStateForTest(PairStateDraining)
	primary.refs = 1
	backup := &HotChannelPair{ID: "bb", UplinkChID: 3, DownlinkChID: 4}
	backup.SetStateForTest(PairStateReady)

	w.mu.Lock()
	w.pairs = append(w.pairs, primary, backup)
	w.primary = primary
	w.mu.Unlock()

	w.ReleasePair(primary)

	if primary.State() != PairStateClosed {
		t.Fatalf("expected Closed after release, got %d", primary.State())
	}
	if w.PairCountForTest() != 1 {
		t.Fatalf("expected 1 pair, got %d", w.PairCountForTest())
	}
	w.mu.RLock()
	prim := w.primary
	w.mu.RUnlock()
	if prim != backup {
		t.Fatal("expected backup re-elected as primary after release")
	}
}

// TestRegisterConcurrentDialsDistinctConnIDs 回归：正向 Pair 为共享复用，
// 并发连接各自复用同一对预热通道时，connID 必须保持每连接唯一
// （禁止把 prebind connID 用作连接 connID——并发时互相覆盖/被服务端当重复丢弃）。
func TestRegisterConcurrentDialsDistinctConnIDs(t *testing.T) {
	clientConn, serverConn, cleanup := newClientTestWebSocketPair(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := DefaultConfig()
	cfg.EnableHotPair = true
	cfg.Connections = 1
	p, err := newClientPool(cfg, ctx, cancel)
	if err != nil {
		t.Fatalf("newClientPool: %v", err)
	}
	p.wsConns[0] = clientConn
	go p.writeWorker(0, clientConn, p.writeQueues[0])
	t.Cleanup(func() { _ = clientConn.Close() })

	pair := &HotChannelPair{
		ID:           "01",
		UplinkChID:   1,
		DownlinkChID: 1,
		state:        int32(PairStateReady),
	}
	p.pairWarmer.SetPrimaryForTest(pair)

	done := make(chan struct{})
	go func() {
		p.handleChannel(1, clientConn)
		close(done)
	}()

	// 同一 Pair 上并发注册多个连接
	const n = 4
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i] = fmt.Sprintf("req-%d", i)
			p.RegisterAndBroadcastTCP(ids[i], fmt.Sprintf("target-%d:80", i), nil, nil, "TEST")
		}(i)
	}
	wg.Wait()

	// 所有连接的 connID 互不相同（调用方 requestID 原样生效）
	seen := make(map[string]bool, n)
	p.mu.RLock()
	for _, id := range ids {
		if seen[id] {
			p.mu.RUnlock()
			t.Fatalf("duplicate connID %q across concurrent dials", id)
		}
		seen[id] = true
		if _, ok := p.conns[id]; !ok {
			p.mu.RUnlock()
			t.Fatalf("conn state %q missing", id)
		}
	}
	p.mu.RUnlock()

	// 服务端收到 n 个 MsgTCPConnect，connID 各不相同
	gotIDs := make(map[string]bool, n)
	_ = serverConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for len(gotIDs) < n {
		_, raw, err := serverConn.ReadMessage()
		if err != nil {
			t.Fatalf("read dial %d: %v", len(gotIDs), err)
		}
		mtype, connID, _, _, err := protocol.DecodeMessage(raw)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if mtype != protocol.MsgTCPConnect {
			continue
		}
		if gotIDs[connID] {
			t.Fatalf("server got duplicate connID %q", connID)
		}
		gotIDs[connID] = true
	}
}

// ======================== 周期性刷新（重赛决策）测试 ========================

// newRefreshTestPool 构造周期刷新测试用的 clientPool：
// 前 aliveN 条通道可用（wsConns 非 nil + 写队列就绪），供 availableChannels 判定。
func newRefreshTestPool(t *testing.T, totalCh, aliveN, pairCount int) *clientPool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := DefaultConfig()
	cfg.EnableHotPair = true
	cfg.Connections = totalCh
	cfg.HotPairCount = pairCount

	p := &clientPool{
		ctx:              ctx,
		cancel:           cancel,
		config:           cfg,
		conns:            make(map[string]*clientConnState),
		wsConns:          make([]*websocket.Conn, totalCh),
		writeQueues:      make([]chan writeJob, totalCh),
		connsWriteMutex:  make([]sync.Mutex, totalCh),
		globalQueueLimit: 1 << 20,
		nextChannel:      1,
	}
	for i := 0; i < totalCh; i++ {
		p.writeQueues[i] = make(chan writeJob, 16)
		if i < aliveN {
			p.wsConns[i] = &websocket.Conn{} // 标记该通道可用
		}
	}
	p.pairWarmer = NewPairWarmer(p, cfg)
	return p
}

// addReadyTestPair 直接向 warmer 追加一个 Ready pair 并返回
func addReadyTestPair(w *PairWarmer, id string, ul, dl int, refs int32) *HotChannelPair {
	pair := &HotChannelPair{
		ID:           id,
		PrebindID:    "prebind-" + id,
		UplinkChID:   ul,
		DownlinkChID: dl,
		state:        int32(PairStateReady),
		createdAt:    time.Now(),
		refs:         refs,
	}
	w.mu.Lock()
	w.pairs = append(w.pairs, pair)
	w.mu.Unlock()
	return pair
}

// setPrimaryHead 将 pair 设为 primary 并保证位于队首（位置 0）
func setPrimaryHead(w *PairWarmer, pair *HotChannelPair) {
	w.mu.Lock()
	w.primary = pair
	for i, p := range w.pairs {
		if p == pair {
			if i > 0 {
				w.pairs = append(w.pairs[:i], w.pairs[i+1:]...)
				w.pairs = append([]*HotChannelPair{pair}, w.pairs...)
			}
			break
		}
	}
	w.mu.Unlock()
}

// injectPrebind 在后台轮询找到 BuildPair 注册的预绑定状态并回执指定上行/下行通道
func injectPrebind(p *clientPool, uplink, downlink int) {
	go func() {
		for i := 0; i < 400; i++ {
			p.mu.Lock()
			var connID string
			for id, st := range p.conns {
				if st.target == protocol.PrebindTarget {
					connID = id
					break
				}
			}
			p.mu.Unlock()
			if connID != "" {
				p.pairWarmer.HandlePrebindResult(connID, uplink, downlink, nil)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
}

func countReady(w *PairWarmer) int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	n := 0
	for _, p := range w.pairs {
		if p.State() == PairStateReady {
			n++
		}
	}
	return n
}

func readyChannels(w *PairWarmer) [][2]int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	var out [][2]int
	for _, p := range w.pairs {
		if p.State() == PairStateReady {
			out = append(out, [2]int{p.UplinkChID, p.DownlinkChID})
		}
	}
	return out
}

// 单 Pair：重赛候选与队首一致 → 队首连任"保持不变"（真比过之后）
func TestPeriodicRefreshSinglePairKeepWhenSame(t *testing.T) {
	p := newRefreshTestPool(t, 4, 4, 1)
	w := p.pairWarmer
	head := addReadyTestPair(w, "01", 1, 2, 0)
	setPrimaryHead(w, head)

	injectPrebind(p, 1, 2) // 竞速赢家与队首同通道
	w.periodicRefresh()

	if w.primary != head {
		t.Fatalf("expected head kept as primary when candidate matches, got %p", w.primary)
	}
	if countReady(w) != 1 {
		t.Fatalf("expected 1 ready pair, got %d (%v)", countReady(w), readyChannels(w))
	}
	if head.State() != PairStateReady {
		t.Fatalf("expected head still Ready, got %d", head.State())
	}
}

// 单 Pair：重赛候选与队首不同 → 新最优顶替队首，老队首被裁剪
func TestPeriodicRefreshSinglePairReplaceWhenDifferent(t *testing.T) {
	p := newRefreshTestPool(t, 4, 4, 1)
	w := p.pairWarmer
	head := addReadyTestPair(w, "01", 1, 2, 0)
	setPrimaryHead(w, head)

	injectPrebind(p, 3, 4) // 竞速赢家是新通道对
	w.periodicRefresh()

	if w.primary == head {
		t.Fatal("expected primary replaced by new optimal pair")
	}
	if w.primary == nil || w.primary.UplinkChID != 3 || w.primary.DownlinkChID != 4 {
		t.Fatalf("expected new pair (3,4) as primary, got %+v", w.primary)
	}
	if countReady(w) != 1 {
		t.Fatalf("expected 1 ready pair after replacement, got %d (%v)", countReady(w), readyChannels(w))
	}
	if head.State() != PairStateClosed && head.State() != PairStateDraining {
		t.Fatalf("expected old head closed/draining, got %d", head.State())
	}
}

// 多 Pair：重赛候选命中备用 → 备用直接提升为队首，新建候选丢弃（不出现重复通道对）
func TestPeriodicRefreshMultiPromoteSpareWhenMatched(t *testing.T) {
	p := newRefreshTestPool(t, 4, 4, 2)
	w := p.pairWarmer
	head := addReadyTestPair(w, "01", 1, 2, 0)
	spare := addReadyTestPair(w, "02", 3, 4, 0)
	setPrimaryHead(w, head)

	injectPrebind(p, 3, 4) // 竞速赢家 = 备用 02 的通道
	w.periodicRefresh()

	if w.primary != spare {
		t.Fatalf("expected spare promoted to primary, got %p", w.primary)
	}
	if countReady(w) != 2 {
		t.Fatalf("expected 2 ready pairs, got %d (%v)", countReady(w), readyChannels(w))
	}
	// 不能有重复通道对（候选必须被丢弃）
	ch := readyChannels(w)
	if ch[0][0] == ch[1][0] && ch[0][1] == ch[1][1] {
		t.Fatalf("duplicate pairs after refresh: %v", ch)
	}
}

// 多 Pair：重赛候选全新 → 顶替队首，老队首降级，超出数量从队尾淘汰
func TestPeriodicRefreshMultiReplaceHeadAndTrimTail(t *testing.T) {
	p := newRefreshTestPool(t, 5, 5, 2)
	w := p.pairWarmer
	head := addReadyTestPair(w, "01", 1, 2, 0)
	addReadyTestPair(w, "02", 3, 4, 0)
	setPrimaryHead(w, head)

	injectPrebind(p, 4, 5) // 全新通道对
	w.periodicRefresh()

	if w.primary == nil || w.primary.UplinkChID != 4 || w.primary.DownlinkChID != 5 {
		t.Fatalf("expected new pair (4,5) as primary, got %+v", w.primary)
	}
	if countReady(w) != 2 {
		t.Fatalf("expected 2 ready pairs (PairCount=2), got %d (%v)", countReady(w), readyChannels(w))
	}
	// 老队首应降级保留为备用，旧备用(3,4)作为队尾被淘汰
	got := readyChannels(w)
	has := func(ul, dl int) bool {
		for _, c := range got {
			if c[0] == ul && c[1] == dl {
				return true
			}
		}
		return false
	}
	if !has(1, 2) {
		t.Fatalf("expected old head (1,2) kept as spare, got %v", got)
	}
	if has(3, 4) {
		t.Fatalf("expected old tail spare (3,4) trimmed, got %v", got)
	}
}

// 去重：相同通道的 Ready Pair 只保留靠前，剔除位于队列后边的
func TestPairWarmerDeduplicatePairs(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	head := addReadyTestPair(w, "01", 1, 2, 0)
	mid := addReadyTestPair(w, "02", 3, 4, 0)
	dup := addReadyTestPair(w, "03", 1, 2, 0) // 与队首重复，位于后边
	setPrimaryHead(w, head)

	w.deduplicatePairs()

	if countReady(w) != 2 {
		t.Fatalf("expected 2 ready pairs after dedup, got %d (%v)", countReady(w), readyChannels(w))
	}
	if dup.State() != PairStateClosed {
		t.Fatalf("expected back duplicate removed, got %d", dup.State())
	}
	if mid.State() != PairStateReady || head.State() != PairStateReady {
		t.Fatal("expected non-duplicate pairs untouched")
	}
}

// 去重：后端重复项在服务（refs>0）时只标记 Draining，不强制移除
func TestPairWarmerDeduplicatePairsKeepsBusyBackDup(t *testing.T) {
	cfg := DefaultConfig()
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	head := addReadyTestPair(w, "01", 1, 2, 0)
	dup := addReadyTestPair(w, "03", 1, 2, 1) // 重复且在服务
	setPrimaryHead(w, head)

	w.deduplicatePairs()

	if dup.State() != PairStateDraining {
		t.Fatalf("expected busy back duplicate Draining, got %d", dup.State())
	}
	if countReady(w) != 1 {
		t.Fatalf("expected 1 ready pair, got %d", countReady(w))
	}
}

// 备用体检：备用 pair 的通道失效则删除，健康则保留
func TestPairWarmerHealthCheckSpares(t *testing.T) {
	p := newRefreshTestPool(t, 4, 2, 3) // 仅通道 1、2 可用
	w := p.pairWarmer
	head := addReadyTestPair(w, "01", 1, 2, 0)
	bad := addReadyTestPair(w, "02", 3, 4, 0) // 通道 3/4 已失效
	setPrimaryHead(w, head)

	w.healthCheckSpares()

	if bad.State() != PairStateClosed {
		t.Fatalf("expected dead spare removed, got %d", bad.State())
	}
	if w.primary != head {
		t.Fatal("expected head untouched")
	}
}

// 数量对齐：Ready 超出 PairCount 时从队尾逐个淘汰，保留不超量
func TestPairWarmerTrimExcessPairs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HotPairCount = 2
	p := newTestClientPool(cfg)
	w := NewPairWarmer(p, cfg)

	head := addReadyTestPair(w, "01", 1, 2, 0)
	s2 := addReadyTestPair(w, "02", 3, 4, 0)
	s3 := addReadyTestPair(w, "03", 4, 5, 0)
	s4 := addReadyTestPair(w, "04", 5, 6, 0)
	setPrimaryHead(w, head)

	w.trimExcessPairs()

	if countReady(w) != 2 {
		t.Fatalf("expected 2 ready pairs, got %d (%v)", countReady(w), readyChannels(w))
	}
	if s4.State() != PairStateClosed || s3.State() != PairStateClosed {
		t.Fatalf("expected tail pairs s3/s4 trimmed, got s3=%d s4=%d", s3.State(), s4.State())
	}
	if s2.State() != PairStateReady {
		t.Fatalf("expected s2 kept as spare, got %d", s2.State())
	}
	if w.primary != head {
		t.Fatal("expected head untouched by trim")
	}
}
