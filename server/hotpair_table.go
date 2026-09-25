package server

import (
	"sync"
	"time"

	"github.com/v2up-32mb/xtunnel/protocol"
)

// HotPairEntry 一条预热通道对表项（来自客户端 MsgHotPairNotify）。
// 通道为绝对方向语义：ChA = client→server（服务端收包）；ChB = server→client（服务端发包）。
type HotPairEntry struct {
	Key string // 预热 Pair 键（预热期双方已知的 prebind connID）
	ChA int
	ChB int
	At  time.Time // 最近一次通知时间（TTL 清扫依据）
}

// hotPairEntryTTL 表项存活期：健康 Pair 由客户端周期心跳刷新时间戳，
// 通道失效由 cleanupChannel 联动清扫，TTL 只是最后防线（客户端长时间静默后回收）。
const hotPairEntryTTL = 10 * time.Minute

// HotPairTable 服务端预热通道对表（被动）：健康维护由客户端负责，
// 客户端预热完成后经 MsgHotPairNotify 批量通知，本表按键 upsert 存储双端通道对。
// 表有两个消费方：正向 handleTCPConnect 按键提升（Lookup），反向 DialStream 择新拨号（Newest）。
type HotPairTable struct {
	mu    sync.Mutex
	table map[string]map[string]*HotPairEntry // clientID → key → entry
}

func NewHotPairTable() *HotPairTable {
	return &HotPairTable{table: make(map[string]map[string]*HotPairEntry)}
}

// HandleNotify 处理客户端批量预热通道对通知：按键 upsert，先清扫该客户端过期表项。
// 通道有效性不在此时校验（拨号/提升使用时按 clientID 通道表校验），客户端乱报
// 通道号只会导致用时不匹配而回退经典路径，无副作用。
func (t *HotPairTable) HandleNotify(clientID string, payload []byte) {
	entries := protocol.DecodeHotPairNotify(payload)
	if len(entries) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	m := t.table[clientID]
	if m == nil {
		m = make(map[string]*HotPairEntry)
		t.table[clientID] = m
	}
	now := time.Now()
	// 心跳到达即顺带清扫该客户端过期表项（客户端预热周期 ~30s，远小于 TTL）
	for k, e := range m {
		if now.Sub(e.At) > hotPairEntryTTL {
			delete(m, k)
		}
	}
	for _, e := range entries {
		if e.Key == "" || e.ChA <= 0 || e.ChB <= 0 {
			continue
		}
		m[e.Key] = &HotPairEntry{Key: e.Key, ChA: e.ChA, ChB: e.ChB, At: now}
	}
	srvLog(LevelInfo, "hotpair_table", "[HotPair] 收到客户端 %s 预热通道对通知 (%d 条)，在表 %d 条",
		protocol.ShortID(clientID), len(entries), len(m))
}

// Lookup 按键查找表项（正向 handleTCPConnect 提升路径）；过期或不存在返回 nil。
func (t *HotPairTable) Lookup(clientID, key string) *HotPairEntry {
	if t == nil || clientID == "" || key == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.table[clientID]
	if m == nil {
		return nil
	}
	e := m[key]
	if e == nil {
		return nil
	}
	if time.Since(e.At) > hotPairEntryTTL {
		delete(m, key)
		return nil
	}
	cp := *e
	return &cp
}

// Newest 返回该客户端最新的表项（反向 DialStream 拨号路径）。
// 通道活性由调用方校验；无表项返回 nil，调用方回退广播竞争。
func (t *HotPairTable) Newest(clientID string) *HotPairEntry {
	if t == nil || clientID == "" {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.table[clientID]
	if m == nil {
		return nil
	}
	now := time.Now()
	var best *HotPairEntry
	for k, e := range m {
		if now.Sub(e.At) > hotPairEntryTTL {
			delete(m, k)
			continue
		}
		if best == nil || e.At.After(best.At) {
			best = e
		}
	}
	if best == nil {
		return nil
	}
	cp := *best
	return &cp
}

// InvalidateChannel 通道断开：清扫所有客户端表中引用该通道的表项。
func (t *HotPairTable) InvalidateChannel(chID int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for clientID, m := range t.table {
		for k, e := range m {
			if e.ChA == chID || e.ChB == chID {
				delete(m, k)
			}
		}
		if len(m) == 0 {
			delete(t.table, clientID)
		}
	}
}

// InvalidateClient 客户端最后通道断开（或注销）：清空其全部表项。
func (t *HotPairTable) InvalidateClient(clientID string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.table, clientID)
}

// SizeForTest 仅测试用：返回 (客户端数, 总表项数)
func (t *HotPairTable) SizeForTest() (clients, entries int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range t.table {
		clients++
		entries += len(m)
	}
	return
}
