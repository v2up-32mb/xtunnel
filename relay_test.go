package xtunnel

import (
	"net"
	"testing"
	"time"
)

func TestMarkNodeSuccessRefreshesScoreAndLastTest(t *testing.T) {
	m := NewRelayNodeManager()
	node := &RelayNode{
		ID:          "1.1.1.1:443",
		Address:     "1.1.1.1:443",
		IP:          "1.1.1.1:443",
		Latency:     100 * time.Millisecond,
		LastTest:    time.Now().Add(-2 * time.Hour),
		SuccessRate: 0,
		Score:       0,
		FailCount:   3,
	}
	m.nodes = []*RelayNode{node}

	before := time.Now()
	m.MarkNodeSuccess(node.IP)

	if node.FailCount != 0 {
		t.Fatalf("FailCount = %d, want 0", node.FailCount)
	}
	if node.SuccessRate != 1.0 {
		t.Fatalf("SuccessRate = %v, want 1.0", node.SuccessRate)
	}
	if !node.LastTest.After(before.Add(-100 * time.Millisecond)) {
		t.Fatalf("LastTest = %v, want refreshed time after %v", node.LastTest, before)
	}
	if node.Score <= 0 {
		t.Fatalf("Score = %v, want refreshed positive score", node.Score)
	}
	if node.Weight != node.Score {
		t.Fatalf("Weight = %v, want match Score %v", node.Weight, node.Score)
	}
}

func TestSelectNodeExcludingFallsBackWhenNoHealthyCandidate(t *testing.T) {
	m := NewRelayNodeManager()
	m.nodes = []*RelayNode{
		{ID: "bad", Address: "bad", IP: "bad", Score: 10, FailCount: 5, SuccessRate: 0},
		{ID: "better", Address: "better", IP: "better", Score: 20, FailCount: 1, SuccessRate: 0},
	}

	node := m.SelectNodeExcluding(nil)
	if node == nil {
		t.Fatalf("SelectNodeExcluding() = nil, want fallback candidate")
	}
	if node.ip != "better" {
		t.Fatalf("fallback node = %q, want %q", node.ip, "better")
	}
}

func TestSelectNodeExcludingRefreshesHostnameNodesAndReturnsNewIP(t *testing.T) {
	m := NewRelayNodeManager()
	lookupCalls := 0
	m.lookupIP = func(host string) ([]net.IP, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return []net.IP{net.ParseIP("192.0.2.10")}, nil
		}
		return []net.IP{net.ParseIP("192.0.2.20")}, nil
	}

	if err := m.AddNode("relay.example.com:443", "443"); err != nil {
		t.Fatalf("AddNode() error = %v", err)
	}
	oldIP := m.nodes[0].IP
	m.MarkNodeFailed(oldIP)

	node := m.SelectNodeExcluding([]string{oldIP})
	if node == nil {
		t.Fatalf("SelectNodeExcluding() = nil, want newly resolved node")
	}
	if node.ip != "192.0.2.20:443" {
		t.Fatalf("selected node IP = %q, want %q", node.ip, "192.0.2.20:443")
	}
	if len(m.nodes) < 2 {
		t.Fatalf("expected refreshed node list to include new IP, got %d nodes", len(m.nodes))
	}
}

func TestSelectNodeExcludingLoadBalancesAcrossCandidates(t *testing.T) {
	m := NewRelayNodeManager()
	m.nodes = []*RelayNode{
		{ID: "a", Address: "a", IP: "node-a", Score: 1.0, SuccessRate: 1.0},
		{ID: "b", Address: "b", IP: "node-b", Score: 0.99, SuccessRate: 1.0},
		{ID: "c", Address: "c", IP: "node-c", Score: 0.99, SuccessRate: 1.0},
	}
	// 模拟 node-a 已承载大量连接（负载因子降到底），其余节点空闲
	for i := 0; i < maxLoadPerNode*2; i++ {
		m.Acquire("node-a")
	}
	// 多次选择应几乎不会命中高负载的 node-a
	counts := map[string]int{}
	for i := 0; i < 300; i++ {
		n := m.SelectNodeExcluding(nil)
		if n == nil {
			t.Fatalf("SelectNodeExcluding() = nil")
		}
		counts[n.ip]++
	}
	// 统计稳健阈值：p(node-a)≈5.1%（300 次均值 15.3、σ≈3.8），40 ≈ +6.5σ；
	// 若负载惩罚失效（p≈33%）则 300 次期望命中 100 次，必然触发。
	if counts["node-a"] > 40 {
		t.Fatalf("node-a selected %d/300 times, want load-balanced distribution", counts["node-a"])
	}
	if counts["node-b"] == 0 || counts["node-c"] == 0 {
		t.Fatalf("expected node-b/node-c to be selected, got %v", counts)
	}
}

func TestRelayAcquireRelease(t *testing.T) {
	m := NewRelayNodeManager()
	m.Acquire("1.1.1.1:443")
	m.Acquire("1.1.1.1:443")
	if got := m.AcquiredCount("1.1.1.1:443"); got != 2 {
		t.Fatalf("AcquiredCount = %d, want 2", got)
	}
	m.Release("1.1.1.1:443")
	if got := m.AcquiredCount("1.1.1.1:443"); got != 1 {
		t.Fatalf("AcquiredCount after release = %d, want 1", got)
	}
	m.Release("1.1.1.1:443")
	if got := m.AcquiredCount("1.1.1.1:443"); got != 0 {
		t.Fatalf("AcquiredCount after final release = %d, want 0", got)
	}
	// 重复释放不应出现负数
	m.Release("1.1.1.1:443")
	if got := m.AcquiredCount("1.1.1.1:443"); got != 0 {
		t.Fatalf("AcquiredCount after over-release = %d, want 0", got)
	}
}
