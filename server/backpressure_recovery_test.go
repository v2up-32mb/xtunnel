package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/v2up-32mb/xtunnel/protocol"
)

// TestBackpressureGradualRecovery 测试分级恢复机制
func TestBackpressureGradualRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBufferSize = 100         // 100 bytes
	cfg.BackpressureLimitBytes = 800 // 测试中显式设置限制为 800 bytes
	pool := newServerPool("test-token", cfg)

	limit := int64(800)
	if pool.globalQueueLimit != limit {
		t.Fatalf("Expected limit %d, got %d", limit, pool.globalQueueLimit)
	}

	// 初始状态应该是 Normal
	if atomic.LoadInt32(&pool.backpressureState) != int32(protocol.BackpressureNormal) {
		t.Error("Initial state should be Normal")
	}

	// 场景1：队列增长到 96% 触发暂停
	atomic.StoreInt64(&pool.globalQueueBytes, 770) // 96.25%
	pool.updateBackpressureState(770)
	time.Sleep(10 * time.Millisecond)
	if atomic.LoadInt32(&pool.backpressureState) != int32(protocol.BackpressurePause) {
		t.Error("Should enter Pause state at 96%")
	}

	// 场景2：队列降到 85%，应该恢复到减速
	pool.removeQueueBytes(90) // 从 770 降到 680 (85%)
	time.Sleep(10 * time.Millisecond)
	state := protocol.BackpressureState(atomic.LoadInt32(&pool.backpressureState))
	if state != protocol.BackpressureSlowDown {
		t.Errorf("Should recover to SlowDown at 85%% (below 90%% threshold), got %v", state)
	}

	// 场景3：队列继续降到 65%，应该恢复到正常
	pool.removeQueueBytes(160) // 从 680 降到 520 (65%)
	time.Sleep(10 * time.Millisecond)
	state = protocol.BackpressureState(atomic.LoadInt32(&pool.backpressureState))
	if state != protocol.BackpressureNormal {
		t.Errorf("Should recover to Normal at 65%% (below 70%% threshold), got %v", state)
	}
}

// TestBackpressureDirectRecovery 测试直接恢复（队列快速清空）
func TestBackpressureDirectRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBufferSize = 100
	cfg.BackpressureLimitBytes = 800
	pool := newServerPool("test-token", cfg)

	// 触发暂停
	atomic.StoreInt64(&pool.globalQueueBytes, 770) // 96%
	pool.updateBackpressureState(770)
	time.Sleep(10 * time.Millisecond)
	if atomic.LoadInt32(&pool.backpressureState) != int32(protocol.BackpressurePause) {
		t.Error("Should be in Pause state")
	}

	// 队列快速清空到 25%，应该直接恢复到正常
	pool.removeQueueBytes(570) // 从 770 降到 200 (25%)
	time.Sleep(10 * time.Millisecond)
	state := protocol.BackpressureState(atomic.LoadInt32(&pool.backpressureState))
	if state != protocol.BackpressureNormal {
		t.Errorf("Should directly recover to Normal at 25%%, got %v", state)
	}
}

// TestBackpressureStuckAtMiddle 测试卡在中间水位的场景（修复前会失败的场景）
func TestBackpressureStuckAtMiddle(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBufferSize = 100
	cfg.BackpressureLimitBytes = 800
	pool := newServerPool("test-token", cfg)

	// 触发暂停
	atomic.StoreInt64(&pool.globalQueueBytes, 770) // 96%
	pool.updateBackpressureState(770)
	time.Sleep(10 * time.Millisecond)
	if atomic.LoadInt32(&pool.backpressureState) != int32(protocol.BackpressurePause) {
		t.Error("Should be in Pause state")
	}

	// 模拟慢速消耗，队列卡在 88%（旧版本会永远卡住，新版本会恢复到减速）
	pool.removeQueueBytes(70) // 从 770 降到 700 (87.5%)
	time.Sleep(10 * time.Millisecond)
	state := protocol.BackpressureState(atomic.LoadInt32(&pool.backpressureState))
	if state != protocol.BackpressureSlowDown {
		t.Errorf("Should recover to SlowDown at 87.5%% (below 90%% threshold), got %v (old version would stuck at Pause)", state)
	}

	// 继续慢速消耗到 65%
	pool.removeQueueBytes(180) // 从 700 降到 520 (65%)
	time.Sleep(10 * time.Millisecond)
	state = protocol.BackpressureState(atomic.LoadInt32(&pool.backpressureState))
	if state != protocol.BackpressureNormal {
		t.Errorf("Should recover to Normal at 65%% (below 70%% threshold), got %v", state)
	}
}

// TestBackpressureAllThresholds 测试所有阈值
func TestBackpressureAllThresholds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBufferSize = 100
	cfg.BackpressureLimitBytes = 800
	pool := newServerPool("test-token", cfg)

	tests := []struct {
		name          string
		queueSize     int64
		expectedState protocol.BackpressureState
		description   string
	}{
		{"正常-10%", 80, protocol.BackpressureNormal, "10% 应该正常"},
		{"正常-50%", 400, protocol.BackpressureNormal, "50% 应该正常"},
		{"正常-79%", 632, protocol.BackpressureNormal, "79% 应该正常"},
		{"减速-81%", 648, protocol.BackpressureSlowDown, "81% 应该减速"},
		{"减速-90%", 720, protocol.BackpressureSlowDown, "90% 应该减速"},
		{"暂停-96%", 768, protocol.BackpressurePause, "96% 应该暂停"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 重置状态
			atomic.StoreInt32(&pool.backpressureState, int32(protocol.BackpressureNormal))
			atomic.StoreInt32(&pool.backpressureCooldown, 0)

			pool.updateBackpressureState(tt.queueSize)
			time.Sleep(10 * time.Millisecond)

			state := protocol.BackpressureState(atomic.LoadInt32(&pool.backpressureState))
			if state != tt.expectedState {
				t.Errorf("%s: expected %v, got %v", tt.description, tt.expectedState, state)
			}
		})
	}
}
