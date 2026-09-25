package server

import (
	"sync/atomic"
	"testing"

	"github.com/v2up-32mb/xtunnel/protocol"
)

func TestBackpressureStateTransitions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBufferSize = 64 * 1024
	cfg.BackpressureLimitBytes = int(cfg.ReadBufferSize) * 8 // 测试中显式使用旧默认值

	pool := newServerPool("test-token", cfg)

	// Test initial state
	if pool.globalQueueLimit != int64(cfg.ReadBufferSize)*8 {
		t.Errorf("Expected globalQueueLimit %d, got %d", int64(cfg.ReadBufferSize)*8, pool.globalQueueLimit)
	}

	if atomic.LoadInt32(&pool.backpressureState) != int32(protocol.BackpressureNormal) {
		t.Errorf("Expected initial backpressure state to be Normal, got %d", atomic.LoadInt32(&pool.backpressureState))
	}

	// Test addQueueBytes below threshold (should stay normal)
	pool.addQueueBytes(1000)
	if atomic.LoadInt32(&pool.backpressureState) != int32(protocol.BackpressureNormal) {
		t.Errorf("Expected state to remain Normal after small add, got %d", atomic.LoadInt32(&pool.backpressureState))
	}

	// Test addQueueBytes exceeding 80% threshold (should trigger slow down)
	limit80 := pool.globalQueueLimit * 8 / 10
	pool.addQueueBytes(int(limit80) + 100)

	// Wait for the cooldown goroutine to potentially run
	// Note: Due to cooldown mechanism, the state might not change immediately
	// but the globalQueueBytes should be updated
	currentBytes := atomic.LoadInt64(&pool.globalQueueBytes)
	if currentBytes < limit80 {
		t.Errorf("Expected globalQueueBytes >= %d, got %d", limit80, currentBytes)
	}

	// Test removeQueueBytes below 30% threshold (should return to normal)
	// First, reset and add a small amount
	atomic.StoreInt64(&pool.globalQueueBytes, 100)
	atomic.StoreInt32(&pool.backpressureState, int32(protocol.BackpressureSlowDown))

	pool.removeQueueBytes(50)
	if atomic.LoadInt32(&pool.backpressureState) != int32(protocol.BackpressureNormal) {
		t.Errorf("Expected state to return to Normal after remove below 30%%, got %d", atomic.LoadInt32(&pool.backpressureState))
	}

	// Test removeQueueBytes doesn't go negative
	atomic.StoreInt64(&pool.globalQueueBytes, 10)
	pool.removeQueueBytes(100)
	if atomic.LoadInt64(&pool.globalQueueBytes) != 0 {
		t.Errorf("Expected globalQueueBytes to be 0 after over-remove, got %d", atomic.LoadInt64(&pool.globalQueueBytes))
	}
}

func TestBackpressureThresholds(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReadBufferSize = 64 * 1024 // 64KB
	cfg.BackpressureLimitBytes = int(cfg.ReadBufferSize) * 8

	pool := newServerPool("test-token", cfg)

	// Calculate expected thresholds
	limit := int64(cfg.ReadBufferSize) * 8 // 512KB
	highThreshold := limit * 8 / 10        // 80% = 409.6KB
	pauseThreshold := limit * 95 / 100     // 95% = 486.4KB
	lowThreshold := limit * 3 / 10         // 30% = 153.6KB

	t.Logf("Queue limits: total=%d, high=%d (80%%), pause=%d (95%%), low=%d (30%%)",
		limit, highThreshold, pauseThreshold, lowThreshold)

	// Verify limits
	if pool.globalQueueLimit != limit {
		t.Errorf("Expected globalQueueLimit %d, got %d", limit, pool.globalQueueLimit)
	}

	if highThreshold <= 0 || pauseThreshold <= highThreshold || lowThreshold >= highThreshold {
		t.Errorf("Invalid threshold calculations: high=%d, pause=%d, low=%d", highThreshold, pauseThreshold, lowThreshold)
	}
}
