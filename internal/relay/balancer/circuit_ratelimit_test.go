package balancer

import (
	"testing"
	"time"
)

// 这些测试覆盖 11210 速率冷却场景（Closed 状态 + rateLimitUntil 分支），
// 不触发 Open 分支的 GetCooldown（依赖 op.SettingGetInt/DB），故无需 mock DB。
// 每个测试使用独立的 channelID/keyID/modelName 组合，避免 globalBreaker 跨测试污染。

func TestRecordRateLimit_SetsCooldownAndIsTripped(t *testing.T) {
	channelID, keyID := 9001, 1
	model := "rl-test-closed-tripped"

	// 初始无记录
	tripped, _ := IsTripped(channelID, keyID, model)
	if tripped {
		t.Fatal("fresh entry should not be tripped")
	}

	RecordRateLimit(channelID, keyID, model)

	tripped, remaining := IsTripped(channelID, keyID, model)
	if !tripped {
		t.Fatal("after RecordRateLimit, IsTripped should be true")
	}
	if remaining <= 0 || remaining > RateLimitCooldown {
		t.Fatalf("remaining = %v, want (0, %v]", remaining, RateLimitCooldown)
	}
}

func TestRecordRateLimit_DoesNotTouchCircuitState(t *testing.T) {
	// RecordRateLimit 不应改变 State/TripCount/ConsecutiveFailures。
	// 验证：速率冷却过期后，IsTripped 应回到 Closed=false（而非 Open/HalfOpen）。
	channelID, keyID := 9002, 1
	model := "rl-test-state-orthogonal"

	RecordRateLimit(channelID, keyID, model)

	// 直接读 entry 确认 State 仍是 Closed
	key := circuitKey(channelID, keyID, model)
	v, ok := globalBreaker.Load(key)
	if !ok {
		t.Fatal("entry should exist after RecordRateLimit")
	}
	entry := v.(*circuitEntry)
	entry.mu.Lock()
	if entry.State != StateClosed {
		t.Fatalf("State = %v, want StateClosed (RecordRateLimit must not change State)", entry.State)
	}
	if entry.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0", entry.ConsecutiveFailures)
	}
	if entry.TripCount != 0 {
		t.Fatalf("TripCount = %d, want 0", entry.TripCount)
	}
	if entry.rateLimitUntil.IsZero() {
		t.Fatal("rateLimitUntil should be set")
	}
	entry.mu.Unlock()
}

func TestIsTripped_LazilyClearsExpiredRateLimit(t *testing.T) {
	// 速率冷却过期后，IsTripped 应惰性清零 rateLimitUntil 并返回 false（Closed 状态）。
	channelID, keyID := 9003, 1
	model := "rl-test-lazy-clear"

	// 手动构造一个已过期的速率冷却条目
	key := circuitKey(channelID, keyID, model)
	entry := &circuitEntry{State: StateClosed, rateLimitUntil: time.Now().Add(-time.Second)}
	globalBreaker.Store(key, entry)

	tripped, _ := IsTripped(channelID, keyID, model)
	if tripped {
		t.Fatal("expired rateLimitUntil should not trip")
	}

	entry.mu.Lock()
	if !entry.rateLimitUntil.IsZero() {
		t.Fatal("expired rateLimitUntil should be lazily cleared to zero")
	}
	entry.mu.Unlock()
}

func TestRecordSuccess_ClearsRateLimit(t *testing.T) {
	channelID, keyID := 9004, 1
	model := "rl-test-success-clear"

	RecordRateLimit(channelID, keyID, model)
	tripped, _ := IsTripped(channelID, keyID, model)
	if !tripped {
		t.Fatal("should be tripped right after RecordRateLimit")
	}

	RecordSuccess(channelID, keyID, model)

	tripped, _ = IsTripped(channelID, keyID, model)
	if tripped {
		t.Fatal("after RecordSuccess, IsTripped should be false (rateLimit cleared)")
	}

	// 直接确认字段清零
	key := circuitKey(channelID, keyID, model)
	v, _ := globalBreaker.Load(key)
	entry := v.(*circuitEntry)
	entry.mu.Lock()
	if !entry.rateLimitUntil.IsZero() {
		t.Fatal("RecordSuccess should clear rateLimitUntil")
	}
	entry.mu.Unlock()
}

func TestRecordRateLimit_PreservesExistingFailures(t *testing.T) {
	// 若 key 已有 ConsecutiveFailures（但未达阈值熔断），RecordRateLimit 不应清零或累加它。
	channelID, keyID := 9005, 1
	model := "rl-test-preserve-failures"

	key := circuitKey(channelID, keyID, model)
	entry := &circuitEntry{State: StateClosed, ConsecutiveFailures: 3}
	globalBreaker.Store(key, entry)

	RecordRateLimit(channelID, keyID, model)

	entry.mu.Lock()
	if entry.ConsecutiveFailures != 3 {
		t.Fatalf("ConsecutiveFailures = %d, want 3 (preserved)", entry.ConsecutiveFailures)
	}
	if entry.rateLimitUntil.IsZero() {
		t.Fatal("rateLimitUntil should be set")
	}
	entry.mu.Unlock()
}