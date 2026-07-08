package balancer

import (
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

// 这些测试覆盖 circuit.go 的熔断状态机分支（IsTripped Open/HalfOpen、RecordFailure、
// RecordSuccess HalfOpen 日志），并验证 rateLimitUntil 与各 State 的正交交互。
//
// op.SettingGetInt 在测试环境未初始化 settingCache 时返回 error，
// getThreshold()/GetCooldown() 走默认值（阈值=5，base=60s，max=600s），
// 故无需 mock DB。每个测试用独立 channelID/keyID/modelName 避免 globalBreaker 污染。

// 用唯一 ID 段 9100+ 隔离本文件与 circuit_ratelimit_test.go（9001-9005）。
func freshEntry(channelID, keyID int, modelName string, state CircuitState, tripCount int, lastFailure time.Time) {
	key := circuitKey(channelID, keyID, modelName)
	globalBreaker.Store(key, &circuitEntry{
		State:           state,
		TripCount:       tripCount,
		LastFailureTime: lastFailure,
	})
}

// --- IsTripped Open 分支 ---

func TestIsTripped_Open_WithinCooldown_Tripped(t *testing.T) {
	// Open + 冷却未过期 → tripped=true，remaining>0
	channelID, keyID := 9101, 1
	model := "state-open-within"
	freshEntry(channelID, keyID, model, StateOpen, 1, time.Now()) // LastFailure=now → 冷却刚开始

	tripped, remaining := IsTripped(channelID, keyID, model)
	if !tripped {
		t.Fatal("Open within cooldown should be tripped")
	}
	if remaining <= 0 {
		t.Fatalf("remaining = %v, want > 0", remaining)
	}
}

func TestIsTripped_Open_CooldownElapsed_TransitionsToHalfOpen(t *testing.T) {
	// Open + 冷却已过期 → 转 HalfOpen，tripped=false（允许试探请求）
	channelID, keyID := 9102, 1
	model := "state-open-elapsed"
	// tripCount=1 → cooldown=60s；LastFailure 设为 61s 前 → 已过期
	freshEntry(channelID, keyID, model, StateOpen, 1, time.Now().Add(-61*time.Second))

	tripped, _ := IsTripped(channelID, keyID, model)
	if tripped {
		t.Fatal("Open with elapsed cooldown should transition to HalfOpen (not tripped)")
	}
	// 验证状态已转为 HalfOpen
	key := circuitKey(channelID, keyID, model)
	v, ok := globalBreaker.Load(key)
	if !ok {
		t.Fatal("entry should exist")
	}
	e := v.(*circuitEntry)
	e.mu.Lock()
	if e.State != StateHalfOpen {
		t.Fatalf("State = %v, want StateHalfOpen", e.State)
	}
	e.mu.Unlock()
}

// --- IsTripped HalfOpen 分支 ---

func TestIsTripped_HalfOpen_Tripped(t *testing.T) {
	// HalfOpen：已有试探请求进行中，拒绝其他请求 → tripped=true，remaining=0
	channelID, keyID := 9103, 1
	model := "state-halfopen"
	freshEntry(channelID, keyID, model, StateHalfOpen, 1, time.Now())

	tripped, remaining := IsTripped(channelID, keyID, model)
	if !tripped {
		t.Fatal("HalfOpen should be tripped (probe in flight)")
	}
	if remaining != 0 {
		t.Fatalf("remaining = %v, want 0 for HalfOpen", remaining)
	}
}

// --- IsTripped 无记录 ---

func TestIsTripped_NoEntry_NotTripped(t *testing.T) {
	tripped, remaining := IsTripped(9104, 1, "state-no-entry")
	if tripped {
		t.Fatal("no entry should not be tripped")
	}
	if remaining != 0 {
		t.Fatalf("remaining = %v, want 0", remaining)
	}
}

// --- IsTripped rateLimitUntil 与 State 正交：Open + rateLimitUntil 未过期 ---

func TestIsTripped_OpenWithRateLimit_RateLimitTakesPriority(t *testing.T) {
	// 业务深度断言：即便处于 Open 状态，rateLimitUntil 未过期时优先返回速率冷却剩余。
	// 这验证了 11210 即时冷却与熔断 Open 的正交性——速率冷却叠加在 Open 之上。
	channelID, keyID := 9105, 1
	modelName := "state-open-with-ratelimit"
	key := circuitKey(channelID, keyID, modelName)
	globalBreaker.Store(key, &circuitEntry{
		State:           StateOpen,
		TripCount:       1,
		LastFailureTime: time.Now(),
		rateLimitUntil:  time.Now().Add(30 * time.Second), // 速率冷却 30s 后到期
	})

	tripped, remaining := IsTripped(channelID, keyID, modelName)
	if !tripped {
		t.Fatal("Open + rateLimit should be tripped")
	}
	// 速率冷却剩余应 ~30s，而非 Open 的 60s 冷却剩余
	if remaining > 35*time.Second || remaining <= 25*time.Second {
		t.Fatalf("remaining = %v, want ~30s (rateLimit takes priority over Open cooldown)", remaining)
	}
}

// --- RecordFailure Closed 分支：累计到阈值熔断 ---

func TestRecordFailure_Closed_AccumulatesToThreshold(t *testing.T) {
	// 默认阈值 5：连续 RecordFailure 5 次 → State Closed→Open，TripCount=1
	channelID, keyID := 9106, 1
	modelName := "rf-closed-accumulate"

	for i := 0; i < 5; i++ {
		RecordFailure(channelID, keyID, modelName)
	}

	key := circuitKey(channelID, keyID, modelName)
	v, ok := globalBreaker.Load(key)
	if !ok {
		t.Fatal("entry should exist after RecordFailure")
	}
	e := v.(*circuitEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.State != StateOpen {
		t.Fatalf("State = %v, want StateOpen (after 5 failures)", e.State)
	}
	if e.TripCount != 1 {
		t.Fatalf("TripCount = %d, want 1", e.TripCount)
	}
	if e.ConsecutiveFailures != 5 {
		t.Fatalf("ConsecutiveFailures = %d, want 5", e.ConsecutiveFailures)
	}
}

func TestRecordFailure_Closed_BelowThresholdStaysClosed(t *testing.T) {
	// 4 次失败（< 阈值 5）→ 仍 Closed，不熔断
	channelID, keyID := 9107, 1
	modelName := "rf-closed-below"

	for i := 0; i < 4; i++ {
		RecordFailure(channelID, keyID, modelName)
	}
	key := circuitKey(channelID, keyID, modelName)
	v, _ := globalBreaker.Load(key)
	e := v.(*circuitEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.State != StateClosed {
		t.Fatalf("State = %v, want StateClosed (below threshold)", e.State)
	}
	if e.ConsecutiveFailures != 4 {
		t.Fatalf("ConsecutiveFailures = %d, want 4", e.ConsecutiveFailures)
	}
}

// --- RecordFailure HalfOpen 分支：试探失败回 Open ---

func TestRecordFailure_HalfOpen_BackToOpenTripCountIncrements(t *testing.T) {
	// HalfOpen + RecordFailure → 回 Open，TripCount++，ConsecutiveFailures 清零
	channelID, keyID := 9108, 1
	modelName := "rf-halfopen"
	freshEntry(channelID, keyID, modelName, StateHalfOpen, 1, time.Now())

	RecordFailure(channelID, keyID, modelName)

	key := circuitKey(channelID, keyID, modelName)
	v, _ := globalBreaker.Load(key)
	e := v.(*circuitEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.State != StateOpen {
		t.Fatalf("State = %v, want StateOpen (HalfOpen probe failed)", e.State)
	}
	if e.TripCount != 2 {
		t.Fatalf("TripCount = %d, want 2 (incremented)", e.TripCount)
	}
	if e.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0 (reset on HalfOpen->Open)", e.ConsecutiveFailures)
	}
}

// --- RecordFailure Open 分支：仅更新失败时间 ---

func TestRecordFailure_Open_OnlyUpdatesLastFailureTime(t *testing.T) {
	// Open 状态下 RecordFailure（理论上不应发生，但兜底）：不改变 TripCount/State，更新 LastFailureTime
	channelID, keyID := 9109, 1
	modelName := "rf-open"
	oldLast := time.Now().Add(-10 * time.Second)
	freshEntry(channelID, keyID, modelName, StateOpen, 2, oldLast)

	RecordFailure(channelID, keyID, modelName)

	key := circuitKey(channelID, keyID, modelName)
	v, _ := globalBreaker.Load(key)
	e := v.(*circuitEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.State != StateOpen {
		t.Fatalf("State = %v, want StateOpen (unchanged)", e.State)
	}
	if e.TripCount != 2 {
		t.Fatalf("TripCount = %d, want 2 (unchanged)", e.TripCount)
	}
	if !e.LastFailureTime.After(oldLast) {
		t.Fatal("LastFailureTime should be updated to now")
	}
}

// --- RecordSuccess HalfOpen → Closed 日志分支 ---

func TestRecordSuccess_HalfOpenToClosed_LogsAndResets(t *testing.T) {
	// HalfOpen + RecordSuccess → Closed，TripCount/ConsecutiveFailures/rateLimitUntil 全清零
	channelID, keyID := 9110, 1
	modelName := "rs-halfopen"
	freshEntry(channelID, keyID, modelName, StateHalfOpen, 2, time.Now())
	// 额外设 rateLimitUntil 验证 RecordSuccess 同时清零它
	key := circuitKey(channelID, keyID, modelName)
	v, _ := globalBreaker.Load(key)
	e := v.(*circuitEntry)
	e.mu.Lock()
	e.ConsecutiveFailures = 3
	e.rateLimitUntil = time.Now().Add(30 * time.Second)
	e.mu.Unlock()

	RecordSuccess(channelID, keyID, modelName)

	v, _ = globalBreaker.Load(key)
	e = v.(*circuitEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.State != StateClosed {
		t.Fatalf("State = %v, want StateClosed (HalfOpen probe succeeded)", e.State)
	}
	if e.TripCount != 0 {
		t.Fatalf("TripCount = %d, want 0 (reset)", e.TripCount)
	}
	if e.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want 0 (reset)", e.ConsecutiveFailures)
	}
	if !e.rateLimitUntil.IsZero() {
		t.Fatal("rateLimitUntil should be cleared by RecordSuccess")
	}
}

// --- RecordSuccess 无记录：no-op ---

func TestRecordSuccess_NoEntry_NoOp(t *testing.T) {
	// 无记录时 RecordSuccess 不创建条目、不 panic
	RecordSuccess(9111, 1, "rs-no-entry")
	if _, ok := globalBreaker.Load(circuitKey(9111, 1, "rs-no-entry")); ok {
		t.Fatal("RecordSuccess on no entry should not create one")
	}
}

// --- getThreshold / GetCooldown 默认值分支（setting 未初始化走默认） ---

func TestGetThreshold_DefaultWhenSettingMissing(t *testing.T) {
	// op.SettingGetInt 在测试环境未初始化 → error → 默认阈值 5
	// 同时验证 op 包可被 balancer 测试 import（无循环依赖）
	if got := getThreshold(); got != 5 {
		t.Fatalf("getThreshold() = %d, want 5 (default when setting missing)", got)
	}
	// 顺带断言 op.SettingGetInt 在未初始化时返回 error（覆盖其 error 路径间接语义）
	if _, err := op.SettingGetInt(model.SettingKeyCircuitBreakerThreshold); err == nil {
		t.Fatal("SettingGetInt should error when settingCache not initialized")
	}
}

func TestGetCooldown_DefaultWhenSettingMissing(t *testing.T) {
	// tripCount=1 → cooldown=base=60s（默认）；tripCount=2 → 60<<1=120s；
	// tripCount=4 → 60<<3=480s；tripCount=5 → 60<<4=960 > 600 → 截断 600s
	cases := []struct {
		tripCount int
		want      time.Duration
	}{
		{1, 60 * time.Second},
		{2, 120 * time.Second},
		{4, 480 * time.Second},
		{5, 600 * time.Second}, // 截断 maxCooldown
		{20, 600 * time.Second},
	}
	for _, c := range cases {
		if got := GetCooldown(c.tripCount); got != c.want {
			t.Fatalf("GetCooldown(%d) = %v, want %v", c.tripCount, got, c.want)
		}
	}
}

func TestGetCooldown_ShiftOverflowCapped(t *testing.T) {
	// tripCount 极大 → shift 截断为 20，cooldown 截断为 maxCooldown=600s，无溢出
	if got := GetCooldown(100); got != 600*time.Second {
		t.Fatalf("GetCooldown(100) = %v, want 600s (shift capped, no overflow)", got)
	}
}