package model

import (
	"testing"
	"time"
)

// 这些测试覆盖 GetChannelKeyExcept 对 RateLimitedUntil 的优先检查（11210 即时冷却主战场）。
// 纯内存操作，无 DB 依赖。

func TestGetChannelKeyExcept_SkipsRateLimitedKey(t *testing.T) {
	ch := &Channel{
		Keys: []ChannelKey{
			{ID: 1, Enabled: true, ChannelKey: "k1", TotalCost: 1.0, RateLimitedUntil: time.Now().Add(30 * time.Second)},
			{ID: 2, Enabled: true, ChannelKey: "k2", TotalCost: 5.0},
		},
	}
	// k1 处于速率冷却（30s 后到期）→ 应选 k2（尽管 cost 更高）
	got := ch.GetChannelKeyExcept(nil)
	if got.ID != 2 {
		t.Fatalf("GetChannelKeyExcept = key %d, want 2 (k1 is rate-limited)", got.ID)
	}
}

func TestGetChannelKeyExcept_AllRateLimitedReturnsEmpty(t *testing.T) {
	ch := &Channel{
		Keys: []ChannelKey{
			{ID: 1, Enabled: true, ChannelKey: "k1", RateLimitedUntil: time.Now().Add(30 * time.Second)},
			{ID: 2, Enabled: true, ChannelKey: "k2", RateLimitedUntil: time.Now().Add(40 * time.Second)},
		},
	}
	got := ch.GetChannelKeyExcept(nil)
	if got.ChannelKey != "" {
		t.Fatalf("all rate-limited should return empty, got key %d", got.ID)
	}
}

func TestGetChannelKeyExcept_ExpiredRateLimitSelectable(t *testing.T) {
	// 速率冷却过期后 key 重新可选（验证 60s 精确语义：到期即放行）
	ch := &Channel{
		Keys: []ChannelKey{
			{ID: 1, Enabled: true, ChannelKey: "k1", TotalCost: 1.0, RateLimitedUntil: time.Now().Add(-time.Second)},
			{ID: 2, Enabled: true, ChannelKey: "k2", TotalCost: 5.0},
		},
	}
	got := ch.GetChannelKeyExcept(nil)
	if got.ID != 1 {
		t.Fatalf("expired rate-limit key should be selectable (lowest cost), got %d, want 1", got.ID)
	}
}

func TestGetChannelKeyExcept_RateLimitPriorityOver429SoftCooldown(t *testing.T) {
	// K-2 核心：11210 清零 StatusCode 后，60s 过期不再被 5min 429 软冷却二次遮蔽。
	// 模拟 11210 命中后的状态：StatusCode=0（已清零）+ RateLimitedUntil 未过期。
	ch := &Channel{
		Keys: []ChannelKey{
			{ID: 1, Enabled: true, ChannelKey: "k1", TotalCost: 1.0, StatusCode: 0, LastUseTimeStamp: time.Now().Unix(), RateLimitedUntil: time.Now().Add(30 * time.Second)},
			{ID: 2, Enabled: true, ChannelKey: "k2", TotalCost: 5.0},
		},
	}
	// k1 速率冷却未过期 → 跳过，选 k2
	got := ch.GetChannelKeyExcept(nil)
	if got.ID != 2 {
		t.Fatalf("rate-limited k1 should be skipped, got %d, want 2", got.ID)
	}

	// 速率冷却过期后：k1 StatusCode==0 → 429 软冷却检查不触发 → k1 重新可选
	ch.Keys[0].RateLimitedUntil = time.Now().Add(-time.Second)
	got = ch.GetChannelKeyExcept(nil)
	if got.ID != 1 {
		t.Fatalf("after rate-limit expiry with StatusCode=0, k1 should be selectable, got %d, want 1", got.ID)
	}
}

func TestGetChannelKeyExcept_Real429SoftCooldownStillWorks(t *testing.T) {
	// 非 11210 真 429：StatusCode 保留 429 → 5min 软冷却照常生效（行为不变）
	ch := &Channel{
		Keys: []ChannelKey{
			{ID: 1, Enabled: true, ChannelKey: "k1", TotalCost: 1.0, StatusCode: 429, LastUseTimeStamp: time.Now().Unix()},
			{ID: 2, Enabled: true, ChannelKey: "k2", TotalCost: 5.0},
		},
	}
	got := ch.GetChannelKeyExcept(nil)
	if got.ID != 2 {
		t.Fatalf("real 429 soft cooldown should skip k1, got %d, want 2", got.ID)
	}
}

func TestGetChannelKeyExcept_ZeroRateLimitedUntilNotSkipped(t *testing.T) {
	// 零值 RateLimitedUntil（未设置）不应被跳过
	ch := &Channel{
		Keys: []ChannelKey{
			{ID: 1, Enabled: true, ChannelKey: "k1", TotalCost: 1.0, RateLimitedUntil: time.Time{}},
		},
	}
	got := ch.GetChannelKeyExcept(nil)
	if got.ID != 1 {
		t.Fatalf("zero RateLimitedUntil should be selectable, got %d, want 1", got.ID)
	}
}