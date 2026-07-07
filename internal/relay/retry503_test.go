package relay

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

// ===== resolveMax503Retries =====

func TestResolveMax503Retries_NilReturnsDefault12(t *testing.T) {
	if got := resolveMax503Retries(nil); got != 12 {
		t.Fatalf("resolveMax503Retries(nil) = %d, want 12 (default)", got)
	}
}

func TestResolveMax503Retries_ZeroMeansInfinite(t *testing.T) {
	v := 0
	if got := resolveMax503Retries(&v); got != 0 {
		t.Fatalf("resolveMax503Retries(0) = %d, want 0 (infinite)", got)
	}
}

func TestResolveMax503Retries_PositivePassedThrough(t *testing.T) {
	cases := []int{1, 5, 12, 100}
	for _, v := range cases {
		v := v
		t.Run("", func(t *testing.T) {
			if got := resolveMax503Retries(&v); got != v {
				t.Fatalf("resolveMax503Retries(%d) = %d, want %d", v, got, v)
			}
		})
	}
}

// ===== retryOn503Enabled =====

func TestRetryOn503Enabled_NilDisabled(t *testing.T) {
	ch := &model.Channel{RetryOn503: nil}
	if retryOn503Enabled(ch) {
		t.Fatal("retryOn503Enabled(nil) should be false (未配置)")
	}
}

func TestRetryOn503Enabled_ZeroDisabled(t *testing.T) {
	zero := 0
	ch := &model.Channel{RetryOn503: &zero}
	if retryOn503Enabled(ch) {
		t.Fatal("retryOn503Enabled(0) should be false (显式关闭)")
	}
}

func TestRetryOn503Enabled_OneEnabled(t *testing.T) {
	one := 1
	ch := &model.Channel{RetryOn503: &one}
	if !retryOn503Enabled(ch) {
		t.Fatal("retryOn503Enabled(1) should be true (开启)")
	}
}

// ===== shouldRetry503 分支全覆盖 =====
//
// 决策表（statusCode, retryEnabled, written, maxRetries, retries）→ (retry, exhaust, matched)
// 业务约束：
//   - "503不返回给客户端"：只要可重试就 retry=true
//   - "抢占式0秒无限"：maxRetries==0 永不 exhaust
//   - "达到最大次数后可返回503"：retries>=maxRetries(>0) 时 exhaust=true

func TestShouldRetry503_Non503NeverRetries(t *testing.T) {
	// 非 503 状态码一律不重试、不耗尽、不匹配
	codes := []int{200, 201, 301, 400, 401, 404, 429, 500, 502, 504}
	for _, code := range codes {
		code := code
		t.Run("", func(t *testing.T) {
			retry, exhaust, matched := shouldRetry503(code, true, false, 12, 0)
			if retry || exhaust || matched {
				t.Fatalf("non-503 code=%d: retry=%v exhaust=%v matched=%v, want all false", code, retry, exhaust, matched)
			}
		})
	}
}

func TestShouldRetry503_503ButRetryDisabled(t *testing.T) {
	// 503 但未开启重试：交给下游处理，不重试
	retry, exhaust, matched := shouldRetry503(503, false, false, 12, 0)
	if retry || exhaust || matched {
		t.Fatalf("503 retryEnabled=false: retry=%v exhaust=%v matched=%v, want all false", retry, exhaust, matched)
	}
}

func TestShouldRetry503_503ButAlreadyWritten(t *testing.T) {
	// 503 且开启重试，但已向客户端写响应：不可重试（HTTP 语义边界）
	retry, exhaust, matched := shouldRetry503(503, true, true, 12, 0)
	if retry || exhaust || matched {
		t.Fatalf("503 written=true: retry=%v exhaust=%v matched=%v, want all false (不可重试)", retry, exhaust, matched)
	}
}

func TestShouldRetry503_InfiniteNeverExhausts(t *testing.T) {
	// maxRetries==0 表示无限重试：无论 retries 多大都不耗尽
	for _, retries := range []int{0, 1, 5, 100, 999999} {
		retries := retries
		t.Run("", func(t *testing.T) {
			retry, exhaust, matched := shouldRetry503(503, true, false, 0, retries)
			if !retry {
				t.Fatalf("infinite: retries=%d retry=false, want true", retries)
			}
			if exhaust {
				t.Fatalf("infinite: retries=%d exhaust=true, want false", retries)
			}
			if !matched {
				t.Fatalf("infinite: retries=%d matched=false, want true", retries)
			}
		})
	}
}

func TestShouldRetry503_BoundedRetriesBelowLimit(t *testing.T) {
	// 未达上限：继续重试
	cases := []struct{ max, retries int }{
		{12, 0}, {12, 5}, {12, 11},
		{1, 0},
		{3, 2},
	}
	for _, c := range cases {
		c := c
		t.Run("", func(t *testing.T) {
			retry, exhaust, matched := shouldRetry503(503, true, false, c.max, c.retries)
			if !retry {
				t.Fatalf("below limit: max=%d retries=%d retry=false, want true", c.max, c.retries)
			}
			if exhaust {
				t.Fatalf("below limit: max=%d retries=%d exhaust=true, want false", c.max, c.retries)
			}
			if !matched {
				t.Fatalf("below limit: max=%d retries=%d matched=false, want true", c.max, c.retries)
			}
		})
	}
}

func TestShouldRetry503_BoundedRetriesAtLimitExhausts(t *testing.T) {
	// 达到上限：停止重试，把 503 交给下游返回客户端
	cases := []struct{ max, retries int }{
		{12, 12}, {12, 13}, // 等于与超过均算耗尽
		{1, 1},
		{5, 5},
	}
	for _, c := range cases {
		c := c
		t.Run("", func(t *testing.T) {
			retry, exhaust, matched := shouldRetry503(503, true, false, c.max, c.retries)
			if retry {
				t.Fatalf("at/over limit: max=%d retries=%d retry=true, want false", c.max, c.retries)
			}
			if !exhaust {
				t.Fatalf("at/over limit: max=%d retries=%d exhaust=false, want true", c.max, c.retries)
			}
			if !matched {
				t.Fatalf("at/over limit: max=%d retries=%d matched=false, want true", c.max, c.retries)
			}
		})
	}
}

func TestShouldRetry503_MatchedOnlyForRetryable503(t *testing.T) {
	// matched 必须仅在 (503 && retryEnabled && !written) 时为 true
	type tc struct {
		code     int
		enabled  bool
		written  bool
		wantMatch bool
	}
	cases := []tc{
		{503, true, false, true},
		{503, false, false, false},
		{503, true, true, false},
		{200, true, false, false},
	}
	for _, c := range cases {
		c := c
		t.Run("", func(t *testing.T) {
			_, _, matched := shouldRetry503(c.code, c.enabled, c.written, 12, 0)
			if matched != c.wantMatch {
				t.Fatalf("matched: code=%d enabled=%v written=%v got=%v want=%v",
					c.code, c.enabled, c.written, matched, c.wantMatch)
			}
		})
	}
}

// 业务深度断言：抢点式 0 秒语义 — 无限模式下 retry 始终为 true，永不 exhaust。
func TestShouldRetry503_PreemptiveZeroDelayInfiniteSemantics(t *testing.T) {
	retry, exhaust, _ := shouldRetry503(503, true, false, 0, 0)
	if !retry {
		t.Fatal("抢占式 0 秒：无限模式下首次 503 必须 retry=true")
	}
	if exhaust {
		t.Fatal("抢占式 0 秒：无限模式下永不 exhaust")
	}
}

// 业务深度断言：默认上限 12 — 第 12 次重试后第 13 次 503 应 exhaust（返回 503 给客户端）。
func TestShouldRetry503_Default12ExhaustsAfter12Retries(t *testing.T) {
	max := resolveMax503Retries(nil) // 默认 12
	if max != 12 {
		t.Fatalf("default max must be 12, got %d", max)
	}
	// 已重试 12 次后再次 503 → 耗尽
	retry, exhaust, _ := shouldRetry503(503, true, false, max, max)
	if retry {
		t.Fatal("达到默认 12 次上限后不应继续重试")
	}
	if !exhaust {
		t.Fatal("达到默认 12 次上限后应 exhaust，允许 503 返回客户端")
	}
}