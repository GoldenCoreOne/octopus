package relay

import (
	"errors"
	"fmt"
	"testing"
)

// classifyFailure 业务深度断言：覆盖 11210 即时冷却方案的全部失败处置路径。
// 每个用例断言 (class, vendorCode) 而非仅调用路径，确保业务语义正确。

// --- 503 短路分支 ---

func TestClassifyFailure_503RetryEnabled_ShortCircuit(t *testing.T) {
	// 503 + 开启重试：分类为短路，不 RecordFailure/不 RecordRateLimit。
	// 业务含义：开启 503 重试的渠道，503 由 forward() 重试，不熔断。
	ue := &upstreamError{StatusCode: 503, VendorCode: 10310, Body: []byte(`{"error":{"code":10310}}`)}
	class, vc := classifyFailure(503, ue, true)
	if class != classRetry503ShortCircuit {
		t.Fatalf("class = %v, want classRetry503ShortCircuit", class)
	}
	// 厂商码仍解析出来用于日志统计（即使 503 短路）
	if vc != 10310 {
		t.Fatalf("vendorCode = %d, want 10310 (for 503 exhaust logging)", vc)
	}
}

func TestClassifyFailure_503RetryEnabled_NoVendorCode(t *testing.T) {
	// 503 + 开启重试 + 非 *upstreamError（理论上 503 总是 upstreamError，但防御性）：
	// 仍短路，vendorCode=0。
	class, vc := classifyFailure(503, fmt.Errorf("network 503"), true)
	if class != classRetry503ShortCircuit {
		t.Fatalf("class = %v, want classRetry503ShortCircuit", class)
	}
	if vc != 0 {
		t.Fatalf("vendorCode = %d, want 0", vc)
	}
}

func TestClassifyFailure_503RetryDisabled_RecordFailure(t *testing.T) {
	// 503 + 关闭重试：不进短路，作为上游错误走分派。
	// 503 的厂商码（如 10310）未命中 vendorCodePolicy → classRecordFailure。
	// 业务含义：关闭重试的渠道，503 视为普通失败，累计熔断。
	ue := &upstreamError{StatusCode: 503, VendorCode: 10310, Body: []byte(`{"error":{"code":10310}}`)}
	class, vc := classifyFailure(503, ue, false)
	if class != classRecordFailure {
		t.Fatalf("class = %v, want classRecordFailure (503 with retry off = normal failure)", class)
	}
	if vc != 10310 {
		t.Fatalf("vendorCode = %d, want 10310", vc)
	}
}

// --- 11210 即时速率冷却分支（K-2 核心）---

func TestClassifyFailure_11210_RecordRateLimit(t *testing.T) {
	// 讯飞 11210 (TPM 超限, 429)：分类为 RecordRateLimit，触发即时 60s 冷却。
	// 这是整个方案的核心断言：11210 不走 RecordFailure，避免累计到 5 次熔断。
	ue := &upstreamError{StatusCode: 429, VendorCode: 11210, Body: []byte(`{"error":{"code":11210}}`)}
	class, vc := classifyFailure(429, ue, false)
	if class != classRecordRateLimit {
		t.Fatalf("class = %v, want classRecordRateLimit (11210 → immediate 60s cooldown)", class)
	}
	if vc != 11210 {
		t.Fatalf("vendorCode = %d, want 11210", vc)
	}
}

func TestClassifyFailure_11210_RetryOn503DoesNotAffect429(t *testing.T) {
	// 11210 是 429，不是 503；即使渠道开启 503 重试，shouldRetry503 也不 match（statusCode!=503）。
	// 故仍走 RecordRateLimit，503 重试开关不影响 11210 处置。
	ue := &upstreamError{StatusCode: 429, VendorCode: 11210, Body: []byte(`{"error":{"code":11210}}`)}
	class, _ := classifyFailure(429, ue, true)
	if class != classRecordRateLimit {
		t.Fatalf("class = %v, want classRecordRateLimit (retry_on_503 must not shadow 11210)", class)
	}
}

// --- 未识别厂商码分支 ---

func TestClassifyFailure_UnknownVendorCode_RecordFailure(t *testing.T) {
	// 10310 (服务忙) 不在 vendorCodePolicy → classRecordFailure，行为不变。
	ue := &upstreamError{StatusCode: 503, VendorCode: 10310, Body: []byte(`{"error":{"code":10310}}`)}
	class, vc := classifyFailure(503, ue, false)
	if class != classRecordFailure {
		t.Fatalf("class = %v, want classRecordFailure (unknown vendor code)", class)
	}
	if vc != 10310 {
		t.Fatalf("vendorCode = %d, want 10310", vc)
	}
}

func TestClassifyFailure_ZeroVendorCode_RecordFailure(t *testing.T) {
	// VendorCode=0（body 无 error.code 或非 JSON）→ classRecordFailure。
	ue := &upstreamError{StatusCode: 500, VendorCode: 0, Body: []byte(`internal error`)}
	class, vc := classifyFailure(500, ue, false)
	if class != classRecordFailure {
		t.Fatalf("class = %v, want classRecordFailure (unidentified upstream error)", class)
	}
	if vc != 0 {
		t.Fatalf("vendorCode = %d, want 0", vc)
	}
}

// --- 非 *upstreamError 分支（网络/构造/body 读取错误）---

func TestClassifyFailure_NetworkError_RecordFailure(t *testing.T) {
	// 非 *upstreamError（网络错误）：走既有 RecordFailure，vendorCode=0。
	// 业务含义：网络层错误不计入速率冷却，按普通失败熔断。
	err := fmt.Errorf("failed to send request: dial tcp: connection refused")
	class, vc := classifyFailure(0, err, false)
	if class != classRecordFailure {
		t.Fatalf("class = %v, want classRecordFailure (network error)", class)
	}
	if vc != 0 {
		t.Fatalf("vendorCode = %d, want 0 (network error has no vendor code)", vc)
	}
}

func TestClassifyFailure_WrappedNetworkError_RecordFailure(t *testing.T) {
	// 被 fmt.Errorf("%w") 包装的网络错误：errors.As(*upstreamError) 不命中 → classRecordFailure。
	// 验证错误包装链不误判（forward() 的网络错误都用 %w 包装）。
	inner := fmt.Errorf("failed to read response body: EOF")
	wrapped := fmt.Errorf("channel forward: %w", inner)
	class, _ := classifyFailure(0, wrapped, false)
	if class != classRecordFailure {
		t.Fatalf("class = %v, want classRecordFailure (wrapped non-upstream error)", class)
	}
}

func TestClassifyFailure_WrappedUpstreamError_StillUnwraps(t *testing.T) {
	// 被 %w 包装的 *upstreamError：errors.As 应穿透包装命中厂商码分派。
	// 业务含义：即使 attempt() 把 fwdErr 再包一层，11210 仍被正确识别。
	ue := &upstreamError{StatusCode: 429, VendorCode: 11210, Body: []byte(`{"error":{"code":11210}}`)}
	wrapped := fmt.Errorf("forward failed: %w", ue)
	class, vc := classifyFailure(429, wrapped, false)
	if class != classRecordRateLimit {
		t.Fatalf("class = %v, want classRecordRateLimit (wrapped 11210 must unwrap)", class)
	}
	if vc != 11210 {
		t.Fatalf("vendorCode = %d, want 11210", vc)
	}
}

// --- 边界：nil upstreamError 指针 ---

func TestClassifyFailure_NilUpstreamErrorPointer_RecordFailure(t *testing.T) {
	// errors.As 命中但解包出 nil 指针：防御性归为 classRecordFailure。
	// 构造一个返回 nil *upstreamError 的 error（通过自定义 error 类型）。
	var nilUE *upstreamError
	class, vc := classifyFailure(500, nilUE, false)
	if class != classRecordFailure {
		t.Fatalf("class = %v, want classRecordFailure (nil upstreamError pointer)", class)
	}
	if vc != 0 {
		t.Fatalf("vendorCode = %d, want 0", vc)
	}
}

// --- errors.As 标准库兼容性自检 ---

func TestClassifyFailure_StdErrorsAsCompat(t *testing.T) {
	// 确认 errors.As 对 *upstreamError 的标准解包行为（与 attempt() 内一致）。
	ue := &upstreamError{StatusCode: 429, VendorCode: 11210}
	var target *upstreamError
	if !errors.As(ue, &target) {
		t.Fatal("errors.As should match *upstreamError")
	}
	if target.VendorCode != 11210 {
		t.Fatalf("unwrapped VendorCode = %d, want 11210", target.VendorCode)
	}
}

// --- lookupVendorPolicy 直接断言 ---

func TestLookupVendorPolicy_11210(t *testing.T) {
	if p := lookupVendorPolicy(11210); p != policyRecordRateLimit {
		t.Fatalf("lookupVendorPolicy(11210) = %v, want policyRecordRateLimit", p)
	}
}

func TestLookupVendorPolicy_UnknownDefault(t *testing.T) {
	if p := lookupVendorPolicy(99999); p != policyRecordFailure {
		t.Fatalf("lookupVendorPolicy(99999) = %v, want policyRecordFailure (default)", p)
	}
	if p := lookupVendorPolicy(0); p != policyRecordFailure {
		t.Fatalf("lookupVendorPolicy(0) = %v, want policyRecordFailure (default)", p)
	}
}