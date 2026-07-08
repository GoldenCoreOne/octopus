package relay

import "testing"

func TestRecordFailureAction_11210_TriggersRecordRateLimit(t *testing.T) {
	rfCount := 0
	rlCount := 0
	recordFailure := func() { rfCount++ }
	recordRateLimit := func() { rlCount++ }

	policy := recordFailureAction(11210, recordFailure, recordRateLimit)
	if policy != policyRecordRateLimit {
		t.Fatalf("policy = %v, want policyRecordRateLimit", policy)
	}
	if rlCount != 1 {
		t.Fatalf("recordRateLimit called %d times, want 1", rlCount)
	}
	if rfCount != 0 {
		t.Fatalf("recordFailure should not be called, got %d", rfCount)
	}
}

func TestRecordFailureAction_UnknownCode_TriggersRecordFailure(t *testing.T) {
	rfCount := 0
	rlCount := 0
	recordFailure := func() { rfCount++ }
	recordRateLimit := func() { rlCount++ }

	// 10310 不在 vendorCodePolicy 映射中 → 默认 RecordFailure
	policy := recordFailureAction(10310, recordFailure, recordRateLimit)
	if policy != policyRecordFailure {
		t.Fatalf("policy = %v, want policyRecordFailure", policy)
	}
	if rfCount != 1 {
		t.Fatalf("recordFailure called %d times, want 1", rfCount)
	}
	if rlCount != 0 {
		t.Fatalf("recordRateLimit should not be called, got %d", rlCount)
	}
}

func TestRecordFailureAction_ZeroCode_TriggersRecordFailure(t *testing.T) {
	// 未识别错误（VendorCode=0）→ RecordFailure
	rfCount := 0
	rlCount := 0
	recordFailure := func() { rfCount++ }
	recordRateLimit := func() { rlCount++ }

	policy := recordFailureAction(0, recordFailure, recordRateLimit)
	if policy != policyRecordFailure {
		t.Fatalf("policy = %v, want policyRecordFailure", policy)
	}
	if rfCount != 1 {
		t.Fatalf("recordFailure called %d times, want 1", rfCount)
	}
}

func TestRecordFailureAction_NilClosuresNoPanic(t *testing.T) {
	// 闭包为 nil 不应 panic
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked with nil closures: %v", r)
		}
	}()
	_ = recordFailureAction(11210, nil, nil)
	_ = recordFailureAction(999, nil, nil)
}

func TestVendorCodePolicy_Has11210Entry(t *testing.T) {
	p, ok := vendorCodePolicy[11210]
	if !ok {
		t.Fatal("vendorCodePolicy should contain 11210")
	}
	if p != policyRecordRateLimit {
		t.Fatalf("11210 policy = %v, want policyRecordRateLimit", p)
	}
}