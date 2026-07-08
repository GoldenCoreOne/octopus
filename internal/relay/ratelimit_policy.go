package relay

// rateLimitPolicy 描述对某个厂商业务错误码（VendorCode）应采取的处置策略。
type rateLimitPolicy int

const (
	// policyRecordFailure 走既有熔断失败路径（RecordFailure），累计 ConsecutiveFailures，
	// 触达阈值后熔断。用于非瞬时速率错误或未识别错误。
	policyRecordFailure rateLimitPolicy = iota
	// policyRecordRateLimit 走即时速率冷却路径（RecordRateLimit + 设置 key 的 RateLimitedUntil），
	// 不触碰熔断器的 State/TripCount/ConsecutiveFailures。用于瞬时 tpm 限流等"等待后可恢复"的错误。
	policyRecordRateLimit
)

// vendorCodePolicy 厂商业务错误码 → 处置策略映射。
//
// 当前仅收敛讯飞 11210（tpm 超限，429，官方建议"等待后重试"）到即时速率冷却。
// 保留 map 结构便于后续扩展其它厂商/错误码（如 10310 服务忙的差异化处置），
// 而不必改动 recordFailureAction 的调用点。
var vendorCodePolicy = map[int]rateLimitPolicy{
	11210: policyRecordRateLimit,
}

// recordFailureAction 是纯函数调度器：根据厂商业务错误码选择处置策略，
// 并调用对应的闭包执行副作用。
//
// 设计为纯函数 + 闭包注入，便于单元测试用计数器断言分支选择，
// 无需依赖 balancer / op 的全局状态。
//
//   - vendorCode 命中 policyRecordRateLimit → 调 recordRateLimit；
//   - 其余（未识别 / policyRecordFailure）→ 调 recordFailure。
//
// 两个闭包参数均为 func()，由调用方在 relay.go 中绑定到
// balancer.RecordRateLimit(...) 与 balancer.RecordFailure(...)。
func recordFailureAction(vendorCode int, recordFailure, recordRateLimit func()) rateLimitPolicy {
	policy := lookupVendorPolicy(vendorCode)
	switch policy {
	case policyRecordRateLimit:
		if recordRateLimit != nil {
			recordRateLimit()
		}
	case policyRecordFailure:
		if recordFailure != nil {
			recordFailure()
		}
	}
	return policy
}