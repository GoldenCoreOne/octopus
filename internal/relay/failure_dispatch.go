package relay

import "errors"

// failureClass 描述一次失败尝试应走的熔断/速率冷却处置路径。
//
// 抽取自 attempt() 失败分支的决策逻辑，便于对每个业务场景做深度断言：
// 11210→rateLimit、未识别厂商码/网络错误→recordFailure、503(开启重试)→短路。
// 副作用（RecordFailure/RecordRateLimit/设置 key 字段）仍在 attempt() 内执行，
// 但绑定关系由本分类显式驱动，消除 attempt() 内嵌的 errors.As + shouldRetry503 复合判定。
type failureClass int

const (
	// classRetry503ShortCircuit 503 且渠道开启重试：不 RecordFailure/不 RecordRateLimit。
	// 保护开启重试的渠道不被反复 503 熔断；503 由 forward() 重试，exhaust 也不累计。
	classRetry503ShortCircuit failureClass = iota
	// classRecordRateLimit 命中瞬时速率错误（如讯飞 11210）：走即时 60s 速率冷却，
	// 不触碰熔断状态机。attempt() 据此设 key.RateLimitedUntil + 清零 StatusCode + RecordRateLimit。
	classRecordRateLimit
	// classRecordFailure 普通失败（未识别厂商码 / 非 *upstreamError 网络错误）：
	// 走既有熔断失败路径 RecordFailure，行为不变。
	classRecordFailure
)

// classifyFailure 纯函数：根据上游状态码、转发错误、是否开启 503 重试，决定失败处置分类。
//
// 决策规则（与 attempt() 原有内嵌逻辑严格等价）：
//  1. shouldRetry503 matched==true（503 + 开启重试 + 未写客户端）→ classRetry503ShortCircuit；
//  2. 否则 errors.As(*upstreamError) 命中 → 按 recordFailureAction 的 vendorCodePolicy 分派：
//     厂商码命中 policyRecordRateLimit → classRecordRateLimit，否则 classRecordFailure；
//  3. 否则（非 *upstreamError，如网络/构造/body 读取错误）→ classRecordFailure。
//
// 返回 vendorCode 供 attempt() 用于 503 短路分支的日志统计（非 503 路径上等价于解析出的厂商码）。
// 纯函数无副作用，单元测试可对所有业务场景做断言而无需 mock balancer/op/gin。
func classifyFailure(statusCode int, fwdErr error, retryOn503 bool) (class failureClass, vendorCode int) {
	_, _, matched := shouldRetry503(statusCode, retryOn503, false, 0, 0)
	if matched {
		// 503 短路：仍尝试解包厂商码用于日志，但分类固定为短路
		var ue *upstreamError
		if errors.As(fwdErr, &ue) && ue != nil {
			return classRetry503ShortCircuit, ue.VendorCode
		}
		return classRetry503ShortCircuit, 0
	}

	var ue *upstreamError
	if !errors.As(fwdErr, &ue) {
		// 网络/构造/body 读取错误：非 *upstreamError → 既有 RecordFailure
		return classRecordFailure, 0
	}
	if ue == nil {
		return classRecordFailure, 0
	}

	// 复用 recordFailureAction 的策略映射，保证分类与实际分派一致
	policy := lookupVendorPolicy(ue.VendorCode)
	switch policy {
	case policyRecordRateLimit:
		return classRecordRateLimit, ue.VendorCode
	default:
		return classRecordFailure, ue.VendorCode
	}
}

// lookupVendorPolicy 查询厂商码策略映射，未命中返回默认 policyRecordFailure。
// 抽出为单独函数便于 classifyFailure 复用且不触发 recordFailureAction 的闭包副作用。
func lookupVendorPolicy(vendorCode int) rateLimitPolicy {
	policy, ok := vendorCodePolicy[vendorCode]
	if !ok {
		return policyRecordFailure
	}
	return policy
}