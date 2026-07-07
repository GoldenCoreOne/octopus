package relay

import "github.com/bestruirui/octopus/internal/model"

// resolveMax503Retries 将 *int 三态映射为重试次数：
// nil → 12（默认值），0 → 0（无限），>0 → 原值
func resolveMax503Retries(v *int) int {
	if v == nil {
		return 12
	}
	return *v
}

// retryOn503Enabled 检查渠道是否开启了 503 自动重试
func retryOn503Enabled(ch *model.Channel) bool {
	return ch.RetryOn503 != nil && *ch.RetryOn503 == 1
}

// shouldRetry503 是 503 重试决策的纯函数，集中所有分支逻辑以便单元测试覆盖。
// 参数：
//   - statusCode: 上游返回的状态码
//   - retryEnabled: 渠道是否开启 503 重试（retryOn503Enabled(channel)）
//   - written: 是否已向客户端写入响应头/体（c.Writer.Written()）
//   - maxRetries: 解析后的重试上限（resolveMax503Retries；0 表示无限）
//   - retries: 已重试次数
//
// 返回：
//   - retry: 是否应继续重试（true 表示立即丢弃响应体重发）
//   - exhaust: 是否已达到上限、应停止重试并把 503 交给下游返回给客户端
//   - matched: 是否为"可重试的 503 情形"（retryEnabled && statusCode==503 && !written）
//     用于 attempt() 决定是否跳过 RecordFailure
func shouldRetry503(statusCode int, retryEnabled, written bool, maxRetries, retries int) (retry, exhaust, matched bool) {
	// 只有 503 + 已开启重试 + 尚未写客户端 才进入重试语义
	if statusCode != 503 || !retryEnabled || written {
		return false, false, false
	}
	// 此处为可重试的 503 情形：attempt() 应跳过 RecordFailure
	matched = true
	// maxRetries==0 表示无限重试，永远不耗尽
	if maxRetries == 0 {
		return true, false, true
	}
	// 达到上限：停止重试，把 503 交给下游
	if retries >= maxRetries {
		return false, true, true
	}
	// 未达上限：继续重试
	return true, false, true
}
