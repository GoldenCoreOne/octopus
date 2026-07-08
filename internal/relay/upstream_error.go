package relay

import (
	"encoding/json"
	"fmt"
)

// maxUpstreamErrorBody 上游错误 body 的最大读取长度。
// 讯飞 11210/10310 等错误 body 通常 <500 字节，8KB 足够覆盖正常错误体，
// 同时防止恶意/异常大 body 占用过多内存。超出部分截断并在 Error() 标记。
const maxUpstreamErrorBody = 8 * 1024

// upstreamError 包装上游非 2xx 响应，携带 HTTP 状态码、解析出的厂商业务错误码与原始 body。
//
// errors.As 仅在 relay 包内（attempt()）使用，用于在不丢失 body 的前提下，
// 把厂商错误码（如讯飞 11210）分流到对应处置策略（见 ratelimit_policy.go）。
// 字段保持未导出：当前无上层（handler/metrics）需要读取 VendorCode 做分类统计，
// 若未来有此需求再导出，与 bodycache.BodyTooLargeError 的导出范式对齐。
type upstreamError struct {
	StatusCode int    // 上游 HTTP 状态码
	VendorCode int    // 解析自 body 的厂商业务错误码（如讯飞 11210）；未识别为 0
	Body       []byte // 原始响应 body（最多 maxUpstreamErrorBody 字节）
}

// Error 实现 error 接口。
// 字面格式与旧代码 `fmt.Errorf("upstream error: %d: %s", code, string(body))` 完全一致，
// 保证 metrics / lastErr / 客户端 502 body 零回归。截断时追加 ...(truncated) 标记。
func (e *upstreamError) Error() string {
	if e == nil {
		return "upstream error"
	}
	s := string(e.Body)
	if len(e.Body) >= maxUpstreamErrorBody {
		// body 达到上限，可能被截断（LimitReader 读满不代表上游已 EOF，
		// 但足以判定"可能不完整"），追加标记便于日志辨识。
		s = s + "...(truncated)"
	}
	return fmt.Sprintf("upstream error: %d: %s", e.StatusCode, s)
}

// parseUpstreamVendorCode 从上游错误 body 中解析厂商业务错误码。
//
// 适配 OpenAI 风格错误体 `{"error":{"code":NNNN,...}}`：讯飞 maas-coding-api 使用 int code
// （如 11210=tpm 超限、10310=服务忙）。解析层保持中立：
//   - 仅做 float64→int 的类型转换（json.Unmarshal 默认把数字解码为 float64）；
//   - 不识别特定厂商，不在此处做策略映射——映射收敛在 ratelimit_policy.go 的 vendorCodePolicy；
//   - 非 JSON / 缺 error.code / code 为字符串 / code ≤ 0 一律返回 0（=未识别，走默认 RecordFailure）。
//
// 返回 0 时调用方按"未识别的上游错误"处理（不触发速率冷却，计入普通熔断失败）。
func parseUpstreamVendorCode(body []byte) int {
	if len(body) == 0 {
		return 0
	}
	var outer struct {
		Error struct {
			Code any `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &outer); err != nil {
		return 0
	}
	switch v := outer.Error.Code.(type) {
	case float64:
		code := int(v)
		if code <= 0 {
			return 0
		}
		return code
	default:
		// 字符串 code（如 OpenAI 官方 "insufficient_quota"）或 null/bool 等：未识别
		return 0
	}
}