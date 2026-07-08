package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
	"github.com/tmaxmax/go-sse"
)

// Handler 处理入站请求并转发到上游服务
func Handler(inboundType inbound.InboundType, c *gin.Context) {
	// 解析请求
	internalRequest, inAdapter, err := parseRequest(inboundType, c)
	if err != nil {
		return
	}
	supportedModels := c.GetString("supported_models")
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		if !slices.Contains(supportedModelsArray, internalRequest.Model) {
			resp.Error(c, http.StatusBadRequest, "model not supported")
			return
		}
	}

	requestModel := internalRequest.Model
	apiKeyID := c.GetInt("api_key_id")

	// 获取 API key 对象（含 MaxConcurrency）
	apiKeyObj, _ := op.APIKeyGet(apiKeyID, c.Request.Context())

	// 全局默认并发上限
	globalLimit := 0
	if v, err := op.SettingGetInt(dbmodel.SettingKeyRelayConcurrencyDefault); err == nil {
		globalLimit = v
	}

	// 获取通道分组
	group, err := op.GroupGetEnabledMap(requestModel, c.Request.Context())
	if err != nil {
		resp.Error(c, http.StatusNotFound, "model not found")
		return
	}

	// 创建迭代器（策略排序 + 粘性优先）
	iter := balancer.NewIterator(group, apiKeyID, requestModel)
	if iter.Len() == 0 {
		resp.Error(c, http.StatusServiceUnavailable, "no available channel")
		return
	}

	// 初始化 Metrics
	metrics := NewRelayMetrics(apiKeyID, requestModel, internalRequest)

	// 请求级上下文
	req := &relayRequest{
		c:               c,
		inAdapter:       inAdapter,
		internalRequest: internalRequest,
		metrics:         metrics,
		apiKeyID:        apiKeyID,
		requestModel:    requestModel,
		iter:            iter,
	}

	var lastErr error

	for iter.Next() {
		select {
		case <-c.Request.Context().Done():
			log.Infof("request context canceled, stopping retry")
			metrics.Save(c.Request.Context(), false, context.Canceled, iter.Attempts())
			return
		default:
		}

		item := iter.Item()

		// 获取通道
		channel, err := op.ChannelGet(item.ChannelID, c.Request.Context())
		if err != nil {
			log.Warnf("failed to get channel %d: %v", item.ChannelID, err)
			iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			continue
		}
		if !channel.Enabled {
			iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			continue
		}

		// 选 key 时跳过熔断中的 key：否则最低 cost 的 key 熔断后整个渠道会被跳过，
		// 其他可用 key 没机会被尝试。excluded 收集本轮已熔断的 key id，循环重选。
		excluded := make(map[int]struct{})
		var usedKey dbmodel.ChannelKey
		for {
			usedKey = channel.GetChannelKeyExcept(excluded)
			if usedKey.ChannelKey == "" {
				iter.Skip(channel.ID, 0, channel.Name, "no available key")
				break
			}
			if iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				excluded[usedKey.ID] = struct{}{}
				continue
			}
			break
		}
		if usedKey.ChannelKey == "" {
			continue
		}

		// 出站适配器
		outAdapter := outbound.Get(channel.Type)
		if outAdapter == nil {
			iter.Skip(channel.ID, usedKey.ID, channel.Name, fmt.Sprintf("unsupported channel type: %d", channel.Type))
			continue
		}

		// 类型兼容性检查
		if internalRequest.IsEmbeddingRequest() && !outbound.IsEmbeddingChannelType(channel.Type) {
			iter.Skip(channel.ID, usedKey.ID, channel.Name, "channel type not compatible with embedding request")
			continue
		}
		if internalRequest.IsChatRequest() && !outbound.IsChatChannelType(channel.Type) {
			iter.Skip(channel.ID, usedKey.ID, channel.Name, "channel type not compatible with chat request")
			continue
		}

		// 设置实际模型
		internalRequest.Model = item.ModelName

		log.Infof("request model %s, mode: %d, forwarding to channel: %s model: %s (attempt %d/%d, sticky=%t)",
			requestModel, group.Mode, channel.Name, item.ModelName,
			iter.Index()+1, iter.Len(), iter.IsSticky())

		// 4 层并发占用：apikey / channel / group / global
		// 每个维度 limit > 0 才占用，nil / 0 = 该层不限制
		slots := make([]slotSpec, 0, 4)
		if apiKeyObj.MaxConcurrency != nil && *apiKeyObj.MaxConcurrency > 0 {
			slots = append(slots, apiKeySlot(apiKeyID, *apiKeyObj.MaxConcurrency))
		}
		if channel.MaxConcurrency != nil && *channel.MaxConcurrency > 0 {
			slots = append(slots, channelSlot(channel.ID, *channel.MaxConcurrency))
		}
		if group.MaxConcurrency != nil && *group.MaxConcurrency > 0 {
			slots = append(slots, groupSlot(group.ID, *group.MaxConcurrency))
		}
		if globalLimit > 0 {
			slots = append(slots, globalSlot(globalLimit))
		}

		release, acquired, blockedTier, blockedCurrent, blockedLimit := acquireRequest(slots)
		if !acquired {
			iter.SkipConcurrencyLimit(channel.ID, usedKey.ID, channel.Name, string(blockedTier), blockedCurrent, blockedLimit)
			lastErr = fmt.Errorf("concurrency limit reached (tier=%s, inflight=%d, limit=%d)", blockedTier, blockedCurrent, blockedLimit)
			continue
		}

		// 构造尝试级上下文 -- 只写变化的 4 个字段
		ra := &relayAttempt{
			relayRequest:         req,
			outAdapter:           outAdapter,
			channel:              channel,
			usedKey:              usedKey,
			firstTokenTimeOutSec: group.FirstTokenTimeOut,
		}

		result := ra.attempt()
		release()
		if result.Success {
			metrics.Save(c.Request.Context(), true, nil, iter.Attempts())
			return
		}
		if result.Written {
			metrics.Save(c.Request.Context(), false, result.Err, iter.Attempts())
			return
		}
		lastErr = result.Err
	}

	// 所有通道都失败
	metrics.Save(c.Request.Context(), false, lastErr, iter.Attempts())
	resp.Error(c, http.StatusBadGateway, "all channels failed")
}

// attempt 统一管理一次通道尝试的完整生命周期
func (ra *relayAttempt) attempt() attemptResult {
	span := ra.iter.StartAttempt(ra.channel.ID, ra.usedKey.ID, ra.channel.Name)

	// 转发请求
	statusCode, fwdErr := ra.forward()

	// 更新 channel key 状态
	ra.usedKey.StatusCode = statusCode
	ra.usedKey.LastUseTimeStamp = time.Now().Unix()

	if fwdErr == nil {
		// ====== 成功 ======
		ra.collectResponse()
		ra.usedKey.TotalCost += ra.metrics.Stats.InputCost + ra.metrics.Stats.OutputCost
		op.ChannelKeyUpdate(ra.usedKey)

		span.End(dbmodel.AttemptSuccess, statusCode, "")

		// Channel 维度统计
		op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
			WaitTime:       span.Duration().Milliseconds(),
			RequestSuccess: 1,
		})

		// 熔断器：记录成功
		balancer.RecordSuccess(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
		// 会话保持：更新粘性记录
		balancer.SetSticky(ra.apiKeyID, ra.requestModel, ra.channel.ID, ra.usedKey.ID)

		ra.metrics.ParamOverride = paramOverrideValue(ra.channel.ParamOverride)

		return attemptResult{Success: true}
	}

	// ====== 失败 ======
	ra.usedKey.TotalCost += 1
	span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())

	// Channel 维度统计
	op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
		WaitTime:      span.Duration().Milliseconds(),
		RequestFailed: 1,
	})

	// 熔断器/速率冷却分派：
	// 1) 503 且开启重试 → 优先短路，不 RecordFailure（保护渠道不被熔断，v1 行为），
	//    503 经 forward() 重试，exhaust 时也不累计——避免开启重试的渠道因反复 503 exhaust 被熔断。
	// 2) 非 503 上游错误 → errors.As 解包 *upstreamError，按 VendorCode 分派：
	//    - 11210 等瞬时速率错误 → RecordRateLimit（即时 60s 冷却，不触碰熔断状态机）+
	//      设置 key.RateLimitedUntil（channel.go 层 60s 主冷却）+ 清零 StatusCode（避免 5min 429 软冷却二次遮蔽）。
	//    - 其它/未识别 → RecordFailure（既有熔断失败路径，行为不变）。
	// 3) 网络错误/请求构造错误（非 *upstreamError）→ RecordFailure，行为不变。
	// 失败处置分类（纯函数决策，副作用由下方各分支绑定）：
	//   classRetry503ShortCircuit → 503 且开启重试，不 RecordFailure（保护渠道不被熔断）；
	//   classRecordRateLimit       → 11210 等瞬时速率错误，即时 60s 冷却（不触碰熔断状态机）；
	//   classRecordFailure         → 未识别厂商码 / 网络错误，既有熔断失败路径。
	class, vendorCode := classifyFailure(statusCode, fwdErr, retryOn503Enabled(ra.channel))
	switch class {
	case classRetry503ShortCircuit:
		// 503 且开启重试：不 RecordFailure，保护渠道不被熔断。
		// 503 exhaust 时 body 仍解析厂商码用于统计/日志，但不触发速率冷却或熔断。
		if vendorCode > 0 {
			log.Infof("channel %s 503 exhausted with vendor code %d (no circuit action)", ra.channel.Name, vendorCode)
		}
	case classRecordRateLimit:
		balancer.RecordRateLimit(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
		// channel.go 层主冷却：设置 RateLimitedUntil，使 GetChannelKeyExcept 在 60s 内跳过该 key。
		ra.usedKey.RateLimitedUntil = time.Now().Add(balancer.RateLimitCooldown)
		// 清零 StatusCode：11210 在 :213 已设为 429，若保留会在 60s 冷却过期后
		// 被 5min 429 软冷却二次遮蔽（LastUseTimeStamp=T0，T0+60s 时 nowSec-T0=60<300）。
		// 清零后 429 软冷却检查 k.StatusCode==429 为 false，不再触发，60s 精确语义达成。
		ra.usedKey.StatusCode = 0
	case classRecordFailure:
		// 既有熔断失败路径：未识别厂商码的上游错误、网络/构造/body 读取错误。
		balancer.RecordFailure(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
	}

	// 统一写回内存缓存：捕获本轮所有 key 变更（StatusCode/LastUseTimeStamp/TotalCost，
	// 以及 11210 分派设置的 RateLimitedUntil + StatusCode=0）。单次写入避免中间态
	// （StatusCode=429 但 RateLimitedUntil 未设）被并发 GetChannelKeyExcept 读到而误触 5min 软冷却。
	op.ChannelKeyUpdate(ra.usedKey)

	ra.metrics.ParamOverride = paramOverrideValue(ra.channel.ParamOverride)

	written := ra.c.Writer.Written()
	if written {
		ra.collectResponse()
	}
	return attemptResult{
		Success: false,
		Written: written,
		Err:     fmt.Errorf("channel %s failed: %w", ra.channel.Name, fwdErr),
	}
}

// parseRequest 解析并验证入站请求
func parseRequest(inboundType inbound.InboundType, c *gin.Context) (*model.InternalLLMRequest, model.Inbound, error) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, err
	}

	inAdapter := inbound.Get(inboundType)
	internalRequest, err := inAdapter.TransformRequest(c.Request.Context(), body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, err
	}

	// Pass through the original query parameters
	internalRequest.Query = c.Request.URL.Query()

	if err := internalRequest.Validate(); err != nil {
		resp.Error(c, http.StatusBadRequest, err.Error())
		return nil, nil, err
	}

	return internalRequest, inAdapter, nil
}

// forward 转发请求到上游服务（支持 503 自动重试）
func (ra *relayAttempt) forward() (int, error) {
	ctx := ra.c.Request.Context()

	// 构建出站请求
	outboundRequest, err := ra.outAdapter.TransformRequest(
		ctx,
		ra.internalRequest,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
	)
	if err != nil {
		log.Warnf("failed to create request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// 预先缓存 body 字节，供 503 重试时重建 io.Reader
	var bodyBytes []byte

	// 应用 ParamOverride 到请求体
	if ra.channel.ParamOverride != nil && *ra.channel.ParamOverride != "" {
		body, err := io.ReadAll(outboundRequest.Body)
		if err != nil {
			return 0, fmt.Errorf("failed to read body: %w", err)
		}
		bodyBytes = body

		var bodyMap map[string]any
		if err := json.Unmarshal(body, &bodyMap); err != nil {
			log.Warnf("failed to unmarshal request body: %v, skipping param_override", err)
			outboundRequest.Body = io.NopCloser(bytes.NewBuffer(body))
			return 0, nil
		}
		var override map[string]any
		if err := json.Unmarshal([]byte(*ra.channel.ParamOverride), &override); err != nil {
			log.Warnf("failed to unmarshal param_override: %v, skipping", err)
			outboundRequest.Body = io.NopCloser(bytes.NewBuffer(body))
			return 0, nil
		}
		maps.Copy(bodyMap, override)
		modifiedBody, err := json.Marshal(bodyMap)
		if err != nil {
			log.Warnf("failed to marshal modified body: %v, skipping param_override", err)
			outboundRequest.Body = io.NopCloser(bytes.NewBuffer(body))
			return 0, nil
		}
		bodyBytes = modifiedBody
		outboundRequest.Body = io.NopCloser(bytes.NewBuffer(modifiedBody))
		outboundRequest.ContentLength = int64(len(modifiedBody))
	} else {
		// 没有 ParamOverride，直接从 Body 读取缓存
		bodyBytes, err = io.ReadAll(outboundRequest.Body)
		if err != nil {
			return 0, fmt.Errorf("failed to read body for retry cache: %w", err)
		}
		outboundRequest.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	}

	// 复制请求头
	ra.copyHeaders(outboundRequest)

	// 503 重试相关：是否启用
	retryEnabled := retryOn503Enabled(ra.channel)
	maxRetries := resolveMax503Retries(ra.channel.Max503Retries)

	var lastResp *http.Response
	retries := 0
	for {
		// 每次迭代前重建 body（bodyBytes 不变）
		outboundRequest.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		// 发送请求
		response, err := ra.sendRequest(outboundRequest)
		if err != nil {
			return 0, fmt.Errorf("failed to send request: %w", err)
		}

		// 503 重试决策（纯函数，集中分支逻辑便于测试）
		retry, exhaust, _ := shouldRetry503(response.StatusCode, retryEnabled, ra.c.Writer.Written(), maxRetries, retries)
		if retry || exhaust {
			if exhaust {
				// 达到上限：把最后一次 503 响应交给下游既有错误分支返回给客户端。
				// 此时才读 body 用于错误信息；retry 分支不读，直接 Close。
				lastResp = response
				break
			}
			// 抢占式 0 秒重试：直接 Close 丢弃响应体，不 drain。
			// 原因：503 body 对客户端无用，读完会阻塞到上游发完 EOF，违背"0 秒抢占式"。
			// 未 drain 的连接 Go transport 会标记不可复用并新建连接——重试新建
			// TCP/TLS 的开销远小于读完一个可能很大的 503 body。
			response.Body.Close()
			retries++
			log.Infof("503 retry #%d for channel %d", retries, ra.channel.ID)
			continue
		}

		// 非 503（含 200、4xx、5xx 等）：交给后续既有分支处理
		lastResp = response
		break
	}

	defer lastResp.Body.Close()

	// 检查响应状态
	if lastResp.StatusCode < 200 || lastResp.StatusCode >= 300 {
		// 最多读 8KB：正常错误体（如讯飞 11210 <500B）足够，恶意大 body 不占用内存。
		// 超出部分由 Close 处理（drain ≤256B 复用连接，超限关连接重连，与 503 close-no-drain 同结论）。
		body, err := io.ReadAll(io.LimitReader(lastResp.Body, maxUpstreamErrorBody))
		if err != nil {
			return 0, fmt.Errorf("failed to read response body: %w", err)
		}
		// 返回 *upstreamError 携带厂商业务错误码，供 attempt() 用 errors.As 解包分派。
		// Error() 字面与旧 fmt.Errorf("upstream error: %d: %s", ...) 一致，零回归。
		return lastResp.StatusCode, &upstreamError{
			StatusCode: lastResp.StatusCode,
			VendorCode: parseUpstreamVendorCode(body),
			Body:       body,
		}
	}

	// 处理响应
	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		if err := ra.handleStreamResponse(ctx, lastResp); err != nil {
			return 0, err
		}
		return lastResp.StatusCode, nil
	}
	if err := ra.handleResponse(ctx, lastResp); err != nil {
		return 0, err
	}
	return lastResp.StatusCode, nil
}

// copyHeaders 复制请求头，过滤 hop-by-hop 头
func (ra *relayAttempt) copyHeaders(outboundRequest *http.Request) {
	for key, values := range ra.c.Request.Header {
		if hopByHopHeaders[strings.ToLower(key)] {
			continue
		}
		for _, value := range values {
			outboundRequest.Header.Set(key, value)
		}
	}
	if len(ra.channel.CustomHeader) > 0 {
		for _, header := range ra.channel.CustomHeader {
			outboundRequest.Header.Set(header.HeaderKey, header.HeaderValue)
		}
	}
}

// sendRequest 发送 HTTP 请求
func (ra *relayAttempt) sendRequest(req *http.Request) (*http.Response, error) {
	httpClient, err := helper.ChannelHttpClient(ra.channel)
	if err != nil {
		log.Warnf("failed to get http client: %v", err)
		return nil, err
	}

	response, err := httpClient.Do(req)
	if err != nil {
		log.Warnf("failed to send request: %v", err)
		return nil, err
	}

	return response, nil
}

// handleStreamResponse 处理流式响应
func (ra *relayAttempt) handleStreamResponse(ctx context.Context, response *http.Response) error {
	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 16*1024))
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request: %s", ct, string(body))
	}

	// 设置 SSE 响应头
	ra.c.Header("Content-Type", "text/event-stream")
	ra.c.Header("Cache-Control", "no-cache")
	ra.c.Header("Connection", "keep-alive")
	ra.c.Header("X-Accel-Buffering", "no")

	firstToken := true

	type sseReadResult struct {
		data string
		err  error
	}
	results := make(chan sseReadResult, 1)
	go func() {
		defer close(results)
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(response.Body, readCfg) {
			if err != nil {
				results <- sseReadResult{err: err}
				return
			}
			results <- sseReadResult{data: ev.Data}
		}
	}()

	var firstTokenTimer *time.Timer
	var firstTokenC <-chan time.Time
	if firstToken && ra.firstTokenTimeOutSec > 0 {
		firstTokenTimer = time.NewTimer(time.Duration(ra.firstTokenTimeOutSec) * time.Second)
		firstTokenC = firstTokenTimer.C
		defer func() {
			if firstTokenTimer != nil {
				firstTokenTimer.Stop()
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			log.Infof("client disconnected, stopping stream")
			return nil
		case <-firstTokenC:
			log.Warnf("first token timeout (%ds), switching channel", ra.firstTokenTimeOutSec)
			_ = response.Body.Close()
			return fmt.Errorf("first token timeout (%ds)", ra.firstTokenTimeOutSec)
		case r, ok := <-results:
			if !ok {
				log.Infof("stream end")
				return nil
			}
			if r.err != nil {
				log.Warnf("failed to read event: %v", r.err)
				return fmt.Errorf("failed to read stream event: %w", r.err)
			}

			data, err := ra.transformStreamData(ctx, r.data)
			if err != nil || len(data) == 0 {
				continue
			}
			if firstToken {
				ra.metrics.SetFirstTokenTime(time.Now())
				firstToken = false
				if firstTokenTimer != nil {
					if !firstTokenTimer.Stop() {
						select {
						case <-firstTokenTimer.C:
						default:
						}
					}
					firstTokenTimer = nil
					firstTokenC = nil
				}
			}

			ra.c.Writer.Write(data)
			ra.c.Writer.Flush()
		}
	}
}

// transformStreamData 转换流式数据
func (ra *relayAttempt) transformStreamData(ctx context.Context, data string) ([]byte, error) {
	internalStream, err := ra.outAdapter.TransformStream(ctx, []byte(data))
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}
	if internalStream == nil {
		return nil, nil
	}

	inStream, err := ra.inAdapter.TransformStream(ctx, internalStream)
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}

	return inStream, nil
}

// handleResponse 处理非流式响应
func (ra *relayAttempt) handleResponse(ctx context.Context, response *http.Response) error {
	internalResponse, err := ra.outAdapter.TransformResponse(ctx, response)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform outbound response: %w", err)
	}

	inResponse, err := ra.inAdapter.TransformResponse(ctx, internalResponse)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform inbound response: %w", err)
	}

	ra.c.Data(http.StatusOK, "application/json", inResponse)
	return nil
}

// collectResponse 收集响应信息
func (ra *relayAttempt) collectResponse() {
	internalResponse, err := ra.inAdapter.GetInternalResponse(ra.c.Request.Context())
	if err != nil || internalResponse == nil {
		return
	}

	ra.metrics.SetInternalResponse(internalResponse, ra.internalRequest.Model)
}

func paramOverrideValue(ptr *string) string {
	if ptr == nil || *ptr == "" {
		return ""
	}
	return *ptr
}
