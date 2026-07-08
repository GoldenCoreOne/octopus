package model

import (
	"time"

	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

type AutoGroupType int

const (
	AutoGroupTypeNone  AutoGroupType = 0 //不自动分组
	AutoGroupTypeFuzzy AutoGroupType = 1 //模糊匹配
	AutoGroupTypeExact AutoGroupType = 2 //准确匹配
	AutoGroupTypeRegex AutoGroupType = 3 //正则匹配
)

type Channel struct {
	ID            int                   `json:"id" gorm:"primaryKey"`
	Name          string                `json:"name" gorm:"unique;not null"`
	Type          outbound.OutboundType `json:"type"`
	Enabled       bool                  `json:"enabled" gorm:"default:true"`
	BaseUrls      []BaseUrl             `json:"base_urls" gorm:"serializer:json"`
	Keys          []ChannelKey          `json:"keys" gorm:"foreignKey:ChannelID"`
	Model         string                `json:"model"`
	CustomModel   string                `json:"custom_model"`
	Proxy         bool                  `json:"proxy" gorm:"default:false"`
	AutoSync      bool                  `json:"auto_sync" gorm:"default:false"`
	AutoGroup     AutoGroupType         `json:"auto_group" gorm:"default:0"`
	CustomHeader  []CustomHeader        `json:"custom_header" gorm:"serializer:json"`
	ParamOverride *string               `json:"param_override"`
	ChannelProxy  *string               `json:"channel_proxy"`
	Stats         *StatsChannel         `json:"stats,omitempty" gorm:"foreignKey:ChannelID"`
	MatchRegex    *string               `json:"match_regex"`
	MaxConcurrency *int                 `json:"max_concurrency"`                                           // nil=未设置, 0=不限制, >0=限制
	RetryOn503    *int                 `json:"retry_on_503" gorm:"column:retry_on_503"`                    // nil=未设置, 0=关闭, 1=开启
	Max503Retries *int                 `json:"max_503_retries" gorm:"column:max_503_retries"`              // nil=未设置(默认12), 0=无限, >0=自定义上限
}

type BaseUrl struct {
	URL   string `json:"url"`
	Delay int    `json:"delay"`
}

type CustomHeader struct {
	HeaderKey   string `json:"header_key"`
	HeaderValue string `json:"header_value"`
}

type ChannelKey struct {
	ID               int       `json:"id" gorm:"primaryKey"`
	ChannelID        int       `json:"channel_id"`
	Enabled          bool      `json:"enabled" gorm:"default:true"`
	ChannelKey       string    `json:"channel_key"`
	StatusCode       int       `json:"status_code"`
	LastUseTimeStamp int64     `json:"last_use_time_stamp"`
	TotalCost        float64   `json:"total_cost"`
	Remark           string    `json:"remark"`
	// RateLimitedUntil 速率限流（如讯飞 11210 tpm 超限）的即时冷却到期时间。
	// 零值 time.Time{}（IsZero()=true）表示未处于速率冷却。
	// 由 relay 层在识别到 11210 时设置（now + 60s），GetChannelKeyExcept 优先检查它，
	// 使该 key 在 60s 内被跳过，等待 tpm 窗口刷新。冷却到期后 key 重新可选。
	RateLimitedUntil time.Time `json:"rate_limited_until" gorm:"index"`
}

// ChannelUpdateRequest 渠道更新请求 - 仅包含变更的数据
type ChannelUpdateRequest struct {
	ID            int                    `json:"id" binding:"required"`
	Name          *string                `json:"name,omitempty"`
	Type          *outbound.OutboundType `json:"type,omitempty"`
	Enabled       *bool                  `json:"enabled,omitempty"`
	BaseUrls      *[]BaseUrl             `json:"base_urls,omitempty"`
	Model         *string                `json:"model,omitempty"`
	CustomModel   *string                `json:"custom_model,omitempty"`
	Proxy         *bool                  `json:"proxy,omitempty"`
	AutoSync      *bool                  `json:"auto_sync,omitempty"`
	AutoGroup     *AutoGroupType         `json:"auto_group,omitempty"`
	CustomHeader  *[]CustomHeader        `json:"custom_header,omitempty"`
	ChannelProxy  *string                `json:"channel_proxy,omitempty"`
	ParamOverride *string                `json:"param_override,omitempty"`
	MatchRegex    *string                `json:"match_regex,omitempty"`
	MaxConcurrency *int                  `json:"max_concurrency,omitempty"` // nil=不更新, 0=不限制, >0=限制
	RetryOn503    *int                  `json:"retry_on_503,omitempty"`    // nil=不更新, 0=关闭, 1=开启
	Max503Retries *int                  `json:"max_503_retries,omitempty"` // nil=不更新, 0=无限, >0=自定义上限

	KeysToAdd    []ChannelKeyAddRequest    `json:"keys_to_add,omitempty"`
	KeysToUpdate []ChannelKeyUpdateRequest `json:"keys_to_update,omitempty"`
	KeysToDelete []int                     `json:"keys_to_delete,omitempty"`
}

type ChannelKeyAddRequest struct {
	Enabled    bool   `json:"enabled"`
	ChannelKey string `json:"channel_key" binding:"required"`
	Remark     string `json:"remark"`
}

type ChannelKeyUpdateRequest struct {
	ID         int     `json:"id" binding:"required"`
	Enabled    *bool   `json:"enabled,omitempty"`
	ChannelKey *string `json:"channel_key,omitempty"`
	Remark     *string `json:"remark,omitempty"`
}

// ChannelFetchModelRequest is used by /channel/fetch-model (not persisted).
type ChannelFetchModelRequest struct {
	Type    outbound.OutboundType `json:"type" binding:"required"`
	BaseURL string                `json:"base_url" binding:"required"`
	Key     string                `json:"key" binding:"required"`
	Proxy   bool                  `json:"proxy"`
}

func (c *Channel) GetBaseUrl() string {
	if c == nil || len(c.BaseUrls) == 0 {
		return ""
	}

	bestURL := ""
	bestDelay := 0
	bestSet := false

	for _, bu := range c.BaseUrls {
		if bu.URL == "" {
			continue
		}
		if !bestSet || bu.Delay < bestDelay {
			bestURL = bu.URL
			bestDelay = bu.Delay
			bestSet = true
		}
	}

	return bestURL
}

func (c *Channel) GetChannelKey() ChannelKey {
	return c.GetChannelKeyExcept(nil)
}

// GetChannelKeyExcept 选择 TotalCost 最低的可用 key，跳过 excluded 中的 key id。
// 调用方把熔断中的 key id 放进 excluded，即可让选路继续尝试其他 key，
// 而不是因为最低 cost 的 key 熔断就放弃整个渠道。
func (c *Channel) GetChannelKeyExcept(excluded map[int]struct{}) ChannelKey {
	if c == nil || len(c.Keys) == 0 {
		return ChannelKey{}
	}

	nowSec := time.Now().Unix()
	now := time.Now()

	best := ChannelKey{}
	bestCost := 0.0
	bestSet := false

	for _, k := range c.Keys {
		if !k.Enabled || k.ChannelKey == "" {
			continue
		}
		if excluded != nil {
			if _, skip := excluded[k.ID]; skip {
				continue
			}
		}
		// 优先检查速率限流冷却（11210 等瞬时速率错误，60s 精确）：
		// 未过期则跳过该 key，等待 tpm 窗口刷新；过期则放行进入后续选路判定。
		// 此检查先于 429 软冷却（5min），确保 60s 精确语义不被 5min 软冷却遮蔽。
		// 11210 命中时 relay 层会同时清零 StatusCode，使 60s 过期后 429 软冷却不再触发。
		if !k.RateLimitedUntil.IsZero() && now.Before(k.RateLimitedUntil) {
			continue
		}
		if k.StatusCode == 429 && k.LastUseTimeStamp > 0 {
			if nowSec-k.LastUseTimeStamp < int64(5*time.Minute/time.Second) {
				continue
			}
		}
		if !bestSet || k.TotalCost < bestCost {
			best = k
			bestCost = k.TotalCost
			bestSet = true
		}
	}

	if !bestSet {
		return ChannelKey{}
	}
	return best
}
