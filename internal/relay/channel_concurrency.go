package relay

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// concurrencyTier 标识 4 层并发中的某一层
type concurrencyTier string

const (
	tierAPIKey  concurrencyTier = "ak"
	tierChannel concurrencyTier = "ch"
	tierGroup   concurrencyTier = "grp"
	tierGlobal  concurrencyTier = "g"
)

type concurrencyEntry struct {
	inflight int64
}

var globalConcurrency sync.Map // key: string -> *concurrencyEntry

func getConcurrencyEntry(key string) *concurrencyEntry {
	if v, ok := globalConcurrency.Load(key); ok {
		return v.(*concurrencyEntry)
	}
	entry := &concurrencyEntry{}
	actual, _ := globalConcurrency.LoadOrStore(key, entry)
	return actual.(*concurrencyEntry)
}

// acquireSlot 尝试在单个 entry 上占用一个槽位
func acquireSlot(key string, limit int) (release func(), ok bool, current int) {
	if limit <= 0 {
		return func() {}, true, 0
	}
	entry := getConcurrencyEntry(key)
	for {
		cur := atomic.LoadInt64(&entry.inflight)
		if cur >= int64(limit) {
			return func() {}, false, int(cur)
		}
		if atomic.CompareAndSwapInt64(&entry.inflight, cur, cur+1) {
			var released atomic.Bool
			release = func() {
				if !released.CompareAndSwap(false, true) {
					return
				}
				for {
					latest := atomic.LoadInt64(&entry.inflight)
					if latest <= 0 {
						return
					}
					if atomic.CompareAndSwapInt64(&entry.inflight, latest, latest-1) {
						return
					}
				}
			}
			return release, true, int(cur + 1)
		}
	}
}

// acquireRequest 尝试同时占用多个维度的槽位。
// 任一维度满 → 整体失败，不占用任何槽位（避免半占用泄漏）。
// 返回的 release 是组合 release：调用一次释放全部已占用槽位，幂等。
func acquireRequest(slots []slotSpec) (release func(), ok bool, blockedTier concurrencyTier, blockedCurrent int, blockedLimit int) {
	releases := make([]func(), 0, len(slots))
	combinedReleased := atomic.Bool{}
	combinedRelease := func() {
		if !combinedReleased.CompareAndSwap(false, true) {
			return
		}
		for _, r := range releases {
			r()
		}
	}

	for _, s := range slots {
		rel, acquired, current := acquireSlot(s.key, s.limit)
		if !acquired {
			// 回滚已占用的
			for _, r := range releases {
				r()
			}
			return func() {}, false, s.tier, current, s.limit
		}
		releases = append(releases, rel)
	}
	return combinedRelease, true, "", 0, 0
}

type slotSpec struct {
	key   string
	limit int
	tier  concurrencyTier
}

func apiKeySlot(apiKeyID, limit int) slotSpec {
	return slotSpec{key: fmt.Sprintf("ak:%d", apiKeyID), limit: limit, tier: tierAPIKey}
}
func channelSlot(channelID, limit int) slotSpec {
	return slotSpec{key: fmt.Sprintf("ch:%d", channelID), limit: limit, tier: tierChannel}
}
func groupSlot(groupID, limit int) slotSpec {
	return slotSpec{key: fmt.Sprintf("grp:%d", groupID), limit: limit, tier: tierGroup}
}
func globalSlot(limit int) slotSpec {
	return slotSpec{key: "g", limit: limit, tier: tierGlobal}
}

func currentConcurrency(tier concurrencyTier, id int) int {
	var key string
	switch tier {
	case tierAPIKey:
		key = fmt.Sprintf("ak:%d", id)
	case tierChannel:
		key = fmt.Sprintf("ch:%d", id)
	case tierGroup:
		key = fmt.Sprintf("grp:%d", id)
	case tierGlobal:
		key = "g"
	}
	v, ok := globalConcurrency.Load(key)
	if !ok {
		return 0
	}
	return int(atomic.LoadInt64(&v.(*concurrencyEntry).inflight))
}

func resetConcurrencyForTest(key string) {
	globalConcurrency.Delete(key)
}