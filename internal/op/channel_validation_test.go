package op

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/cache"
)

// withChannelSQLiteDB swaps the global DB handle onto an isolated SQLite
// instance (auto-migrated), returning a closer that restores the prior handle.
func withChannelSQLiteDB(t *testing.T) func() {
	t.Helper()

	prevDB := db.SwapDBForTest(nil)
	dbPath := filepath.Join(t.TempDir(), "channel-validation.sqlite")
	if err := db.InitDB("sqlite", dbPath, false); err != nil {
		db.SwapDBForTest(prevDB)
		t.Fatalf("InitDB() error = %v", err)
	}

	return func() {
		tempDB := db.SwapDBForTest(prevDB)
		if tempDB == nil {
			return
		}
		sqlDB, err := tempDB.DB()
		if err != nil {
			t.Fatalf("tempDB.DB() error = %v", err)
		}
		if err := sqlDB.Close(); err != nil {
			t.Fatalf("sqlDB.Close() error = %v", err)
		}
	}
}

// isolateChannelCaches swaps the process-wide channel caches for empty ones
// and returns a restorer. Without this, tests would mutate (and observe)
// caches populated by other tests.
func isolateChannelCaches(t *testing.T) func() {
	t.Helper()

	oldChannelCache := channelCache
	channelCache = cache.New[int, model.Channel](16)
	oldKeyCache := channelKeyCache
	channelKeyCache = cache.New[int, model.ChannelKey](16)
	channelKeyCacheNeedUpdateLock.Lock()
	oldNeedUpdate := channelKeyCacheNeedUpdate
	channelKeyCacheNeedUpdate = make(map[int]struct{})
	channelKeyCacheNeedUpdateLock.Unlock()

	return func() {
		channelCache = oldChannelCache
		channelKeyCache = oldKeyCache
		channelKeyCacheNeedUpdateLock.Lock()
		channelKeyCacheNeedUpdate = oldNeedUpdate
		channelKeyCacheNeedUpdateLock.Unlock()
	}
}

// seedChannelRecord inserts a channel row directly into the DB and seeds the
// in-memory cache so ChannelUpdate's `channelCache.Get(req.ID)` lookup succeeds.
func seedChannelRecord(t *testing.T, ch model.Channel) {
	t.Helper()

	ch.ID = 100
	if err := db.GetDB().Create(&ch).Error; err != nil {
		t.Fatalf("seed channel error = %v", err)
	}
	channelCache.Set(ch.ID, ch)
}

// loadChannelRecord reloads a channel row from the DB (raw, no cache).
func loadChannelRecord(t *testing.T, id int) model.Channel {
	t.Helper()

	var ch model.Channel
	if err := db.GetDB().First(&ch, id).Error; err != nil {
		t.Fatalf("load channel error = %v", err)
	}
	return ch
}

func TestChannelUpdateRejectsInvalidRetryOn503(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		val  int
	}{
		{"two_rejected", 2},
		{"negative_rejected", -1},
		{"seven_rejected", 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			restore := isolateChannelCaches(t)
			defer restore()
			closeDB := withChannelSQLiteDB(t)
			defer closeDB()

			seedChannelRecord(t, model.Channel{Name: "ch-retry-invalid"})

			v := tc.val
			_, err := ChannelUpdate(&model.ChannelUpdateRequest{
				ID:         100,
				RetryOn503: &v,
			}, ctx)
			if err == nil || err.Error() != "retry_on_503 must be 0 or 1" {
				t.Fatalf("expected retry_on_503 rejection, got %v", err)
			}

			// DB must be untouched on rollback.
			record := loadChannelRecord(t, 100)
			if record.RetryOn503 != nil {
				t.Fatalf("expected retry_on_503 untouched in DB, got %v", *record.RetryOn503)
			}
		})
	}
}

func TestChannelUpdateRejectsNegativeMax503Retries(t *testing.T) {
	ctx := context.Background()
	v := -1
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-max-neg"})

	_, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:            100,
		Max503Retries: &v,
	}, ctx)
	if err == nil || err.Error() != "max_503_retries must be >= 0" {
		t.Fatalf("expected max_503_retries rejection, got %v", err)
	}

	record := loadChannelRecord(t, 100)
	if record.Max503Retries != nil {
		t.Fatalf("expected max_503_retries untouched in DB, got %v", *record.Max503Retries)
	}
}

func TestChannelUpdateRejectsNegativeMaxConcurrency(t *testing.T) {
	ctx := context.Background()
	v := -3
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-conc-neg"})

	_, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:             100,
		MaxConcurrency: &v,
	}, ctx)
	if err == nil || err.Error() != "max_concurrency must be >= 0" {
		t.Fatalf("expected max_concurrency rejection, got %v", err)
	}

	record := loadChannelRecord(t, 100)
	if record.MaxConcurrency != nil {
		t.Fatalf("expected max_concurrency untouched in DB, got %v", *record.MaxConcurrency)
	}
}

func TestChannelUpdateRejectsRetryOn503BeforeMax503Retries(t *testing.T) {
	// retry_on_503 validation is ordered before max_503_retries in ChannelUpdate.
	// An invalid retry_on_503 must short-circuit even when max_503_retries is also
	// invalid — proving the order-dependent early return.
	ctx := context.Background()
	badRetry := 2
	goodMax := 5
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-order"})

	_, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:            100,
		RetryOn503:    &badRetry,
		Max503Retries: &goodMax,
	}, ctx)
	if err == nil || err.Error() != "retry_on_503 must be 0 or 1" {
		t.Fatalf("expected retry_on_503 rejection to win ordering, got %v", err)
	}
}

func TestChannelUpdateAcceptsRetryOn503On(t *testing.T) {
	ctx := context.Background()
	v := 1
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-retry-on"})

	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:         100,
		RetryOn503: &v,
	}, ctx)
	if err != nil {
		t.Fatalf("ChannelUpdate() error = %v", err)
	}
	if updated.RetryOn503 == nil || *updated.RetryOn503 != 1 {
		t.Fatalf("expected returned retry_on_503=1, got %+v", updated.RetryOn503)
	}

	record := loadChannelRecord(t, 100)
	if record.RetryOn503 == nil || *record.RetryOn503 != 1 {
		t.Fatalf("expected persisted retry_on_503=1, got %+v", record.RetryOn503)
	}
}

func TestChannelUpdateAcceptsRetryOn503Off(t *testing.T) {
	ctx := context.Background()
	v := 0
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-retry-off"})

	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:         100,
		RetryOn503: &v,
	}, ctx)
	if err != nil {
		t.Fatalf("ChannelUpdate() error = %v", err)
	}
	if updated.RetryOn503 == nil || *updated.RetryOn503 != 0 {
		t.Fatalf("expected returned retry_on_503=0, got %+v", updated.RetryOn503)
	}

	record := loadChannelRecord(t, 100)
	if record.RetryOn503 == nil || *record.RetryOn503 != 0 {
		t.Fatalf("expected persisted retry_on_503=0, got %+v", record.RetryOn503)
	}
}

func TestChannelUpdateAcceptsMax503RetriesZeroInfinite(t *testing.T) {
	// max_503_retries=0 means "infinite" and is a valid accept path.
	ctx := context.Background()
	v := 0
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-max-zero"})

	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:            100,
		Max503Retries: &v,
	}, ctx)
	if err != nil {
		t.Fatalf("ChannelUpdate() error = %v", err)
	}
	if updated.Max503Retries == nil || *updated.Max503Retries != 0 {
		t.Fatalf("expected returned max_503_retries=0, got %+v", updated.Max503Retries)
	}

	record := loadChannelRecord(t, 100)
	if record.Max503Retries == nil || *record.Max503Retries != 0 {
		t.Fatalf("expected persisted max_503_retries=0, got %+v", record.Max503Retries)
	}
}

func TestChannelUpdateAcceptsMax503RetriesPositive(t *testing.T) {
	ctx := context.Background()
	v := 12
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-max-12"})

	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:            100,
		Max503Retries: &v,
	}, ctx)
	if err != nil {
		t.Fatalf("ChannelUpdate() error = %v", err)
	}
	if updated.Max503Retries == nil || *updated.Max503Retries != 12 {
		t.Fatalf("expected returned max_503_retries=12, got %+v", updated.Max503Retries)
	}

	record := loadChannelRecord(t, 100)
	if record.Max503Retries == nil || *record.Max503Retries != 12 {
		t.Fatalf("expected persisted max_503_retries=12, got %+v", record.Max503Retries)
	}
}

func TestChannelUpdatePreservesNilFields(t *testing.T) {
	// Omitted fields (nil pointers) must not be touched. Seed a channel that
	// already has retry_on_503=1 and max_503_retries=5 persisted, then update an
	// unrelated field (name) and confirm the retry settings survive.
	ctx := context.Background()
	existingRetry := 1
	existingMax := 5
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seeded := model.Channel{Name: "ch-preserve", RetryOn503: &existingRetry, Max503Retries: &existingMax}
	seedChannelRecord(t, seeded)

	newName := "ch-preserve-renamed"
	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:   100,
		Name: &newName,
	}, ctx)
	if err != nil {
		t.Fatalf("ChannelUpdate() error = %v", err)
	}
	if updated.Name != "ch-preserve-renamed" {
		t.Fatalf("expected name updated, got %q", updated.Name)
	}
	if updated.RetryOn503 == nil || *updated.RetryOn503 != 1 {
		t.Fatalf("expected retry_on_503 preserved=1, got %+v", updated.RetryOn503)
	}
	if updated.Max503Retries == nil || *updated.Max503Retries != 5 {
		t.Fatalf("expected max_503_retries preserved=5, got %+v", updated.Max503Retries)
	}

	record := loadChannelRecord(t, 100)
	if record.RetryOn503 == nil || *record.RetryOn503 != 1 {
		t.Fatalf("expected persisted retry_on_503=1, got %+v", record.RetryOn503)
	}
	if record.Max503Retries == nil || *record.Max503Retries != 5 {
		t.Fatalf("expected persisted max_503_retries=5, got %+v", record.Max503Retries)
	}
}

func TestChannelUpdateDifferentialSelectsOnlyProvidedFields(t *testing.T) {
	// Provide only retry_on_503 (not max_503_retries). The UPDATE statement
	// must select only "retry_on_503"; max_503_retries (already 8 in DB) must
	// remain 8, proving differential PATCH does not zero-out omitted fields.
	ctx := context.Background()
	existingMax := 8
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seeded := model.Channel{Name: "ch-diff", Max503Retries: &existingMax}
	seedChannelRecord(t, seeded)

	v := 1
	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:         100,
		RetryOn503: &v,
	}, ctx)
	if err != nil {
		t.Fatalf("ChannelUpdate() error = %v", err)
	}
	if updated.RetryOn503 == nil || *updated.RetryOn503 != 1 {
		t.Fatalf("expected retry_on_503=1, got %+v", updated.RetryOn503)
	}
	if updated.Max503Retries == nil || *updated.Max503Retries != 8 {
		t.Fatalf("expected max_503_retries preserved=8, got %+v", updated.Max503Retries)
	}

	record := loadChannelRecord(t, 100)
	if record.Max503Retries == nil || *record.Max503Retries != 8 {
		t.Fatalf("expected persisted max_503_retries=8, got %+v", record.Max503Retries)
	}
	if record.RetryOn503 == nil || *record.RetryOn503 != 1 {
		t.Fatalf("expected persisted retry_on_503=1, got %+v", record.RetryOn503)
	}
}

func TestChannelUpdateReturnsChannelNotFoundWhenMissingFromCache(t *testing.T) {
	ctx := context.Background()
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	// No seed: cache lookup misses.
	_, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: 999}, ctx)
	if err == nil || err.Error() != "channel not found" {
		t.Fatalf("expected channel not found, got %v", err)
	}
}

func TestChannelUpdateAcceptsAllThreeFieldsTogether(t *testing.T) {
	ctx := context.Background()
	retry := 1
	maxRetries := 3
	conc := 5
	restore := isolateChannelCaches(t)
	defer restore()
	closeDB := withChannelSQLiteDB(t)
	defer closeDB()

	seedChannelRecord(t, model.Channel{Name: "ch-all"})

	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:             100,
		RetryOn503:     &retry,
		Max503Retries:  &maxRetries,
		MaxConcurrency: &conc,
	}, ctx)
	if err != nil {
		t.Fatalf("ChannelUpdate() error = %v", err)
	}
	if updated.RetryOn503 == nil || *updated.RetryOn503 != 1 {
		t.Fatalf("expected retry_on_503=1, got %+v", updated.RetryOn503)
	}
	if updated.Max503Retries == nil || *updated.Max503Retries != 3 {
		t.Fatalf("expected max_503_retries=3, got %+v", updated.Max503Retries)
	}
	if updated.MaxConcurrency == nil || *updated.MaxConcurrency != 5 {
		t.Fatalf("expected max_concurrency=5, got %+v", updated.MaxConcurrency)
	}

	record := loadChannelRecord(t, 100)
	if record.RetryOn503 == nil || *record.RetryOn503 != 1 {
		t.Fatalf("expected persisted retry_on_503=1, got %+v", record.RetryOn503)
	}
	if record.Max503Retries == nil || *record.Max503Retries != 3 {
		t.Fatalf("expected persisted max_503_retries=3, got %+v", record.Max503Retries)
	}
	if record.MaxConcurrency == nil || *record.MaxConcurrency != 5 {
		t.Fatalf("expected persisted max_concurrency=5, got %+v", record.MaxConcurrency)
	}
}