package indexer

import (
	"runtime"
	"testing"
	"time"

	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
)

func TestStartTelaPrewarm_Lifecycle(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "indexer-prewarm.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	// Seed some TELA metadata.
	if err := bbs.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", IsTelaIndex: true}); err != nil {
		t.Fatalf("failed to store tela metadata: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight("scid-a", 10); err != nil {
		t.Fatalf("failed to store interaction height: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 1, "", "", false, false, nil, nil, false)
	idx.InteractionIndexReady.Store(true)

	before := runtime.NumGoroutine()
	idx.startTelaPrewarm()
	time.Sleep(100 * time.Millisecond) // let the goroutine start

	after := runtime.NumGoroutine()
	if after <= before {
		t.Fatal("expected prewarm goroutine to be running")
	}

	// Signal shutdown and give it time to exit.
	idx.Closing.Store(true)
	time.Sleep(100 * time.Millisecond)

	// Goroutine should have exited. We allow a small margin for runtime
	// scheduling differences.
	for i := 0; i < 10; i++ {
		if runtime.NumGoroutine() <= before {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if runtime.NumGoroutine() > before+2 {
		t.Fatalf("prewarm goroutine did not exit cleanly: before=%d after=%d", before, runtime.NumGoroutine())
	}
}
