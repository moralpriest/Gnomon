package storage

import (
	"testing"

	"github.com/civilware/Gnomon/structures"
)

func testTelaCacheStore(t *testing.T, store interface {
	StoreTelaIndexCache(scid string, data []byte) error
	GetTelaIndexCache(scid string) []byte
	GetAllTelaIndexCaches() map[string][]byte
	StoreTelaCandidate(scid string, status string) error
	GetTelaCandidate(scid string) string
	GetAllTelaCandidates() map[string]string
}) {
	// Index cache
	if err := store.StoreTelaIndexCache("scid-1", []byte(`{"name":"app1"}`)); err != nil {
		t.Fatalf("store index cache failed: %v", err)
	}
	if err := store.StoreTelaIndexCache("scid-2", []byte(`{"name":"app2"}`)); err != nil {
		t.Fatalf("store index cache failed: %v", err)
	}

	data := store.GetTelaIndexCache("scid-1")
	if string(data) != `{"name":"app1"}` {
		t.Fatalf("unexpected cache data: %s", string(data))
	}

	all := store.GetAllTelaIndexCaches()
	if len(all) != 2 {
		t.Fatalf("expected 2 caches, got %d", len(all))
	}

	// Candidate cache
	if err := store.StoreTelaCandidate("scid-1", "valid"); err != nil {
		t.Fatalf("store candidate failed: %v", err)
	}
	if err := store.StoreTelaCandidate("scid-2", "invalid"); err != nil {
		t.Fatalf("store candidate failed: %v", err)
	}

	status := store.GetTelaCandidate("scid-1")
	if status != "valid" {
		t.Fatalf("unexpected candidate status: %s", status)
	}

	allCandidates := store.GetAllTelaCandidates()
	if len(allCandidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(allCandidates))
	}
}

func TestTelaCache_Bbolt(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela-cache-bbolt.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()
	testTelaCacheStore(t, bbs)
}

func TestTelaCache_Graviton(t *testing.T) {
	grav, err := NewGravDB(t.TempDir(), "20ms")
	if err != nil {
		t.Fatalf("failed to create gravdb store: %v", err)
	}
	testTelaCacheStore(t, grav)
}

func TestTelaCache_Parity(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela-cache-parity-bbolt.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	grav, err := NewGravDB(t.TempDir(), "20ms")
	if err != nil {
		t.Fatalf("failed to create gravdb store: %v", err)
	}

	for i := 0; i < 3; i++ {
		scid := structures.SCData{SCID: "scid-" + string(rune('a'+i))}.SCID
		bbs.StoreTelaIndexCache(scid, []byte(`data`))
		grav.StoreTelaIndexCache(scid, []byte(`data`))
		bbs.StoreTelaCandidate(scid, "valid")
		grav.StoreTelaCandidate(scid, "valid")
	}

	bbsAll := bbs.GetAllTelaIndexCaches()
	gravAll := grav.GetAllTelaIndexCaches()
	if len(bbsAll) != len(gravAll) {
		t.Fatalf("index cache parity mismatch: bbolt=%d graviton=%d", len(bbsAll), len(gravAll))
	}

	bbsCands := bbs.GetAllTelaCandidates()
	gravCands := grav.GetAllTelaCandidates()
	if len(bbsCands) != len(gravCands) {
		t.Fatalf("candidate parity mismatch: bbolt=%d graviton=%d", len(bbsCands), len(gravCands))
	}
}
