package indexer

import (
	"testing"

	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
)

func TestPruneDerivedDataAbove_BoltDB(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "rollback.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	if err := bbs.StoreSCIDChange("scid-old", 5); err != nil {
		t.Fatalf("failed to store old change: %v", err)
	}
	if err := bbs.StoreSCIDChange("scid-new", 10); err != nil {
		t.Fatalf("failed to store new change: %v", err)
	}
	if err := bbs.StoreTelaMetadata("scid-old", &structures.TelaMetadata{SCID: "scid-old", IsTelaIndex: true, Topoheight: 5}); err != nil {
		t.Fatalf("failed to store old metadata: %v", err)
	}
	if err := bbs.StoreTelaMetadata("scid-new", &structures.TelaMetadata{SCID: "scid-new", IsTelaIndex: true, Topoheight: 10}); err != nil {
		t.Fatalf("failed to store new metadata: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	if err := idx.PruneDerivedDataAbove(5); err != nil {
		t.Fatalf("failed pruning derived data: %v", err)
	}

	if got := bbs.GetSCIDChangesSince(0); len(got) != 1 || got[0] != "scid-old" {
		t.Fatalf("unexpected remaining change journal contents: %v", got)
	}
	if meta := bbs.GetTelaMetadata("scid-new"); meta != nil {
		t.Fatalf("expected new tela metadata to be pruned, got %#v", meta)
	}
	if meta := bbs.GetTelaMetadata("scid-old"); meta == nil {
		t.Fatalf("expected old tela metadata to remain")
	}
}

func TestPruneDerivedDataAbove_RebuildsOlderTelaMetadata(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "rollback-rebuild.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	oldVars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: "Old Name"}}
	newVars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: "New Name"}}

	if _, err := bbs.StoreSCIDVariableDetails("scid-a", oldVars, 5); err != nil {
		t.Fatalf("failed to store old vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight("scid-a", 5); err != nil {
		t.Fatalf("failed to store old interaction height: %v", err)
	}
	if _, err := bbs.StoreSCIDVariableDetails("scid-a", newVars, 10); err != nil {
		t.Fatalf("failed to store new vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight("scid-a", 10); err != nil {
		t.Fatalf("failed to store new interaction height: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	if err := idx.PruneDerivedDataAbove(5); err != nil {
		t.Fatalf("failed pruning derived data: %v", err)
	}

	meta := bbs.GetTelaMetadata("scid-a")
	if meta == nil || meta.NameHdr != "Old Name" || meta.Topoheight != 5 {
		t.Fatalf("expected metadata to rebuild to old version, got %#v", meta)
	}
}
