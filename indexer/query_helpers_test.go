package indexer

import (
	"testing"

	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
	"github.com/sirupsen/logrus"
)

func init() {
	structures.Logger = *logrus.New()
}

func TestIndexerGetChangedSCIDsSince_BoltDB(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "indexer-query.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	if err := bbs.StoreSCIDChange("scid-a", 5); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}
	if err := bbs.StoreSCIDChange("scid-b", 9); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	got := idx.GetChangedSCIDsSince(5)
	if len(got) != 1 || got[0] != "scid-b" {
		t.Fatalf("unexpected changed SCIDs: %v", got)
	}
}

func TestIndexerGetChangedSCIDsSince_GravDB(t *testing.T) {
	grav, err := storage.NewGravDB(t.TempDir(), "1ms")
	if err != nil {
		t.Fatalf("failed to create graviton store: %v", err)
	}

	if err := grav.StoreSCIDChange("scid-a", 5); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}
	if err := grav.StoreSCIDChange("scid-b", 9); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}

	idx := NewIndexer(grav, nil, "gravdb", nil, 0, "", "", false, false, nil, nil, false)
	got := idx.GetChangedSCIDsSince(5)
	if len(got) != 1 || got[0] != "scid-b" {
		t.Fatalf("unexpected changed SCIDs: %v", got)
	}
}

func TestIndexerGetChangedSCIDsSummarySince(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "indexer-summary.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	if err := bbs.StoreSCIDChange("scid-a", 8); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	summary := idx.GetChangedSCIDsSummarySince(5)

	if summary["topoheight"].(int64) != 5 {
		t.Fatalf("unexpected topoheight in summary: %#v", summary)
	}
	if summary["count"].(int) != 1 {
		t.Fatalf("unexpected count in summary: %#v", summary)
	}
	scids := summary["scids"].([]string)
	if len(scids) != 1 || scids[0] != "scid-a" {
		t.Fatalf("unexpected scids in summary: %#v", summary)
	}
}

func TestIndexerGetFilteredChangedSCIDsSince_BoltDBTelaFilter(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "indexer-filter.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	if _, err := bbs.StoreSCIDVariableDetails("scid-tela", []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}}, 5); err != nil {
		t.Fatalf("failed to store tela vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store tela height: %v", err)
	}
	if _, err := bbs.StoreSCIDVariableDetails("scid-other", []*structures.SCIDVariable{{Key: "C", Value: "plain contract"}}, 7); err != nil {
		t.Fatalf("failed to store other vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight("scid-other", 7); err != nil {
		t.Fatalf("failed to store other height: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	got := idx.GetFilteredChangedSCIDsSince(0, storage.ClassifierTelaIndex)
	if len(got) != 1 || got[0] != "scid-tela" {
		t.Fatalf("unexpected filtered changed SCIDs: %v", got)
	}
}

func TestIndexerGetTelaChangedSCIDsSince_BoltDB(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "indexer-tela.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	if _, err := bbs.StoreSCIDVariableDetails("scid-tela", []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}}, 5); err != nil {
		t.Fatalf("failed to store tela vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store tela height: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	got := idx.GetTelaChangedSCIDsSince(0)
	if len(got) != 1 || got[0] != "scid-tela" {
		t.Fatalf("unexpected tela changed SCIDs: %v", got)
	}
}
