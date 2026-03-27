package indexer

import (
	"testing"

	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
)

func TestIndexerGetTelaMetadataSummarySince(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "indexer-tela-metadata.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	vars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: "Example App"}}
	if _, err := bbs.StoreSCIDVariableDetails("scid-tela", vars, 5); err != nil {
		t.Fatalf("failed to store vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store height: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	result := idx.GetTelaMetadataSummarySince(0)
	if result.Count != 1 || len(result.Results) != 1 || result.Results[0].NameHdr != "Example App" {
		t.Fatalf("unexpected tela metadata summary: %#v", result)
	}
}

func TestIndexerGetAllTelaMetadata(t *testing.T) {
	bbs, err := storage.NewBBoltDB(t.TempDir(), "indexer-tela-all.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	if err := bbs.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", NameHdr: "Example", DisplayName: "Example", ArtifactKind: "index", IsTelaIndex: true}); err != nil {
		t.Fatalf("failed to store tela metadata: %v", err)
	}
	if err := bbs.StoreTelaMetadata("scid-b", &structures.TelaMetadata{SCID: "scid-b", NameHdr: "Other", IsTelaIndex: false}); err != nil {
		t.Fatalf("failed to store non-tela metadata: %v", err)
	}

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	result := idx.GetAllTelaMetadata()
	if result.Count != 1 || len(result.Results) != 1 || result.Results[0].SCID != "scid-a" {
		t.Fatalf("unexpected all tela metadata result: %#v", result)
	}
}
