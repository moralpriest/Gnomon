package wsserver

import (
	"context"
	"testing"

	"github.com/civilware/Gnomon/indexer"
	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
)

func testBoltIndexer(t *testing.T) *indexer.Indexer {
	t.Helper()
	bbs, err := storage.NewBBoltDB(t.TempDir(), "ws-changed.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	t.Cleanup(func() { _ = bbs.DB.Close() })
	return indexer.NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
}

func TestChangedSCIDsWSHelper(t *testing.T) {
	idx := testBoltIndexer(t)
	if err := idx.BBSBackend.StoreSCIDChange("scid-a", 5); err != nil {
		t.Fatalf("failed to store change: %v", err)
	}

	result, err := ChangedSCIDs(context.Background(), structures.WS_ChangedSCIDs_Params{Height: 0}, idx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Count != 1 || len(result.SCIDs) != 1 || result.SCIDs[0] != "scid-a" {
		t.Fatalf("unexpected changed ws result: %#v", result)
	}
}

func TestChangedTelaWSHelper(t *testing.T) {
	idx := testBoltIndexer(t)
	vars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: "App"}}
	if _, err := idx.BBSBackend.StoreSCIDVariableDetails("scid-tela", vars, 5); err != nil {
		t.Fatalf("failed to store vars: %v", err)
	}
	if _, err := idx.BBSBackend.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store interaction height: %v", err)
	}

	result, err := ChangedTela(context.Background(), structures.WS_ChangedSCIDs_Params{Height: 0}, idx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Count != 1 || len(result.SCIDs) != 1 || result.SCIDs[0] != "scid-tela" {
		t.Fatalf("unexpected tela changed ws result: %#v", result)
	}
}

func TestTelaMetadataWSHelper(t *testing.T) {
	idx := testBoltIndexer(t)
	vars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: "App"}}
	if _, err := idx.BBSBackend.StoreSCIDVariableDetails("scid-tela", vars, 5); err != nil {
		t.Fatalf("failed to store vars: %v", err)
	}
	if _, err := idx.BBSBackend.StoreSCIDInteractionHeight("scid-tela", 5); err != nil {
		t.Fatalf("failed to store interaction height: %v", err)
	}

	result, err := TelaMetadata(context.Background(), structures.WS_TelaMetadata_Params{}, idx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Count != 1 || len(result.Results) != 1 || result.Results[0].NameHdr != "App" {
		t.Fatalf("unexpected tela metadata ws result: %#v", result)
	}
}

func TestTelaMetadataWSHelper_LimitApplies(t *testing.T) {
	idx := testBoltIndexer(t)
	for i, scid := range []string{"scid-a", "scid-b"} {
		vars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: scid}}
		if _, err := idx.BBSBackend.StoreSCIDVariableDetails(scid, vars, int64(i+1)); err != nil {
			t.Fatalf("failed to store vars: %v", err)
		}
		if _, err := idx.BBSBackend.StoreSCIDInteractionHeight(scid, int64(i+1)); err != nil {
			t.Fatalf("failed to store interaction height: %v", err)
		}
	}

	result, err := TelaMetadata(context.Background(), structures.WS_TelaMetadata_Params{Limit: 1}, idx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Count != 1 || len(result.Results) != 1 {
		t.Fatalf("unexpected limited tela metadata ws result: %#v", result)
	}
}

func TestTelaMetadataWSHelper_OffsetAndLimitApply(t *testing.T) {
	idx := testBoltIndexer(t)
	for i, scid := range []string{"scid-a", "scid-b", "scid-c"} {
		vars := []*structures.SCIDVariable{{Key: "C", Value: "TELA INDEX"}, {Key: "NameHdr", Value: scid}}
		if _, err := idx.BBSBackend.StoreSCIDVariableDetails(scid, vars, int64(i+1)); err != nil {
			t.Fatalf("failed to store vars: %v", err)
		}
		if _, err := idx.BBSBackend.StoreSCIDInteractionHeight(scid, int64(i+1)); err != nil {
			t.Fatalf("failed to store interaction height: %v", err)
		}
	}

	result, err := TelaMetadata(context.Background(), structures.WS_TelaMetadata_Params{Offset: 1, Limit: 1}, idx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Count != 1 || len(result.Results) != 1 || result.Results[0].SCID != "scid-b" || result.Offset != 1 || result.Limit != 1 {
		t.Fatalf("unexpected paged tela metadata ws result: %#v", result)
	}
}
