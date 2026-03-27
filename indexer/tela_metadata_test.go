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

func TestPreferTopLevelTelaMetadata(t *testing.T) {
	results := []structures.TelaMetadata{
		{SCID: "scid-doc", DURL: "1.2.0.example-app.tela", DisplayName: "1.2.0.example-app.tela", ArtifactKind: "doc", IsTelaIndex: true},
		{SCID: "scid-index", DURL: "example-app.tela", NameHdr: "Example Exchange", DisplayName: "Example Exchange", ArtifactKind: "index", DescrHdr: "Trading app", IconHdr: "https://example/icon.png", IsTelaIndex: true},
	}

	idx := &Indexer{}
	filtered := idx.preferTopLevelTelaMetadata(results)
	if len(filtered) != 1 || filtered[0].SCID != "scid-index" {
		t.Fatalf("unexpected top-level preferred metadata: %#v", filtered)
	}
}

func TestPreferTopLevelTelaMetadata_PrefersHeaderBearingDocSibling(t *testing.T) {
	results := []structures.TelaMetadata{
		{SCID: "scid-index-file", DURL: "index.example.self.tela", DisplayName: "index.example.self.tela", ArtifactKind: "doc", IsTelaIndex: true},
		{SCID: "scid-app", DURL: "example.self.tela", NameHdr: "Hello from Example", DisplayName: "Hello from Example", ArtifactKind: "doc", DescrHdr: "self post", IconHdr: "https://example/icon.png", IsTelaIndex: true},
	}

	idx := &Indexer{}
	filtered := idx.preferTopLevelTelaMetadata(results)
	if len(filtered) != 1 || filtered[0].SCID != "scid-app" {
		t.Fatalf("unexpected preferred doc sibling metadata: %#v", filtered)
	}
}

func TestPreferTopLevelTelaMetadata_PromotesSiblingFields(t *testing.T) {
	results := []structures.TelaMetadata{
		{SCID: "scid-top", DURL: "example.self.tela", DisplayName: "example.self.tela", ArtifactKind: "index", IsTelaIndex: true},
		{SCID: "scid-doc", DURL: "example.self.tela", NameHdr: "Hello from Example", DisplayName: "Hello from Example", ArtifactKind: "doc", DescrHdr: "self post", IconHdr: "https://example/icon.png", IsTelaIndex: true},
	}

	idx := &Indexer{}
	filtered := idx.preferTopLevelTelaMetadata(results)
	if len(filtered) != 1 {
		t.Fatalf("unexpected filtered metadata: %#v", filtered)
	}
	if filtered[0].DisplayName != "Hello from Example" || filtered[0].DescrHdr != "self post" || filtered[0].IconHdr == "" {
		t.Fatalf("expected sibling field promotion, got %#v", filtered[0])
	}
}
