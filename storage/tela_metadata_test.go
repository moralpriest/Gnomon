package storage

import (
	"path/filepath"
	"testing"

	"github.com/civilware/Gnomon/structures"
)

func TestDeriveTelaMetadata(t *testing.T) {
	meta := DeriveTelaMetadata("scid-a", 10, []*structures.SCIDVariable{
		{Key: "C", Value: "TELA INDEX"},
		{Key: "dURL", Value: "https://example.test"},
		{Key: "NameHdr", Value: "Example"},
		{Key: "DescrHdr", Value: "Description"},
		{Key: "IconHdr", Value: "icon.png"},
		{Key: "DOC1", Value: "doc"},
	})

	if meta == nil {
		t.Fatalf("expected metadata to be derived")
	}
	if !meta.IsTelaIndex || meta.DURL != "https://example.test" || meta.NameHdr != "Example" || meta.DocCount != 1 {
		t.Fatalf("unexpected derived metadata: %#v", meta)
	}
}

func TestTelaMetadataRoundTrip_BoltDB(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	meta := &structures.TelaMetadata{SCID: "scid-a", NameHdr: "Example", IsTelaIndex: true, Topoheight: 5}
	if err := bbs.StoreTelaMetadata("scid-a", meta); err != nil {
		t.Fatalf("failed to store tela metadata: %v", err)
	}

	got := bbs.GetTelaMetadata("scid-a")
	if got == nil || got.NameHdr != "Example" || !got.IsTelaIndex {
		t.Fatalf("unexpected tela metadata: %#v", got)
	}

	all := bbs.GetAllTelaMetadata()
	if len(all) != 1 || all[0].SCID != "scid-a" {
		t.Fatalf("unexpected tela metadata list: %#v", all)
	}
}

func TestTelaMetadataRoundTrip_GravDB(t *testing.T) {
	grav, err := NewGravDB(filepath.Join(t.TempDir(), "grav"), "1ms")
	if err != nil {
		t.Fatalf("failed to create gravdb store: %v", err)
	}

	meta := &structures.TelaMetadata{SCID: "scid-a", NameHdr: "Example", IsTelaIndex: true, Topoheight: 5}
	if err := grav.StoreTelaMetadata("scid-a", meta); err != nil {
		t.Fatalf("failed to store tela metadata: %v", err)
	}

	got := grav.GetTelaMetadata("scid-a")
	if got == nil || got.NameHdr != "Example" || !got.IsTelaIndex {
		t.Fatalf("unexpected tela metadata: %#v", got)
	}

	all := grav.GetAllTelaMetadata()
	if len(all) != 1 || all[0].SCID != "scid-a" {
		t.Fatalf("unexpected tela metadata list: %#v", all)
	}
}

func TestDeriveTelaMetadata_AlternateFields(t *testing.T) {
	meta := DeriveTelaMetadata("scid-alt", 20, []*structures.SCIDVariable{
		{Key: "C", Value: "TELA INDEX"},
		{Key: "name", Value: "Alt Name"},
		{Key: "description", Value: "Alt Description"},
		{Key: "icon", Value: "alt-icon.png"},
		{Key: "docCount", Value: "2"},
	})

	if meta == nil {
		t.Fatalf("expected metadata to be derived")
	}
	if meta.NameHdr != "Alt Name" || meta.DescrHdr != "Alt Description" || meta.IconHdr != "alt-icon.png" || meta.DocCount != 2 {
		t.Fatalf("unexpected alternate derived metadata: %#v", meta)
	}
}

func TestDeriveTelaMetadata_DerobeatsStyleFields(t *testing.T) {
	meta := DeriveTelaMetadata("scid-derobeats", 20, []*structures.SCIDVariable{
		{Key: "C", Value: "Function InitializePrivate() Uint64"},
		{Key: "var_header_name", Value: "DeroBeats"},
		{Key: "var_header_description", Value: "Decentralized music platform."},
		{Key: "var_header_icon", Value: "https://example/icon.png"},
		{Key: "dURL", Value: "derobeats.tela"},
		{Key: "telaVersion", Value: "1.1.0"},
		{Key: "DOC1", Value: "doc-hash"},
	})

	if meta == nil {
		t.Fatalf("expected metadata to be derived")
	}
	if !meta.IsTelaIndex || meta.NameHdr != "DeroBeats" || meta.DescrHdr == "" || meta.IconHdr == "" || meta.DURL != "derobeats.tela" || meta.DocCount != 1 {
		t.Fatalf("unexpected derobeats-style metadata: %#v", meta)
	}
}

func TestBackfillTelaMetadata_BoltDB(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela-backfill.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	scid := "b1e1cba50cbfd8edbb12b01220ffebbece300d4936516a87fc2255fa8e23d8a2"
	if _, err := bbs.StoreOwner(scid, "owner"); err != nil {
		t.Fatalf("failed to store owner: %v", err)
	}
	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "DeroBeats"},
		{Key: "var_header_description", Value: "Decentralized music platform."},
		{Key: "var_header_icon", Value: "https://example/icon.png"},
		{Key: "dURL", Value: "derobeats.tela"},
		{Key: "telaVersion", Value: "1.1.0"},
		{Key: "DOC1", Value: "doc-hash"},
		{Key: "C", Value: "Function InitializePrivate() Uint64"},
	}
	if _, err := bbs.StoreSCIDVariableDetails(scid, vars, 5); err != nil {
		t.Fatalf("failed to store vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight(scid, 5); err != nil {
		t.Fatalf("failed to store interaction height: %v", err)
	}
	if err := bbs.DeleteTelaMetadata(scid); err != nil {
		t.Fatalf("failed to clear tela metadata before backfill: %v", err)
	}

	if err := BackfillTelaMetadata(bbs); err != nil {
		t.Fatalf("failed to backfill tela metadata: %v", err)
	}

	meta := bbs.GetTelaMetadata(scid)
	if meta == nil || meta.NameHdr != "DeroBeats" || meta.DURL != "derobeats.tela" || !meta.IsTelaIndex {
		t.Fatalf("unexpected backfilled tela metadata: %#v", meta)
	}
}

func TestRebuildTelaMetadataAtOrBelow_MaxTopoheightUsesLatestInteraction(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela-rebuild-max.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	scid := "b1e1cba50cbfd8edbb12b01220ffebbece300d4936516a87fc2255fa8e23d8a2"
	if _, err := bbs.StoreOwner(scid, "owner"); err != nil {
		t.Fatalf("failed to store owner: %v", err)
	}
	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "DeroBeats"},
		{Key: "var_header_description", Value: "Decentralized music platform."},
		{Key: "var_header_icon", Value: "https://example/icon.png"},
		{Key: "dURL", Value: "derobeats.tela"},
		{Key: "telaVersion", Value: "1.1.0"},
		{Key: "DOC1", Value: "doc-hash"},
		{Key: "C", Value: "Function InitializePrivate() Uint64"},
	}
	if _, err := bbs.StoreSCIDVariableDetails(scid, vars, 6784686); err != nil {
		t.Fatalf("failed to store vars: %v", err)
	}
	if _, err := bbs.StoreSCIDInteractionHeight(scid, 6784686); err != nil {
		t.Fatalf("failed to store interaction height: %v", err)
	}

	if err := RebuildTelaMetadataAtOrBelow(bbs, scid, int64(^uint64(0)>>1)); err != nil {
		t.Fatalf("failed rebuilding tela metadata: %v", err)
	}

	meta := bbs.GetTelaMetadata(scid)
	if meta == nil || meta.NameHdr != "DeroBeats" || meta.Topoheight != 6784686 {
		t.Fatalf("unexpected rebuilt tela metadata: %#v", meta)
	}
}
