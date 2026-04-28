package storage

import (
	"testing"

	"github.com/civilware/Gnomon/structures"
)

func TestGetAllTelaSCIDs_Bbolt(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela-scids-bbolt.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	bbs.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", IsTelaIndex: true})
	bbs.StoreTelaMetadata("scid-b", &structures.TelaMetadata{SCID: "scid-b", IsTelaIndex: false})
	bbs.StoreTelaMetadata("scid-c", &structures.TelaMetadata{SCID: "scid-c", IsTelaIndex: true})

	scids := bbs.GetAllTelaSCIDs()
	if len(scids) != 2 {
		t.Fatalf("expected 2 tela scids, got %d", len(scids))
	}
}

func TestGetAllTelaSCIDs_Graviton(t *testing.T) {
	grav, err := NewGravDB(t.TempDir(), "20ms")
	if err != nil {
		t.Fatalf("failed to create gravdb store: %v", err)
	}

	grav.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", IsTelaIndex: true})
	grav.StoreTelaMetadata("scid-b", &structures.TelaMetadata{SCID: "scid-b", IsTelaIndex: false})
	grav.StoreTelaMetadata("scid-c", &structures.TelaMetadata{SCID: "scid-c", IsTelaIndex: true})

	scids := grav.GetAllTelaSCIDs()
	if len(scids) != 2 {
		t.Fatalf("expected 2 tela scids, got %d", len(scids))
	}
}

func TestGetQueryableSCIDs_Bbolt(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela-queryable-bbolt.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	bbs.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", IsTelaIndex: true})
	bbs.StoreSCIDInteractionHeight("scid-a", 10)

	bbs.StoreTelaMetadata("scid-b", &structures.TelaMetadata{SCID: "scid-b", IsTelaIndex: true})
	// no interaction height for scid-b

	bbs.StoreTelaMetadata("scid-c", &structures.TelaMetadata{SCID: "scid-c", IsTelaIndex: false})
	bbs.StoreSCIDInteractionHeight("scid-c", 10)

	scids := bbs.GetQueryableSCIDs()
	if len(scids) != 1 || scids[0] != "scid-a" {
		t.Fatalf("expected [scid-a], got %v", scids)
	}
}

func TestGetQueryableSCIDs_Graviton(t *testing.T) {
	grav, err := NewGravDB(t.TempDir(), "20ms")
	if err != nil {
		t.Fatalf("failed to create gravdb store: %v", err)
	}

	grav.StoreTelaMetadata("scid-a", &structures.TelaMetadata{SCID: "scid-a", IsTelaIndex: true})
	grav.StoreSCIDInteractionHeight("scid-a", 10, false)

	grav.StoreTelaMetadata("scid-b", &structures.TelaMetadata{SCID: "scid-b", IsTelaIndex: true})
	// no interaction height for scid-b

	grav.StoreTelaMetadata("scid-c", &structures.TelaMetadata{SCID: "scid-c", IsTelaIndex: false})
	grav.StoreSCIDInteractionHeight("scid-c", 10, false)

	scids := grav.GetQueryableSCIDs()
	if len(scids) != 1 || scids[0] != "scid-a" {
		t.Fatalf("expected [scid-a], got %v", scids)
	}
}

func TestGetQueryableSCIDs_Parity(t *testing.T) {
	bbs, err := NewBBoltDB(t.TempDir(), "tela-parity-bbolt.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}
	defer bbs.DB.Close()

	grav, err := NewGravDB(t.TempDir(), "20ms")
	if err != nil {
		t.Fatalf("failed to create gravdb store: %v", err)
	}

	for _, scid := range []string{"scid-1", "scid-2", "scid-3"} {
		meta := &structures.TelaMetadata{SCID: scid, IsTelaIndex: true}
		bbs.StoreTelaMetadata(scid, meta)
		grav.StoreTelaMetadata(scid, meta)
	}

	// Only scid-1 and scid-3 have interaction heights
	bbs.StoreSCIDInteractionHeight("scid-1", 5)
	bbs.StoreSCIDInteractionHeight("scid-3", 15)
	grav.StoreSCIDInteractionHeight("scid-1", 5, false)
	grav.StoreSCIDInteractionHeight("scid-3", 15, false)

	bbsResult := bbs.GetQueryableSCIDs()
	gravResult := grav.GetQueryableSCIDs()

	if len(bbsResult) != len(gravResult) {
		t.Fatalf("parity mismatch: bbolt=%v graviton=%v", bbsResult, gravResult)
	}

	bbsMap := make(map[string]bool)
	for _, s := range bbsResult {
		bbsMap[s] = true
	}
	for _, s := range gravResult {
		if !bbsMap[s] {
			t.Fatalf("parity mismatch: graviton has %s not in bbolt", s)
		}
	}
}
