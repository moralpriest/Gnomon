package storage

import (
	"path/filepath"
	"testing"

	"github.com/civilware/Gnomon/structures"
	"github.com/sirupsen/logrus"
)

func init() {
	structures.Logger = *logrus.New()
}

type parityStore struct {
	name            string
	storeLast       func(int64) error
	getLast         func() (int64, error)
	storeOwner      func(string, string) error
	getOwner        func(string) string
	getAllOwners    func() map[string]string
	storeInstH      func(string, int64) error
	getInstH        func(string) int64
	getAllInstH     func() map[string]int64
	storeInteract   func(string, int64) error
	getInteract     func(string) []int64
	getIntIndex     func(int64, []int64, bool) int64
	storeChange     func(string, int64) error
	getChangesAt    func(int64) []string
	getChangesSince func(int64) []string
	storeInvoke     func(string, int64) error
	storeVars       func(string, int64) error
	storeInstDetail func(string, int64) error
	close           func()
}

func newBoltParityStore(t *testing.T) parityStore {
	t.Helper()

	bbs, err := NewBBoltDB(t.TempDir(), "parity.db")
	if err != nil {
		t.Fatalf("failed to create bbolt store: %v", err)
	}

	return parityStore{
		name: "boltdb",
		storeLast: func(v int64) error {
			_, err := bbs.StoreLastIndexHeight(v)
			return err
		},
		getLast: bbs.GetLastIndexHeight,
		storeOwner: func(scid, owner string) error {
			_, err := bbs.StoreOwner(scid, owner)
			return err
		},
		getOwner:     bbs.GetOwner,
		getAllOwners: bbs.GetAllOwnersAndSCIDs,
		storeInstH: func(scid string, height int64) error {
			_, err := bbs.StoreInstallHeight(scid, height)
			return err
		},
		getInstH:    bbs.GetInstallHeight,
		getAllInstH: bbs.GetAllSCIDsAndInstallHeights,
		storeInteract: func(scid string, height int64) error {
			_, err := bbs.StoreSCIDInteractionHeight(scid, height)
			return err
		},
		getInteract:     bbs.GetSCIDInteractionHeight,
		getIntIndex:     bbs.GetInteractionIndex,
		storeChange:     bbs.StoreSCIDChange,
		getChangesAt:    bbs.GetSCIDChangesAtTopoheight,
		getChangesSince: bbs.GetSCIDChangesSince,
		storeInvoke: func(scid string, height int64) error {
			_, err := bbs.StoreInvokeDetails(scid, "sender", "entrypoint", height, &structures.SCTXParse{Scid: scid, Height: height, Sender: "sender", Entrypoint: "entrypoint", Txid: "abcdef123456"})
			return err
		},
		storeVars: func(scid string, height int64) error {
			_, err := bbs.StoreSCIDVariableDetails(scid, []*structures.SCIDVariable{{Key: "C", Value: "code"}}, height)
			return err
		},
		storeInstDetail: func(scid string, height int64) error {
			_, err := bbs.StoreSCIDInstallSCDetails(scid, &structures.SCTXParse{Scid: scid, Height: height, Sender: "sender", Entrypoint: "installsc"})
			return err
		},
		close: func() {
			if bbs.DB != nil {
				_ = bbs.DB.Close()
			}
		},
	}
}

func newGravParityStore(t *testing.T) parityStore {
	t.Helper()

	grav, err := NewGravDB(filepath.Join(t.TempDir(), "grav"), "1ms")
	if err != nil {
		t.Fatalf("failed to create graviton store: %v", err)
	}

	return parityStore{
		name: "gravdb",
		storeLast: func(v int64) error {
			_, _, err := grav.StoreLastIndexHeight(v, false)
			return err
		},
		getLast: grav.GetLastIndexHeight,
		storeOwner: func(scid, owner string) error {
			_, _, err := grav.StoreOwner(scid, owner, false)
			return err
		},
		getOwner:     grav.GetOwner,
		getAllOwners: grav.GetAllOwnersAndSCIDs,
		storeInstH: func(scid string, height int64) error {
			_, _, err := grav.StoreInstallHeight(scid, height, false)
			return err
		},
		getInstH:    grav.GetInstallHeight,
		getAllInstH: grav.GetAllSCIDsAndInstallHeights,
		storeInteract: func(scid string, height int64) error {
			_, _, err := grav.StoreSCIDInteractionHeight(scid, height, false)
			return err
		},
		getInteract:     grav.GetSCIDInteractionHeight,
		getIntIndex:     grav.GetInteractionIndex,
		storeChange:     grav.StoreSCIDChange,
		getChangesAt:    grav.GetSCIDChangesAtTopoheight,
		getChangesSince: grav.GetSCIDChangesSince,
		storeInvoke: func(scid string, height int64) error {
			_, _, err := grav.StoreInvokeDetails(scid, "sender", "entrypoint", height, &structures.SCTXParse{Scid: scid, Height: height, Sender: "sender", Entrypoint: "entrypoint", Txid: "abcdef123456"}, false)
			return err
		},
		storeVars: func(scid string, height int64) error {
			_, _, err := grav.StoreSCIDVariableDetails(scid, []*structures.SCIDVariable{{Key: "C", Value: "code"}}, height, false)
			return err
		},
		storeInstDetail: func(scid string, height int64) error {
			_, _, err := grav.StoreSCIDInstallSCDetails(scid, &structures.SCTXParse{Scid: scid, Height: height, Sender: "sender", Entrypoint: "installsc"}, false)
			return err
		},
		close: func() {},
	}
}

func TestStorageParity_LastIndexHeight(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}
	for _, store := range stores {
		defer store.close()

		if err := store.storeLast(321); err != nil {
			t.Fatalf("%s: failed storing last index height: %v", store.name, err)
		}

		got, err := store.getLast()
		if err != nil {
			t.Fatalf("%s: failed reading last index height: %v", store.name, err)
		}
		if got != 321 {
			t.Fatalf("%s: unexpected last index height: got %d want 321", store.name, got)
		}
	}
}

func TestStorageParity_OwnerRoundTrip(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}
	const scid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const owner = "owner-address"

	for _, store := range stores {
		defer store.close()

		if err := store.storeOwner(scid, owner); err != nil {
			t.Fatalf("%s: failed storing owner: %v", store.name, err)
		}

		got := store.getOwner(scid)
		if got != owner {
			t.Fatalf("%s: unexpected owner: got %q want %q", store.name, got, owner)
		}
	}
}

func TestStorageParity_InstallHeightRoundTrip(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}
	const scid = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

	for _, store := range stores {
		defer store.close()

		if err := store.storeInstH(scid, 999); err != nil {
			t.Fatalf("%s: failed storing install height: %v", store.name, err)
		}

		got := store.getInstH(scid)
		if got != 999 {
			t.Fatalf("%s: unexpected install height: got %d want 999", store.name, got)
		}
	}
}

func TestStorageParity_AllOwnersAndInstallHeights(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}
	entries := map[string]struct {
		owner  string
		height int64
	}{
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": {owner: "owner-one", height: 100},
		"fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210": {owner: "owner-two", height: 200},
	}

	for _, store := range stores {
		defer store.close()

		for scid, entry := range entries {
			if err := store.storeOwner(scid, entry.owner); err != nil {
				t.Fatalf("%s: failed storing owner: %v", store.name, err)
			}
			if err := store.storeInstH(scid, entry.height); err != nil {
				t.Fatalf("%s: failed storing install height: %v", store.name, err)
			}
		}

		owners := store.getAllOwners()
		if len(owners) != len(entries) {
			t.Fatalf("%s: unexpected owner map size: got %d want %d", store.name, len(owners), len(entries))
		}
		for scid, entry := range entries {
			if owners[scid] != entry.owner {
				t.Fatalf("%s: unexpected owner for %s: got %q want %q", store.name, scid, owners[scid], entry.owner)
			}
		}

		heights := store.getAllInstH()
		if len(heights) != len(entries) {
			t.Fatalf("%s: unexpected install height map size: got %d want %d", store.name, len(heights), len(entries))
		}
		for scid, entry := range entries {
			if heights[scid] != entry.height {
				t.Fatalf("%s: unexpected install height for %s: got %d want %d", store.name, scid, heights[scid], entry.height)
			}
		}
	}
}

func TestStorageParity_InteractionHeightsRoundTrip(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}
	const scid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	expected := map[int64]bool{5: true, 12: true, 18: true}

	for _, store := range stores {
		defer store.close()

		for _, height := range []int64{12, 5, 18, 12} {
			if err := store.storeInteract(scid, height); err != nil {
				t.Fatalf("%s: failed storing interaction height %d: %v", store.name, height, err)
			}
		}

		got := store.getInteract(scid)
		if len(got) != len(expected) {
			t.Fatalf("%s: unexpected interaction count: got %d want %d (%v)", store.name, len(got), len(expected), got)
		}
		for _, height := range got {
			if !expected[height] {
				t.Fatalf("%s: unexpected interaction height %d in %v", store.name, height, got)
			}
		}
	}
}

func TestStorageParity_GetInteractionIndex(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}
	heights := []int64{12, 5, 18}

	for _, store := range stores {
		defer store.close()

		if got := store.getIntIndex(17, append([]int64(nil), heights...), false); got != 12 {
			t.Fatalf("%s: unexpected interaction index for 17: got %d want 12", store.name, got)
		}
		if got := store.getIntIndex(18, append([]int64(nil), heights...), false); got != 12 {
			t.Fatalf("%s: unexpected interaction index for 18: got %d want 12", store.name, got)
		}
		if got := store.getIntIndex(4, append([]int64(nil), heights...), false); got != 0 {
			t.Fatalf("%s: unexpected interaction index for 4: got %d want 0", store.name, got)
		}
		if got := store.getIntIndex(4, append([]int64(nil), heights...), true); got != 18 {
			t.Fatalf("%s: unexpected reverse-max interaction index: got %d want 18", store.name, got)
		}
	}
}

func TestStorageCurrentBehavior_InteractionHeightsDoNotRollbackAutomatically(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}
	const scid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	for _, store := range stores {
		defer store.close()

		for _, height := range []int64{9, 15} {
			if err := store.storeInteract(scid, height); err != nil {
				t.Fatalf("%s: failed storing interaction height %d: %v", store.name, height, err)
			}
		}

		got := store.getInteract(scid)
		if len(got) != 2 {
			t.Fatalf("%s: unexpected initial interaction set: %v", store.name, got)
		}

		// This test intentionally documents current behavior only: there is no
		// rollback/removal primitive yet, so once stored, heights remain present.
		if gotIdx := store.getIntIndex(10, append([]int64(nil), got...), false); gotIdx != 9 {
			t.Fatalf("%s: unexpected retained interaction index after simulated rewind: got %d want 9", store.name, gotIdx)
		}
	}
}

func TestStorageParity_SCIDChangeJournal(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}

	for _, store := range stores {
		defer store.close()

		entries := []struct {
			scid   string
			height int64
		}{
			{scid: "scid-b", height: 10},
			{scid: "scid-a", height: 10},
			{scid: "scid-c", height: 15},
			{scid: "scid-b", height: 15},
			{scid: "scid-b", height: 10},
		}

		for _, entry := range entries {
			if err := store.storeChange(entry.scid, entry.height); err != nil {
				t.Fatalf("%s: failed storing change %+v: %v", store.name, entry, err)
			}
		}

		gotAt := store.getChangesAt(10)
		if len(gotAt) != 2 || gotAt[0] != "scid-a" || gotAt[1] != "scid-b" {
			t.Fatalf("%s: unexpected changes at 10: %v", store.name, gotAt)
		}

		gotSince := store.getChangesSince(10)
		if len(gotSince) != 2 || gotSince[0] != "scid-b" || gotSince[1] != "scid-c" {
			t.Fatalf("%s: unexpected changes since 10: %v", store.name, gotSince)
		}
	}
}

func TestStorageChangeJournal_IsPopulatedByWritePaths(t *testing.T) {
	stores := []parityStore{newBoltParityStore(t), newGravParityStore(t)}

	for _, store := range stores {
		defer store.close()

		if err := store.storeInstH("scid-install", 5); err != nil {
			t.Fatalf("%s: failed storing install height: %v", store.name, err)
		}
		if err := store.storeInteract("scid-interact", 7); err != nil {
			t.Fatalf("%s: failed storing interaction height: %v", store.name, err)
		}
		if err := store.storeInvoke("scid-invoke", 9); err != nil {
			t.Fatalf("%s: failed storing invoke details: %v", store.name, err)
		}
		if err := store.storeVars("scid-vars", 11); err != nil {
			t.Fatalf("%s: failed storing variable details: %v", store.name, err)
		}
		if err := store.storeInstDetail("scid-install-detail", 13); err != nil {
			t.Fatalf("%s: failed storing install detail: %v", store.name, err)
		}

		expected := map[int64]string{
			5:  "scid-install",
			7:  "scid-interact",
			9:  "scid-invoke",
			11: "scid-vars",
			13: "scid-install-detail",
		}

		for height, scid := range expected {
			got := store.getChangesAt(height)
			if len(got) != 1 || got[0] != scid {
				t.Fatalf("%s: unexpected change journal contents at %d: %v", store.name, height, got)
			}
		}
	}
}
