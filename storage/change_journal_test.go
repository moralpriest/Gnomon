package storage

import (
	"reflect"
	"testing"
)

func TestSCIDChangeJournal_DeduplicatesAndSortsPerHeight(t *testing.T) {
	j := NewSCIDChangeJournal()
	_ = j.StoreSCIDChange("scid-b", 10)
	_ = j.StoreSCIDChange("scid-a", 10)
	_ = j.StoreSCIDChange("scid-b", 10)

	got := j.GetSCIDChangesAtTopoheight(10)
	want := []string{"scid-a", "scid-b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected changes at topoheight: got %v want %v", got, want)
	}
}

func TestSCIDChangeJournal_ReturnsChangesSinceExclusiveHeight(t *testing.T) {
	j := NewSCIDChangeJournal()
	_ = j.StoreSCIDChange("scid-a", 5)
	_ = j.StoreSCIDChange("scid-b", 10)
	_ = j.StoreSCIDChange("scid-c", 12)
	_ = j.StoreSCIDChange("scid-b", 15)

	got := j.GetSCIDChangesSince(10)
	want := []string{"scid-b", "scid-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected changes since height: got %v want %v", got, want)
	}
}

func TestSCIDChangeJournal_IgnoresInvalidWrites(t *testing.T) {
	j := NewSCIDChangeJournal()
	_ = j.StoreSCIDChange("", 5)
	_ = j.StoreSCIDChange("scid-a", -1)

	if got := j.GetSCIDChangesSince(0); got != nil {
		t.Fatalf("expected no changes for invalid writes, got %v", got)
	}
}

func TestSCIDChangeJournal_DeleteOperations(t *testing.T) {
	j := NewSCIDChangeJournal()
	_ = j.StoreSCIDChange("scid-a", 5)
	_ = j.StoreSCIDChange("scid-b", 10)
	_ = j.StoreSCIDChange("scid-c", 15)

	_ = j.DeleteSCIDChange("scid-b", 10)
	if got := j.GetSCIDChangesAtTopoheight(10); got != nil {
		t.Fatalf("expected height 10 to be empty after delete, got %v", got)
	}

	_ = j.DeleteSCIDChangesAbove(5)
	if got := j.GetSCIDChangesSince(0); !reflect.DeepEqual(got, []string{"scid-a"}) {
		t.Fatalf("unexpected journal contents after delete above: %v", got)
	}
}
