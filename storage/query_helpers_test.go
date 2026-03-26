package storage

import (
	"reflect"
	"testing"
)

func TestGetChangedSCIDsSinceHelper(t *testing.T) {
	j := NewSCIDChangeJournal()
	_ = j.StoreSCIDChange("scid-a", 5)
	_ = j.StoreSCIDChange("scid-b", 10)

	got := GetChangedSCIDsSince(j, 5)
	want := []string{"scid-b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected helper result: got %v want %v", got, want)
	}
}

func TestGetChangedSCIDsAtTopoheightHelper(t *testing.T) {
	j := NewSCIDChangeJournal()
	_ = j.StoreSCIDChange("scid-b", 10)
	_ = j.StoreSCIDChange("scid-a", 10)

	got := GetChangedSCIDsAtTopoheight(j, 10)
	want := []string{"scid-a", "scid-b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected helper result: got %v want %v", got, want)
	}
}

func TestGetChangedSCIDsHelpersHandleNilStore(t *testing.T) {
	if got := GetChangedSCIDsSince(nil, 5); got != nil {
		t.Fatalf("expected nil result for nil store, got %v", got)
	}
	if got := GetChangedSCIDsAtTopoheight(nil, 5); got != nil {
		t.Fatalf("expected nil result for nil store, got %v", got)
	}
}
