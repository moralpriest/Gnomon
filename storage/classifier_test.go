package storage

import (
	"reflect"
	"testing"

	"github.com/civilware/Gnomon/structures"
)

type classifierStore struct {
	vars    map[string][]*structures.SCIDVariable
	heights map[string][]int64
}

func (s classifierStore) GetOwner(string) string                         { return "" }
func (s classifierStore) GetAllOwnersAndSCIDs() map[string]string        { return nil }
func (s classifierStore) GetInstallHeight(string) int64                  { return 0 }
func (s classifierStore) GetAllSCIDsAndInstallHeights() map[string]int64 { return nil }
func (s classifierStore) GetSCIDInteractionHeight(scid string) []int64   { return s.heights[scid] }
func (s classifierStore) GetInteractionIndex(_ int64, heights []int64, rmax bool) int64 {
	if len(heights) == 0 {
		return 0
	}
	if rmax {
		max := heights[0]
		for _, h := range heights[1:] {
			if h > max {
				max = h
			}
		}
		return max
	}
	return 0
}
func (s classifierStore) GetSCIDVariableDetailsAtTopoheight(scid string, _ int64) []*structures.SCIDVariable {
	return s.vars[scid]
}

func TestTelaIndexClassifierMatchesCodeMarkers(t *testing.T) {
	store := classifierStore{
		heights: map[string][]int64{"scid-a": {5}, "scid-b": {7}},
		vars: map[string][]*structures.SCIDVariable{
			"scid-a": {{Key: "C", Value: "Function TELA_INDEX() Uint64"}},
			"scid-b": {{Key: "C", Value: "Function SOMETHING() Uint64"}},
		},
	}

	classifier := NewClassifier(ClassifierTelaIndex, store)
	if !classifier.Matches("scid-a") {
		t.Fatalf("expected tela classifier to match scid-a")
	}
	if classifier.Matches("scid-b") {
		t.Fatalf("expected tela classifier not to match scid-b")
	}
}

func TestFilterChangedSCIDs(t *testing.T) {
	store := classifierStore{
		heights: map[string][]int64{"scid-a": {5}, "scid-b": {7}},
		vars: map[string][]*structures.SCIDVariable{
			"scid-a": {{Key: "C", Value: "TELA INDEX"}},
			"scid-b": {{Key: "C", Value: "other"}},
		},
	}

	got := FilterChangedSCIDs([]string{"scid-a", "scid-b"}, NewClassifier(ClassifierTelaIndex, store))
	want := []string{"scid-a"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected filtered scids: got %v want %v", got, want)
	}
}

func TestTelaIndexClassifierMatchesAppStyleFields(t *testing.T) {
	store := classifierStore{
		heights: map[string][]int64{"scid-a": {5}},
		vars: map[string][]*structures.SCIDVariable{
			"scid-a": {
				{Key: "var_header_name", Value: "Example App"},
				{Key: "var_header_description", Value: "A decentralized application."},
				{Key: "var_header_icon", Value: "https://example/icon.png"},
				{Key: "dURL", Value: "example-app.tela"},
				{Key: "telaVersion", Value: "1.1.0"},
				{Key: "DOC1", Value: "doc-hash"},
			},
		},
	}

	classifier := NewClassifier(ClassifierTelaIndex, store)
	if !classifier.Matches("scid-a") {
		t.Fatalf("expected app-style scid to match tela classifier")
	}
}
