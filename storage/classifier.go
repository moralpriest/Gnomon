package storage

import (
	"strings"

	"github.com/civilware/Gnomon/structures"
)

const (
	ClassifierAll       = "all"
	ClassifierTelaIndex = "tela-index"
)

// SCIDClassifier is an additive read-side seam for filtering changed SCIDs.
// The first classifier is intentionally conservative and only uses indexed
// store data already available today.
type SCIDClassifier interface {
	Matches(scid string) bool
}

type noOpClassifier struct{}

func (noOpClassifier) Matches(string) bool { return true }

type telaIndexClassifier struct {
	store VariableStore
}

func (c telaIndexClassifier) Matches(scid string) bool {
	if c.store == nil || scid == "" {
		return false
	}

	vars := getVariablesAtLatest(c.store, scid)
	var hasDURL, hasTelaVersion, hasDoc, hasHeader bool
	for _, v := range vars {
		key, ok := v.Key.(string)
		if !ok {
			continue
		}
		switch key {
		case "C":
			code, ok := v.Value.(string)
			if !ok {
				continue
			}
			upper := strings.ToUpper(code)
			if strings.Contains(upper, "TELA") && strings.Contains(upper, "INDEX") {
				return true
			}
		case "dURL":
			if value, ok := v.Value.(string); ok && value != "" {
				hasDURL = true
			}
		case "telaVersion":
			hasTelaVersion = true
		case "NameHdr", "DescrHdr", "IconHdr", "var_header_name", "var_header_description", "var_header_icon":
			hasHeader = true
		default:
			if strings.HasPrefix(key, "DOC") {
				hasDoc = true
			}
		}
	}

	return hasDURL && (hasTelaVersion || hasDoc || hasHeader)
}

// VariableStore extends QueryStore with the variable access needed for
// conservative classifier logic.
type VariableStore interface {
	QueryStore
	GetSCIDVariableDetailsAtTopoheight(scid string, topoheight int64) []*structures.SCIDVariable
	GetSCIDInteractionHeight(scid string) []int64
	GetInteractionIndex(topoheight int64, heights []int64, rmax bool) int64
}

func NewClassifier(name string, store QueryStore) SCIDClassifier {
	switch strings.ToLower(name) {
	case "", ClassifierAll:
		return noOpClassifier{}
	case ClassifierTelaIndex:
		vs, ok := store.(VariableStore)
		if !ok {
			return noOpClassifier{}
		}
		return telaIndexClassifier{store: vs}
	default:
		return noOpClassifier{}
	}
}

func FilterChangedSCIDs(scids []string, classifier SCIDClassifier) []string {
	if classifier == nil {
		return scids
	}
	filtered := make([]string, 0, len(scids))
	for _, scid := range scids {
		if classifier.Matches(scid) {
			filtered = append(filtered, scid)
		}
	}
	return filtered
}

func getVariablesAtLatest(store VariableStore, scid string) []*structures.SCIDVariable {
	heights := store.GetSCIDInteractionHeight(scid)
	if len(heights) == 0 {
		return nil
	}
	latest := store.GetInteractionIndex(0, heights, true)
	if latest == 0 && len(heights) > 0 {
		latest = heights[0]
	}
	return store.GetSCIDVariableDetailsAtTopoheight(scid, latest)
}
