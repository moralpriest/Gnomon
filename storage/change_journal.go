package storage

import "sort"

// SCIDChangeJournal is an additive, in-memory helper for modeling changed-SCID
// semantics before backend-backed persistence is introduced.
//
// It is used first as a tested behavioral contract for future persisted change
// indexes. Current consumers are not switched to it automatically.
type SCIDChangeJournal struct {
	byHeight map[int64]map[string]struct{}
}

func NewSCIDChangeJournal() *SCIDChangeJournal {
	return &SCIDChangeJournal{byHeight: make(map[int64]map[string]struct{})}
}

func (j *SCIDChangeJournal) StoreSCIDChange(scid string, topoheight int64) error {
	if topoheight < 0 || scid == "" {
		return nil
	}
	if j.byHeight[topoheight] == nil {
		j.byHeight[topoheight] = make(map[string]struct{})
	}
	j.byHeight[topoheight][scid] = struct{}{}
	return nil
}

func (j *SCIDChangeJournal) GetSCIDChangesAtTopoheight(topoheight int64) []string {
	return sortedSCIDSet(j.byHeight[topoheight])
}

func (j *SCIDChangeJournal) GetSCIDChangesSince(topoheight int64) []string {
	all := make(map[string]struct{})
	for height, scids := range j.byHeight {
		if height > topoheight {
			for scid := range scids {
				all[scid] = struct{}{}
			}
		}
	}
	return sortedSCIDSet(all)
}

func (j *SCIDChangeJournal) DeleteSCIDChange(scid string, topoheight int64) error {
	if set := j.byHeight[topoheight]; set != nil {
		delete(set, scid)
		if len(set) == 0 {
			delete(j.byHeight, topoheight)
		}
	}
	return nil
}

func (j *SCIDChangeJournal) DeleteSCIDChangesAbove(topoheight int64) error {
	for height := range j.byHeight {
		if height > topoheight {
			delete(j.byHeight, height)
		}
	}
	return nil
}

func sortedSCIDSet(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	results := make([]string, 0, len(set))
	for scid := range set {
		results = append(results, scid)
	}
	sort.Strings(results)
	return results
}
