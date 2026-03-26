package storage

// GetChangedSCIDsSince is a small additive helper that prefers the persisted
// change journal for delta-oriented reads.
func GetChangedSCIDsSince(store ChangeJournalStore, topoheight int64) []string {
	if store == nil {
		return nil
	}
	return store.GetSCIDChangesSince(topoheight)
}

// GetChangedSCIDsAtTopoheight is a small additive helper that reads the
// persisted change journal for a single topoheight.
func GetChangedSCIDsAtTopoheight(store ChangeJournalStore, topoheight int64) []string {
	if store == nil {
		return nil
	}
	return store.GetSCIDChangesAtTopoheight(topoheight)
}

// GetFilteredChangedSCIDsSince combines additive journal-backed change reads
// with a classifier-based filter for future consumer-specific query paths.
func GetFilteredChangedSCIDsSince(store ChangeJournalStore, classifier SCIDClassifier, topoheight int64) []string {
	return FilterChangedSCIDs(GetChangedSCIDsSince(store, topoheight), classifier)
}
