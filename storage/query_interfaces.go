package storage

// QueryStore defines generic additive query capabilities that can be shared by
// both storage backends as new indexed query surfaces are introduced.
//
// This interface is intentionally minimal and only wraps existing stable read
// behavior so it can be adopted without switching current consumers.
type QueryStore interface {
	GetOwner(scid string) string
	GetAllOwnersAndSCIDs() map[string]string
	GetInstallHeight(scid string) int64
	GetAllSCIDsAndInstallHeights() map[string]int64
	GetSCIDInteractionHeight(scid string) []int64
	GetInteractionIndex(topoheight int64, heights []int64, rmax bool) int64
}

// ChangeJournalStore defines additive change-tracking primitives for future
// indexed delta refresh support.
//
// The first implementation can back these methods with dedicated index data
// structures while legacy query paths remain untouched.
type ChangeJournalStore interface {
	StoreSCIDChange(scid string, topoheight int64) error
	GetSCIDChangesSince(topoheight int64) []string
	GetSCIDChangesAtTopoheight(topoheight int64) []string
	DeleteSCIDChange(scid string, topoheight int64) error
	DeleteSCIDChangesAbove(topoheight int64) error
}

const scidChangeJournalBucket = "scidchangejournal"
