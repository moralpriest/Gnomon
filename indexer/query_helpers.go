package indexer

import "github.com/civilware/Gnomon/storage"

// GetChangedSCIDsSince provides an additive internal seam for delta-oriented
// consumers. It does not change any public API or websocket surface yet.
func (indexer *Indexer) GetChangedSCIDsSince(topoheight int64) []string {
	switch indexer.DBType {
	case "gravdb":
		return storage.GetChangedSCIDsSince(indexer.GravDBBackend, topoheight)
	case "boltdb":
		return storage.GetChangedSCIDsSince(indexer.BBSBackend, topoheight)
	default:
		return nil
	}
}

// GetChangedSCIDsAtTopoheight exposes a single-height delta seam for internal
// consumers and future TELA-specific query work.
func (indexer *Indexer) GetChangedSCIDsAtTopoheight(topoheight int64) []string {
	switch indexer.DBType {
	case "gravdb":
		return storage.GetChangedSCIDsAtTopoheight(indexer.GravDBBackend, topoheight)
	case "boltdb":
		return storage.GetChangedSCIDsAtTopoheight(indexer.BBSBackend, topoheight)
	default:
		return nil
	}
}

// GetChangedSCIDsSummarySince returns a lightweight delta summary that is safe
// for future API exposure and TELA-specific filtering layers.
func (indexer *Indexer) GetChangedSCIDsSummarySince(topoheight int64) map[string]interface{} {
	scids := indexer.GetChangedSCIDsSince(topoheight)
	return map[string]interface{}{
		"topoheight": topoheight,
		"scids":      scids,
		"count":      len(scids),
	}
}

// GetFilteredChangedSCIDsSince provides an additive internal seam for future
// TELA-oriented delta consumers.
func (indexer *Indexer) GetFilteredChangedSCIDsSince(topoheight int64, classifierName string) []string {
	switch indexer.DBType {
	case "gravdb":
		classifier := storage.NewClassifier(classifierName, indexer.GravDBBackend)
		return storage.GetFilteredChangedSCIDsSince(indexer.GravDBBackend, classifier, topoheight)
	case "boltdb":
		classifier := storage.NewClassifier(classifierName, indexer.BBSBackend)
		return storage.GetFilteredChangedSCIDsSince(indexer.BBSBackend, classifier, topoheight)
	default:
		return nil
	}
}

// GetTelaChangedSCIDsSince is a dedicated internal TELA delta seam with a more
// explicit contract than generic classifier-based filtering.
func (indexer *Indexer) GetTelaChangedSCIDsSince(topoheight int64) []string {
	return indexer.GetFilteredChangedSCIDsSince(topoheight, storage.ClassifierTelaIndex)
}
