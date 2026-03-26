package indexer

import "github.com/civilware/Gnomon/structures"

func (indexer *Indexer) GetTelaMetadata(scid string) *structures.TelaMetadata {
	switch indexer.DBType {
	case "gravdb":
		return indexer.GravDBBackend.GetTelaMetadata(scid)
	case "boltdb":
		return indexer.BBSBackend.GetTelaMetadata(scid)
	default:
		return nil
	}
}

func (indexer *Indexer) GetTelaMetadataSummarySince(topoheight int64) *structures.TelaMetadata_Result {
	changed := indexer.GetTelaChangedSCIDsSince(topoheight)
	results := make([]structures.TelaMetadata, 0, len(changed))
	for _, scid := range changed {
		meta := indexer.GetTelaMetadata(scid)
		if meta != nil {
			results = append(results, *meta)
		}
	}
	return &structures.TelaMetadata_Result{Topoheight: topoheight, Results: results, Count: len(results)}
}

func (indexer *Indexer) GetAllTelaMetadata() *structures.TelaMetadata_Result {
	var results []structures.TelaMetadata
	switch indexer.DBType {
	case "gravdb":
		for _, meta := range indexer.GravDBBackend.GetAllTelaMetadata() {
			if meta != nil && meta.IsTelaIndex {
				results = append(results, *meta)
			}
		}
	case "boltdb":
		for _, meta := range indexer.BBSBackend.GetAllTelaMetadata() {
			if meta != nil && meta.IsTelaIndex {
				results = append(results, *meta)
			}
		}
	}
	return &structures.TelaMetadata_Result{Topoheight: indexer.LastIndexedHeight, Results: results, Count: len(results)}
}
