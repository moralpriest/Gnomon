package indexer

import "github.com/civilware/Gnomon/storage"

func (indexer *Indexer) PruneDerivedDataAbove(topoheight int64) error {
	switch indexer.DBType {
	case "gravdb":
		if err := indexer.GravDBBackend.DeleteSCIDChangesAbove(topoheight); err != nil {
			return err
		}
		for _, meta := range indexer.GravDBBackend.GetAllTelaMetadata() {
			if meta != nil && meta.Topoheight > topoheight {
				if err := indexer.GravDBBackend.DeleteTelaMetadata(meta.SCID); err != nil {
					return err
				}
				if err := storage.RebuildTelaMetadataAtOrBelow(indexer.GravDBBackend, meta.SCID, topoheight); err != nil {
					return err
				}
			}
		}
	case "boltdb":
		if err := indexer.BBSBackend.DeleteSCIDChangesAbove(topoheight); err != nil {
			return err
		}
		for _, meta := range indexer.BBSBackend.GetAllTelaMetadata() {
			if meta != nil && meta.Topoheight > topoheight {
				if err := indexer.BBSBackend.DeleteTelaMetadata(meta.SCID); err != nil {
					return err
				}
				if err := storage.RebuildTelaMetadataAtOrBelow(indexer.BBSBackend, meta.SCID, topoheight); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
