package wsserver

import (
	"context"

	"github.com/civilware/Gnomon/indexer"
	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
	"github.com/sirupsen/logrus"
)

func ChangedSCIDs(ctx context.Context, p structures.WS_ChangedSCIDs_Params, idx *indexer.Indexer) (result structures.WS_ChangedSCIDs_Result, err error) {
	_ = ctx
	logger = structures.Logger.WithFields(logrus.Fields{})

	if idx == nil {
		return result, nil
	}

	filter := p.Filter
	if filter == "" {
		result.SCIDs = idx.GetChangedSCIDsSince(p.Height)
	} else {
		result.SCIDs = idx.GetFilteredChangedSCIDsSince(p.Height, filter)
		result.Filter = filter
	}
	result.Topoheight = p.Height
	result.Count = len(result.SCIDs)
	return result, nil
}

func ChangedTela(ctx context.Context, p structures.WS_ChangedSCIDs_Params, idx *indexer.Indexer) (result structures.WS_ChangedSCIDs_Result, err error) {
	_ = ctx
	logger = structures.Logger.WithFields(logrus.Fields{})

	if idx == nil {
		return result, nil
	}

	result.SCIDs = idx.GetTelaChangedSCIDsSince(p.Height)
	result.Topoheight = p.Height
	result.Count = len(result.SCIDs)
	result.Filter = storage.ClassifierTelaIndex
	return result, nil
}

func TelaMetadata(ctx context.Context, p structures.WS_TelaMetadata_Params, idx *indexer.Indexer) (result structures.WS_TelaMetadata_Result, err error) {
	_ = ctx
	logger = structures.Logger.WithFields(logrus.Fields{})

	if idx == nil {
		return result, nil
	}

	if p.Height > 0 {
		summary := idx.GetTelaMetadataSummarySince(p.Height)
		if summary != nil {
			result.Topoheight = summary.Topoheight
			result.Results = summary.Results
			if p.Offset > 0 && p.Offset < len(result.Results) {
				result.Results = result.Results[p.Offset:]
			} else if p.Offset >= len(result.Results) {
				result.Results = []structures.TelaMetadata{}
			}
			if p.Limit > 0 && len(result.Results) > p.Limit {
				result.Results = result.Results[:p.Limit]
			}
			result.Count = len(result.Results)
			result.Offset = p.Offset
			result.Limit = p.Limit
		}
		return result, nil
	}

	summary := idx.GetAllTelaMetadata()
	if summary != nil {
		result.Topoheight = summary.Topoheight
		result.Results = summary.Results
		if p.Offset > 0 && p.Offset < len(result.Results) {
			result.Results = result.Results[p.Offset:]
		} else if p.Offset >= len(result.Results) {
			result.Results = []structures.TelaMetadata{}
		}
		if p.Limit > 0 && len(result.Results) > p.Limit {
			result.Results = result.Results[:p.Limit]
		}
		result.Count = len(result.Results)
		result.Offset = p.Offset
		result.Limit = p.Limit
	}
	return result, nil
}
