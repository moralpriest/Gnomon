package indexer

import (
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
)

var versionPrefixPattern = regexp.MustCompile(`^v?\d+(?:\.\d+)+\.`)

func (indexer *Indexer) GetTelaMetadata(scid string) *structures.TelaMetadata {
	switch indexer.DBType {
	case "gravdb":
		meta, err := storage.RefreshTelaMetadataIfIncomplete(indexer.GravDBBackend, scid)
		if err == nil {
			return meta
		}
		return indexer.GravDBBackend.GetTelaMetadata(scid)
	case "boltdb":
		meta, err := storage.RefreshTelaMetadataIfIncomplete(indexer.BBSBackend, scid)
		if err == nil {
			return meta
		}
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
			if meta != nil && storage.NeedsTelaMetadataRefresh(meta) {
				meta, _ = storage.RefreshTelaMetadataIfIncomplete(indexer.GravDBBackend, meta.SCID)
			}
			if meta != nil && meta.IsTelaIndex {
				results = append(results, *meta)
			}
		}
	case "boltdb":
		for _, meta := range indexer.BBSBackend.GetAllTelaMetadata() {
			if meta != nil && storage.NeedsTelaMetadataRefresh(meta) {
				meta, _ = storage.RefreshTelaMetadataIfIncomplete(indexer.BBSBackend, meta.SCID)
			}
			if meta != nil && meta.IsTelaIndex {
				results = append(results, *meta)
			}
		}
	}
	results = indexer.preferTopLevelTelaMetadata(results)
	return &structures.TelaMetadata_Result{Topoheight: indexer.LastIndexedHeight, Results: results, Count: len(results)}
}

func (indexer *Indexer) preferTopLevelTelaMetadata(results []structures.TelaMetadata) []structures.TelaMetadata {
	if len(results) == 0 {
		return results
	}
	bestByGroup := make(map[string]structures.TelaMetadata)
	grouped := make(map[string][]structures.TelaMetadata)
	for _, meta := range results {
		key := normalizeTelaGroupingKey(&meta)
		if key == "" {
			key = meta.SCID
		}
		grouped[key] = append(grouped[key], meta)
		curr, ok := bestByGroup[key]
		if !ok || scoreTelaMetadata(&meta) > scoreTelaMetadata(&curr) {
			bestByGroup[key] = meta
		}
	}
	filtered := make([]structures.TelaMetadata, 0, len(bestByGroup))
	for key, meta := range bestByGroup {
		promoteTelaSiblingFields(&meta, grouped[key])
		meta = *indexer.promoteTelaLinkedDOCMetadata(&meta)
		filtered = append(filtered, meta)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(filtered[i].DisplayName))
		right := strings.ToLower(strings.TrimSpace(filtered[j].DisplayName))
		if left == right {
			return filtered[i].SCID < filtered[j].SCID
		}
		return left < right
	})
	return filtered
}

func (indexer *Indexer) promoteTelaLinkedDOCMetadata(meta *structures.TelaMetadata) *structures.TelaMetadata {
	if meta == nil || strings.TrimSpace(meta.DURL) == "" {
		return meta
	}
	if !isTechnicalTelaName(meta.DisplayName) && strings.TrimSpace(meta.NameHdr) != "" && strings.TrimSpace(meta.DescrHdr) != "" {
		return meta
	}

	var vars []*structures.SCIDVariable
	switch indexer.DBType {
	case "gravdb":
		vars = indexer.GravDBBackend.GetAllSCIDVariableDetails(meta.SCID)
	case "boltdb":
		vars = indexer.BBSBackend.GetAllSCIDVariableDetails(meta.SCID)
	default:
		return meta
	}

	best := *meta
	for _, v := range vars {
		key, ok := v.Key.(string)
		if !ok || !strings.HasPrefix(key, "DOC") {
			continue
		}
		docSCID, ok := v.Value.(string)
		if !ok || docSCID == "" {
			continue
		}
		candidate := indexer.GetTelaMetadata(docSCID)
		if candidate == nil {
			continue
		}
		if strongerTelaPresentation(candidate, &best) {
			promoteTelaFields(&best, candidate)
		}
	}
	return &best
}

func strongerTelaPresentation(candidate, current *structures.TelaMetadata) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	if strings.TrimSpace(candidate.NameHdr) != "" && !isTechnicalTelaName(candidate.NameHdr) && (strings.TrimSpace(current.NameHdr) == "" || isTechnicalTelaName(current.NameHdr)) {
		return true
	}
	if strings.TrimSpace(candidate.DisplayName) != "" && !isTechnicalTelaName(candidate.DisplayName) && (strings.TrimSpace(current.DisplayName) == "" || isTechnicalTelaName(current.DisplayName)) {
		return true
	}
	if strings.TrimSpace(candidate.DescrHdr) != "" && strings.TrimSpace(current.DescrHdr) == "" {
		return true
	}
	if strings.TrimSpace(candidate.IconHdr) != "" && strings.TrimSpace(current.IconHdr) == "" {
		return true
	}
	return false
}

func promoteTelaFields(target *structures.TelaMetadata, source *structures.TelaMetadata) {
	if target == nil || source == nil {
		return
	}
	if strings.TrimSpace(target.NameHdr) == "" || isTechnicalTelaName(target.NameHdr) {
		if strings.TrimSpace(source.NameHdr) != "" && !isTechnicalTelaName(source.NameHdr) {
			target.NameHdr = source.NameHdr
		}
	}
	if strings.TrimSpace(target.DisplayName) == "" || isTechnicalTelaName(target.DisplayName) {
		if strings.TrimSpace(source.DisplayName) != "" && !isTechnicalTelaName(source.DisplayName) {
			target.DisplayName = source.DisplayName
		}
	}
	if strings.TrimSpace(target.DescrHdr) == "" && strings.TrimSpace(source.DescrHdr) != "" {
		target.DescrHdr = source.DescrHdr
	}
	if strings.TrimSpace(target.IconHdr) == "" && strings.TrimSpace(source.IconHdr) != "" {
		target.IconHdr = source.IconHdr
	}
}

func promoteTelaSiblingFields(target *structures.TelaMetadata, siblings []structures.TelaMetadata) {
	if target == nil || len(siblings) == 0 {
		return
	}
	for _, candidate := range siblings {
		if strings.TrimSpace(target.NameHdr) == "" && strings.TrimSpace(candidate.NameHdr) != "" && !isTechnicalTelaName(candidate.NameHdr) {
			target.NameHdr = candidate.NameHdr
		}
		if strings.TrimSpace(target.DescrHdr) == "" && strings.TrimSpace(candidate.DescrHdr) != "" {
			target.DescrHdr = candidate.DescrHdr
		}
		if strings.TrimSpace(target.IconHdr) == "" && strings.TrimSpace(candidate.IconHdr) != "" {
			target.IconHdr = candidate.IconHdr
		}
		if strings.TrimSpace(target.DisplayName) == "" || isTechnicalTelaName(target.DisplayName) {
			if strings.TrimSpace(candidate.DisplayName) != "" && !isTechnicalTelaName(candidate.DisplayName) {
				target.DisplayName = candidate.DisplayName
			}
		}
	}
	if strings.TrimSpace(target.DisplayName) == "" || isTechnicalTelaName(target.DisplayName) {
		if strings.TrimSpace(target.NameHdr) != "" && !isTechnicalTelaName(target.NameHdr) {
			target.DisplayName = target.NameHdr
		}
	}
}

func normalizeTelaGroupingKey(meta *structures.TelaMetadata) string {
	if meta == nil {
		return ""
	}
	durl := strings.ToLower(strings.TrimSpace(meta.DURL))
	if durl != "" {
		durl = strings.TrimSuffix(durl, ".tela")
		durl = strings.TrimSuffix(durl, ".self")
		durl = versionPrefixPattern.ReplaceAllString(durl, "")
		for _, prefix := range []string{"index.", "main.", "app."} {
			durl = strings.TrimPrefix(durl, prefix)
		}
		ext := strings.ToLower(filepath.Ext(durl))
		switch ext {
		case ".js", ".css", ".json", ".wasm", ".gz", ".png", ".jpg", ".jpeg", ".svg", ".webp", ".ico", ".html":
			return strings.TrimSuffix(durl, ext)
		default:
			return durl
		}
	}
	if meta.DisplayName != "" {
		return strings.ToLower(strings.TrimSpace(meta.DisplayName))
	}
	if meta.NameHdr != "" {
		return strings.ToLower(strings.TrimSpace(meta.NameHdr))
	}
	return meta.SCID
}

func scoreTelaMetadata(meta *structures.TelaMetadata) int {
	if meta == nil {
		return -1000
	}
	score := 0
	switch meta.ArtifactKind {
	case "index":
		score += 50
	case "doc":
		score += 20
	case "library":
		score -= 20
	case "docshards":
		score -= 30
	case "bootstrap":
		score -= 25
	}
	if name := strings.TrimSpace(meta.NameHdr); name != "" && !isTechnicalTelaName(name) {
		score += 40
	}
	if strings.TrimSpace(meta.DescrHdr) != "" {
		score += 20
	}
	if strings.TrimSpace(meta.IconHdr) != "" {
		score += 10
	}
	if strings.TrimSpace(meta.NameHdr) == "index.html" || strings.TrimSpace(meta.NameHdr) == "index.html.gz" {
		score -= 15
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(meta.DURL)), "index.") {
		score -= 15
	}
	if isTechnicalTelaName(meta.DisplayName) {
		score -= 20
	}
	return score
}

func isTechnicalTelaName(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return true
	}
	if ext := strings.ToLower(filepath.Ext(name)); ext != "" {
		switch ext {
		case ".js", ".css", ".json", ".wasm", ".gz", ".png", ".jpg", ".jpeg", ".svg", ".webp", ".ico", ".html":
			return true
		}
	}
	return strings.Contains(name, ".tela") || strings.Contains(name, "script") || strings.Contains(name, "bundle") || strings.Contains(name, "runtime")
}
