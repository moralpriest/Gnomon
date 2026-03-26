package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/civilware/Gnomon/structures"
	"github.com/deroproject/graviton"
	bolt "go.etcd.io/bbolt"
)

const telaMetadataBucket = "telametadata"

type TelaMetadataStore interface {
	StoreTelaMetadata(scid string, metadata *structures.TelaMetadata) error
	GetTelaMetadata(scid string) *structures.TelaMetadata
	GetAllTelaMetadata() []*structures.TelaMetadata
	DeleteTelaMetadata(scid string) error
}

type TelaMetadataVariableStore interface {
	TelaMetadataStore
	GetAllOwnersAndSCIDs() map[string]string
	GetSCIDVariableDetailsAtTopoheight(scid string, topoheight int64) []*structures.SCIDVariable
	GetSCIDInteractionHeight(scid string) []int64
	GetInteractionIndex(topoheight int64, heights []int64, rmax bool) int64
}

func DeriveTelaMetadata(scid string, topoheight int64, variables []*structures.SCIDVariable) *structures.TelaMetadata {
	if scid == "" || len(variables) == 0 {
		return nil
	}

	meta := &structures.TelaMetadata{SCID: scid, Topoheight: topoheight}
	for _, variable := range variables {
		key, ok := variable.Key.(string)
		if !ok {
			continue
		}

		switch key {
		case "C":
			if code, ok := variable.Value.(string); ok {
				meta.Code = code
				upper := strings.ToUpper(code)
				meta.IsTelaIndex = strings.Contains(upper, "TELA") && strings.Contains(upper, "INDEX")
			}
		case "dURL":
			if v, ok := variable.Value.(string); ok {
				meta.DURL = v
			}
		case "NameHdr":
			if v, ok := variable.Value.(string); ok {
				meta.NameHdr = v
			}
		case "var_header_name":
			if v, ok := variable.Value.(string); ok {
				meta.NameHdr = v
			}
		case "DescrHdr":
			if v, ok := variable.Value.(string); ok {
				meta.DescrHdr = v
			}
		case "var_header_description":
			if v, ok := variable.Value.(string); ok {
				meta.DescrHdr = v
			}
		case "IconHdr":
			if v, ok := variable.Value.(string); ok {
				meta.IconHdr = v
			}
		case "var_header_icon":
			if v, ok := variable.Value.(string); ok {
				meta.IconHdr = v
			}
		case "telaVersion":
			meta.IsTelaIndex = true
		case "DOC", "DOC1":
			meta.DocCount++
			if meta.DocType == "" {
				meta.DocType = key
			}
		default:
			if strings.HasPrefix(key, "DOC") {
				meta.DocCount++
				if meta.DocType == "" {
					meta.DocType = key
				}
			}
			if strings.EqualFold(key, "name") && meta.NameHdr == "" {
				if v, ok := variable.Value.(string); ok {
					meta.NameHdr = v
				}
			}
			if (strings.EqualFold(key, "description") || strings.EqualFold(key, "descr")) && meta.DescrHdr == "" {
				if v, ok := variable.Value.(string); ok {
					meta.DescrHdr = v
				}
			}
			if strings.EqualFold(key, "icon") && meta.IconHdr == "" {
				if v, ok := variable.Value.(string); ok {
					meta.IconHdr = v
				}
			}
			if strings.EqualFold(key, "docCount") && meta.DocCount == 0 {
				switch v := variable.Value.(type) {
				case uint64:
					meta.DocCount = int(v)
				case float64:
					meta.DocCount = int(v)
				case string:
					if parsed, err := strconv.Atoi(v); err == nil {
						meta.DocCount = parsed
					}
				}
			}
		}
	}

	if meta.DURL != "" && (meta.DocCount > 0 || meta.NameHdr != "" || meta.DescrHdr != "" || meta.IconHdr != "") {
		meta.IsTelaIndex = true
	}

	if !meta.IsTelaIndex && meta.DURL == "" && meta.NameHdr == "" && meta.DescrHdr == "" && meta.IconHdr == "" && meta.DocCount == 0 {
		return nil
	}

	return meta
}

func UpsertTelaMetadataFromVariables(store TelaMetadataStore, scid string, topoheight int64, variables []*structures.SCIDVariable) error {
	if store == nil {
		return nil
	}
	meta := DeriveTelaMetadata(scid, topoheight, variables)
	if meta == nil {
		return nil
	}
	return store.StoreTelaMetadata(scid, meta)
}

func RebuildTelaMetadataAtOrBelow(store TelaMetadataVariableStore, scid string, topoheight int64) error {
	if store == nil || scid == "" {
		return nil
	}
	heights := store.GetSCIDInteractionHeight(scid)
	if len(heights) == 0 {
		return store.DeleteTelaMetadata(scid)
	}

	rebuildHeight := store.GetInteractionIndex(topoheight+1, heights, false)
	if rebuildHeight == 0 {
		for _, height := range heights {
			if height <= topoheight && height > rebuildHeight {
				rebuildHeight = height
			}
		}
	}
	if rebuildHeight == 0 {
		if topoheight == int64(^uint64(0)>>1) && len(heights) > 0 {
			rebuildHeight = store.GetInteractionIndex(0, heights, true)
		}
	}
	if rebuildHeight == 0 {
		return store.DeleteTelaMetadata(scid)
	}

	variables := store.GetSCIDVariableDetailsAtTopoheight(scid, rebuildHeight)
	meta := DeriveTelaMetadata(scid, rebuildHeight, variables)
	if meta == nil {
		return store.DeleteTelaMetadata(scid)
	}
	return store.StoreTelaMetadata(scid, meta)
}

func BackfillTelaMetadata(store TelaMetadataVariableStore) error {
	if store == nil {
		return nil
	}
	for scid := range store.GetAllOwnersAndSCIDs() {
		if err := RebuildTelaMetadataAtOrBelow(store, scid, int64(^uint64(0)>>1)); err != nil {
			return err
		}
	}
	return nil
}

func (bbs *BboltStore) StoreTelaMetadata(scid string, metadata *structures.TelaMetadata) error {
	if scid == "" || metadata == nil {
		return nil
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("[StoreTelaMetadata] could not marshal tela metadata: %v", err)
	}
	return bbs.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(telaMetadataBucket))
		if err != nil {
			return fmt.Errorf("bucket: %s", err)
		}
		return b.Put([]byte(scid), encoded)
	})
}

func (bbs *BboltStore) GetTelaMetadata(scid string) (metadata *structures.TelaMetadata) {
	bbs.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(telaMetadataBucket))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(scid))
		if v != nil {
			_ = json.Unmarshal(v, &metadata)
		}
		return nil
	})
	return metadata
}

func (bbs *BboltStore) GetAllTelaMetadata() (results []*structures.TelaMetadata) {
	bbs.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(telaMetadataBucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for _, v := c.First(); v != nil; _, v = c.Next() {
			var curr *structures.TelaMetadata
			_ = json.Unmarshal(v, &curr)
			if curr != nil {
				results = append(results, curr)
			}
		}
		return nil
	})
	sort.SliceStable(results, func(i, j int) bool { return results[i].SCID < results[j].SCID })
	return results
}

func (bbs *BboltStore) DeleteTelaMetadata(scid string) error {
	if scid == "" {
		return nil
	}
	return bbs.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(telaMetadataBucket))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(scid))
	})
}

func (g *GravitonStore) StoreTelaMetadata(scid string, metadata *structures.TelaMetadata) error {
	if scid == "" || metadata == nil {
		return nil
	}
	g.waitForMigration()
	store := g.DB
	ss, err := store.LoadSnapshot(0)
	if err != nil {
		return err
	}
	tree, _ := ss.GetTree(telaMetadataBucket)
	if tree == nil {
		var terr error
		prevss, preverr := store.LoadSnapshot(ss.GetVersion() - 1)
		if preverr != nil {
			return preverr
		}
		tree, terr = prevss.GetTree(telaMetadataBucket)
		if tree == nil {
			return terr
		}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("[Graviton] could not marshal tela metadata: %v", err)
	}
	tree.Put([]byte(scid), encoded)
	_, cerr := graviton.Commit(tree)
	return cerr
}

func (g *GravitonStore) GetTelaMetadata(scid string) (metadata *structures.TelaMetadata) {
	g.waitForMigration()
	store := g.DB
	ss, err := store.LoadSnapshot(0)
	if err != nil {
		return nil
	}
	tree, _ := ss.GetTree(telaMetadataBucket)
	if tree == nil {
		prevss, preverr := store.LoadSnapshot(ss.GetVersion() - 1)
		if preverr != nil {
			return nil
		}
		tree, _ = prevss.GetTree(telaMetadataBucket)
		if tree == nil {
			return nil
		}
	}
	v, _ := tree.Get([]byte(scid))
	if v != nil {
		_ = json.Unmarshal(v, &metadata)
	}
	return metadata
}

func (g *GravitonStore) GetAllTelaMetadata() (results []*structures.TelaMetadata) {
	g.waitForMigration()
	store := g.DB
	ss, err := store.LoadSnapshot(0)
	if err != nil {
		return nil
	}
	tree, _ := ss.GetTree(telaMetadataBucket)
	if tree == nil {
		prevss, preverr := store.LoadSnapshot(ss.GetVersion() - 1)
		if preverr != nil {
			return nil
		}
		tree, _ = prevss.GetTree(telaMetadataBucket)
		if tree == nil {
			return nil
		}
	}
	c := tree.Cursor()
	for _, v, err := c.First(); err == nil; _, v, err = c.Next() {
		var curr *structures.TelaMetadata
		_ = json.Unmarshal(v, &curr)
		if curr != nil {
			results = append(results, curr)
		}
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].SCID < results[j].SCID })
	return results
}

func (g *GravitonStore) DeleteTelaMetadata(scid string) error {
	if scid == "" {
		return nil
	}
	g.waitForMigration()
	store := g.DB
	ss, err := store.LoadSnapshot(0)
	if err != nil {
		return err
	}
	tree, _ := ss.GetTree(telaMetadataBucket)
	if tree == nil {
		prevss, preverr := store.LoadSnapshot(ss.GetVersion() - 1)
		if preverr != nil {
			return preverr
		}
		tree, _ = prevss.GetTree(telaMetadataBucket)
		if tree == nil {
			return nil
		}
	}
	if err := tree.Delete([]byte(scid)); err != nil {
		return err
	}
	_, cerr := graviton.Commit(tree)
	return cerr
}
