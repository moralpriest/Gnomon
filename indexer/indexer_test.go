package indexer

import (
	"testing"

	"github.com/creachadair/jrpc2"
	"github.com/civilware/Gnomon/storage"
)

func TestIsFullyQueryable_NilIndexer(t *testing.T) {
	var idx *Indexer
	if idx.IsFullyQueryable() {
		t.Fatal("expected false for nil indexer")
	}
}

func TestIsFullyQueryable_NilRPC(t *testing.T) {
	bbs, _ := storage.NewBBoltDB(t.TempDir(), "indexer-queryable-nilrpc.db")
	defer bbs.DB.Close()

	idx := NewIndexer(nil, bbs, "boltdb", nil, 1, "", "", false, false, nil, nil, false)
	if idx.IsFullyQueryable() {
		t.Fatal("expected false when RPC is nil")
	}
}

func TestIsFullyQueryable_ZeroHeight(t *testing.T) {
	bbs, _ := storage.NewBBoltDB(t.TempDir(), "indexer-queryable-zero.db")
	defer bbs.DB.Close()

	idx := NewIndexer(nil, bbs, "boltdb", nil, 0, "", "", false, false, nil, nil, false)
	idx.RPC = &Client{RPC: &jrpc2.Client{}}
	idx.InteractionIndexReady.Store(true)
	SetConnected(true)
	defer SetConnected(false)

	if idx.IsFullyQueryable() {
		t.Fatal("expected false when LastIndexedHeight is 0")
	}
}

func TestIsFullyQueryable_NotReady(t *testing.T) {
	bbs, _ := storage.NewBBoltDB(t.TempDir(), "indexer-queryable-notready.db")
	defer bbs.DB.Close()

	idx := NewIndexer(nil, bbs, "boltdb", nil, 5, "", "", false, false, nil, nil, false)
	idx.RPC = &Client{RPC: &jrpc2.Client{}}
	idx.InteractionIndexReady.Store(false)
	SetConnected(true)
	defer SetConnected(false)

	if idx.IsFullyQueryable() {
		t.Fatal("expected false when InteractionIndexReady is false")
	}
}

func TestIsFullyQueryable_NotConnected(t *testing.T) {
	bbs, _ := storage.NewBBoltDB(t.TempDir(), "indexer-queryable-noconn.db")
	defer bbs.DB.Close()

	idx := NewIndexer(nil, bbs, "boltdb", nil, 5, "", "", false, false, nil, nil, false)
	idx.RPC = &Client{RPC: &jrpc2.Client{}}
	idx.InteractionIndexReady.Store(true)
	SetConnected(false)

	if idx.IsFullyQueryable() {
		t.Fatal("expected false when not connected")
	}
}

func TestIsFullyQueryable_AllConditionsMet(t *testing.T) {
	bbs, _ := storage.NewBBoltDB(t.TempDir(), "indexer-queryable-all.db")
	defer bbs.DB.Close()

	idx := NewIndexer(nil, bbs, "boltdb", nil, 5, "", "", false, false, nil, nil, false)
	idx.RPC = &Client{RPC: &jrpc2.Client{}}
	idx.InteractionIndexReady.Store(true)
	SetConnected(true)
	defer SetConnected(false)

	if !idx.IsFullyQueryable() {
		t.Fatal("expected true when all conditions are met")
	}
}
