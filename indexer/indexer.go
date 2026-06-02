package indexer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/civilware/Gnomon/mbllookup"
	"github.com/civilware/Gnomon/storage"
	"github.com/civilware/Gnomon/structures"
	"github.com/schollz/progressbar/v3"

	"github.com/creachadair/jrpc2"
	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/cryptography/bn256"
	"github.com/deroproject/derohe/cryptography/crypto"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
	"github.com/deroproject/graviton"

	"github.com/sirupsen/logrus"
)

type SCIDToIndexStage struct {
	scid     string
	fsi      *structures.FastSyncImport
	scVars   []*structures.SCIDVariable
	scCode   string
	contains bool
}

type Indexer struct {
	LastIndexedHeight     int64
	ChainHeight           int64
	SearchFilter          []string
	SFSCIDExclusion       []string
	GravDBBackend         *storage.GravitonStore
	BBSBackend            *storage.BboltStore
	DBType                string
	Closing               atomic.Bool
	RPC                   *Client
	Endpoint              string
	RunMode               string
	MBLLookup             bool
	StoreIntegrators      bool
	ValidatedSCs          []string
	CloseOnDisconnect     bool
	FastSyncConfig        *structures.FastSyncConfig
	Status                string
	InteractionIndexReady atomic.Bool
	sync.RWMutex
}

// Defines the number of blocks to jump when testing pruned nodes.
const block_jump = int64(10000)

var connected atomic.Value

func IsConnected() bool {
	v := connected.Load()
	if v == nil {
		return false
	}
	return v.(bool)
}

func SetConnected(b bool) {
	connected.Store(b)
}

// local logger
var logger *logrus.Entry

func NewIndexer(Graviton_backend *storage.GravitonStore, Bbs_backend *storage.BboltStore, dbtype string, search_filter []string, last_indexedheight int64, endpoint string, runmode string, mbllookup bool, closeondisconnect bool, fsc *structures.FastSyncConfig, sfscidexclusion []string, storeintegrators bool) *Indexer {
	logger = structures.Logger.WithFields(logrus.Fields{})

	if fsc == nil {
		fsc = &structures.FastSyncConfig{
			Enabled:           false,
			SkipFSRecheck:     false,
			ForceFastSync:     false,
			ForceFastSyncDiff: structures.FORCE_FASTSYNC_DIFF,
			NoCode:            false,
		}
	} else if fsc.ForceFastSyncDiff < 1 {
		fsc.ForceFastSyncDiff = structures.FORCE_FASTSYNC_DIFF
	}

	return &Indexer{
		LastIndexedHeight: last_indexedheight,
		SearchFilter:      search_filter,
		SFSCIDExclusion:   sfscidexclusion,
		GravDBBackend:     Graviton_backend,
		BBSBackend:        Bbs_backend,
		DBType:            dbtype,
		RPC:               &Client{},
		Endpoint:          endpoint,
		RunMode:           runmode,
		MBLLookup:         mbllookup,
		StoreIntegrators:  storeintegrators,
		CloseOnDisconnect: closeondisconnect,
		FastSyncConfig:    fsc,
	}
}

func (indexer *Indexer) StartDaemonMode(blockParallelNum int) {
	var err error

	// Simple connect loop .. if connection fails initially then keep trying, else break out and continue on. Connect() is handled in getInfo() for retries later on if connection ceases again
	for {
		if indexer.Closing.Load() {
			// Break out on closing call
			break
		}
		indexer.Status = "initializing"
		logger.Printf("[StartDaemonMode] Trying to connect...")
		err = indexer.RPC.Connect(indexer.Endpoint)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	// Continuously getInfo from daemon to update topoheight globally
	go indexer.getInfo()

	logger.Printf("[StartDaemonMode] Waiting on GetInfo...")
	for {
		if indexer.Closing.Load() {
			// Break out on closing call
			break
		}
		indexer.RLock()
		chainHeight := indexer.ChainHeight
		indexer.RUnlock()
		if chainHeight == int64(0) {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		break
	}

	if indexer.FastSyncConfig.Enabled {
		logger.Printf("[StartDaemonMode] Fastsync Configuration: %v", indexer.FastSyncConfig)
	}

	var storedindex int64
	switch indexer.DBType {
	case "gravdb":
		storedindex, err = indexer.GravDBBackend.GetLastIndexHeight()
		if err != nil {
			logger.Fatalf("[gravdb-StartDaemonMode] Could not get last index height - %v", err)
		}
	case "boltdb":
		storedindex, err = indexer.BBSBackend.GetLastIndexHeight()
		if err != nil {
			logger.Fatalf("[bbs-StartDaemonMode] Could not get last index height - %v", err)
		}
	}

	// If storedindex returns 0, first opening, and fastsync is enabled set index to current chain height
	// If forcefastsync is used and lastindexheight is indexer.FastSyncConfig.ForceFastSyncDiff away then re-fastsync and catchup to chain height
	if (storedindex == 0 && indexer.FastSyncConfig.Enabled) || (indexer.FastSyncConfig.ForceFastSync && indexer.FastSyncConfig.Enabled && indexer.ChainHeight-storedindex > indexer.FastSyncConfig.ForceFastSyncDiff) {
		indexer.Status = "fastsyncing"
		logger.Printf("[StartDaemonMode] Fastsync initiated, setting to chainheight (%v)", indexer.ChainHeight)
		storedindex = indexer.ChainHeight
	} else if storedindex == 0 {
		// If no stored index (first run) and not fastsync, set storedIndex to 1. Utilizing a storedindex of 0 when getting sc vars (esp for builtin) will result in the chain height return data
		storedindex = int64(1)
	}

	// We can also assume this check to mean we have stored validated SCs potentially. TODO: Do we just get stored SCs regardless of sync cycle?
	pre_validatedSCIDs := make(map[string]string)
	switch indexer.DBType {
	case "gravdb":
		pre_validatedSCIDs = indexer.GravDBBackend.GetAllOwnersAndSCIDs()
	case "boltdb":
		pre_validatedSCIDs = indexer.BBSBackend.GetAllOwnersAndSCIDs()
	}

	if len(pre_validatedSCIDs) > 0 {
		logger.Printf("[StartDaemonMode] Appending '%d' pre-validated SCIDs from store to memory.", len(pre_validatedSCIDs))

		for k := range pre_validatedSCIDs {
			if scidExist(indexer.SFSCIDExclusion, k) {
				logger.Debugf("[StartDaemonMode] Not appending pre-validated SCID '%s' as it resides within SFSCIDExclusion - '%v'.", k, indexer.SFSCIDExclusion)
				continue
			}
			indexer.Lock()
			indexer.ValidatedSCs = append(indexer.ValidatedSCs, k)
			indexer.Unlock()
		}

	}

	for _, vi := range structures.Hardcoded_SCIDS {
		if scidExist(indexer.ValidatedSCs, vi) {
			// Hardcoded SCID already exists, no need to re-add
			continue
		}

		if scidExist(indexer.SFSCIDExclusion, vi) {
			logger.Debugf("[StartDaemonMode] Not appending hardcoded SCID '%s' as it resides within SFSCIDExclusion - '%v'.", vi, indexer.SFSCIDExclusion)
			continue
		}

		scVars, scCode, _, _ := indexer.RPC.GetSCVariables(vi, storedindex, nil, nil, nil, false)

		var contains bool

		// If we can get the SC and searchfilter is "" (get all), contains is true. Otherwise evaluate code against searchfilter
		if len(indexer.SearchFilter) == 0 {
			contains = true
		} else {
			// Ensure scCode is not blank (e.g. an invalid scid)
			if scCode != "" {
				for _, sfv := range indexer.SearchFilter {
					contains = strings.Contains(scCode, sfv)
					if contains {
						// Break b/c we want to ensure contains remains true. Only care if it matches at least 1 case
						break
					}
				}
			}
		}

		if contains {
			//logger.Debugf("[AddSCIDToIndex] Hardcoded SCID matches search filter. Adding SCID %v", vi)
			indexer.Lock()
			indexer.ValidatedSCs = append(indexer.ValidatedSCs, vi)
			indexer.Unlock()
			writeWait, _ := time.ParseDuration("20ms")
			switch indexer.DBType {
			case "gravdb":
				for indexer.GravDBBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
					time.Sleep(writeWait)
				}
				indexer.GravDBBackend.Writing.Store(true)
				var ctrees []*graviton.Tree
				// Hardcoded SCIDs are stored with the chain, assume no owner since none is returned
				sotree, sochanges, err := indexer.GravDBBackend.StoreOwner(vi, "", true)
				if err != nil {
					logger.Errorf("[StartDaemonMode-hardcodedscids] Error storing owner: %v", err)
				} else {
					if sochanges {
						ctrees = append(ctrees, sotree)
					}
				}
				// Hardcoded SCIDs are stored with the chain, assume install height of 1
				shtree, shchanges, err := indexer.GravDBBackend.StoreInstallHeight(vi, 1, true)
				if err != nil {
					logger.Errorf("[StartDaemonMode-hardcodedscids] Error storing install height: %v", err)
				} else {
					if shchanges {
						ctrees = append(ctrees, shtree)
					}
				}
				// If scVarsStore length is greater than 0, we can assume there were diffs. Otherwise the varstores are equal and move on.
				if len(scVars) > 0 {
					svdtree, svdchanges, err := indexer.GravDBBackend.StoreSCIDVariableDetails(vi, scVars, storedindex, true)
					if err != nil {
						logger.Errorf("[StartDaemonMode-hardcodedscids] ERR - storing scid variable details: %v", err)
					} else {
						if svdchanges {
							ctrees = append(ctrees, svdtree)
						}
					}
					sihtree, sihchanges, err := indexer.GravDBBackend.StoreSCIDInteractionHeight(vi, storedindex, true)
					if err != nil {
						logger.Errorf("[StartDaemonMode-hardcodedscids] ERR - storing scid interaction height: %v", err)
					} else {
						if sihchanges {
							ctrees = append(ctrees, sihtree)
						}
					}
				}
				if len(ctrees) > 0 {
					_, err := indexer.GravDBBackend.CommitTrees(ctrees)
					if err != nil {
						logger.Errorf("[StartDaemonMode-hardcodedscids] ERR - committing trees: %v", err)
					} else {
						//logger.Debugf("[StartDaemonMode-hardcodedscids] DEBUG - cv [%v]", cv)
					}
				}
				indexer.GravDBBackend.Writing.Store(false)
			case "boltdb":
				for indexer.BBSBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-StartDaemonMode-hardcodedscids] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
					time.Sleep(writeWait)
				}
				indexer.BBSBackend.Writing.Store(true)
				//indexer.BBSBackend.Writer = "StartDaemonMode"
				// Hardcoded SCIDs are stored with the chain, assume no owner since none is returned
				_, err := indexer.BBSBackend.StoreOwner(vi, "")
				if err != nil {
					logger.Errorf("[StartDaemonMode-hardcodedscids] Error storing owner: %v", err)
				}
				// Hardcoded SCIDs are stored with the chain, assume install height of 1
				_, err = indexer.BBSBackend.StoreInstallHeight(vi, 1)
				if err != nil {
					logger.Errorf("[StartDaemonMode-hardcodedscids] Error storing owner: %v", err)
				}
				// If scVarsStore length is greater than 0, we can assume there were diffs. Otherwise the varstores are equal and move on.
				if len(scVars) > 0 {
					_, err = indexer.BBSBackend.StoreSCIDVariableDetails(vi, scVars, storedindex)
					if err != nil {
						logger.Errorf("[StartDaemonMode-hardcodedscids] ERR - storing scid variable details: %v", err)
					}
					_, err = indexer.BBSBackend.StoreSCIDInteractionHeight(vi, storedindex)
					if err != nil {
						logger.Errorf("[StartDaemonMode-hardcodedscids] ERR - storing scid interaction height: %v", err)
					}
				}
				indexer.BBSBackend.Writing.Store(false)
				//indexer.BBSBackend.Writer = ""
			}
		}
	}

	// Mark that the initial interaction height indexing round is complete.
	indexer.InteractionIndexReady.Store(true)

	if storedindex > indexer.LastIndexedHeight {
		logger.Printf("[StartDaemonMode-storedIndex] Continuing from last indexed height %v", storedindex)
		indexer.Lock()
		indexer.LastIndexedHeight = storedindex
		indexer.Unlock()

		var getinfo *structures.GetInfo
		switch indexer.DBType {
		case "gravdb":
			getinfo = indexer.GravDBBackend.GetGetInfoDetails()
		case "boltdb":
			getinfo = indexer.BBSBackend.GetGetInfoDetails()
		}

		// Only pull in gnomonsc data if fastsync is defined. TODO: Maybe extra flag for checking this on startup as well.
		if getinfo != nil && indexer.FastSyncConfig.Enabled {
			// Define gnomon builtin scid for indexing
			var gnomon_scid string
			if !getinfo.Testnet {
				gnomon_scid = structures.MAINNET_GNOMON_SCID
			} else {
				gnomon_scid = structures.TESTNET_GNOMON_SCID
			}

			// All could be future optimized .. for now it's slower but works.
			logger.Printf("[StartDaemonMode-fastsync] Checking signature and validity of '%s'...", gnomon_scid)
			variables, code, _, err := indexer.RPC.GetSCVariables(gnomon_scid, indexer.ChainHeight, nil, nil, nil, false)
			if err == nil && len(variables) > 0 {
				_ = code
				keysstring, _, _ := indexer.GetSCIDValuesByKey(variables, gnomon_scid, "signature", indexer.ChainHeight)

				// Check  if keysstring is nil or not to avoid any sort of panics
				var sigstr string
				if len(keysstring) > 0 {
					sigstr = keysstring[0]
				}

				validated, _, err := indexer.ValidateSCSignature(code, sigstr)
				if err != nil {
					logger.Errorf("[StartDaemonMode-ValidateSCSignature] ERR - %v", err)
				}

				// Ensure SC signature is validated (LOAD("signature") checks out to code validation)
				if validated || err != nil {
					logger.Printf("[StartDaemonMode-fastsync] Gnomon SC '%v' code VALID - proceeding to inject scid data.", gnomon_scid)

					scidstoadd := make(map[string]*structures.FastSyncImport)

					// Check k/v pairs for the necessary info: keys/values - scid/headers, scidowner/owner, scidheight/height
					for _, v := range variables {
						switch ckey := v.Key.(type) {
						case string:
							if v.Value != nil {
								switch len(ckey) {
								case 64:
									if scidExist(indexer.SFSCIDExclusion, ckey) {
										logger.Debugf("[StartDaemonMode] Not appending gnomonsc data SCID '%s' as it resides within SFSCIDExclusion - '%v'.", ckey, indexer.SFSCIDExclusion)
										continue
									}
									// Check for k/v scid/headers
									if scidstoadd[ckey] == nil {
										scidstoadd[ckey] = &structures.FastSyncImport{}
									}
									scidstoadd[ckey].Headers = v.Value.(string)
								case 69:
									if scidExist(indexer.SFSCIDExclusion, ckey[0:64]) {
										logger.Debugf("[StartDaemonMode] Not appending gnomonsc data SCID '%s' as it resides within SFSCIDExclusion - '%v'.", ckey[0:64], indexer.SFSCIDExclusion)
										continue
									}
									// Check for k/v scidowner/owner
									if scidstoadd[ckey[0:64]] == nil {
										scidstoadd[ckey[0:64]] = &structures.FastSyncImport{}
									}
									scidstoadd[ckey[0:64]].Owner = v.Value.(string)
								case 70:
									if scidExist(indexer.SFSCIDExclusion, ckey[0:64]) {
										logger.Debugf("[StartDaemonMode] Not appending gnomonsc data SCID '%s' as it resides within SFSCIDExclusion - '%v'.", ckey[0:64], indexer.SFSCIDExclusion)
										continue
									}
									// Check for k/v scidheight/height
									if scidstoadd[ckey[0:64]] == nil {
										scidstoadd[ckey[0:64]] = &structures.FastSyncImport{}
									}
									scidstoadd[ckey[0:64]].Height = v.Value.(uint64)
								default:
									// Nothing - only should match defined ckey lengths
								}
							}
						default:
							// Nothing - expect only string for value types specifically to Gnomon
						}
					}

					err := indexer.AddSCIDToIndex(scidstoadd, indexer.FastSyncConfig.SkipFSRecheck, false)
					if err != nil {
						logger.Errorf("[StartDaemonMode-fastsync] ERR - adding scids to index - %v", err)
					}
				} else {
					logger.Errorf("[StartDaemonMode-fastsync] Gnomon SC '%v' code was NOT validated against in-built signature variable. Skipping auto-population of scids.", gnomon_scid)
				}
			} else {
				if err != nil {
					logger.Errorf("[StartDaemonMode] Fastsync failed to build GnomonSC index. Error - '%v'. Are you using daemon v139? Syncing from current chain height.", err)
				} else {
					logger.Errorf("[StartDaemonMode] Fastsync failed to build GnomonSC index. Variables returned - '%v'. Are you using daemon v139? Syncing from current chain height.", len(variables))
				}
			}
		}
	}

	if blockParallelNum <= 0 {
		blockParallelNum = 1
	}

	if len(pre_validatedSCIDs) > 0 && !indexer.FastSyncConfig.NoCode {
		switch indexer.DBType {
		case "gravdb":
			if err := storage.BackfillTelaMetadata(indexer.GravDBBackend); err != nil {
				logger.Errorf("[StartDaemonMode] Error backfilling TELA metadata: %v", err)
			}
		case "boltdb":
			if err := storage.BackfillTelaMetadata(indexer.BBSBackend); err != nil {
				logger.Errorf("[StartDaemonMode] Error backfilling TELA metadata: %v", err)
			}
		}
	}

	logger.Printf("[StartDaemonMode] Set number of parallel blocks to index to '%d'. Starting index routine...", blockParallelNum)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Fatalf("[StartDaemonMode] PANIC recovered: %v", r)
				indexer.Closing.Store(true)
			}
		}()
		k := 0
		for {
			if indexer.Closing.Load() {
				indexer.Status = "closing"
				logger.Printf("[StartDaemonMode] Closing indexer...")
				// Break out on closing call
				break
			}

			indexer.RLock()
			lastIndexed := indexer.LastIndexedHeight
			chainHeight := indexer.ChainHeight
			indexer.RUnlock()

			if lastIndexed >= chainHeight {
				indexer.Status = "indexed"
				time.Sleep(1 * time.Second)
				continue
			}

			indexer.Status = "indexing"

			// Check to cover fastsync scenarios; no reason to pull all this logic into the multi-block scanning components - this will only run once
			if k == 0 {
				_, err := indexer.RPC.getBlockHash(uint64(indexer.LastIndexedHeight))
				if err != nil {
					// Handle pruned nodes index errors... find height that they have blocks able to be indexed
					if strings.Contains(err.Error(), "err occured empty block") || strings.Contains(err.Error(), "err occured file does not exist") {
						currIndex := indexer.LastIndexedHeight
						rewindIndex := int64(0)
						for {
							if indexer.Closing.Load() {
								// If we do concurrent blocks in the future, this will need to move/be modified to be *after* all concurrent blocks are done incase exit etc.
								writeWait, _ := time.ParseDuration("20ms")
								switch indexer.DBType {
								case "gravdb":
									for indexer.GravDBBackend.Writing.Load() {
										if indexer.Closing.Load() {
											return
										}
										//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
										time.Sleep(writeWait)
									}
									indexer.GravDBBackend.Writing.Store(true)
									indexer.GravDBBackend.StoreLastIndexHeight(currIndex, false)
									indexer.GravDBBackend.Writing.Store(false)
								case "boltdb":
									for indexer.BBSBackend.Writing.Load() {
										if indexer.Closing.Load() {
											return
										}
										//logger.Debugf("[Indexer-StartDaemonMode-StoreLastIndexHeight] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
										time.Sleep(writeWait)
									}
									indexer.BBSBackend.Writing.Store(true)
									//indexer.BBSBackend.Writer = "StartDaemonMode"
									indexer.BBSBackend.StoreLastIndexHeight(currIndex)
									indexer.BBSBackend.Writing.Store(false)
									//indexer.BBSBackend.Writer = ""
								}
								// Break out on closing call
								break
							}
							_, err = indexer.RPC.getBlockHash(uint64(currIndex))
							if err != nil {
								//if strings.Contains(err.Error(), "err occured empty block") {
								//time.Sleep(200 * time.Millisecond)	// sleep for node spam, not *required* but can be useful for lesser nodes in brief catchup time.
								// Increase block by 10 to not spam the daemon at every single block, but skip along a little bit to move faster/more less impact to node. This can be modified if required.
								if (currIndex + block_jump) > indexer.ChainHeight {
									currIndex = indexer.ChainHeight
								} else {
									currIndex += block_jump
								}
								// Should this be an err? We'll get this at least when pinpointing the pruned node states
								logger.Errorf("GetBlock failed - checking %v", currIndex)
								//}
							} else {
								// Self-contain and loop through at most 10 or X blocks
								logger.Printf("GetBlock worked at %v", currIndex)
								for {
									if indexer.Closing.Load() {
										// If we do concurrent blocks in the future, this will need to move/be modified to be *after* all concurrent blocks are done incase exit etc.
										writeWait, _ := time.ParseDuration("20ms")
										switch indexer.DBType {
										case "gravdb":
											for indexer.GravDBBackend.Writing.Load() {
												if indexer.Closing.Load() {
													return
												}
												//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
												time.Sleep(writeWait)
											}
											indexer.GravDBBackend.Writing.Store(true)
											indexer.PruneDerivedDataAbove(rewindIndex)
											indexer.GravDBBackend.StoreLastIndexHeight(rewindIndex, false)
											indexer.GravDBBackend.Writing.Store(false)
										case "boltdb":
											for indexer.BBSBackend.Writing.Load() {
												if indexer.Closing.Load() {
													return
												}
												//logger.Debugf("[Indexer-StartDaemonMode-StoreLastIndexHeight] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
												time.Sleep(writeWait)
											}
											indexer.BBSBackend.Writing.Store(true)
											//indexer.BBSBackend.Writer = "StartDaemonMode"
											indexer.PruneDerivedDataAbove(rewindIndex)
											indexer.BBSBackend.StoreLastIndexHeight(rewindIndex)
											indexer.BBSBackend.Writing.Store(false)
											//indexer.BBSBackend.Writer = ""
										}

										// Break out on closing call
										break
									}
									if rewindIndex == 0 {
										rewindIndex = currIndex - block_jump + 1
										if rewindIndex < 0 {
											rewindIndex = 1
										}
									} else {
										logger.Printf("Checking GetBlock at %v", rewindIndex)
										_, err = indexer.RPC.getBlockHash(uint64(rewindIndex))
										if err != nil {
											rewindIndex++
											// TODO: If lowcpuram option defined, uncomment below to add w/ such clause
											//time.Sleep(200 * time.Millisecond)	// sleep for node spam, not *required* but can be useful for lesser nodes in brief catchup time.
										} else {
											logger.Printf("GetBlock worked at %v - continuing as normal", rewindIndex+1)
											// Break out, we found the earliest block detail
											indexer.Lock()
											indexer.LastIndexedHeight = rewindIndex + 1
											indexer.Unlock()
											break
										}
									}
								}
								break
							}
						}
					}

					logger.Errorf("[mainFOR] ERROR - %v", err)
					time.Sleep(1 * time.Second)
					continue
				}
				k++
			}

			indexer.RLock()
			lastIndexed = indexer.LastIndexedHeight
			chainHeight = indexer.ChainHeight
			indexer.RUnlock()

			if lastIndexed+int64(blockParallelNum) > chainHeight {
				blockParallelNum = int(chainHeight - lastIndexed)

				if blockParallelNum <= 0 || lastIndexed == chainHeight {
					time.Sleep(1 * time.Second)
					continue
				}
			}

			var regTxCount int64
			var burnTxCount int64
			var normTxCount int64
			var wg sync.WaitGroup
			wg.Add(blockParallelNum)

			var blsctxnsLock sync.RWMutex
			var blIndexTxns []*structures.BlockTxns

			for i := 1; i <= blockParallelNum; i++ {
				go func(i int) {
					if indexer.Closing.Load() {
						wg.Done()
						return
					}
					currBlHeight := indexer.LastIndexedHeight + int64(i)

					blid, err := indexer.RPC.getBlockHash(uint64(currBlHeight))
					if err != nil {
						logger.Errorf("[StartDaemonMode-mainFOR-getBlockHash] %v - ERROR - getBlockHash(%v) - %v", currBlHeight, uint64(currBlHeight), err)
						wg.Done()
						return
					}

					blockTxns, err := indexer.indexBlock(blid, currBlHeight)
					if err != nil {
						logger.Errorf("[StartDaemonMode-mainFOR-indexBlock] %v - ERROR - indexBlock(%v) - %v", currBlHeight, blid, err)
						wg.Done()
						return
					}

					if len(blockTxns.Tx_hashes) > 0 {
						blsctxnsLock.Lock()
						blIndexTxns = append(blIndexTxns, blockTxns)
						blsctxnsLock.Unlock()
						wg.Done()
					} else {
						wg.Done()
					}
				}(i)
			}
			wg.Wait()

			if indexer.Closing.Load() {
				break
			}

			// Arrange blIndexTxns by height so processed linearly
			sort.SliceStable(blIndexTxns, func(i, j int) bool {
				return blIndexTxns[i].Topoheight < blIndexTxns[j].Topoheight
			})

			// Run through blocks one at a time here to max cpu on a given block if large txns rather than split cpu across go routines of multiple blocks
			for _, v := range blIndexTxns {
				if len(v.Tx_hashes) > 0 {
					c_sctxs, cregTxCount, cburnTxCount, cnormTxCount, err := indexer.IndexTxn(v, false)
					if err != nil {
						logger.Errorf("[StartDaemonMode-mainFOR-IndexTxn] %v - ERROR - IndexTxn(%v) - %v", v.Topoheight, v.Tx_hashes, err)
						return
					}

					regTxCount += cregTxCount
					burnTxCount += cburnTxCount
					normTxCount += cnormTxCount

					err = indexer.indexInvokes(c_sctxs, v)
					if err != nil {
						logger.Errorf("[StartDaemonMode-mainFOR-indexInvokes]  ERROR - %v", err)
						break
					}
				}
			}
			if err != nil {
				logger.Errorf("[StartDaemonMode-mainFOR-TxnIndexErrs] ERROR - %v", err)
				continue
			}

			if (regTxCount > 0 || burnTxCount > 0 || normTxCount > 0) && !(indexer.RunMode == "asset") {
				err = indexer.indexTxCounts(regTxCount, burnTxCount, normTxCount)
				if err != nil {
					logger.Errorf("[StartDaemonMode-mainFOR-indexTxCounts] ERROR - %v", err)
					continue
				}
			}

			if indexer.LastIndexedHeight <= indexer.LastIndexedHeight+int64(blockParallelNum) {
				indexer.Lock()
				indexer.LastIndexedHeight += int64(blockParallelNum)
				indexer.Unlock()

				writeWait, _ := time.ParseDuration("20ms")
				switch indexer.DBType {
				case "gravdb":
					for indexer.GravDBBackend.Writing.Load() {
						if indexer.Closing.Load() {
							return
						}
						//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
						time.Sleep(writeWait)
					}
					indexer.GravDBBackend.Writing.Store(true)
					_, _, err := indexer.GravDBBackend.StoreLastIndexHeight(indexer.LastIndexedHeight, false)
					if err != nil {
						logger.Errorf("[StartDaemonMode-mainFOR-StoreLastIndexHeight] ERROR - %v", err)
					}
					indexer.GravDBBackend.Writing.Store(false)
				case "boltdb":
					for indexer.BBSBackend.Writing.Load() {
						if indexer.Closing.Load() {
							return
						}
						//logger.Debugf("[Indexer-StartDaemonMode-StoreLastIndexHeight] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
						time.Sleep(writeWait)
					}
					indexer.BBSBackend.Writing.Store(true)
					//indexer.BBSBackend.Writer = "StartDaemonMode"
					indexer.BBSBackend.StoreLastIndexHeight(indexer.LastIndexedHeight)
					indexer.BBSBackend.Writing.Store(false)
					//indexer.BBSBackend.Writer = ""
				}
			}
		}
	}()
}

// Potential future item - may be removed as primary service of Gnomon is against daemon and not wallet due to security [unless future black box scenarios]
func (indexer *Indexer) StartWalletMode(runType string) {
	var err error

	// Simple connect loop .. if connection fails initially then keep trying, else break out and continue on. Connect() is handled in getInfo() for retries later on if connection ceases again
	/*
		TODO:
		var astr []string
		astr = append(astr, "Basic dGVzdDp0ZXN0cGFzcw==")
		client.WS, _, err = websocket.DefaultDialer.Dial("ws://"+endpoint+"/ws", http.Header{"Authorization": astr})
	*/
	for {
		if indexer.Closing.Load() {
			// Break out on closing call
			break
		}
		logger.Printf("[StartDaemonMode] Trying to connect...")
		err = indexer.RPC.Connect(indexer.Endpoint)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	time.Sleep(1 * time.Second)

	// Continuously getInfo from daemon to update topoheight globally
	switch runType {
	case "receive":
		// do receive actions here (e.g. from data source via API/WS)
		// TODO: is there anything we need to do within indexer itself if just receiving?
	default:
		// 'retrieve'/etc.
		go indexer.getWalletHeight()
		time.Sleep(1 * time.Second)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Fatalf("[StartWalletMode] PANIC recovered: %v", r)
					indexer.Closing.Store(true)
				}
			}()
			for {
				if indexer.Closing.Load() {
					// Break out on closing call
					break
				}

				if indexer.LastIndexedHeight > indexer.ChainHeight {
					time.Sleep(1 * time.Second)
					continue
				}

				// Do indexing calls here

				// TODO: Modify this to be the height of the *next* tx index etc.
				indexer.Lock()
				indexer.LastIndexedHeight++
				indexer.Unlock()
			}
		}()
	}

	// Hold
	select {}
}

// Manually add/inject a SCID to be indexed. Checks validity and then stores within owner tree (no signer addr) and stores a set of current variables.
func (indexer *Indexer) AddSCIDToIndex(scidstoadd map[string]*structures.FastSyncImport, skipfsrecheck bool, varstoreonly bool) (err error) {
	logger.Printf("[AddSCIDToIndex] Starting - Sorting %v SCIDs to index", len(scidstoadd))

	var tempdb *storage.GravitonStore
	tempdb, err = storage.NewGravDBRAM("25ms")
	if err != nil {
		return fmt.Errorf("[AddSCIDToIndex] Error creating new gravdb: %v", err)
	}

	// Use map for O(1) treename lookups instead of O(n) slice scans.
	treenamesMap := map[string]struct{}{"owner": {}}

	bar := progressbar.Default(int64(len(scidstoadd)), "Adding SCIDs")

	// --- Phase 1: Filter and collect SCIDs that need processing ---
	var scidsToFetch []string
	var fsiMap = make(map[string]*structures.FastSyncImport, len(scidstoadd))
	for scid, fsi := range scidstoadd {
		bar.Add(1)
		if (scidExist(indexer.ValidatedSCs, scid) || indexer.Closing.Load()) && !varstoreonly {
			continue
		}
		if scidExist(indexer.SFSCIDExclusion, scid) {
			logger.Debugf("[StartDaemonMode] Not appending scidstoadd SCID '%s' as it resides within SFSCIDExclusion - '%v'.", scid, indexer.SFSCIDExclusion)
			continue
		}
		fsiMap[scid] = fsi
		// Only need RPC fetch if we must evaluate search filter or store variables.
		// NoCode fastsync skips all fetching (contains stays false).
		if !skipfsrecheck || !indexer.FastSyncConfig.NoCode {
			scidsToFetch = append(scidsToFetch, scid)
		}
	}

	logger.Printf("[AddSCIDToIndex-DEBUG] After Phase 1: fsiMap=%d scidsToFetch=%d validatedSCs=%d", len(fsiMap), len(scidsToFetch), len(indexer.ValidatedSCs))

	// --- Phase 2: Batch fetch SC data using existing RPC client ---
	scidstoindexstage := make([]SCIDToIndexStage, 0, len(fsiMap))

	if len(scidsToFetch) > 0 {
		if indexer.RPC == nil || indexer.RPC.RPC == nil {
			logger.Printf("[AddSCIDToIndex] RPC client unavailable, skipping fetch for %d SCIDs", len(scidsToFetch))
			return fmt.Errorf("rpc client unavailable")
		}

		batchSize := 500
		var telaCandidates []string

		for i := 0; i < len(scidsToFetch); i += batchSize {
			if indexer.Closing.Load() {
				return nil
			}
			end := i + batchSize
			if end > len(scidsToFetch) {
				end = len(scidsToFetch)
			}
			batch := scidsToFetch[i:end]

			specs := make([]jrpc2.Spec, len(batch))
			for j, scid := range batch {
				params := rpc.GetSC_Params{SCID: scid, Variables: true}
				if skipfsrecheck && !indexer.FastSyncConfig.NoCode {
					params.Code = true
				}
				specs[j] = jrpc2.Spec{Method: "DERO.GetSC", Params: params}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			responses, err := indexer.RPC.RPC.Batch(ctx, specs)
			cancel()
			if err != nil {
				logger.Printf("[AddSCIDToIndex] Batch fetch error at offset %d: %v", i, err)
				continue
			}

			for j, resp := range responses {
				if j >= len(batch) || resp == nil || resp.Error() != nil {
					continue
				}
				var out rpc.GetSC_Result
				if err := resp.UnmarshalResult(&out); err != nil {
					continue
				}

				scid := batch[j]
				scVars, scCode, _ := parseGetSCResult(scid, out, nil, nil, nil)
				var contains bool
				var isTela bool

				// Inline TELA classification: check for telaVersion key
				for _, v := range scVars {
					if key, ok := v.Key.(string); ok && key == "telaVersion" {
						if val, ok := v.Value.(string); ok && val != "" {
							isTela = true
							break
						}
					}
				}

				if len(indexer.SearchFilter) == 0 {
					contains = true
				} else if scCode != "" {
					for _, sfv := range indexer.SearchFilter {
						if strings.Contains(scCode, sfv) {
							contains = true
							break
						}
					}
				}

				if isTela {
					telaCandidates = append(telaCandidates, scid)
				}

				scidstoindexstage = append(scidstoindexstage, SCIDToIndexStage{
					scid:     scid,
					fsi:      fsiMap[scid],
					scVars:   scVars,
					scCode:   scCode,
					contains: contains,
				})
			}
		}

		// Batch-store TELA candidates after classification
		if len(telaCandidates) > 0 {
			for _, scid := range telaCandidates {
				switch indexer.DBType {
				case "gravdb":
					indexer.GravDBBackend.StoreTelaCandidate(scid, "valid_index")
				case "boltdb":
					indexer.BBSBackend.StoreTelaCandidate(scid, "valid_index")
				}
			}
			logger.Printf("[AddSCIDToIndex] Classified and stored %d TELA candidates", len(telaCandidates))
		}
	} else {
		// NoCode fastsync path: no RPC needed, stage all SCIDs with contains=false.
		logger.Printf("[AddSCIDToIndex-DEBUG] NoCode path: staging %d SCIDs from fsiMap", len(fsiMap))
		for scid, fsi := range fsiMap {
			scidstoindexstage = append(scidstoindexstage, SCIDToIndexStage{
				scid:     scid,
				fsi:      fsi,
				contains: false,
			})
		}

		// Classify TELA candidates from existing DB variables without RPC.
		var telaCandidates []string
		for scid := range fsiMap {
			switch indexer.DBType {
			case "gravdb":
				if indexer.GravDBBackend.HasSCIDVariable(scid, "telaVersion") {
					telaCandidates = append(telaCandidates, scid)
				}
			case "boltdb":
				if indexer.BBSBackend.HasSCIDVariable(scid, "telaVersion") {
					telaCandidates = append(telaCandidates, scid)
				}
			}
		}
		if len(telaCandidates) > 0 {
			for _, scid := range telaCandidates {
				switch indexer.DBType {
				case "gravdb":
					indexer.GravDBBackend.StoreTelaCandidate(scid, "valid_index")
				case "boltdb":
					indexer.BBSBackend.StoreTelaCandidate(scid, "valid_index")
				}
			}
			logger.Printf("[AddSCIDToIndex] NoCode fastsync: classified and stored %d TELA candidates", len(telaCandidates))
		}
	}

	logger.Printf("[AddSCIDToIndex-DEBUG] Phase 3 start: scidstoindexstage=%d", len(scidstoindexstage))

	// --- Phase 3: Batch store to tempDB using snapshot-safe batch methods ---
	batchCommitSize := 500
	for i := 0; i < len(scidstoindexstage); i += batchCommitSize {
		if indexer.Closing.Load() {
			return nil
		}

		end := i + batchCommitSize
		if end > len(scidstoindexstage) {
			end = len(scidstoindexstage)
		}

		owners := make(map[string]string)
		heights := make(map[string]int64)

		for _, v := range scidstoindexstage[i:end] {
			if v.contains || varstoreonly {
				if len(v.scVars) > 0 || skipfsrecheck {
					indexer.Lock()
					indexer.ValidatedSCs = append(indexer.ValidatedSCs, v.scid)
					indexer.Unlock()

					owner := ""
					if v.fsi != nil {
						owner = v.fsi.Owner
					}
					owners[v.scid] = owner

					height := int64(1)
					if v.fsi != nil {
						height = int64(v.fsi.Height)
					}
					heights[v.scid] = height

					// Variable details and interaction heights (use legacy per-call for these)
					svdtree, svdchanges, err := tempdb.StoreSCIDVariableDetails(v.scid, v.scVars, indexer.ChainHeight, true)
					if err != nil {
						logger.Errorf("[AddSCIDToIndex] ERR - storing scid variable details: %v", err)
					} else if svdchanges {
						tempdb.CommitTrees([]*graviton.Tree{svdtree})
					}
					treenamesMap[v.scid+"vars"] = struct{}{}

					sihtree, sihchanges, err := tempdb.StoreSCIDInteractionHeight(v.scid, indexer.ChainHeight, true)
					if err != nil {
						logger.Errorf("[AddSCIDToIndex] ERR - storing scid interaction height: %v", err)
					} else if sihchanges {
						tempdb.CommitTrees([]*graviton.Tree{sihtree})
					}
					treenamesMap[v.scid+"heights"] = struct{}{}
				}
			} else if skipfsrecheck {
				// Limited format: owner + install height only
				indexer.Lock()
				indexer.ValidatedSCs = append(indexer.ValidatedSCs, v.scid)
				indexer.Unlock()

				owner := ""
				if v.fsi != nil {
					owner = v.fsi.Owner
				}
				owners[v.scid] = owner

				height := int64(1)
				if v.fsi != nil {
					height = int64(v.fsi.Height)
				}
				heights[v.scid] = height
			}
		}

		if len(owners) > 0 {
			if err := tempdb.BatchStoreOwners(owners); err != nil {
				logger.Errorf("[AddSCIDToIndex] ERR - batch storing owners: %v", err)
			}
		}
		if len(heights) > 0 {
			if err := tempdb.BatchStoreInstallHeights(heights); err != nil {
				logger.Errorf("[AddSCIDToIndex] ERR - batch storing heights: %v", err)
			}
		}

		logger.Printf("[AddSCIDToIndex-DEBUG] Batch %d-%d: owners=%d heights=%d", i, end, len(owners), len(heights))
	}

	// Convert treenames map to slice for StoreAltDBInput.
	treenames := make([]string, 0, len(treenamesMap))
	for name := range treenamesMap {
		treenames = append(treenames, name)
	}

	logger.Printf("[AddSCIDToIndex] Done - Sorting %v SCIDs to index", len(scidstoadd))
	switch indexer.DBType {
	case "gravdb":
		logger.Printf("[AddSCIDToIndex] Current stored disk: %v", len(indexer.GravDBBackend.GetAllOwnersAndSCIDs()))
		logger.Printf("[AddSCIDToIndex] Current stored ram: %v", len(tempdb.GetAllOwnersAndSCIDs()))

		logger.Printf("[AddSCIDToIndex] Starting - Committing RAM SCID sort to disk storage...")
		writeWait, _ := time.ParseDuration("10ms")
		for tempdb.Writing.Load() || indexer.GravDBBackend.Writing.Load() {
			if indexer.Closing.Load() {
				return
			}
			time.Sleep(writeWait)
		}
		tempdb.Writing.Store(true)
		indexer.GravDBBackend.Writing.Store(true)
		indexer.GravDBBackend.StoreAltDBInput(treenames, tempdb)
		tempdb.Writing.Store(false)
		indexer.GravDBBackend.Writing.Store(false)
		logger.Printf("[AddSCIDToIndex] Done - Committing RAM SCID sort to disk storage...")
		logger.Printf("[AddSCIDToIndex] New stored disk: %v", len(indexer.GravDBBackend.GetAllOwnersAndSCIDs()))
	case "boltdb":
		logger.Printf("[AddSCIDToIndex] Current stored disk: %v", len(indexer.BBSBackend.GetAllOwnersAndSCIDs()))
		logger.Printf("[AddSCIDToIndex] Current stored ram: %v", len(tempdb.GetAllOwnersAndSCIDs()))

		logger.Printf("[AddSCIDToIndex] Starting - Committing RAM SCID sort to disk storage...")
		writeWait, _ := time.ParseDuration("10ms")
		for tempdb.Writing.Load() || indexer.BBSBackend.Writing.Load() {
			if indexer.Closing.Load() {
				return
			}
			time.Sleep(writeWait)
		}
		tempdb.Writing.Store(true)
		indexer.BBSBackend.Writing.Store(true)
		indexer.BBSBackend.StoreAltDBInput(treenames, tempdb)
		tempdb.Writing.Store(false)
		indexer.BBSBackend.Writing.Store(false)
		logger.Printf("[AddSCIDToIndex] Done - Committing RAM SCID sort to disk storage...")
		logger.Printf("[AddSCIDToIndex] New stored disk: %v", len(indexer.BBSBackend.GetAllOwnersAndSCIDs()))
	}

	return err
}

// batchGetSCCode fetches only SC code (not variables) for a batch of SCIDs.
// This is lighter than BatchGetSCData when only search filter evaluation is needed.
func (indexer *Indexer) indexBlock(blid string, topoheight int64) (blockTxns *structures.BlockTxns, err error) {
	blockTxns = &structures.BlockTxns{}

	var io rpc.GetBlock_Result
	var ip = rpc.GetBlock_Params{Hash: blid}

	if indexer.Closing.Load() {
		return
	}

	// TODO: Make this a consumable func with rpc calls and timeout / wait / retry logic for deduplication of code. Or use alternate method of checking [primary use case is remote nodes]
	var reconnect_count int
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = indexer.RPC.RPC.CallResult(ctx, "DERO.GetBlock", ip, &io)
		cancel()
		if err != nil {
			logger.Debugf("[indexBlock] ERROR - GetBlock failed: %v . Trying again (%v / 5) ", err, reconnect_count)
			if reconnect_count >= 5 {
				return blockTxns, fmt.Errorf("[indexBlock] ERROR - GetBlock failed: %v", err)
			}
			time.Sleep(time.Duration(1<<reconnect_count) * time.Second) // exponential backoff

			reconnect_count++

			continue
		}

		break
	}

	var bl block.Block
	var block_bin []byte
	var addr *rpc.Address
	writeWait, _ := time.ParseDuration("20ms")

	block_bin, _ = hex.DecodeString(io.Blob)
	bl.Deserialize(block_bin)

	if indexer.StoreIntegrators {
		p := new(crypto.Point)
		if err := p.DecodeCompressed(bl.Miner_TX.MinerAddress[:]); err == nil {
			addr = rpc.NewAddressFromKeys(p)
		}

		switch indexer.DBType {
		case "gravdb":
			for indexer.GravDBBackend.Writing.Load() {
				if indexer.Closing.Load() {
					return
				}
				//logger.Debugf("[Indexer-IndexBlock] GravitonDB is writing... sleeping for %v...", writeWait)
				time.Sleep(writeWait)
			}

			indexer.GravDBBackend.Writing.Store(true)
			_, _, err = indexer.GravDBBackend.StoreIntegrators(addr.String(), false)
			if err != nil {
				logger.Errorf("[indexBlock] Error storing integrator details for blid %v", err)
				indexer.GravDBBackend.Writing.Store(false)
				return blockTxns, err
			}
			indexer.GravDBBackend.Writing.Store(false)
		case "boltdb":
			for indexer.BBSBackend.Writing.Load() {
				if indexer.Closing.Load() {
					return
				}
				//logger.Debugf("[Indexer-IndexBlock] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
				time.Sleep(writeWait)
			}

			indexer.BBSBackend.Writing.Store(true)
			//indexer.BBSBackend.Writer = "IndexBlock"
			_, err = indexer.BBSBackend.StoreIntegrators(addr.String())
			if err != nil {
				logger.Errorf("[indexBlock] Error storing integrator details for blid %v", err)
				indexer.BBSBackend.Writing.Store(false)
				//indexer.BBSBackend.Writer = ""
				return blockTxns, err
			}
			indexer.BBSBackend.Writing.Store(false)
			//indexer.BBSBackend.Writer = ""
		}
	}

	if indexer.MBLLookup {
		mbldetails, err2 := mbllookup.GetMBLByBLHash(bl)
		if err2 != nil {
			logger.Errorf("[indexBlock] Error getting miniblock details for blid %v", bl.GetHash().String())
			return blockTxns, err2
		}

		switch indexer.DBType {
		case "gravdb":
			if !(indexer.RunMode == "asset") {
				for indexer.GravDBBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
					time.Sleep(writeWait)
				}

				indexer.GravDBBackend.Writing.Store(true)
				_, _, err2 = indexer.GravDBBackend.StoreMiniblockDetailsByHash(blid, mbldetails, false)
				if err2 != nil {
					logger.Errorf("[indexBlock] Error storing miniblock details for blid %v", err2)
					indexer.GravDBBackend.Writing.Store(false)
					return blockTxns, err2
				}
				indexer.GravDBBackend.Writing.Store(false)
			}
		case "boltdb":
			if !(indexer.RunMode == "asset") {
				for indexer.BBSBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-IndexBlock-StoreMiniblockDetailsByHash] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
					time.Sleep(writeWait)
				}

				indexer.BBSBackend.Writing.Store(true)
				//indexer.BBSBackend.Writer = "IndexBlock"
				_, err2 = indexer.BBSBackend.StoreMiniblockDetailsByHash(blid, mbldetails)
				if err2 != nil {
					logger.Errorf("[indexBlock] Error storing miniblock details for blid %v", err2)
					indexer.BBSBackend.Writing.Store(false)
					//indexer.BBSBackend.Writer = ""
					return blockTxns, err2
				}
				indexer.BBSBackend.Writing.Store(false)
				//indexer.BBSBackend.Writer = ""
			}
		}
	}

	blockTxns.Topoheight = int64(bl.Height)
	blockTxns.Tx_hashes = bl.Tx_hashes

	return
}

func (indexer *Indexer) IndexTxn(blTxns *structures.BlockTxns, noStore bool) (bl_sctxs []structures.SCTXParse, regTxCount int64, burnTxCount int64, normTxCount int64, err error) {
	var txslock sync.RWMutex

	var wg sync.WaitGroup
	wg.Add(len(blTxns.Tx_hashes))

	for i := 0; i < len(blTxns.Tx_hashes); i++ {
		go func(i int) {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("[IndexTxn] PANIC recovered for tx index %d: %v", i, r)
					wg.Done()
				}
			}()
			if indexer.Closing.Load() {
				wg.Done()
				return
			}

			// We can match the PoW scheme result to filter out reg txns without needing to waste GetTransaction calls against saving time - https://github.com/deroproject/derohe/blob/main/cmd/dero-wallet-cli/easymenu_post_open.go#L150
			if blTxns.Tx_hashes[i][0] == 0 && blTxns.Tx_hashes[i][1] == 0 && blTxns.Tx_hashes[i][2] == 0 {
				txslock.Lock()
				regTxCount++
				txslock.Unlock()
				wg.Done()
				return
			}

			var tx transaction.Transaction
			var sc_args rpc.Arguments
			var sc_fees uint64
			var sender string

			var inputparam rpc.GetTransaction_Params
			var output rpc.GetTransaction_Result

			inputparam.Tx_Hashes = append(inputparam.Tx_Hashes, blTxns.Tx_hashes[i].String())

			// TODO: Make this a consumable func with rpc calls and timeout / wait / retry logic for deduplication of code. Or use alternate method of checking [primary use case is remote nodes]
			var reconnect_count int
			var callErr error
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				callErr = indexer.RPC.RPC.CallResult(ctx, "DERO.GetTransaction", inputparam, &output)
				cancel()
				if callErr != nil {
					logger.Debugf("[IndexTxn] ERROR - GetTransaction for txid '%v' failed: %v . Trying again (%v / 5)", inputparam.Tx_Hashes, callErr, reconnect_count)
					if reconnect_count >= 5 {
						// TODO - In event indexer.Endpoint is being swapped, this case will fail and you could miss a txn. Need another handle rather than just "assume" skip/move on.
						wg.Done()
						// If we error, this could be due to regtxn not valid on pruned node or other reasons. We will just nil the err and then return and move on.
						logger.Errorf("[IndexTxn] ERROR - GetTransaction for txid '%v' failed: %v . (%v / 5 times)", inputparam.Tx_Hashes, callErr, reconnect_count)
						return
					}
					time.Sleep(time.Duration(1<<reconnect_count) * time.Second) // exponential backoff

					reconnect_count++

					continue
				}

				break
			}

			tx_bin, _ := hex.DecodeString(output.Txs_as_hex[0])
			tx.Deserialize(tx_bin)

			// TODO: Add count for registration TXs and store the following on normal txs: IF SCID IS PRESENT, store tx details + ring members + fees + etc. Use later for scid balance queries
			if tx.TransactionType == transaction.SC_TX {
				sc_args = tx.SCDATA
				sc_fees = tx.Fees()
				var method string
				var scid string
				var scid_hex []byte

				entrypoint := fmt.Sprintf("%v", sc_args.Value("entrypoint", "S"))

				sc_action := fmt.Sprintf("%v", sc_args.Value("SC_ACTION", "U"))

				// Other ways to parse this, but will do for now --> see https://github.com/deroproject/derohe/blob/main/blockchain/blockchain.go#L688
				if sc_action == "1" {
					method = "installsc"
					scid = string(blTxns.Tx_hashes[i].String())
					scid_hex = []byte(scid)
				} else {
					method = "scinvoke"
					// Get "SC_ID" which is of type H to byte.. then to string
					scid_hex = []byte(fmt.Sprintf("%v", sc_args.Value("SC_ID", "H")))
					scid = string(scid_hex)
				}

				// TODO: What if there are multiple payloads with potentially different ringsizes, can that happen?
				if tx.Payloads[0].Statement.RingSize == 2 {
					sender = output.Txs[0].Signer
				} else {
					//logger.Errorf("[indexBlock] ERR - Ringsize for %v is != 2. Storing blank value for txid sender.", bl.Tx_hashes[i])
					//continue
					/*
						if method == "installsc" {
							// We do not store a ringsize > 2 of installsc calls. Only of SC interactions via sc_invoke for ringsize > 2 and just blank out the sender
							//continue
							//wg.Done()
						}
					*/
				}
				//time.Sleep(2 * time.Second)
				txslock.Lock()
				bl_sctxs = append(bl_sctxs, structures.SCTXParse{Txid: blTxns.Tx_hashes[i].String(), Scid: scid, Scid_hex: scid_hex, Entrypoint: entrypoint, Method: method, Sc_args: sc_args, Sender: sender, Payloads: tx.Payloads, Fees: sc_fees, Height: blTxns.Topoheight})
				txslock.Unlock()
			} else if tx.TransactionType == transaction.REGISTRATION {
				txslock.Lock()
				regTxCount++
				txslock.Unlock()
			} else if tx.TransactionType == transaction.BURN_TX {
				// TODO: Handle burn_tx here
				txslock.Lock()
				burnTxCount++
				txslock.Unlock()
			} else if tx.TransactionType == transaction.NORMAL {
				// TODO: Handle normal tx here
				txslock.Lock()
				normTxCount++
				txslock.Unlock()

				for j := 0; j < len(tx.Payloads); j++ {
					var zhash crypto.Hash
					if tx.Payloads[j].SCID != zhash {
						logger.Debugf("[indexBlock] TXID '%v' has SCID in payload of '%v' and ring members: %v.", blTxns.Tx_hashes[i].String(), tx.Payloads[j].SCID, output.Txs[0].Ring[j])
						for _, v := range output.Txs[0].Ring[j] {
							//bl_normtxs = append(bl_normtxs, structures.NormalTXWithSCIDParse{Txid: blTxns.Tx_hashes[i].String(), Scid: tx.Payloads[j].SCID.String(), Fees: tx_fees, Height: int64(bl.Height)})
							if !noStore {
								writeWait, _ := time.ParseDuration("20ms")
								switch indexer.DBType {
								case "gravdb":
									if !(indexer.RunMode == "asset") {
										for indexer.GravDBBackend.Writing.Load() {
											if indexer.Closing.Load() {
												return
											}
											//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
											time.Sleep(writeWait)
										}
										indexer.GravDBBackend.Writing.Store(true)
										indexer.GravDBBackend.StoreNormalTxWithSCIDByAddr(v, &structures.NormalTXWithSCIDParse{Txid: blTxns.Tx_hashes[i].String(), Scid: tx.Payloads[j].SCID.String(), Fees: sc_fees, Height: int64(blTxns.Topoheight)}, false)
										indexer.GravDBBackend.Writing.Store(false)
									}
								case "boltdb":
									if !(indexer.RunMode == "asset") {
										for indexer.BBSBackend.Writing.Load() {
											if indexer.Closing.Load() {
												return
											}
											//logger.Debugf("[Indexer-IndexTxn-StoreNormalTxWithSCIDByAddr] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
											time.Sleep(writeWait)
										}
										indexer.BBSBackend.Writing.Store(true)
										//indexer.BBSBackend.Writer = "IndexTxn"
										indexer.BBSBackend.StoreNormalTxWithSCIDByAddr(v, &structures.NormalTXWithSCIDParse{Txid: blTxns.Tx_hashes[i].String(), Scid: tx.Payloads[j].SCID.String(), Fees: sc_fees, Height: int64(blTxns.Topoheight)})
										indexer.BBSBackend.Writing.Store(false)
										//indexer.BBSBackend.Writer = ""
									}
								}
							}
						}
					}
				}
			} else {
				logger.Debugf("TX %v type is NOT handled - %v.", tx.TransactionType, blTxns.Tx_hashes[i].String())
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	return bl_sctxs, regTxCount, burnTxCount, normTxCount, err
}

func (indexer *Indexer) indexTxCounts(regTxCount int64, burnTxCount int64, normTxCount int64) (err error) {
	if indexer.Closing.Load() {
		return
	}
	var ctrees []*graviton.Tree

	writeWait, _ := time.ParseDuration("20ms")
	switch indexer.DBType {
	case "gravdb":
		for indexer.GravDBBackend.Writing.Load() {
			if indexer.Closing.Load() {
				return
			}
			//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
			time.Sleep(writeWait)
		}
		indexer.GravDBBackend.Writing.Store(true)
		if regTxCount > 0 && !indexer.FastSyncConfig.Enabled {
			// Load from mem existing regTxCount and append new value
			currRegTxCount := indexer.GravDBBackend.GetTxCount("registration")
			/*
				writeWait, _ := time.ParseDuration("50ms")
				for indexer.GravDBBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-indexBlock-regTxCount] GravitonDB is writing... sleeping for %v...", writeWait)
					time.Sleep(writeWait)
				}
				indexer.GravDBBackend.Writing.Store(true)
			*/
			rtxtree, rtxchanges, err := indexer.GravDBBackend.StoreTxCount(regTxCount+currRegTxCount, "registration", true)
			//indexer.GravDBBackend.Writing.Store(false)
			if err != nil {
				logger.Errorf("[indexBlock] ERROR - Error storing registration tx count. DB '%v' - this block count '%v' - total '%v'", currRegTxCount, regTxCount, regTxCount+currRegTxCount)
				indexer.GravDBBackend.Writing.Store(false)
				return err
			} else {
				if rtxchanges {
					ctrees = append(ctrees, rtxtree)
				}
			}
		}

		if burnTxCount > 0 && !indexer.FastSyncConfig.Enabled {
			// Load from mem existing burnTxCount and append new value
			currBurnTxCount := indexer.GravDBBackend.GetTxCount("burn")
			/*
				writeWait, _ := time.ParseDuration("50ms")
				for indexer.GravDBBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-indexBlock-burnTxCount] GravitonDB is writing... sleeping for %v...", writeWait)
					time.Sleep(writeWait)
				}
				indexer.GravDBBackend.Writing.Store(true)
			*/
			btxtree, btxchanges, err := indexer.GravDBBackend.StoreTxCount(burnTxCount+currBurnTxCount, "burn", true)
			if err != nil {
				logger.Errorf("[indexBlock] ERROR - Error storing burn tx count. DB '%v' - this block count '%v' - total '%v'", currBurnTxCount, burnTxCount, regTxCount+currBurnTxCount)
				indexer.GravDBBackend.Writing.Store(false)
				return err
			} else {
				if btxchanges {
					ctrees = append(ctrees, btxtree)
				}
			}
			//indexer.GravDBBackend.Writing.Store(false)
		}

		if normTxCount > 0 && !indexer.FastSyncConfig.Enabled {
			/*
				// Test code for finding highest tps block
				var io rpc.GetBlockHeaderByHeight_Result
				var ip = rpc.GetBlockHeaderByTopoHeight_Params{TopoHeight: bl.Height - 1}

				if err = client.RPC.CallResult(context.Background(), "DERO.GetBlockHeaderByTopoHeight", ip, &io); err != nil {
					logger.Errorf("[getBlockHash] GetBlockHeaderByTopoHeight failed: %v", err)
					return err
				} else {
					//logger.Debugf("[getBlockHash] Retrieved block header from topoheight %v", height)
					//mainnet = !info.Testnet // inverse of testnet is mainnet
					//logger.Debugf("%v", io)
				}

				blid := io.Block_Header.Hash

				var io2 rpc.GetBlock_Result
				var ip2 = rpc.GetBlock_Params{Hash: blid}

				if err = client.RPC.CallResult(context.Background(), "DERO.GetBlock", ip2, &io2); err != nil {
					logger.Errorf("[indexBlock] ERROR - GetBlock failed: %v", err)
					return err
				}

				var bl2 block.Block
				var block_bin2 []byte

				block_bin2, _ = hex.DecodeString(io2.Blob)
				bl2.Deserialize(block_bin2)

				prevtimestamp := bl2.Timestamp

				// Load from mem existing normTxCount and append new value
				currNormTxCount := Graviton_backend.GetTxCount("normal")

				//logger.Debugf("%v / (%v - %v)", normTxCount, int64(bl.Timestamp), int64(prevtimestamp))
				tps := normTxCount / ((int64(bl.Timestamp) - int64(prevtimestamp)) / 1000)

				//err := Graviton_backend.StoreTxCount(normTxCount+currNormTxCount, "normal")
				if tps > currNormTxCount {
					err := Graviton_backend.StoreTxCount(tps, "normal")
					if err != nil {
						logger.Errorf("ERROR - Error storing normal tx count. DB '%v' - this block count '%v' - total '%v'", currNormTxCount, tps, regTxCount+currNormTxCount)
					}

					err = Graviton_backend.StoreTxCount(blheight, "registration")
					if err != nil {
						logger.Errorf("ERROR - Error storing registration tx count. DB '%v' - this block count '%v' - total '%v'", currNormTxCount, normTxCount, regTxCount+currNormTxCount)
					}

					err = Graviton_backend.StoreTxCount((int64(bl.Timestamp) - int64(prevtimestamp)), "burn")
					if err != nil {
						logger.Errorf("ERROR - Error storing registration tx count. DB '%v' - this block count '%v' - total '%v'", currNormTxCount, normTxCount, regTxCount+currNormTxCount)
					}
				}
			*/

			// Load from mem existing normTxCount and append new value
			currNormTxCount := indexer.GravDBBackend.GetTxCount("normal")
			/*
				writeWait, _ := time.ParseDuration("50ms")
				for indexer.GravDBBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-indexBlock-normTxCount] GravitonDB is writing... sleeping for %v...", writeWait)
					time.Sleep(writeWait)
				}
				indexer.GravDBBackend.Writing.Store(true)
			*/
			ntxtree, ntxchanges, err := indexer.GravDBBackend.StoreTxCount(normTxCount+currNormTxCount, "normal", true)
			if err != nil {
				logger.Errorf("[indexBlock] ERROR - Error storing normal tx count. DB '%v' - this block count '%v' - total '%v'", currNormTxCount, currNormTxCount, normTxCount+currNormTxCount)
				indexer.GravDBBackend.Writing.Store(false)
				return err
			} else {
				if ntxchanges {
					ctrees = append(ctrees, ntxtree)
				}
			}
		}
		if len(ctrees) > 0 {
			_, err := indexer.GravDBBackend.CommitTrees(ctrees)
			if err != nil {
				logger.Errorf("[indexBlock-indexTxCounts] ERR - committing trees: %v", err)
			} else {
				//logger.Debugf("[indexBlock-installsc] DEBUG - cv [%v]", cv)
			}
		}
		indexer.GravDBBackend.Writing.Store(false)
	case "boltdb":
		for indexer.BBSBackend.Writing.Load() {
			if indexer.Closing.Load() {
				return
			}
			//logger.Debugf("[Indexer-IndexTxCounts] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
			time.Sleep(writeWait)
		}
		indexer.BBSBackend.Writing.Store(true)
		//indexer.BBSBackend.Writer = "IndexTxCounts"
		if regTxCount > 0 && !indexer.FastSyncConfig.Enabled {
			// Load from mem existing regTxCount and append new value
			currRegTxCount := indexer.BBSBackend.GetTxCount("registration")
			_, err := indexer.BBSBackend.StoreTxCount(regTxCount+currRegTxCount, "registration")
			if err != nil {
				logger.Errorf("[indexBlock] ERROR - Error storing registration tx count. DB '%v' - this block count '%v' - total '%v'", currRegTxCount, regTxCount, regTxCount+currRegTxCount)
				indexer.BBSBackend.Writing.Store(false)
				//indexer.BBSBackend.Writer = ""
				return err
			}
		}

		if burnTxCount > 0 && !indexer.FastSyncConfig.Enabled {
			// Load from mem existing burnTxCount and append new value
			currBurnTxCount := indexer.BBSBackend.GetTxCount("burn")
			_, err := indexer.BBSBackend.StoreTxCount(burnTxCount+currBurnTxCount, "burn")
			if err != nil {
				logger.Errorf("[indexBlock] ERROR - Error storing burn tx count. DB '%v' - this block count '%v' - total '%v'", currBurnTxCount, burnTxCount, regTxCount+currBurnTxCount)
				indexer.BBSBackend.Writing.Store(false)
				//indexer.BBSBackend.Writer = ""
				return err
			}
		}

		if normTxCount > 0 && !indexer.FastSyncConfig.Enabled {
			// Load from mem existing normTxCount and append new value
			currNormTxCount := indexer.BBSBackend.GetTxCount("normal")
			_, err := indexer.BBSBackend.StoreTxCount(normTxCount+currNormTxCount, "normal")
			if err != nil {
				logger.Errorf("[indexBlock] ERROR - Error storing normal tx count. DB '%v' - this block count '%v' - total '%v'", currNormTxCount, currNormTxCount, normTxCount+currNormTxCount)
				indexer.BBSBackend.Writing.Store(false)
				//indexer.BBSBackend.Writer = ""
				return err
			}
		}
		indexer.BBSBackend.Writing.Store(false)
		//indexer.BBSBackend.Writer = ""
	}

	return nil
}

func (indexer *Indexer) indexInvokes(bl_sctxs []structures.SCTXParse, bl_txns *structures.BlockTxns) (err error) {

	if indexer.Closing.Load() {
		return
	}

	if len(bl_sctxs) > 0 {
		//logger.Debugf("Block %v has %v SC tx(s).", bl.GetHash(), len(bl_sctxs))

		// TODO: Go routine possible for pre-storage components given the number of 'potential' getscvar calls that may be required.. could speed up indexing some more.
		for i := 0; i < len(bl_sctxs); i++ {
			// Go ahead and skip any in sfscidexclusion ahead of looking at method. Doesn't matter as we won't store it at all.
			if scidExist(indexer.SFSCIDExclusion, bl_sctxs[i].Scid) {
				logger.Debugf("[indexInvokes] Not appending invoke data SCID '%s' as it resides within SFSCIDExclusion - '%v'.", bl_sctxs[i].Scid, indexer.SFSCIDExclusion)
				continue
			}

			if bl_sctxs[i].Method == "installsc" {
				var contains bool

				code := fmt.Sprintf("%v", bl_sctxs[i].Sc_args.Value("SC_CODE", "S"))

				// Temporary check - will need something more robust to code compare potentially all except InitializePrivate() with a given template file or other filter inputs.
				//contains := strings.Contains(code, "200 STORE(\"somevar\", 1)")
				if len(indexer.SearchFilter) == 0 {
					contains = true
				} else {
					for _, sfv := range indexer.SearchFilter {
						contains = strings.Contains(code, sfv)
						if contains {
							// Break b/c we want to ensure contains remains true. Only care if it matches at least 1 case
							break
						}
					}
				}

				if !contains {
					// Then reject the validation that this is an installsc action and move on
					logger.Debugf("[indexInvokes-installsc] SCID %v does not contain the search filter string, moving on.", bl_sctxs[i].Scid)
				} else {
					// Gets the SC variables (key/value) at a given topoheight and then stores them
					scVars, _, _, _ := indexer.RPC.GetSCVariables(bl_sctxs[i].Scid, bl_txns.Topoheight, nil, nil, nil, false)

					if len(scVars) > 0 {
						// Append into db for validated SC
						logger.Debugf("[indexInvokes-installsc] SCID matches search filter. Adding SCID %v / Signer %v", bl_sctxs[i].Scid, bl_sctxs[i].Sender)
						indexer.Lock()
						indexer.ValidatedSCs = append(indexer.ValidatedSCs, bl_sctxs[i].Scid)
						indexer.Unlock()

						writeWait, _ := time.ParseDuration("20ms")
						switch indexer.DBType {
						case "gravdb":
							for indexer.GravDBBackend.Writing.Load() {
								if indexer.Closing.Load() {
									return
								}
								//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
								time.Sleep(writeWait)
							}
							indexer.GravDBBackend.Writing.Store(true)
							var ctrees []*graviton.Tree
							sotree, sochanges, err := indexer.GravDBBackend.StoreOwner(bl_sctxs[i].Scid, bl_sctxs[i].Sender, true)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] Error storing owner: %v", err)
							} else {
								if sochanges {
									ctrees = append(ctrees, sotree)
								}
							}

							shtree, shchanges, err := indexer.GravDBBackend.StoreInstallHeight(bl_sctxs[i].Scid, bl_sctxs[i].Height, true)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] Error storing install height: %v", err)
							} else {
								if shchanges {
									ctrees = append(ctrees, shtree)
								}
							}

							sidtree, sidchanges, err := indexer.GravDBBackend.StoreInvokeDetails(bl_sctxs[i].Scid, bl_sctxs[i].Sender, bl_sctxs[i].Entrypoint, bl_txns.Topoheight, &bl_sctxs[i], true)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] Err storing invoke details. Err: %v", err)
								time.Sleep(5 * time.Second)
								return err
							} else {
								if sidchanges {
									ctrees = append(ctrees, sidtree)
								}
							}

							svdtree, svdchanges, err := indexer.GravDBBackend.StoreSCIDVariableDetails(bl_sctxs[i].Scid, scVars, bl_txns.Topoheight, true)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] ERR - storing scid variable details: %v", err)
							} else {
								if svdchanges {
									ctrees = append(ctrees, svdtree)
								}
							}
							sihtree, sihchanges, err := indexer.GravDBBackend.StoreSCIDInteractionHeight(bl_sctxs[i].Scid, bl_txns.Topoheight, true)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] ERR - storing scid interaction height: %v", err)
							} else {
								if sihchanges {
									ctrees = append(ctrees, sihtree)
								}
							}
							if len(ctrees) > 0 {
								_, err := indexer.GravDBBackend.CommitTrees(ctrees)
								if err != nil {
									logger.Errorf("[indexInvokes-installsc] ERR - committing trees: %v", err)
								} else {
									//logger.Debugf("[indexInvokes-installsc] DEBUG - cv [%v]", cv)
								}
							}
							indexer.GravDBBackend.Writing.Store(false)
						case "boltdb":
							for indexer.BBSBackend.Writing.Load() {
								if indexer.Closing.Load() {
									return
								}
								//logger.Debugf("[Indexer-IndexInvokes] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
								time.Sleep(writeWait)
							}
							indexer.BBSBackend.Writing.Store(true)
							//indexer.BBSBackend.Writer = "IndexInvokes"

							_, err := indexer.BBSBackend.StoreOwner(bl_sctxs[i].Scid, bl_sctxs[i].Sender)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] Error storing owner: %v", err)
							}

							_, err = indexer.BBSBackend.StoreInstallHeight(bl_sctxs[i].Scid, bl_sctxs[i].Height)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] Error storing install height: %v", err)
							}

							_, err = indexer.BBSBackend.StoreInvokeDetails(bl_sctxs[i].Scid, bl_sctxs[i].Sender, bl_sctxs[i].Entrypoint, bl_txns.Topoheight, &bl_sctxs[i])
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] Err storing invoke details. Err: %v", err)
								time.Sleep(5 * time.Second)
								return err
							}

							_, err = indexer.BBSBackend.StoreSCIDVariableDetails(bl_sctxs[i].Scid, scVars, bl_txns.Topoheight)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] ERR - storing scid variable details: %v", err)
							}
							_, err = indexer.BBSBackend.StoreSCIDInteractionHeight(bl_sctxs[i].Scid, bl_txns.Topoheight)
							if err != nil {
								logger.Errorf("[indexInvokes-installsc] ERR - storing scid interaction height: %v", err)
							}
							indexer.BBSBackend.Writing.Store(false)
							//indexer.BBSBackend.Writer = ""
						}

						//logger.Debugf("[IndexInvokes] SCID: %v ; Sender: %v ; Entrypoint: %v ; topoheight : %v ; info: %v", bl_sctxs[i].Scid, bl_sctxs[i].Sender, bl_sctxs[i].Entrypoint, topoheight, &bl_sctxs[i])
						logger.Debugf("[IndexInvokes] Sender: %v ; topoheight : %v ; args: %v ; burnValue: %v", bl_sctxs[i].Sender, bl_txns.Topoheight, bl_sctxs[i].Sc_args, bl_sctxs[i].Payloads[0].BurnValue)
					} else {
						logger.Debugf("[indexInvokes-installsc] SCID '%v' appears to be invalid.", bl_sctxs[i].Scid)
						writeWait, _ := time.ParseDuration("20ms")
						switch indexer.DBType {
						case "gravdb":
							if !(indexer.RunMode == "asset") {
								for indexer.GravDBBackend.Writing.Load() {
									if indexer.Closing.Load() {
										return
									}
									//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
									time.Sleep(writeWait)
								}
								indexer.GravDBBackend.Writing.Store(true)
								indexer.GravDBBackend.StoreInvalidSCIDDeploys(bl_sctxs[i].Scid, bl_sctxs[i].Fees, false)
								indexer.GravDBBackend.Writing.Store(false)
							}
						case "boltdb":
							if !(indexer.RunMode == "asset") {
								for indexer.BBSBackend.Writing.Load() {
									if indexer.Closing.Load() {
										return
									}
									//logger.Debugf("[Indexer-IndexInvokes-StoreInvalidSCIDDeploys] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
									time.Sleep(writeWait)
								}
								indexer.BBSBackend.Writing.Store(true)
								//indexer.BBSBackend.Writer = "IndexInvokes"
								indexer.BBSBackend.StoreInvalidSCIDDeploys(bl_sctxs[i].Scid, bl_sctxs[i].Fees)
								indexer.BBSBackend.Writing.Store(false)
								//indexer.BBSBackend.Writer = ""
							}
						}
					}
				}
			} else {
				if !scidExist(indexer.ValidatedSCs, bl_sctxs[i].Scid) {

					// Validate SCID is *actually* a valid SCID
					// This assumes we can return all variables.
					// TODO: For daemon v139 should we append []string "C" to lookup key C to confirm if >1024 k/v pairs exist?
					valVars, _, _, _ := indexer.RPC.GetSCVariables(bl_sctxs[i].Scid, bl_txns.Topoheight, nil, nil, nil, false)

					// By returning valid variables of a given Scid (GetSC --> parse vars), we can conclude it is a valid SCID. Otherwise, skip adding to validated scids
					if len(valVars) > 0 {
						logger.Debugf("[indexBlock] SCID matches search filter. Adding SCID %v / Signer %v", bl_sctxs[i].Scid, "")
						indexer.Lock()
						indexer.ValidatedSCs = append(indexer.ValidatedSCs, bl_sctxs[i].Scid)
						indexer.Unlock()

						writeWait, _ := time.ParseDuration("20ms")
						switch indexer.DBType {
						case "gravdb":
							for indexer.GravDBBackend.Writing.Load() {
								if indexer.Closing.Load() {
									return
								}
								//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
								time.Sleep(writeWait)
							}
							var ctrees []*graviton.Tree
							indexer.GravDBBackend.Writing.Store(true)
							sotree, sochanges, err := indexer.GravDBBackend.StoreOwner(bl_sctxs[i].Scid, "", true)
							if err != nil {
								logger.Errorf("[indexInvokes] Error storing owner: %v", err)
							} else {
								if sochanges {
									ctrees = append(ctrees, sotree)
								}
							}

							shtree, shchanges, err := indexer.GravDBBackend.StoreInstallHeight(bl_sctxs[i].Scid, bl_sctxs[i].Height, true)
							if err != nil {
								logger.Errorf("[indexInvokes] Error storing install height: %v", err)
							} else {
								if shchanges {
									ctrees = append(ctrees, shtree)
								}
							}

							if len(ctrees) > 0 {
								_, err := indexer.GravDBBackend.CommitTrees(ctrees)
								if err != nil {
									logger.Errorf("[indexInvokes-installsc] ERR - committing trees: %v", err)
								} else {
									//logger.Debugf("[indexInvokes-installsc] DEBUG - cv [%v]", cv)
								}
							}
							indexer.GravDBBackend.Writing.Store(false)
						case "boltdb":
							for indexer.BBSBackend.Writing.Load() {
								if indexer.Closing.Load() {
									return
								}
								//logger.Debugf("[Indexer-IndexInvokes-StoreOwner] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
								time.Sleep(writeWait)
							}
							indexer.BBSBackend.Writing.Store(true)
							//indexer.BBSBackend.Writer = "IndexInvokesOwnerStore"

							_, err = indexer.BBSBackend.StoreOwner(bl_sctxs[i].Scid, "")
							if err != nil {
								logger.Errorf("[indexInvokes] Error storing owner: %v", err)
							}

							_, err = indexer.BBSBackend.StoreInstallHeight(bl_sctxs[i].Scid, bl_sctxs[i].Height)
							if err != nil {
								logger.Errorf("[indexInvokes] Error storing install height: %v", err)
							}

							indexer.BBSBackend.Writing.Store(false)
							//indexer.BBSBackend.Writer = ""
						}
					}
				}

				if scidExist(indexer.ValidatedSCs, bl_sctxs[i].Scid) {
					//logger.Debugf("SCID %v is validated, checking the SC TX entrypoints to see if they should be logged.", bl_sctxs[i].Scid)
					// TODO: Modify this to be either all entrypoints, just Start, or a subset that is defined in pre-run params or not needed?
					//if bl_sctxs[i].entrypoint == "Start" {
					//if bl_sctxs[i].Entrypoint == "InputStr" {
					if true {
						currsctx := bl_sctxs[i]

						//logger.Debugf("Tx %v matches scinvoke call filter(s). Adding %v to DB.", bl_sctxs[i].Txid, currsctx)

						writeWait, _ := time.ParseDuration("20ms")
						switch indexer.DBType {
						case "gravdb":
							if !(indexer.RunMode == "asset") {
								// We can pre-get the relevant scvar details outside a write block due to daemon lookup and nothing relevant to db stores
								var scVarsDiff []*structures.SCIDVariable
								var scVars []*structures.SCIDVariable
								var scCode string

								// v2.0.2-alpha.6 - Removing this as it is no longer needed to be skipped due to the scidexclusion list being supported
								// If a hardcodedscid invoke + fastsync is enabled, do not log any new details. We will only retain within DB on-launch data.
								/*
									if scidExist(structures.Hardcoded_SCIDS, bl_sctxs[i].Scid) && indexer.FastSyncConfig.Enabled {
										logger.Debugf("[indexInvokes] Skipping invoke detail store of '%v' since fastsync is '%v'.", bl_sctxs[i].Scid, indexer.FastSyncConfig.Enabled)
										return
									} else {
								*/
								// Gets the SC variables (key/value) at a given topoheight -1 and then will compare differences to executed height and store the diffs
								scVarsDiff = indexer.GravDBBackend.GetAllSCIDVariableDetails(bl_sctxs[i].Scid)

								// Gets the SC variables (key/value) at a given topoheight
								scVars, scCode, _, _ = indexer.RPC.GetSCVariables(bl_sctxs[i].Scid, bl_txns.Topoheight, nil, nil, nil, false)
								//}

								for indexer.GravDBBackend.Writing.Load() {
									if indexer.Closing.Load() {
										return
									}
									//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
									time.Sleep(writeWait)
								}
								indexer.GravDBBackend.Writing.Store(true)
								var ctrees []*graviton.Tree

								sidtree, sidchanges, err := indexer.GravDBBackend.StoreInvokeDetails(bl_sctxs[i].Scid, bl_sctxs[i].Sender, bl_sctxs[i].Entrypoint, bl_txns.Topoheight, &currsctx, true)
								if err != nil {
									logger.Errorf("[indexInvokes] Err storing invoke details. Err: %v", err)
									time.Sleep(5 * time.Second)
									indexer.GravDBBackend.Writing.Store(false)
									return err
								} else {
									if sidchanges {
										ctrees = append(ctrees, sidtree)
									}
								}

								indexer.InterpretSC(bl_sctxs[i].Scid, scCode)
								scVarsStore, err := indexer.DiffSCIDVariables(scVarsDiff, scVars, bl_sctxs[i].Scid, bl_txns.Topoheight)
								if err != nil {
									// This could be flagged as 'err' if say there were no variables to begin with and still. Is that necessary?
									logger.Errorf("[indexInvokes-installsc] ERR - %v", err)
								} else if len(scVarsStore) > 0 {
									// If scVarsStore length is greater than 0, we can assume there were diffs. Otherwise the varstores are equal and move on.
									svdtree, svdchanges, err := indexer.GravDBBackend.StoreSCIDVariableDetails(bl_sctxs[i].Scid, scVarsStore, bl_txns.Topoheight, true)
									if err != nil {
										logger.Errorf("[indexInvokes-installsc] ERR - storing scid variable details: %v", err)
									} else {
										if svdchanges {
											ctrees = append(ctrees, svdtree)
										}
									}
								}
								sihtree, sihchanges, err := indexer.GravDBBackend.StoreSCIDInteractionHeight(bl_sctxs[i].Scid, bl_txns.Topoheight, true)
								if err != nil {
									logger.Errorf("[indexInvokes] ERR - storing scid interaction height: %v", err)
								} else {
									if sihchanges {
										ctrees = append(ctrees, sihtree)
									}
								}

								if len(ctrees) > 0 {
									_, err := indexer.GravDBBackend.CommitTrees(ctrees)
									if err != nil {
										logger.Errorf("[indexInvokes] ERR - committing trees: %v", err)
									} else {
										//logger.Debugf("[indexInvokes] DEBUG - cv [%v]", cv)
									}
								}
								indexer.GravDBBackend.Writing.Store(false)
							}
						case "boltdb":
							if !(indexer.RunMode == "asset") {
								// We can pre-get the relevant scvar details outside a write block due to daemon lookup and nothing relevant to db stores
								var scVarsDiff []*structures.SCIDVariable
								//var scVarsDiff2 []*structures.SCIDVariable
								var scVars []*structures.SCIDVariable
								var scCode string

								// v2.0.2-alpha.6 - Removing this as it is no longer needed to be skipped due to the scidexclusion list being supported
								// If a hardcodedscid invoke + fastsync is enabled, do not log any new details. We will only retain within DB on-launch data.
								/*
									if scidExist(structures.Hardcoded_SCIDS, bl_sctxs[i].Scid) && indexer.FastSyncConfig.Enabled {
										logger.Debugf("[indexInvokes] Skipping invoke detail store of '%v' since fastsync is '%v'.", bl_sctxs[i].Scid, indexer.FastSyncConfig.Enabled)
										return
									} else {
								*/
								// Gets the SC variables (key/value) at a given topoheight -1 and then will compare differences to executed height and store the diffs
								scVarsDiff = indexer.BBSBackend.GetAllSCIDVariableDetails(bl_sctxs[i].Scid)

								// Gets the SC variables (key/value) at a given topoheight
								scVars, scCode, _, _ = indexer.RPC.GetSCVariables(bl_sctxs[i].Scid, bl_txns.Topoheight, nil, nil, nil, false)
								//}

								for indexer.BBSBackend.Writing.Load() {
									if indexer.Closing.Load() {
										return
									}
									//logger.Debugf("[Indexer-IndexInvokes] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
									time.Sleep(writeWait)
								}
								indexer.BBSBackend.Writing.Store(true)
								//indexer.BBSBackend.Writer = "IndexInvokesDetailsStore"

								_, err := indexer.BBSBackend.StoreInvokeDetails(bl_sctxs[i].Scid, bl_sctxs[i].Sender, bl_sctxs[i].Entrypoint, bl_txns.Topoheight, &currsctx)
								if err != nil {
									logger.Errorf("[indexInvokes] Err storing invoke details. Err: %v", err)
									time.Sleep(5 * time.Second)
									indexer.BBSBackend.Writing.Store(false)
									//indexer.BBSBackend.Writer = ""
									return err
								}
								indexer.InterpretSC(bl_sctxs[i].Scid, scCode)
								scVarsStore, err := indexer.DiffSCIDVariables(scVarsDiff, scVars, bl_sctxs[i].Scid, bl_txns.Topoheight)
								if err != nil {
									// This could be flagged as 'err' if say there were no variables to begin with and still. Is that necessary?
									logger.Errorf("[indexInvokes-installsc] ERR - %v", err)
								} else if len(scVarsStore) > 0 {
									// If scVarsStore length is greater than 0, we can assume there were diffs. Otherwise the varstores are equal and move on.
									_, err = indexer.BBSBackend.StoreSCIDVariableDetails(bl_sctxs[i].Scid, scVarsStore, bl_txns.Topoheight)
									if err != nil {
										logger.Errorf("[indexInvokes] ERR - storing scid variable details: %v", err)
									}
								}
								_, err = indexer.BBSBackend.StoreSCIDInteractionHeight(bl_sctxs[i].Scid, bl_txns.Topoheight)
								if err != nil {
									logger.Errorf("[indexInvokes] ERR - storing scid interaction height: %v", err)
								}

								indexer.BBSBackend.Writing.Store(false)
								//indexer.BBSBackend.Writer = ""
							}
						}

						//logger.Debugf("[IndexInvokes] SCID: %v ; Sender: %v ; Entrypoint: %v ; topoheight : %v ; info: %v", bl_sctxs[i].Scid, bl_sctxs[i].Sender, bl_sctxs[i].Entrypoint, topoheight, &currsctx)
						logger.Debugf("[IndexInvokes] Sender: %v ; topoheight : %v ; args: %v ; burnValue: %v", bl_sctxs[i].Sender, bl_txns.Topoheight, bl_sctxs[i].Sc_args, bl_sctxs[i].Payloads[0].BurnValue)
					} else {
						//logger.Debugf("Tx %v does not match scinvoke call filter(s), but %v instead. This should not (currently) be added to DB.", bl_sctxs[i].Txid, bl_sctxs[i].Entrypoint)
					}
				} else {
					logger.Debugf("SCID %v is not validated and thus we do not log SC interactions for this. Moving on. TXID: %s", bl_sctxs[i].Scid, bl_sctxs[i].Txid)
				}
			}
		}
	} else {
		//logger.Debugf("Block %v does not have any SC txs", bl.GetHash())
	}

	return nil
}

// Looped interval to probe DERO.GetInfo rpc call for updating chain topoheight. Also handles keeping connection to daemon via RPC.Connect() calls
func (indexer *Indexer) getInfo() {
	defer func() {
		if r := recover(); r != nil {
			logger.Fatalf("[getInfo] PANIC recovered: %v", r)
			indexer.Closing.Store(true)
		}
	}()

	var reconnect_count int
	for {
		if indexer.Closing.Load() {
			// Break out on closing call
			break
		}
		var err error

		// Check connection to be sure indexer.Endpoint hasn't changed. If it has, then update. Otherwise Connect will just return back no issues
		indexer.RPC.Connect(indexer.Endpoint)

		var info *structures.GetInfo

		// collect all the data afresh,  execute rpc to service
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = indexer.RPC.RPC.CallResult(ctx, "DERO.GetInfo", nil, &info)
		cancel()
		if err != nil {
			logger.Debugf("[getInfo] ERROR - GetInfo failed: %v . Trying again (%v / 5)", err, reconnect_count)

			// TODO: Perhaps just a .Closing = true call here and then gnomonserver can be polling for any indexers with .Closing then close the rest cleanly. If packaged, then just have to handle themselves w/ .Close()
			if reconnect_count >= 5 && indexer.CloseOnDisconnect {
				indexer.Close()
				logger.Errorf("[getInfo] ERROR - GetInfo failed: %v . (%v / 5 times)", err, reconnect_count)
				break
			}
			time.Sleep(time.Duration(1<<reconnect_count) * time.Second) // exponential backoff
			indexer.RPC.Connect(indexer.Endpoint)                       // Attempt to re-connect now

			reconnect_count++

			continue
		} else {
			if reconnect_count > 0 {
				reconnect_count = 0
			}
			//mainnet = !info.Testnet // inverse of testnet is mainnet
			//logger.Debugf("%v", info)
		}

		var currStoreGetInfo *structures.GetInfo
		switch indexer.DBType {
		case "gravdb":
			currStoreGetInfo = indexer.GravDBBackend.GetGetInfoDetails()
		case "boltdb":
			currStoreGetInfo = indexer.BBSBackend.GetGetInfoDetails()
		}

		if currStoreGetInfo != nil {
			// Ensure you are not connecting to testnet or mainnet unintentionally based on store getinfo history
			if currStoreGetInfo.Testnet == info.Testnet {
				if currStoreGetInfo.Height < info.Height {
					structureGetInfo := info

					writeWait, _ := time.ParseDuration("20ms")
					switch indexer.DBType {
					case "gravdb":
						for indexer.GravDBBackend.Writing.Load() {
							if indexer.Closing.Load() {
								return
							}
							//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
							time.Sleep(writeWait)
						}
						indexer.GravDBBackend.Writing.Store(true)
						_, _, err := indexer.GravDBBackend.StoreGetInfoDetails(structureGetInfo, false)
						if err != nil {
							logger.Errorf("[getInfo] ERROR - GetInfo store failed: %v", err)
						}
						indexer.GravDBBackend.Writing.Store(false)
					case "boltdb":
						for indexer.BBSBackend.Writing.Load() {
							if indexer.Closing.Load() {
								return
							}
							//logger.Debugf("[Indexer-getinfo-StoreGetInfoDetails] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
							time.Sleep(writeWait)
						}
						indexer.BBSBackend.Writing.Store(true)
						//indexer.BBSBackend.Writer = "getInfo"
						_, err := indexer.BBSBackend.StoreGetInfoDetails(structureGetInfo)
						if err != nil {
							logger.Errorf("[getInfo] ERROR - GetInfo store failed: %v", err)
						}
						indexer.BBSBackend.Writing.Store(false)
						//indexer.BBSBackend.Writer = ""
					}
				}
			} else {
				if indexer.RPC.WS != nil {
					// Remote addr (current ws connection endpoint) does not match indexer endpoint - re-connecting
					logger.Errorf("[getInfo] ERROR - Endpoint network (testnet - %v) is not the same as past stored network (testnet - %v)", info.Testnet, currStoreGetInfo.Testnet)
					indexer.RPC.Lock()
					indexer.RPC.WS.Close()
					indexer.RPC.Unlock()

					indexer.Lock()
					indexer.ChainHeight = 0
					indexer.Unlock()

					time.Sleep(5 * time.Second)
					continue
				}
			}
		} else {
			structureGetInfo := info

			writeWait, _ := time.ParseDuration("20ms")
			switch indexer.DBType {
			case "gravdb":
				for indexer.GravDBBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
					time.Sleep(writeWait)
				}
				indexer.GravDBBackend.Writing.Store(true)
				_, _, err := indexer.GravDBBackend.StoreGetInfoDetails(structureGetInfo, false)
				if err != nil {
					logger.Errorf("[getInfo] ERROR - GetInfo store failed: %v", err)
				}
				indexer.GravDBBackend.Writing.Store(false)
			case "boltdb":
				for indexer.BBSBackend.Writing.Load() {
					if indexer.Closing.Load() {
						return
					}
					//logger.Debugf("[Indexer-getinfo-StoreGetInfoDetails] BoltDB is writing... sleeping for %v... writer %v...", writeWait, indexer.BBSBackend.Writer)
					time.Sleep(writeWait)
				}
				indexer.BBSBackend.Writing.Store(true)
				//indexer.BBSBackend.Writer = "getInfo"
				_, err := indexer.BBSBackend.StoreGetInfoDetails(structureGetInfo)
				if err != nil {
					logger.Errorf("[getInfo] ERROR - GetInfo store failed: %v", err)
				}
				indexer.BBSBackend.Writing.Store(false)
				//indexer.BBSBackend.Writer = ""
			}
		}
		indexer.Lock()
		indexer.ChainHeight = info.TopoHeight
		indexer.Unlock()

		time.Sleep(5 * time.Second)
	}
}

// Looped interval to probe WALLET.GetHeight rpc call for updating wallet height
func (indexer *Indexer) getWalletHeight() {
	defer func() {
		if r := recover(); r != nil {
			logger.Fatalf("[getWalletHeight] PANIC recovered: %v", r)
			indexer.Closing.Store(true)
		}
	}()

	for {
		if indexer.Closing.Load() {
			// Break out on closing call
			break
		}
		var err error

		var info rpc.GetHeight_Result

		// collect all the data afresh,  execute rpc to service
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = indexer.RPC.RPC.CallResult(ctx, "WALLET.GetHeight", nil, &info)
		cancel()
		if err != nil {
			logger.Errorf("[getWalletHeight] ERROR - GetHeight failed: %v", err)
			time.Sleep(1 * time.Second)
			indexer.RPC.Connect(indexer.Endpoint) // Attempt to re-connect now
			continue
		} else {
			//mainnet = !info.Testnet // inverse of testnet is mainnet
			//logger.Debugf("%v", info)
		}

		indexer.Lock()
		indexer.ChainHeight = int64(info.Height)
		indexer.Unlock()

		time.Sleep(5 * time.Second)
	}
}

// Gets SC variable keys at given topoheight who's value equates to a given interface{} (string/uint64)
func (indexer *Indexer) GetSCIDKeysByValue(variables []*structures.SCIDVariable, scid string, val interface{}, height int64) (keysstring []string, keysuint64 []uint64, err error) {
	// If variables were not provided, then fetch them.
	if len(variables) <= 0 {
		// Can't pass the val interface{} as the input params for getsc only are in reference to key lookup or all variables return (edge in event of v139 daemon)
		variables, _, _, err = indexer.RPC.GetSCVariables(scid, height, nil, nil, nil, false)
		if err != nil {
			logger.Errorf("[GetSCIDKeysByValue] ERROR during GetSCVariables - %v", err)
			return
		}
	}

	// Switch against the value passed. If it's a uint64 or string
	switch inpvar := val.(type) {
	case uint64:
		for _, v := range variables {
			switch cval := v.Value.(type) {
			case uint64:
				if inpvar == cval {
					switch ckey := v.Key.(type) {
					case float64:
						keysuint64 = append(keysuint64, uint64(ckey))
					case uint64:
						keysuint64 = append(keysuint64, ckey)
					default:
						// default just store as string. Keys should only ever be strings or uint64, however, but assume default to string
						keysstring = append(keysstring, v.Key.(string))
					}
				}
			default:
				// Nothing - expect only string/uint64 for value types
			}
		}
	case string:
		for _, v := range variables {
			switch cval := v.Value.(type) {
			case string:
				if inpvar == cval {
					switch ckey := v.Key.(type) {
					case float64:
						keysuint64 = append(keysuint64, uint64(ckey))
					case uint64:
						keysuint64 = append(keysuint64, ckey)
					default:
						// default just store as string. Keys should only ever be strings or uint64, however, but assume default to string
						keysstring = append(keysstring, v.Key.(string))
					}
				}
			default:
				// Nothing - expect only string/uint64 for value types
			}
		}
	default:
		// Nothing - expect only string/uint64 for value types
	}

	return keysstring, keysuint64, err
}

// Gets SC values by key at given topoheight who's key equates to a given interface{} (string/uint64)
func (indexer *Indexer) GetSCIDValuesByKey(variables []*structures.SCIDVariable, scid string, key interface{}, height int64) (valuesstring []string, valuesuint64 []uint64, err error) {
	// If variables were not provided, then fetch them.
	if len(variables) <= 0 {
		// Can pass the key interface{} in the input param.
		var keysuint64 []uint64
		var keysstring []string
		var keysbytes [][]byte
		switch ta := key.(type) {
		case uint64:
			keysuint64 = append(keysuint64, ta)
		case string:
			keysstring = append(keysstring, ta)
		default:
			// Nothing - expect only string/uint64 for value types
		}

		variables, _, _, err = indexer.RPC.GetSCVariables(scid, height, keysuint64, keysstring, keysbytes, false)
		if err != nil {
			logger.Errorf("[GetSCIDValuesByKey] ERROR during GetSCVariables - %v", err)
			return
		}
	}

	// Switch against the value passed. If it's a uint64 or string
	switch inpvar := key.(type) {
	case uint64:
		for _, v := range variables {
			switch ckey := v.Key.(type) {
			case uint64:
				if inpvar == ckey {
					switch cval := v.Value.(type) {
					case float64:
						valuesuint64 = append(valuesuint64, uint64(cval))
					case uint64:
						valuesuint64 = append(valuesuint64, cval)
					default:
						// default just store as string. Keys should only ever be strings or uint64, however, but assume default to string
						valuesstring = append(valuesstring, v.Value.(string))
					}
				}
			default:
				// Nothing - expect only string/uint64 for value types
			}
		}
	case string:
		for _, v := range variables {
			switch ckey := v.Key.(type) {
			case string:
				if inpvar == ckey {
					switch cval := v.Value.(type) {
					case float64:
						valuesuint64 = append(valuesuint64, uint64(cval))
					case uint64:
						valuesuint64 = append(valuesuint64, cval)
					default:
						// default just store as string. Values should only ever be strings or uint64, however, but assume default to string
						valuesstring = append(valuesstring, v.Value.(string))
					}
				}
			default:
				// Nothing - expect only string/uint64 for value types
			}
		}
	default:
		// Nothing - expect only string/uint64 for value types
	}

	return valuesstring, valuesuint64, err
}

// Converts returned SCIDVariables KEY values who's values equates to a given interface{} (string/uint64)
func (indexer *Indexer) ConvertSCIDKeys(variables []*structures.SCIDVariable) (keysstring []string, keysuint64 []uint64) {
	for _, v := range variables {
		switch ckey := v.Key.(type) {
		case float64:
			keysuint64 = append(keysuint64, uint64(ckey))
		case uint64:
			keysuint64 = append(keysuint64, ckey)
		default:
			// default just store as string. Keys should only ever be strings or uint64, however, but assume default to string
			keysstring = append(keysstring, v.Key.(string))
		}
	}

	return keysstring, keysuint64
}

// Converts returned SCIDVariables VALUE values who's values equates to a given interface{} (string/uint64)
func (indexer *Indexer) ConvertSCIDValues(variables []*structures.SCIDVariable) (valuesstring []string, valuesuint64 []uint64) {
	for _, v := range variables {
		switch cval := v.Value.(type) {
		case float64:
			valuesuint64 = append(valuesuint64, uint64(cval))
		case uint64:
			valuesuint64 = append(valuesuint64, cval)
		default:
			// default just store as string. Keys should only ever be strings or uint64, however, but assume default to string
			valuesstring = append(valuesstring, v.Value.(string))
		}
	}

	return valuesstring, valuesuint64
}

// Compares k/v pairs of two array sets of *structures.SCIDVariable
func (indexer *Indexer) DiffSCIDVariables(varset1 []*structures.SCIDVariable, varset2 []*structures.SCIDVariable, scid string, height int64) (diffset []*structures.SCIDVariable, err error) {
	// We assume varset1 is the past set of variables and varset2 is the current/modified set of variables

	// If variables were not provided, then fetch them.
	if len(varset1) <= 0 && len(varset2) <= 0 {
		return diffset, fmt.Errorf("[DiffSCIDVariables] Input variable set 1 and 2 for scid '%s' have no variables.", scid)
	}

	// DeepEqual check prior to looping the k/v pairs of each varset to check for differences. If we're equal, can quit here and move on.
	if reflect.DeepEqual(varset1, varset2) {
		logger.Debugf("[DiffSCIDVariables] Before and after variables are the same for '%s' at height '%v'. No changes necessary to store.", scid, height)
		return
	}

	// Create RAM gravdb store with two trees then diff said trees
	logger.Debugf("[DiffSCIDVariables] scid: %s - height: %v", scid, height)
	var tempdb *storage.GravitonStore
	tempdb, err = storage.NewGravDBRAM("25ms")
	if err != nil {
		logger.Errorf("[DiffSCIDVariables] Error creating new temp gravdb for diff: %v", err)
		return
	}

	// Handle storing
	vs1kuint64 := make(map[string]bool)
	vs1kstring := make(map[string]bool)

	store := tempdb.DB
	ss, err := store.LoadSnapshot(0) // load most recent snapshot
	if err != nil {
		return
	}

	treename := "varset1"
	tree, _ := ss.GetTree(treename)
	for _, vs1 := range varset1 {
		switch ckey := vs1.Key.(type) {
		case float64:
			uintkey, err := json.Marshal(uint64(ckey))
			if err != nil {
				logger.Errorf("[DiffSCIDVariables] ERR Marshalling key - %v", err)
				break
			}

			switch cval := vs1.Value.(type) {
			case float64:
				uintval, err := json.Marshal(uint64(cval))
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs1kuint64[fmt.Sprintf("%v", uint64(ckey))] = true
			case uint64:
				uintval, err := json.Marshal(cval)
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs1kuint64[fmt.Sprintf("%v", uint64(ckey))] = true
			case string:
				strval := []byte(cval)

				tree.Put(uintkey, strval) // insert a value

				// Add to tracking maps
				vs1kuint64[fmt.Sprintf("%v", uint64(ckey))] = true
			default:
				logger.Errorf("[DiffSCIDVariables] ERR Value doesn't match string or uint64 - %v", cval)
			}
		case uint64:
			uintkey, err := json.Marshal(ckey)
			if err != nil {
				logger.Errorf("[DiffSCIDVariables] ERR Marshalling key - %v", err)
				break
			}

			switch cval := vs1.Value.(type) {
			case float64:
				uintval, err := json.Marshal(uint64(cval))
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs1kuint64[fmt.Sprintf("%v", ckey)] = true
			case uint64:

				uintval, err := json.Marshal(cval)
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs1kuint64[fmt.Sprintf("%v", ckey)] = true
			case string:
				strval := []byte(cval)

				tree.Put(uintkey, strval) // insert a value

				// Add to tracking maps
				vs1kuint64[fmt.Sprintf("%v", ckey)] = true
			default:
				logger.Errorf("[DiffSCIDVariables] ERR Value doesn't match string or uint64 - %v", cval)
			}
		case string:
			strkey := []byte(ckey)

			switch cval := vs1.Value.(type) {
			case float64:
				uintval, err := json.Marshal(uint64(cval))
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree.Put(strkey, uintval) // insert a value

				// Add to tracking maps
				vs1kstring[ckey] = true
			case uint64:

				uintval, err := json.Marshal(cval)
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree.Put(strkey, uintval) // insert a value

				// Add to tracking maps
				vs1kstring[ckey] = true
			case string:
				strval := []byte(cval)

				tree.Put(strkey, strval) // insert a value

				// Add to tracking maps
				vs1kstring[ckey] = true
			default:
				logger.Errorf("[DiffSCIDVariables] ERR Value doesn't match string or uint64 - %v", cval)
			}
		default:
			// Do nothing
			logger.Errorf("[DiffSCIDVariables] ERR Key doesn't match string or uint64 - %v", ckey)
		}
	}
	// End storing varset1

	// Handle storing varset2
	vs2kuu := make(map[uint64]uint64)
	vs2kus := make(map[uint64]string)
	vs2ksu := make(map[string]uint64)
	vs2kss := make(map[string]string)

	vs2kuint64 := make(map[string]bool)
	vs2kstring := make(map[string]bool)
	vs2vuint64 := make(map[string]bool)
	vs2vstring := make(map[string]bool)

	treename2 := "varset2"
	tree2, _ := ss.GetTree(treename2)
	for _, vs2 := range varset2 {
		switch ckey := vs2.Key.(type) {
		case float64:
			uintkey, err := json.Marshal(ckey)
			if err != nil {
				logger.Errorf("[DiffSCIDVariables] ERR Marshalling key - %v", err)
				break
			}

			switch cval := vs2.Value.(type) {
			case float64:
				uintval, err := json.Marshal(uint64(cval))
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree2.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs2kuint64[fmt.Sprintf("%v", uint64(ckey))] = true
				vs2vuint64[fmt.Sprintf("%v", cval)] = true

				vs2kuu[uint64(ckey)] = uint64(cval)
			case uint64:
				uintval, err := json.Marshal(cval)
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree2.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs2kuint64[fmt.Sprintf("%v", uint64(ckey))] = true
				vs2vuint64[fmt.Sprintf("%v", cval)] = true

				vs2kuu[uint64(ckey)] = cval
			case string:
				strval := []byte(cval)

				tree2.Put(uintkey, strval) // insert a value

				// Add to tracking maps
				vs2kuint64[fmt.Sprintf("%v", uint64(ckey))] = true
				vs2vstring[cval] = true

				vs2kus[uint64(ckey)] = cval
			default:
				logger.Errorf("[DiffSCIDVariables] ERR Value doesn't match string or uint64 - %v", cval)
			}
		case uint64:
			uintkey, err := json.Marshal(ckey)
			if err != nil {
				logger.Errorf("[DiffSCIDVariables] ERR Marshalling key - %v", err)
				break
			}

			switch cval := vs2.Value.(type) {
			case float64:
				uintval, err := json.Marshal(uint64(cval))
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree2.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs2kuint64[fmt.Sprintf("%v", ckey)] = true
				vs2vuint64[fmt.Sprintf("%v", cval)] = true

				vs2kuu[ckey] = uint64(cval)
			case uint64:
				uintval, err := json.Marshal(cval)
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree2.Put(uintkey, uintval) // insert a value

				// Add to tracking maps
				vs2kuint64[fmt.Sprintf("%v", ckey)] = true
				vs2vuint64[fmt.Sprintf("%v", cval)] = true

				vs2kuu[ckey] = cval
			case string:
				strval := []byte(cval)

				tree2.Put(uintkey, strval) // insert a value

				// Add to tracking maps
				vs2kuint64[fmt.Sprintf("%v", ckey)] = true
				vs2vstring[cval] = true

				vs2kus[ckey] = cval
			default:
				logger.Errorf("[DiffSCIDVariables] ERR Value doesn't match string or uint64 - %v", cval)
			}
		case string:
			strkey := []byte(ckey)

			switch cval := vs2.Value.(type) {
			case float64:

				uintval, err := json.Marshal(uint64(cval))
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree2.Put(strkey, uintval) // insert a value

				// Add to tracking maps
				vs2kstring[ckey] = true
				vs2vuint64[fmt.Sprintf("%v", cval)] = true
				vs2ksu[ckey] = uint64(cval)
			case uint64:

				uintval, err := json.Marshal(cval)
				if err != nil {
					logger.Errorf("[DiffSCIDVariables] ERR Marshalling value - %v", err)
					break
				}

				tree2.Put(strkey, uintval) // insert a value

				// Add to tracking maps
				vs2kstring[ckey] = true
				vs2vuint64[fmt.Sprintf("%v", cval)] = true
				vs2ksu[ckey] = cval
			case string:
				strval := []byte(cval)

				tree2.Put(strkey, strval) // insert a value

				// Add to tracking maps
				vs2kstring[ckey] = true
				vs2vstring[cval] = true

				vs2kss[ckey] = cval
			default:
				logger.Errorf("[DiffSCIDVariables] ERR Value doesn't match string or uint64 - %v", cval)
			}
		default:
			// Do nothing
			logger.Errorf("[DiffSCIDVariables] ERR Key doesn't match string or uint64 - %v", ckey)
		}
	}
	_, cerr := graviton.Commit(tree, tree2)
	if cerr != nil {
		logger.Errorf("[Graviton] ERROR: %v", cerr)
		return
	}
	// End storing

	// Diff trees
	insert_map_actual := map[string]string{}
	delete_map_actual := map[string]string{}
	modify_map_actual := map[string]string{}

	insert_handler := func(k, v []byte) {
		insert_map_actual[string(k)] = string(v)
	}
	delete_handler := func(k, v []byte) {
		delete_map_actual[string(k)] = string(v)
	}

	modify_handler := func(k, v []byte) {
		modify_map_actual[string(k)] = string(v)
	}

	store2 := tempdb.DB
	ss2, err := store2.LoadSnapshot(0) // load most recent snapshot
	if err != nil {
		return
	}
	varset1_tree, _ := ss2.GetTree("varset1")
	varset2_tree, _ := ss2.GetTree("varset2")

	err = graviton.Diff(varset1_tree, varset2_tree, delete_handler, modify_handler, insert_handler)

	if len(delete_map_actual) > 0 {
		logger.Debugf("[DiffSCIDVariables] delete: %v", delete_map_actual)
	}
	if len(modify_map_actual) > 0 {
		logger.Debugf("[DiffSCIDVariables] modify: %v", modify_map_actual)
	}
	if len(insert_map_actual) > 0 {
		logger.Debugf("[DiffSCIDVariables] insert: %v", insert_map_actual)
	}
	// End Diff trees

	// Because diff data is returned in a map[string]string , we want to check to maintain type for data consistency.
	// Check against slices to determine to store a string or uint64 interface variable
	for mak, mav := range modify_map_actual {
		co := &structures.SCIDVariable{}

		// String data coming out of the compare seems to append "" around strings, so we pop those off
		mak2 := mak
		mav2 := mav

		if len(mak2) > 0 && mak2[0] == '"' {
			mak2 = mak2[1:]
		}
		if len(mak2) > 0 && mak2[len(mak2)-1] == '"' {
			mak2 = mak2[:len(mak2)-1]
		}
		if len(mav2) > 0 && mav2[0] == '"' {
			mav2 = mav2[1:]
		}
		if len(mav2) > 0 && mav2[len(mav2)-1] == '"' {
			mav2 = mav2[:len(mav2)-1]
		}

		// Loop through populated string slices from above to determine 'actual' key/variable types prior to storing them into db
		if vs2kstring[mak] || vs2kstring[mak2] {
			// Key is string
			if vs2kstring[mak2] {
				mak = mak2
			}
			if vs2vuint64[mav] || vs2vuint64[mav2] {
				// Value is uint64
				if vs2vuint64[mav2] {
					mav = mav2
				}
				co.Key = mak
				mavuint, _ := strconv.ParseUint(mav, 10, 64)
				co.Value = mavuint
				//logger.Debugf("[DiffSCIDVariables-Modify] Key '%v' is a string. Value '%v' is a uint64.", mak, mav)
			} else if vs2vstring[mav] || vs2vstring[mav2] {
				// Value is string
				if vs2vstring[mav2] {
					mav = mav2
				}
				co.Key = mak
				co.Value = mav
				//logger.Debugf("[DiffSCIDVariables-Modify] Key '%v' is a string. Value '%v' is a string.", mak, mav)
			} else {
				// No match
				// We can assume it is a string, however we want to add the pulled value rather than perhaps the UTF-8 return value from the Diff()
				//logger.Debugf("[DiffSCIDVariables-Modify] Key '%v' is a string, but value '%v' does not match string or uint64. Using varset2 string value '%v' instead.", mak, mav, vs2kss[mak])
				co.Key = mak
				co.Value = vs2kss[mak]
			}
		} else if vs2kuint64[mak] || vs2kuint64[mak2] {
			// Key is uint64
			if vs2kuint64[mak2] {
				mak = mak2
			}
			if vs2vuint64[mav] || vs2vuint64[mav2] {
				// Value is uint64
				if vs2vuint64[mav2] {
					mav = mav2
				}
				makuint, _ := strconv.ParseUint(mak, 10, 64)
				co.Key = makuint
				mavuint, _ := strconv.ParseUint(mav, 10, 64)
				co.Value = mavuint
				//logger.Debugf("[DiffSCIDVariables-Modify] Key '%v' is a uint64. Value '%v' is a uint64.", mak, mav)
			} else if vs2vstring[mav] || vs2vstring[mav2] {
				// Value is string
				if vs2vstring[mav2] {
					mav = mav2
				}
				makuint, _ := strconv.ParseUint(mak, 10, 64)
				co.Key = makuint
				co.Value = mav
				//logger.Debugf("[DiffSCIDVariables-Modify] Key '%v' is a uint64. Value '%v' is a string.", mak, mav)
			} else {
				// No match
				// We can assume it is a string, however we want to add the pulled value rather than perhaps the UTF-8 return value from the Diff()
				makuint, _ := strconv.ParseUint(mak, 10, 64)
				//logger.Debugf("[DiffSCIDVariables-Modify] Key '%v' is a uint64, but value '%v' does not match string or uint64. Using varset2 string value '%v' instead.", mak, mav, vs2kus[makuint])
				co.Key = makuint
				co.Value = vs2kus[makuint]
			}
		} else {
			// No match on key. Check values and report errors accordingly [We should not generally get here if above logic works. Edge cases perhaps.]
			if vs2vuint64[mav] || vs2vuint64[mav2] {
				if vs2vuint64[mav2] {
					mav = mav2
				}
				for k, v := range vs2ksu {
					mavuint, _ := strconv.ParseUint(mav, 10, 64)
					if v == mavuint {
						logger.Errorf("[DiffSCIDVariables-Modify] Key '%v' - does not match string or uint64. Value is a uint64: '%v' . Using key '%v' instead.", mak, mav, k)
						co.Key = k
						co.Value = mavuint
						break
					}
				}
				if co.Key == nil || co.Value == nil {
					logger.Fatalf("[DiffSCIDVariables-Modify] ERR - nil.")
				}
			} else if vs2vstring[mav] || vs2vstring[mav2] {
				if vs2vstring[mav2] {
					mav = mav2
				}
				for k, v := range vs2kss {
					if v == mav {
						logger.Errorf("[DiffSCIDVariables-Modify] Key '%v' - does not match string or uint64. Value is a string: '%v' . Using key '%v' instead.", mak, mav, k)
						co.Key = k
						co.Value = mav
						break
					}
				}
				if co.Key == nil || co.Value == nil {
					logger.Fatalf("[DiffSCIDVariables-Modify] ERR - nil.")
				}
			} else {
				logger.Fatalf("[DiffSCIDVariables-Modify] Key '%v' - does not match string or uint64. Value %v - does not match string or uint64", mak, mav)
			}
		}
		diffset = append(diffset, co)
	}

	for mak, mav := range insert_map_actual {
		co := &structures.SCIDVariable{}

		// String data coming out of the compare seems to append "" around strings, so we pop those off
		mak2 := mak
		mav2 := mav

		if len(mak2) > 0 && mak2[0] == '"' {
			mak2 = mak2[1:]
		}
		if len(mak2) > 0 && mak2[len(mak2)-1] == '"' {
			mak2 = mak2[:len(mak2)-1]
		}
		if len(mav2) > 0 && mav2[0] == '"' {
			mav2 = mav2[1:]
		}
		if len(mav2) > 0 && mav2[len(mav2)-1] == '"' {
			mav2 = mav2[:len(mav2)-1]
		}

		// Loop through populated string slices from above to determine 'actual' key/variable types prior to storing them into db
		if vs2kstring[mak] || vs2kstring[mak2] {
			// Key is string
			if vs2kstring[mak2] {
				mak = mak2
			}
			if vs2vuint64[mav] || vs2vuint64[mav2] {
				// Value is uint64
				if vs2vuint64[mav2] {
					mav = mav2
				}
				co.Key = mak
				mavuint, _ := strconv.ParseUint(mav, 10, 64)
				co.Value = mavuint
				//logger.Debugf("[DiffSCIDVariables-Insert] Key '%v' is a string. Value '%v' is a uint64.", mak, mav)
			} else if vs2vstring[mav] || vs2vstring[mav2] {
				// Value is string
				if vs2vstring[mav2] {
					mav = mav2
				}
				co.Key = mak
				co.Value = mav
				//logger.Debugf("[DiffSCIDVariables-Insert] Key '%v' is a string. Value '%v' is a string.", mak, mav)
			} else {
				// No match
				// We can assume it is a string, however we want to add the pulled value rather than perhaps the UTF-8 return value from the Diff()
				//logger.Debugf("[DiffSCIDVariables-Insert] Key '%v' is a string, but value '%v' does not match string or uint64. Using varset2 string value '%v' instead.", mak, mav, vs2kss[mak])
				co.Key = mak
				co.Value = vs2kss[mak]
			}
		} else if vs2kuint64[mak] || vs2kuint64[mak2] {
			// Key is uint64
			if vs2kuint64[mak2] {
				mak = mak2
			}
			if vs2vuint64[mav] || vs2vuint64[mav2] {
				// Value is uint64
				if vs2vuint64[mav2] {
					mav = mav2
				}
				makuint, _ := strconv.ParseUint(mak, 10, 64)
				co.Key = makuint
				mavuint, _ := strconv.ParseUint(mav, 10, 64)
				co.Value = mavuint
				//logger.Debugf("[DiffSCIDVariables-Insert] Key '%v' is a uint64. Value '%v' is a uint64.", mak, mav)
			} else if vs2vstring[mav] || vs2vstring[mav2] {
				// Value is string
				if vs2vstring[mav2] {
					mav = mav2
				}
				makuint, _ := strconv.ParseUint(mak, 10, 64)
				co.Key = makuint
				co.Value = mav
				//logger.Debugf("[DiffSCIDVariables-Insert] Key '%v' is a uint64. Value '%v' is a string.", mak, mav)
			} else {
				// No match
				// We can assume it is a string, however we want to add the pulled value rather than perhaps the UTF-8 return value from the Diff()
				makuint, _ := strconv.ParseUint(mak, 10, 64)
				//logger.Debugf("[DiffSCIDVariables-Insert] Key '%v' is a uint64, but value '%v' does not match string or uint64. Using varset2 string value '%v' instead.", mak, mav, vs2kus[makuint])
				co.Key = makuint
				co.Value = vs2kus[makuint]
			}
		} else {
			// No match on key. Check values and report errors accordingly [We should not generally get here if above logic works. Edge cases perhaps.]
			if vs2vuint64[mav] || vs2vuint64[mav2] {
				if vs2vuint64[mav2] {
					mav = mav2
				}
				for k, v := range vs2ksu {
					mavuint, _ := strconv.ParseUint(mav, 10, 64)
					if v == mavuint {
						logger.Errorf("[DiffSCIDVariables-Insert] Key '%v' - does not match string or uint64. Value is a uint64: '%v' . Using key '%v' instead.", mak, mav, k)
						co.Key = k
						co.Value = mavuint
						break
					}
				}
				if co.Key == nil || co.Value == nil {
					logger.Fatalf("[DiffSCIDVariables-Insert] ERR - nil.")
				}
			} else if vs2vstring[mav] || vs2vstring[mav2] {
				if vs2vstring[mav2] {
					mav = mav2
				}
				for k, v := range vs2kss {
					if v == mav {
						logger.Errorf("[DiffSCIDVariables-Insert] Key '%v' - does not match string or uint64. Value is a string: '%v' . Using key '%v' instead.", mak, mav, k)
						co.Key = k
						co.Value = mav
						break
					}
				}
				if co.Key == nil || co.Value == nil {
					logger.Fatalf("[DiffSCIDVariables-Insert] ERR - nil.")
				}
			} else {
				logger.Fatalf("[DiffSCIDVariables-Insert] Key '%v' - does not match string or uint64. Value %v - does not match string or uint64", mak, mav)
			}
		}
		diffset = append(diffset, co)
	}

	// Checking through the delete set we can assume (given the input to the diff) that any values returned can simply be stored with nil values as they were deleted
	for mak := range delete_map_actual {
		co := &structures.SCIDVariable{}

		// String data coming out of the compare seems to append "" around strings, so we pop those off to at least compare
		// Edge cases like this can come up with sha256 or TXID()-like data exports that we aren't fully decoding for data stores
		mak2 := mak
		if len(mak2) > 0 && mak2[0] == '"' {
			mak2 = mak2[1:]
		}
		if len(mak2) > 0 && mak2[len(mak2)-1] == '"' {
			mak2 = mak2[:len(mak2)-1]
		}

		// Loop through populated string slices from above to determine 'actual' key types prior to storing them into db
		if vs1kstring[mak] || vs1kstring[mak2] {
			// Key is string
			if vs1kstring[mak] {
				co.Key = mak
				co.Value = nil
				//logger.Debugf("[DiffSCIDVariables-Delete] Key '%v' is a string.", mak)
			} else {
				co.Key = mak2
				co.Value = nil
				//logger.Debugf("[DiffSCIDVariables-Delete] Key '%v' is a string.", mak2)
			}
		} else if vs1kuint64[mak] || vs1kuint64[mak2] {
			// Key is uint64
			if vs1kuint64[mak] {
				makuint, _ := strconv.ParseUint(mak, 10, 64)
				co.Key = makuint
				co.Value = nil
				//logger.Debugf("[DiffSCIDVariables-Delete] Key '%v' is a uint64.", mak)
			} else {
				makuint, _ := strconv.ParseUint(mak2, 10, 64)
				co.Key = makuint
				co.Value = nil
				//logger.Debugf("[DiffSCIDVariables-Delete] Key '%v' is a uint64.", mak2)
			}
		} else {
			// No match on key. Check values and report errors accordingly [We should not get here if above logic works]
			logger.Fatalf("[DiffSCIDVariables-Delete] Key '%v' - does not match string or uint64.", mak)
		}
		// Delete map references have a key and a nil value
		diffset = append(diffset, co)
	}

	return
}

// Validates that a stored signature results in the code deployed to a SC - currently allowing any 'key' to be passed through, however intended key is 'signature' or similar
func (indexer *Indexer) ValidateSCSignature(code string, key string) (validated bool, signer string, err error) {
	if key == "" {
		return
	}

	// CheckSignature
	filedata := []byte(key)
	p, _ := pem.Decode(filedata)
	if p == nil {
		logger.Errorf("[ValidateSCSignature] ERR - Unknown format of input data - %v", key)
		return
	}

	astr := p.Headers["Address"]
	cstr := p.Headers["C"]
	sstr := p.Headers["S"]

	addr, err := rpc.NewAddress(astr)
	if err != nil {
		logger.Errorf("[ValidateSCSignature] ERR - Cannot validate Address header")
		return
	}

	c, ok := new(big.Int).SetString(cstr, 16)
	if !ok {
		err = fmt.Errorf("[ValidateSCSignature] Unknown C format")
		return
	}

	s, ok := new(big.Int).SetString(sstr, 16)
	if !ok {
		err = fmt.Errorf("[ValidateSCSignature] Unknown S format")
		return
	}

	tmppoint := new(bn256.G1).Add(new(bn256.G1).ScalarMult(crypto.G, s), new(bn256.G1).ScalarMult(addr.PublicKey.G1(), new(big.Int).Neg(c)))
	serialize := []byte(fmt.Sprintf("%s%s%x", addr.PublicKey.G1().String(), tmppoint.String(), p.Bytes))

	c_calculated := crypto.ReducedHash(serialize)
	if c.String() != c_calculated.String() {
		err = fmt.Errorf("[ValidateSCSignature] signature mismatch")
		return
	}

	signer = addr.String()
	message := p.Bytes

	if string(message) == code {
		validated = true
	}

	return
}

// Returns a list of addresses that have indexed 'interactions' with the network
func (indexer *Indexer) GetInteractionAddresses(config *structures.InteractionAddrs_Params) (interAddrs map[string]*structures.IATrack, interCounts *structures.IATrack) {
	interAddrs = make(map[string]*structures.IATrack)
	interCounts = &structures.IATrack{}

	logger.Debugf("[GetInteractionAddresses] Getting owners and integrators")
	// Get all SCID owners
	sclist := make(map[string]string)
	integrators := make(map[string]int64)
	switch indexer.DBType {
	case "gravdb":
		sclist = indexer.GravDBBackend.GetAllOwnersAndSCIDs()
		integrators, _ = indexer.GravDBBackend.GetIntegrators()
	case "boltdb":
		sclist = indexer.BBSBackend.GetAllOwnersAndSCIDs()
		integrators = indexer.BBSBackend.GetIntegrators()
	}

	// Build an interaction list
	logger.Debugf("[GetInteractionAddresses] Building interaction list - integrator")
	if config.Integrator {
		for k, _ := range integrators {
			interCounts.Integrator++
			if interAddrs[k] == nil {
				interAddrs[k] = &structures.IATrack{}
			}
			interAddrs[k].Integrator++
		}
	}

	logger.Debugf("[GetInteractionAddresses] Building interaction list - invokes and installs")
	for k, v := range sclist {
		var invokedetails []*structures.SCTXParse
		if config.Invokes {
			switch indexer.DBType {
			case "gravdb":
				invokedetails = indexer.GravDBBackend.GetAllSCIDInvokeDetails(k)
			case "boltdb":
				invokedetails = indexer.BBSBackend.GetAllSCIDInvokeDetails(k)
			}
		}

		// Add each invoke sender interaction
		for _, vi := range invokedetails {
			sc_action := fmt.Sprintf("%v", vi.Sc_args.Value("SC_ACTION", "U"))
			if sc_action == "0" {
				interCounts.Invokes++
				if interAddrs[vi.Sender] == nil {
					interAddrs[vi.Sender] = &structures.IATrack{}
				}
				interAddrs[vi.Sender].Invokes++
			}
		}

		// Append to interaction list the install
		if config.Installs {
			if v != "" {
				interCounts.Installs++
				if interAddrs[v] == nil {
					interAddrs[v] = &structures.IATrack{}
				}
				interAddrs[v].Installs++
			}
		}
	}

	return
}

// Returns a random n number of interaction addresses
func (indexer *Indexer) GetRandInteractionAddresses(count int64, config *structures.InteractionAddrs_Params) (rAddr []string, err error) {
	interAddrs := make(map[string]*structures.IATrack)
	interAddrs, _ = indexer.GetInteractionAddresses(config)

	i := int64(0)
	for k, _ := range interAddrs {
		if i >= count {
			break
		}
		rAddr = append(rAddr, k)
		i++
	}

	// Push err after generating list since the list could still be of use to the func call
	if count > int64(len(interAddrs)) {
		return rAddr, fmt.Errorf("Provided count '%d' is more than returned interaction addresses '%d'", count, len(interAddrs))
	}

	return
}

// GetTelaCandidates returns all SCIDs that have been classified as TELA apps
// during AddSCIDToIndex. This allows consumers to skip the expensive 49K-SCID
// prefilter and query only known TELA candidates.
func (indexer *Indexer) GetTelaCandidates() []string {
	var candidates map[string]string
	switch indexer.DBType {
	case "gravdb":
		candidates = indexer.GravDBBackend.GetAllTelaCandidates()
	case "boltdb":
		candidates = indexer.BBSBackend.GetAllTelaCandidates()
	}
	// Filter to only valid_index entries
	var result []string
	for scid, status := range candidates {
		if status == "valid_index" || status == "tela" {
			result = append(result, scid)
		}
	}
	return result
}

// BackfillTelaCandidates scans all existing SCIDs in storage and classifies
// TELA candidates using a dedicated pool of fresh RPC connections. This avoids
// conflicts with the main indexing loop which constantly uses indexer.RPC.
// The workers parameter controls how many parallel RPC connections to use.
// It should be called in a background goroutine.
func (indexer *Indexer) BackfillTelaCandidates(workers int) error {
	existing := indexer.GetTelaCandidates()
	existingMap := make(map[string]bool, len(existing))
	for _, scid := range existing {
		existingMap[scid] = true
	}

	var allSCIDs map[string]string
	switch indexer.DBType {
	case "gravdb":
		allSCIDs = indexer.GravDBBackend.GetAllOwnersAndSCIDs()
	case "boltdb":
		allSCIDs = indexer.BBSBackend.GetAllOwnersAndSCIDs()
	}
	if len(allSCIDs) == 0 {
		logger.Printf("[BackfillTelaCandidates] No SCIDs in storage, skipping\n")
		return nil
	}

	scids := make([]string, 0, len(allSCIDs))
	for scid := range allSCIDs {
		if !existingMap[scid] {
			scids = append(scids, scid)
		}
	}
	if len(scids) == 0 {
		logger.Printf("[BackfillTelaCandidates] All %d SCIDs already known as TELA candidates, skipping\n", len(allSCIDs))
		return nil
	}
	sort.Strings(scids)

	logger.Printf("[BackfillTelaCandidates] Starting backfill for %d unknown SCIDs (already know %d) with %d workers\n", len(scids), len(existing), workers)

	if workers <= 0 {
		workers = 4
	}

	pool, cleanup, err := DialRPCPool(indexer.Endpoint, workers)
	if err != nil {
		logger.Printf("[BackfillTelaCandidates] Failed to dial RPC pool: %v\n", err)
		return err
	}
	defer cleanup()

	batchSize := 500
	total := len(scids)
	type result struct {
		scid   string
		isTela bool
	}

	workCh := make(chan []string, workers*2)
	resultCh := make(chan result, workers*2)
	var wg sync.WaitGroup

	// Launch workers
	for w := 0; w < len(pool); w++ {
		wg.Add(1)
		go func(client *jrpc2.Client) {
			defer wg.Done()
			for batch := range workCh {
				if indexer.Closing.Load() {
					return
				}
				specs := make([]jrpc2.Spec, len(batch))
				for j, scid := range batch {
					specs[j] = jrpc2.Spec{
						Method: "DERO.GetSC",
						Params: rpc.GetSC_Params{
							SCID:       scid,
							KeysString: []string{"telaVersion"},
						},
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				responses, err := client.Batch(ctx, specs)
				cancel()
				if err != nil {
					logger.Printf("[BackfillTelaCandidates] Worker batch error: %v\n", err)
					continue
				}
				for j, resp := range responses {
					if j >= len(batch) || resp == nil || resp.Error() != nil {
						continue
					}
					var out rpc.GetSC_Result
					if err := resp.UnmarshalResult(&out); err != nil {
						continue
					}
					isTela := false
					if len(out.ValuesString) > 0 && out.ValuesString[0] != "" &&
						!strings.HasPrefix(out.ValuesString[0], "NOT AVAILABLE") {
						isTela = true
					}
					resultCh <- result{scid: batch[j], isTela: isTela}
				}
			}
		}(pool[w])
	}

	// Collector goroutine: stores candidates and counts progress
	var found int
	var processed int
	doneCh := make(chan struct{})
	go func() {
		for r := range resultCh {
			if r.isTela {
				found++
				switch indexer.DBType {
				case "gravdb":
					indexer.GravDBBackend.StoreTelaCandidate(r.scid, "valid_index")
				case "boltdb":
					indexer.BBSBackend.StoreTelaCandidate(r.scid, "valid_index")
				}
			}
			processed++
		}
		close(doneCh)
	}()

	// Feed work queue
	batchCount := 0
	for i := 0; i < total; i += batchSize {
		if indexer.Closing.Load() {
			logger.Printf("[BackfillTelaCandidates] Interrupted: indexer closing\n")
			break
		}
		end := i + batchSize
		if end > total {
			end = total
		}
		workCh <- scids[i:end]
		batchCount++
		if batchCount%10 == 0 {
			logger.Printf("[BackfillTelaCandidates] Progress: %d/%d SCIDs checked, found %d candidates\n", end, total, found)
		}
	}
	close(workCh)

	// Wait for workers to finish, then close result channel
	wg.Wait()
	close(resultCh)

	// Wait for collector to finish
	<-doneCh

	logger.Printf("[BackfillTelaCandidates] Done. Checked %d SCIDs, found %d TELA candidates\n", total, found)
	return nil
}

// Close cleanly the indexer
func (ind *Indexer) Close() {
	// Tell indexer a closing operation is happening; this will close out loops on next iteration
	ind.Closing.Store(true)

	switch ind.DBType {
	case "gravdb":
		ind.GravDBBackend.Closing.Store(true)
	case "boltdb":
		ind.BBSBackend.Closing.Store(true)
	}

	// Sleep for safety
	time.Sleep(time.Second * 1)

	// Close websocket connection cleanly
	if ind.RPC.WS != nil {
		ind.RPC.WS.Close()
	}

	// Close out grav db cleanly
	writeWait, _ := time.ParseDuration("20ms")
	switch ind.DBType {
	case "gravdb":
		for ind.GravDBBackend.Writing.Load() {
			if ind.Closing.Load() {
				return
			}
			//logger.Debugf("[Indexer-NewIndexer] GravitonDB is writing... sleeping for %v...", writeWait)
			time.Sleep(writeWait)
		}
		ind.GravDBBackend.Writing.Store(true)
		ind.GravDBBackend.DB.Close()
		ind.GravDBBackend.Writing.Store(false)
	case "boltdb":
		for ind.BBSBackend.Writing.Load() {
			if ind.Closing.Load() {
				return
			}
			//logger.Debugf("[Indexer-Close] BoltDB is writing... sleeping for %v... writer %v...", writeWait, ind.BBSBackend.Writer)
			time.Sleep(writeWait)
		}
		ind.BBSBackend.Writing.Store(true)
		//ind.BBSBackend.Writer = "Close"
		ind.BBSBackend.DB.Sync()
		ind.BBSBackend.DB.Close()
		ind.BBSBackend.Writing.Store(false)
		//ind.BBSBackend.Writer = ""
	}
}

// Check if value exists within a string array/slice
func scidExist(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

// Check if value exists within an interface array/slice
func vExist(arr []interface{}, val interface{}) bool {
	for _, v := range arr {
		switch ca := v.(type) {
		case uint64:
			switch va := val.(type) {
			case uint64:
				if ca == va {
					return true
				}
			default:
				// Do nothing
			}
		case string:
			switch va := val.(type) {
			case string:
				if ca == va {
					return true
				}
			default:
				// Do nothing
			}
		default:
			// Do nothing
		}
	}

	return false
}
