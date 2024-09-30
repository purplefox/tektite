package cworker

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/common"
	iteration2 "github.com/spirit-labs/tektite/iteration"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/lsm"
	"github.com/spirit-labs/tektite/objstore"
	"github.com/spirit-labs/tektite/sst"
	"github.com/spirit-labs/tektite/tabcache"
	"sync"
	"sync/atomic"
	"time"
)

func NewCompactionWorkerService(cfg *conf.Config, levelMgrClientFactory lsm.ClientFactory, tableCache *tabcache.Cache,
	objStoreClient objstore.Client, retentions bool) *CompactionWorkerService {
	return &CompactionWorkerService{
		cfg:                   cfg,
		levelMgrClientFactory: levelMgrClientFactory,
		tableCache:            tableCache,
		objStoreClient:        objStoreClient,
		retentions:            retentions,
	}
}

type CompactionWorkerService struct {
	cfg                   *conf.Config
	levelMgrClientFactory lsm.ClientFactory
	tableCache            *tabcache.Cache
	objStoreClient        objstore.Client
	workers               []*compactionWorker
	started               bool
	lock                  sync.Mutex
	gotPrefixRetentions   bool
	retentions            bool
}

const workerRetryInterval = 100 * time.Millisecond

func (c *CompactionWorkerService) Start() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.started {
		return nil
	}
	for i := 0; i < c.cfg.CompactionWorkerCount; i++ {
		worker := &compactionWorker{
			cws:            c,
			client:         c.levelMgrClientFactory.CreateLevelManagerClient(),
			slabRetentions: map[int]time.Duration{},
		}
		c.workers = append(c.workers, worker)
		worker.start()
	}
	c.started = true
	return nil
}

func (c *CompactionWorkerService) Stop() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.started {
		return nil
	}
	var chans []chan struct{}
	for _, worker := range c.workers {
		// stop them in parallel - for quicker shutdown
		theWorker := worker
		ch := make(chan struct{}, 1)
		chans = append(chans, ch)
		common.Go(func() {
			theWorker.stop()
			ch <- struct{}{}
		})
	}
	for _, ch := range chans {
		<-ch
	}
	c.started = false
	return nil
}

type compactionWorker struct {
	cws            *CompactionWorkerService
	started        atomic.Bool
	stopWg         sync.WaitGroup
	client         lsm.Client
	slabRetentions map[int]time.Duration
}

func (c *compactionWorker) GetSlabRetention(slabID int) (time.Duration, error) {
	// we cache the slab retentions
	ret, ok := c.slabRetentions[slabID]
	if ok {
		return ret, nil
	}
	ret, err := c.client.GetSlabRetention(slabID)
	if err != nil {
		return 0, err
	}
	c.slabRetentions[slabID] = ret
	return ret, nil
}

func (c *compactionWorker) start() {
	c.started.Store(true)
	c.stopWg.Add(1)
	common.Go(c.loop)
}

func (c *compactionWorker) stop() {
	if err := c.client.Stop(); err != nil {
		log.Warnf("failed to stop levelMgr client: %v", err)
	}
	c.started.Store(false)
	c.stopWg.Wait()
}

func (c *compactionWorker) loop() {
	defer c.stopWg.Done()
	for c.started.Load() {
		job, err := c.client.PollForJob()
		if err != nil {
			var tErr common.TektiteError
			if errwrap.As(err, &tErr) {
				if tErr.Code == common.Unavailable || tErr.Code == common.LevelManagerNotLeaderNode {
					// transient unavailability
					time.Sleep(workerRetryInterval)
					continue
				}
				if tErr.Code == common.CompactionPollTimeout {
					// OK
					continue
				}
			}
			log.Errorf("error in polling for compaction job: %v", err)
			continue
		}
		for c.started.Load() {
			registrations, deRegistrations, err := c.processJob(job)
			if err == nil {
				regBatch := lsm.RegistrationBatch{
					Compaction:      true,
					JobID:           job.Id,
					Registrations:   registrations,
					DeRegistrations: deRegistrations,
				}
				_, err := c.client.ApplyChanges(regBatch)
				if err == nil {
					// success
					break
				}
			}
			var tErr common.TektiteError
			if errwrap.As(err, &tErr) {
				if tErr.Code == common.Unavailable || tErr.Code == common.LevelManagerNotLeaderNode {
					// transient unavailability - retry after delay
					log.Debugf("transient error in compaction. will retry: %v", err)
					time.Sleep(workerRetryInterval)
					continue
				}
			}
			log.Warnf("failed to process compaction:%v", err)
			break
		}
	}
}

func (c *compactionWorker) processJob(job *lsm.CompactionJob) ([]lsm.RegistrationEntry, []lsm.RegistrationEntry, error) {
	if job.IsMove {
		if len(job.Tables) != 1 {
			panic("move requires single run to move")
		}
		registrations, deRegistrations := c.moveTables(job.Tables[0], job.LevelFrom, job.PreserveTombstones)
		return registrations, deRegistrations, nil
	}
	start := time.Now()
	tablesInMerge := 0
	for _, overlapping := range job.Tables {
		tablesInMerge += len(overlapping)
	}
	var maxAddedTime uint64
	tablesToMerge := make([][]tableToMerge, len(job.Tables))
	for i, overlapping := range job.Tables {
		tables := make([]tableToMerge, len(overlapping))
		for j, t := range overlapping {
			ssTable, err := c.cws.tableCache.GetSSTable(t.Table.SSTableID)
			if err != nil {
				return nil, nil, err
			}
			if ssTable == nil {
				return nil, nil, errwrap.Errorf("cannot process compaction job as cannot find sstable: %v (%s)", t.Table.SSTableID,
					string(t.Table.SSTableID))
			}
			tables[j] = tableToMerge{
				deadVersionRanges: t.Table.DeadVersionRanges,
				sst:               ssTable,
				id:                t.Table.SSTableID,
			}
			if t.Table.AddedTime > maxAddedTime {
				// We compute the maxAddedTime time - this is used for the AddedTime of any new sstables created.
				maxAddedTime = t.Table.AddedTime
			}
		}
		tablesToMerge[i] = tables
	}
	mergeStart := time.Now()
	var retProvider lsm.RetentionProvider
	if c.cws.retentions {
		retProvider = c
	}
	infos, err := mergeSSTables(common.DataFormatV1, tablesToMerge, job.PreserveTombstones,
		c.cws.cfg.CompactionMaxSSTableSize, job.LastFlushedVersion, job.Id, retProvider, job.ServerTime)
	if err != nil {
		return nil, nil, err
	}
	mergeDur := time.Now().Sub(mergeStart)
	log.Debugf("merge for job %s took %d ms", job.Id, mergeDur.Milliseconds())
	// Now push the tables to the cloud store
	var ids []sst.SSTableID
	for _, info := range infos {
		id := []byte(fmt.Sprintf("sst-%s", uuid.New().String()))
		log.Debugf("compaction job %s created sstable %v", job.Id, id)
		ids = append(ids, id)
		for {
			tableBytes := info.sst.Serialize()
			// Add to object store
			err := objstore.PutWithTimeout(c.cws.objStoreClient, c.cws.cfg.BucketName, string(id), tableBytes, objstore.DefaultCallTimeout)
			if err == nil {
				// Add to the local cache
				err = c.cws.tableCache.AddSSTableWithMaxAge(id, info.sst)
				if err != nil {
					panic(err) // never happens
				}
				break
			}
			if !common.IsUnavailableError(err) {
				return nil, nil, err
			}
			log.Debugf("failed to push compacted table, will retry: %v", err)
			time.Sleep(c.cws.cfg.SSTablePushRetryDelay)
		}
	}
	var tableEntries []lsm.TableEntry
	for i, info := range infos {
		tableEntries = append(tableEntries, lsm.TableEntry{
			SSTableID:        ids[i],
			RangeStart:       info.rangeStart,
			RangeEnd:         info.rangeEnd,
			MinVersion:       info.minVersion,
			MaxVersion:       info.maxVersion,
			DeleteRatio:      info.deleteRatio,
			NumEntries:       uint64(info.sst.NumEntries()),
			Size:             uint64(info.sst.SizeBytes()),
			AddedTime:        maxAddedTime,
			NumPrefixDeletes: info.numPrefixDeletes,
		})
		log.Debugf("compaction %s created table %v delete ratio %f preserve tombstones %t", job.Id, ids[i], info.deleteRatio,
			job.PreserveTombstones)
	}
	registrations, deRegistrations := lsm.ChangesToApply(tableEntries, job)
	dur := time.Now().Sub(start)
	log.Debugf("compaction job %s took %d ms to process on worker %p", job.Id, dur.Milliseconds(), c)
	return registrations, deRegistrations, nil
}

// no overlap, so we can move tables directly from one level to another without merging anything
func (c *compactionWorker) moveTables(tables []lsm.TableToCompact, fromLevel int, preserveTombstones bool) ([]lsm.RegistrationEntry, []lsm.RegistrationEntry) {
	var registrations []lsm.RegistrationEntry
	for _, tableToCompact := range tables {
		if tableToCompact.Table.DeleteRatio == float64(1) && !preserveTombstones {
			// The table is just deletes, and we're moving it into the last level - we can just drop it
		} else {
			registrations = append(registrations, lsm.RegistrationEntry{
				Level:       fromLevel + 1,
				TableID:     tableToCompact.Table.SSTableID,
				KeyStart:    tableToCompact.Table.RangeStart,
				KeyEnd:      tableToCompact.Table.RangeEnd,
				MinVersion:  tableToCompact.Table.MinVersion,
				MaxVersion:  tableToCompact.Table.MaxVersion,
				DeleteRatio: tableToCompact.Table.DeleteRatio,
				NumEntries:  tableToCompact.Table.NumEntries,
				TableSize:   tableToCompact.Table.Size,
				AddedTime:   tableToCompact.Table.AddedTime,
			})
		}
	}
	var deRegistrations []lsm.RegistrationEntry
	for _, tableToCompact := range tables {
		deRegistrations = append(deRegistrations, lsm.RegistrationEntry{
			Level:       fromLevel,
			TableID:     tableToCompact.Table.SSTableID,
			KeyStart:    tableToCompact.Table.RangeStart,
			KeyEnd:      tableToCompact.Table.RangeEnd,
			DeleteRatio: tableToCompact.Table.DeleteRatio,
			NumEntries:  tableToCompact.Table.NumEntries,
			TableSize:   tableToCompact.Table.Size,
		})
	}
	return registrations, deRegistrations
}

type tableToMerge struct {
	deadVersionRanges []lsm.VersionRange
	sst               *sst.SSTable
	id                sst.SSTableID
}

// MergeSSTables takes a list of SSTables, and merges them to produce one or more output SSTables
// Tables lower in the list take precedence to tables higher in the list when a common key is found

type ssTableInfo struct {
	sst              *sst.SSTable
	rangeStart       []byte
	rangeEnd         []byte
	minVersion       uint64
	maxVersion       uint64
	deleteRatio      float64
	numPrefixDeletes uint32
}

func mergeSSTables(format common.DataFormat, tables [][]tableToMerge, preserveTombstones bool, maxTableSize int,
	lastFlushedVersion int64, jobID string, retentionProvider lsm.RetentionProvider, serverTime uint64) ([]ssTableInfo, error) {

	totEntries := 0
	chainIters := make([]iteration2.Iterator, len(tables))
	for i, overlapping := range tables {
		sourceIters := make([]iteration2.Iterator, len(overlapping))
		for j, table := range overlapping {
			sstIter, err := table.sst.NewIterator(nil, nil)
			log.Debugf("mergingSSTables with dead version range %v", table.deadVersionRanges)
			if len(table.deadVersionRanges) > 0 {
				sstIter = lsm.NewRemoveDeadVersionsIterator(sstIter, table.deadVersionRanges)
			}
			if retentionProvider != nil {
				sstIter = lsm.NewRemoveExpiredEntriesIterator(sstIter, table.sst.CreationTime(), serverTime, retentionProvider)
			}
			sourceIters[j] = sstIter

			if err != nil {
				return nil, err
			}
			totEntries += table.sst.NumEntries()
		}
		chainIter := iteration2.NewChainingIterator(sourceIters)
		chainIters[i] = chainIter
	}

	var minNonCompactableVersion uint64
	if lastFlushedVersion == -1 {
		// Nothing flushed yet, no versions can be overwritten
		minNonCompactableVersion = 0
	} else {
		// We can only overwrite older versions of same key if version < lastFlushedVersion
		// This ensures we don't lose any keys that we need after rolling back to lastFlushedVersion on failure
		minNonCompactableVersion = uint64(lastFlushedVersion)
	}
	mi, err := iteration2.NewCompactionMergingIterator(chainIters, preserveTombstones, minNonCompactableVersion)
	if err != nil {
		return nil, err
	}
	log.Debugf("compaction created merging iterator: %p with minnoncompactable version %d for job %s",
		mi, minNonCompactableVersion, jobID)

	mergeResults := make([]common.KV, 0, totEntries)
	for {
		valid, curr, err := mi.Next()
		if err != nil {
			return nil, err
		}
		if !valid {
			break
		}
		mergeResults = append(mergeResults, curr)
	}

	size := 0
	iLast := 0
	var outTables []ssTableInfo
	var lastKeyNoVersion []byte
	for i, curr := range mergeResults {

		k := curr.Key
		size += 12 + 2*len(k) + len(curr.Value)

		isLast := i == len(mergeResults)-1

		keyNoVersion := k[:len(k)-8]
		if lastKeyNoVersion != nil && bytes.Equal(lastKeyNoVersion, keyNoVersion) && !isLast {
			// If keys only differ by version they must not be split across different sstables
			continue
		}
		lastKeyNoVersion = keyNoVersion

		// estimate of how much space an entry takes up in the sstable (data and index)

		if size >= maxTableSize || isLast {
			iter := common.NewKvSliceIterator(mergeResults[iLast : i+1])
			ssTable, smallestKey, largestKey, minVersion, maxVersion, err := sst.BuildSSTable(format, size, i+1-iLast,
				iter)
			if err != nil {
				return nil, err
			}
			outTables = append(outTables, ssTableInfo{
				sst:              ssTable,
				rangeStart:       smallestKey,
				rangeEnd:         largestKey,
				minVersion:       minVersion,
				maxVersion:       maxVersion,
				deleteRatio:      ssTable.DeleteRatio(),
				numPrefixDeletes: uint32(ssTable.NumPrefixDeletes()),
			})
			iLast = i + 1
			size = 0
		}
	}

	log.Debugf("compaction job merged %d entries into %d entries", totEntries, len(mergeResults))

	return outTables, nil
}

func validateRegBatch(regBatch lsm.RegistrationBatch, objStore objstore.Client, bucketName string) {
	for _, entry := range regBatch.Registrations {
		validateRegEntry(entry, objStore, bucketName)
	}
	for _, entry := range regBatch.DeRegistrations {
		validateRegEntry(entry, objStore, bucketName)
	}
}

func validateRegEntry(entry lsm.RegistrationEntry, objStore objstore.Client, bucketName string) {
	buff, err := objstore.GetWithTimeout(objStore, bucketName, string(entry.TableID), objstore.DefaultCallTimeout)
	if err != nil {
		panic(err)
	}
	table := &sst.SSTable{}
	table.Deserialize(buff, 0)
	iter, err := table.NewIterator(nil, nil)
	if err != nil {
		panic(err)
	}
	var firstKey []byte
	var lastKey []byte
	for {
		valid, curr, err := iter.Next()
		if err != nil {
			panic(err)
		}
		if !valid {
			break
		}
		if lastKey != nil && bytes.Compare(curr.Key, lastKey) <= 0 {
			panic(fmt.Sprintf("key out of order or duplicate %s", string(curr.Key)))
		}
		if firstKey == nil {
			firstKey = curr.Key
		}
		lastKey = curr.Key
	}
	if !bytes.Equal(firstKey, entry.KeyStart) {
		panic(fmt.Sprintf("first key %v (%s) and range start %v (%s) are not equal",
			firstKey, string(firstKey), entry.KeyStart, string(entry.KeyStart)))
	}
	if !bytes.Equal(lastKey, entry.KeyEnd) {
		panic(fmt.Sprintf("last key %v (%s) and range end %v (%s) are not equal",
			lastKey, string(lastKey), entry.KeyEnd, string(entry.KeyEnd)))
	}
}
