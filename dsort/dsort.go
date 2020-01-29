// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type (
	dsorter interface {
		name() string

		init() error
		start() error
		postExtraction()
		postRecordDistribution()
		createShardsLocally() (err error)
		preShardCreation(shardName string, mpathInfo *fs.MountpathInfo) error
		postShardCreation(mpathInfo *fs.MountpathInfo)
		cleanup()
		finalCleanup() error

		loadContent() extract.LoadContentFunc

		makeRecvRequestFunc() transport.Receive

		preShardExtraction(expectedUncompressedSize uint64) (toDisk bool)
		postShardExtraction(expectedUncompressedSize uint64)

		onAbort()
	}
)

var js = jsoniter.ConfigFastest

func (m *Manager) start() (err error) {
	defer func() {
		m.lock()
		m.setInProgressTo(false)
		m.unlock()

		// Trigger decrement reference counter. If it is already 0 it will
		// trigger cleanup because progress is set to false. Otherwise, the
		// cleanup will be triggered by decrementRef in load content handlers.
		m.decrementRef(0)
	}()

	if err := m.initStreams(); err != nil {
		return err
	}

	glog.Infof("starting %s %s with dsorter: %q", cmn.DSortName, m.ManagerUUID, m.dsorter.name())

	if err := m.dsorter.start(); err != nil {
		return err
	}

	// Phase 1.
	if err := m.extractLocalShards(); err != nil {
		return err
	}

	s := binary.BigEndian.Uint64(m.rs.TargetOrderSalt)
	targetOrder := randomTargetOrder(s, m.smap.Tmap)
	glog.V(4).Infof("final target in targetOrder => URL: %s, Daemon ID: %s",
		targetOrder[len(targetOrder)-1].PublicNet.DirectURL, targetOrder[len(targetOrder)-1].DaemonID)

	// Phase 2.
	curTargetIsFinal, err := m.participateInRecordDistribution(targetOrder)
	if err != nil {
		return err
	}

	// Run phase 3. only if you are final target (and actually have any sorted records)
	if curTargetIsFinal && m.recManager.Records.Len() > 0 {
		shardSize := m.rs.OutputShardSize
		if m.extractCreator.UsingCompression() {
			// By making the assumption that the input content is reasonably
			// uniform across all shards, the output shard size required (such
			// that each gzip compressed output shard will have a size close to
			// rs.ShardSizeBytes) can be estimated.
			avgCompressRatio := m.avgCompressionRatio()
			shardSize = int64(float64(m.rs.OutputShardSize) / avgCompressRatio)
			glog.V(4).Infof("estimated output shard size required before gzip compression: %d", shardSize)
		}

		// Phase 3.
		if err := m.distributeShardRecords(shardSize); err != nil {
			return err
		}
	}

	cmn.FreeMemToOS()

	// After each target participates in the cluster-wide record distribution,
	// start listening for the signal to start creating shards locally.
	if err := m.dsorter.createShardsLocally(); err != nil {
		return err
	}

	glog.Infof("finished %s %s successfully", cmn.DSortName, m.ManagerUUID)
	return nil
}

func (m *Manager) extractShard(name string, metrics *LocalExtraction, cfg *cmn.DSortConf) func() error {
	return func() error {
		var (
			warnPossibleOOM          bool
			estimateTotalRecordsSize uint64
			phaseInfo                = &m.extractionPhase
		)

		defer phaseInfo.adjuster.releaseGoroutineSema()

		shardName := name + m.rs.Extension
		lom := &cluster.LOM{T: m.ctx.t, Objname: shardName}
		if err := lom.Init(cmn.Bck{Name: m.rs.Bucket, Provider: m.rs.Provider}); err != nil {
			return err
		}

		si, err := cluster.HrwTarget(lom.Uname(), m.smap)
		if err != nil {
			return err
		}
		if si.DaemonID != m.ctx.node.DaemonID {
			return nil
		}
		if err = lom.Load(false); err != nil {
			if cmn.IsErrObjNought(err) {
				msg := fmt.Sprintf("shard %q does not exist (is missing)", shardName)
				return m.react(cfg.MissingShards, msg)
			}
			return err
		}

		phaseInfo.adjuster.acquireSema(lom.ParsedFQN.MpathInfo)
		if m.aborted() {
			phaseInfo.adjuster.releaseSema(lom.ParsedFQN.MpathInfo)
			return newAbortError(m.ManagerUUID)
		}
		//
		// TODO -- FIXME: all targets must check t.AvgCapUsed() for high watermark *prior* to starting
		//
		if capInfo := m.ctx.t.AvgCapUsed(nil); capInfo.OOS { // TODO -- FIXME: too late & not enough
			phaseInfo.adjuster.releaseSema(lom.ParsedFQN.MpathInfo)
			return capInfo.Err
		}

		lom.Lock(false)
		f, err := os.Open(lom.FQN)
		if err != nil {
			phaseInfo.adjuster.releaseSema(lom.ParsedFQN.MpathInfo)
			lom.Unlock(false)
			return errors.Errorf("unable to open local file, err: %v", err)
		}
		var compressedSize int64
		if m.extractCreator.UsingCompression() {
			compressedSize = lom.Size()
		}

		expectedUncompressedSize := uint64(float64(lom.Size()) / m.avgCompressionRatio())
		toDisk := m.dsorter.preShardExtraction(expectedUncompressedSize)

		beforeExtraction := time.Now()
		reader := io.NewSectionReader(f, 0, lom.Size())
		extractedSize, extractedCount, err := m.extractCreator.ExtractShard(lom.ParsedFQN, reader, m.recManager, toDisk)

		dur := time.Since(beforeExtraction)

		// Make sure that compression rate is updated before releasing
		// next extractor goroutine.
		if m.extractCreator.UsingCompression() {
			m.addCompressionSizes(compressedSize, extractedSize)
		}

		phaseInfo.adjuster.releaseSema(lom.ParsedFQN.MpathInfo)
		lom.Unlock(false)

		m.dsorter.postShardExtraction(expectedUncompressedSize) // schedule unreserving reserved memory on next memory update
		if err != nil {
			f.Close()
			return errors.Errorf("error in ExtractShard, file: %s, err: %v", f.Name(), err)
		}
		f.Close()

		metrics.Lock()
		metrics.ExtractedRecordCnt += int64(extractedCount)
		metrics.ExtractedCnt++

		if metrics.ExtractedCnt == 1 && extractedCount > 0 {
			// After extracting first shard estimate how much memory
			// will be required to keep all records in memory. One node
			// will eventually have all records from all shards so we
			// don't calculate estimates only for single node.
			recordSize := int(m.recManager.Records.RecordMemorySize())
			estimateTotalRecordsSize = uint64(metrics.TotalCnt * int64(extractedCount*recordSize))
			if estimateTotalRecordsSize > m.freeMemory() {
				warnPossibleOOM = true
			}
		}

		metrics.ExtractedSize += extractedSize
		if toDisk {
			metrics.ExtractedToDiskCnt++
			metrics.ExtractedToDiskSize += extractedSize
		}
		if m.Metrics.extended {
			metrics.ShardExtractionStats.updateTime(dur)
			metrics.ShardExtractionStats.updateThroughput(extractedSize, dur)
		}
		metrics.Unlock()

		if warnPossibleOOM {
			msg := fmt.Sprintf("(estimated) total size of records (%d) will possibly exceed available memory (%s) during sorting phase", estimateTotalRecordsSize, m.rs.MaxMemUsage)
			return m.react(cmn.WarnReaction, msg)
		}

		return nil
	}
}

// extractLocalShards iterates through files local to the current target and
// calls ExtractShard on matching files based on the given ParsedRequestSpec.
func (m *Manager) extractLocalShards() (err error) {
	var (
		cfg       = &cmn.GCO.Get().DSort
		phaseInfo = &m.extractionPhase
	)

	phaseInfo.adjuster.start()
	defer phaseInfo.adjuster.stop()

	// Metrics
	metrics := m.Metrics.Extraction
	metrics.begin()
	defer metrics.finish()

	metrics.Lock()
	metrics.TotalCnt = m.rs.InputFormat.Template.Count()
	metrics.Unlock()

	group, ctx := errgroup.WithContext(context.Background())
	namesIt := m.rs.InputFormat.Template.Iter()
ExtractAllShards:
	for name, hasNext := namesIt(); hasNext; name, hasNext = namesIt() {
		select {
		case <-m.listenAborted():
			group.Wait()
			return newAbortError(m.ManagerUUID)
		case <-ctx.Done():
			break ExtractAllShards // context was canceled, therefore we have an error
		default:
		}

		phaseInfo.adjuster.acquireGoroutineSema()
		group.Go(m.extractShard(name, metrics, cfg))
	}
	if err := group.Wait(); err != nil {
		return err
	}

	// We will no longer reserve any memory
	m.dsorter.postExtraction()

	metrics.Lock()
	totalExtractedCount := metrics.ExtractedRecordCnt
	metrics.Unlock()
	m.incrementRef(totalExtractedCount)
	return nil
}

func (m *Manager) createShard(s *extract.Shard) (err error) {
	var (
		loadContent = m.dsorter.loadContent()
		metrics     = m.Metrics.Creation

		// object related variables
		shardName = s.Name
		bucket    = m.rs.OutputBucket
		provider  = m.rs.OutputProvider

		errCh = make(chan error, 2)
	)
	lom := &cluster.LOM{T: m.ctx.t, Objname: shardName}
	if err = lom.Init(cmn.Bck{Name: bucket, Provider: provider}); err != nil {
		return
	}
	lom.SetAtimeUnix(time.Now().UnixNano())
	workFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, filetype.DSortWorkfileType, filetype.WorkfileCreateShard)

	// Check if aborted
	select {
	case <-m.listenAborted():
		return newAbortError(m.ManagerUUID)
	default:
	}

	if err := m.dsorter.preShardCreation(s.Name, lom.ParsedFQN.MpathInfo); err != nil {
		return err
	}
	defer m.dsorter.postShardCreation(lom.ParsedFQN.MpathInfo)

	// TODO -- FIXME: all targets must check t.AvgCapUsed() for high watermark *prior* to starting
	if capInfo := m.ctx.t.AvgCapUsed(nil); capInfo.OOS {
		return capInfo.Err
	}

	beforeCreation := time.Now()

	var (
		wg   = &sync.WaitGroup{}
		r, w = io.Pipe()
		n    int64
	)
	wg.Add(1)
	go func() {
		var err error
		if !m.rs.DryRun {
			err = m.ctx.t.PutObject(workFQN, r, lom, cluster.WarmGet, nil, beforeCreation)
			n = lom.Size()
		} else {
			n, err = io.Copy(ioutil.Discard, r)
		}
		errCh <- err
		wg.Done()
	}()

	_, err = m.extractCreator.CreateShard(s, w, loadContent)
	w.CloseWithError(err)
	if err != nil {
		r.CloseWithError(err)
		return err
	}

	select {
	case err = <-errCh:
		if err != nil {
			r.CloseWithError(err)
			w.CloseWithError(err)
		}
	case <-m.listenAborted():
		err = newAbortError(m.ManagerUUID)
		r.CloseWithError(err)
		w.CloseWithError(err)
	}

	wg.Wait()
	close(errCh)

	if err != nil {
		return err
	}

	si, err := cluster.HrwTarget(lom.Uname(), m.smap)
	cmn.AssertNoErr(err) // TODO -- FIXME: remove this assert, handle errors

	// If the newly created shard belongs on a different target
	// according to HRW, send it there. Since it doesn't really matter
	// if we have an extra copy of the object local to this target, we
	// optimize for performance by not removing the object now.
	if si.DaemonID != m.ctx.node.DaemonID && !m.rs.DryRun {
		lom.Lock(false)
		defer lom.Unlock(false)

		file, err := cmn.NewFileHandle(lom.FQN)
		if err != nil {
			return err
		}

		if lom.Size() <= 0 {
			goto exit
		}

		cksumType, cksumValue := lom.Cksum().Get()
		hdr := transport.Header{
			Bck:     lom.Bck().Bck,
			ObjName: shardName,
			ObjAttrs: transport.ObjectAttrs{
				Size:       lom.Size(),
				CksumType:  cksumType,
				CksumValue: cksumValue,
			},
		}

		// Make send synchronous
		streamWg := &sync.WaitGroup{}
		errCh := make(chan error, 1)
		cb := func(_ transport.Header, _ io.ReadCloser, _ unsafe.Pointer, err error) {
			errCh <- err
			streamWg.Done()
		}
		streamWg.Add(1)
		err = m.streams.shards.Send(transport.Obj{Hdr: hdr, Callback: cb}, file, si)
		if err != nil {
			return err
		}
		streamWg.Wait()
		if err := <-errCh; err != nil {
			return err
		}
	}

exit:
	metrics.Lock()
	metrics.CreatedCnt++
	if si.DaemonID != m.ctx.node.DaemonID {
		metrics.MovedShardCnt++
	}
	if m.Metrics.extended {
		dur := time.Since(beforeCreation)
		metrics.ShardCreationStats.updateTime(dur)
		metrics.ShardCreationStats.updateThroughput(n, dur)
	}
	metrics.Unlock()

	return nil
}

// participateInRecordDistribution coordinates the distributed merging and
// sorting of each target's SortedRecords based on the order defined by
// targetOrder. It returns a bool, currentTargetIsFinal, which is true iff the
// current target is the final target in targetOrder, which by construction of
// the algorithm, should contain the final, complete, sorted slice of Record
// structs.
//
// The algorithm uses the following premise: for a target T at index i in
// targetOrder, if i is even, then T will send its FileMeta slice to the target
// at index i+1 in targetOrder. If i is odd, then it will do a blocking receive
// on the FileMeta slice from the target at index i-1 in targetOrder, and will
// remove all even-indexed targets in targetOrder after receiving. This pattern
// repeats until len(targetOrder) == 1, in which case the single target in the
// slice is the final target with the final, complete, sorted slice of Record
// structs.
func (m *Manager) participateInRecordDistribution(targetOrder cluster.Nodes) (currentTargetIsFinal bool, err error) {
	var (
		i           int
		d           *cluster.Snode
		dummyTarget *cluster.Snode // dummy target is represented as nil value
	)

	// Metrics
	metrics := m.Metrics.Sorting
	metrics.begin()
	defer metrics.finish()

	expectedReceived := int32(1)
	for len(targetOrder) > 1 {
		if len(targetOrder)%2 == 1 {
			// For simplicity, we always work with an even-length slice of targets. If len(targetOrder) is odd,
			// we put a "dummy target" into the slice at index len(targetOrder)-2 which simulates sending its
			// metadata to the next target in targetOrder (which is actually itself).
			targetOrder = append(
				targetOrder[:len(targetOrder)-1],
				dummyTarget,
				targetOrder[len(targetOrder)-1],
			)
		}

		for i, d = range targetOrder {
			if d != dummyTarget && d.DaemonID == m.ctx.node.DaemonID {
				break
			}
		}

		if i%2 == 0 {
			m.dsorter.postRecordDistribution()

			beforeSend := time.Now()
			body, e := js.Marshal(m.recManager.Records)
			if e != nil {
				err = errors.Errorf("failed to marshal into JSON, err: %v", e)
				return
			}
			sendTo := targetOrder[i+1]

			query := url.Values{}
			query.Add(cmn.URLParamTotalCompressedSize, strconv.FormatInt(m.totalCompressedSize(), 10))
			query.Add(cmn.URLParamTotalUncompressedSize, strconv.FormatInt(m.totalUncompressedSize(), 10))
			query.Add(cmn.URLParamTotalInputShardsExtracted, strconv.Itoa(m.recManager.Records.Len()))
			reqArgs := &cmn.ReqArgs{
				Method: http.MethodPost,
				Base:   sendTo.URL(cmn.NetworkIntraData),
				Path:   cmn.URLPath(cmn.Version, cmn.Sort, cmn.Records, m.ManagerUUID),
				Query:  query,
				Body:   body,
			}

			if e := m.doWithAbort(reqArgs); e != nil {
				err = errors.Errorf("failed to send SortedRecords to next target (%s), err: %v", sendTo.DaemonID, e)
				return
			}

			m.recManager.Records.Drain() // we do not need it anymore

			metrics.Lock()
			metrics.SentStats.updateTime(time.Since(beforeSend))
			metrics.Unlock()
			return
		}

		beforeRecv := time.Now()

		// i%2 == 1
		receiveFrom := targetOrder[i-1]
		if receiveFrom == dummyTarget { // dummy target
			m.incrementReceived()
		}

		for m.received.count.Load() < expectedReceived {
			select {
			case <-m.listenReceived():
			case <-m.listenAborted():
				err = newAbortError(m.ManagerUUID)
				return
			}
		}
		expectedReceived++

		metrics.Lock()
		metrics.RecvStats.updateTime(time.Since(beforeRecv))
		metrics.Unlock()

		t := targetOrder[:0]
		for i, d = range targetOrder {
			if i%2 == 1 {
				t = append(t, d)
			}
		}
		targetOrder = t

		m.recManager.MergeEnqueuedRecords()
	}

	err = sortRecords(m.recManager.Records, m.rs.Algorithm)
	m.dsorter.postRecordDistribution()
	return true, err
}

func (m *Manager) generateShardsWithTemplate(maxSize int64) ([]*extract.Shard, error) {
	var (
		n               = m.recManager.Records.Len()
		names           = m.rs.OutputFormat.Template.Iter()
		shardCount      = m.rs.OutputFormat.Template.Count()
		start           int
		curShardSize    int64
		shards          = make([]*extract.Shard, 0)
		numLocalRecords = make(map[string]int, m.smap.CountTargets())
	)

	if maxSize <= 0 {
		// Heuristic: to count desired size of shard in case when maxSize is not
		// specified
		maxSize = int64(math.Ceil(float64(m.totalUncompressedSize()) / float64(shardCount)))
	}

	for i, r := range m.recManager.Records.All() {
		numLocalRecords[r.DaemonID]++
		curShardSize += r.TotalSize() + m.extractCreator.MetadataSize()*int64(len(r.Objects))
		if curShardSize < maxSize && i < n-1 {
			continue
		}

		name, hasNext := names()
		if !hasNext {
			// no more shard names are available
			return nil, errors.Errorf("number of shards to be created exceeds expected number of shards (%d)", shardCount)
		}
		shard := &extract.Shard{
			Name: name + m.rs.Extension,
		}

		shard.Size = curShardSize
		shard.Records = m.recManager.Records.Slice(start, i+1)
		shards = append(shards, shard)

		start = i + 1
		curShardSize = 0
		for k := range numLocalRecords {
			numLocalRecords[k] = 0
		}
	}

	return shards, nil
}

func (m *Manager) generateShardsWithOrderingFile(maxSize int64) ([]*extract.Shard, error) {
	var (
		cfg            = cmn.GCO.Get().DSort
		shards         = make([]*extract.Shard, 0)
		externalKeyMap = make(map[string]string)
		shardsBuilder  = make(map[string][]*extract.Shard)
	)

	if maxSize <= 0 {
		return nil, errors.New("invalid max size of shard was specified when using external key map")
	}

	resp, err := http.Get(m.rs.OrderFileURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// TODO: handle very large files > GB - in case the file is very big we
	// need to save file to the disk and operate on the file directly rather
	// than keeping everything in memory.
	var (
		lineReader = bufio.NewReader(resp.Body)
	)
	for idx := 0; ; idx++ {
		l, _, err := lineReader.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		line := strings.TrimSpace(string(l))
		if line == "" {
			continue
		}

		parts := strings.Split(line, m.rs.OrderFileSep)
		if len(parts) != 2 {
			msg := fmt.Sprintf("malformed line (%d) in external key map: %s", idx, line)
			if err := m.react(cfg.EKMMalformedLine, msg); err != nil {
				return nil, err
			}
		}

		recordKey, shardNameFmt := parts[0], parts[1]
		externalKeyMap[recordKey] = shardNameFmt
	}

	for _, r := range m.recManager.Records.All() {
		key := fmt.Sprintf("%v", r.Key)
		shardNameFmt, ok := externalKeyMap[key]
		if !ok {
			msg := fmt.Sprintf("extracted record %q which does not belong in external key map", key)
			if err := m.react(cfg.EKMMissingKey, msg); err != nil {
				return nil, err
			}
		}

		shards := shardsBuilder[shardNameFmt]
		recordSize := r.TotalSize() + m.extractCreator.MetadataSize()*int64(len(r.Objects))
		shardCount := len(shards)
		if shardCount == 0 || shards[shardCount-1].Size > maxSize {
			shard := &extract.Shard{
				Name:    fmt.Sprintf(shardNameFmt, shardCount),
				Size:    recordSize,
				Records: extract.NewRecords(1),
			}
			shard.Records.Insert(r)
			shardsBuilder[shardNameFmt] = append(shardsBuilder[shardNameFmt], shard)
		} else {
			// Append records
			lastShard := shards[shardCount-1]
			lastShard.Size += recordSize
			lastShard.Records.Insert(r)
		}
	}

	for _, s := range shardsBuilder {
		shards = append(shards, s...)
	}

	return shards, nil
}

// distributeShardRecords creates Shard structs in the order of
// dsortManager.Records corresponding to a maximum size maxSize. Each Shard is
// sent in an HTTP request to the appropriate target to create the actual file
// itself. The strategy used to determine the appropriate target differs
// depending on whether compression is used.
//
// 1) By HRW (not using compression)
// 2) By locality (using compression),
//  using two maps:
//      i) shardsToTarget - tracks the total number of shards creation requests sent to each target URL
//      ii) numLocalRecords - tracks the number of records in the current shardMeta each target has locally
//
//      The appropriate target is determined firstly by locality (i.e. the target with the most local records)
//      and secondly (if there is a tie), by least load (i.e. the target with the least number of shard creation requests
//      sent to it already).
func (m *Manager) distributeShardRecords(maxSize int64) error {
	var (
		shards []*extract.Shard
		err    error

		wg             = &sync.WaitGroup{}
		shardsToTarget = make(map[*cluster.Snode][]*extract.Shard, m.smap.CountTargets())
		sendOrder      = make(map[string]map[string]*extract.Shard, m.smap.CountTargets())

		errCh = make(chan error, m.smap.CountTargets())
	)

	for _, d := range m.smap.Tmap {
		shardsToTarget[d] = nil
		sendOrder[d.DaemonID] = make(map[string]*extract.Shard, 100)
	}

	if m.rs.OrderFileURL != "" {
		shards, err = m.generateShardsWithOrderingFile(maxSize)
	} else {
		shards, err = m.generateShardsWithTemplate(maxSize)
	}

	if err != nil {
		return err
	}

	// TODO: Following heuristic doesn't seem to be working correctly in
	// all cases. When there is not much shards at each disk (like 1-5)
	// then it may happen that some target will have more shards than other
	// targets and will "win" all output shards what will result in enormous
	// skew and result in slow creation phase (single target will be
	// responsible for creating all shards).
	//
	// if m.extractCreator.UsingCompression() {
	// 	daemonID := nodeForShardRequest(shardsToTarget, numLocalRecords)
	// 	baseURL = m.smap.GetTarget(daemonID).URL(cmn.NetworkIntraData)
	// } else {
	// 	// If output shards are not compressed, there will always be less
	// 	// data sent over the network if the shard is constructed on the
	// 	// correct HRW target as opposed to constructing it on the target
	// 	// with optimal file content locality and then sent to the correct
	// 	// target.
	// }

	bck := cluster.NewBck(m.rs.OutputBucket, m.rs.OutputProvider, cmn.NsGlobal)
	if err := bck.Init(m.ctx.bmdowner); err != nil {
		return err
	}

	for _, s := range shards {
		si, err := cluster.HrwTarget(bck.MakeUname(s.Name), m.smap)
		cmn.AssertNoErr(err)
		shardsToTarget[si] = append(shardsToTarget[si], s)

		singleSendOrder := make(map[string]*extract.Shard)
		for _, record := range s.Records.All() {
			shard, ok := singleSendOrder[record.DaemonID]
			if !ok {
				shard = &extract.Shard{
					Name:    s.Name,
					Records: extract.NewRecords(100),
				}
				singleSendOrder[record.DaemonID] = shard
			}
			shard.Records.Insert(record)
		}

		for daemonID, shard := range singleSendOrder {
			sendOrder[daemonID][shard.Name] = shard
		}
	}

	m.recManager.Records.Drain()

	for si, s := range shardsToTarget {
		wg.Add(1)
		go func(si *cluster.Snode, s []*extract.Shard, order map[string]*extract.Shard) {
			defer wg.Done()

			body, err := js.Marshal(creationPhaseMetadata{
				Shards:    s,
				SendOrder: order,
			})
			if err != nil {
				errCh <- err
				return
			}

			query := cmn.AddBckToQuery(nil, cmn.Bck{Provider: m.rs.Provider, Ns: cmn.NsGlobal})
			reqArgs := &cmn.ReqArgs{
				Method: http.MethodPost,
				Base:   si.URL(cmn.NetworkIntraData),
				Path:   cmn.URLPath(cmn.Version, cmn.Sort, cmn.Shards, m.ManagerUUID),
				Query:  query,
				Body:   body,
			}
			if err := m.doWithAbort(reqArgs); err != nil {
				errCh <- err
				return
			}
		}(si, s, sendOrder[si.DaemonID])
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		return errors.Errorf("error while sending shards, err: %v", err)
	}
	glog.Infof("finished sending all shards")
	return nil
}

// nodeForShardRequest returns the optimal daemon id for a shard
// creation request. The target chosen is determined based on:
//  1) Locality of shard source files, and in a tie situation,
//  2) Number of shard creation requests previously sent to the target.
//
// nolint:unused,deadcode // has TODO to fix it
func nodeForShardRequest(shardsToTarget map[string][]*extract.Shard, numLocalRecords map[string]int) string {
	var max int
	var id string
	var numSentToCur int

	for node, localRecordCount := range numLocalRecords {
		if localRecordCount > max {
			numSentToCur = len(shardsToTarget[node])
			max = localRecordCount
			id = node
		} else if localRecordCount == max {
			// If a shard has equal number of source files in multiple targets,
			// send request to the target with the least requests sent to it so
			// far.
			if len(shardsToTarget[node]) < numSentToCur {
				numSentToCur = len(shardsToTarget[node])
				id = node
			}
		}
	}
	return id
}

// randomTargetOrder returns a cluster.Snode slice for targets in a pseudorandom order.
func randomTargetOrder(salt uint64, tmap cluster.NodeMap) []*cluster.Snode {
	targets := make(map[uint64]*cluster.Snode, len(tmap))
	keys := make([]uint64, 0, len(tmap))
	for i, d := range tmap {
		c := xxhash.ChecksumString64S(i, salt)
		targets[c] = d
		keys = append(keys, c)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	t := make([]*cluster.Snode, len(keys))
	for i, k := range keys {
		t[i] = targets[k]
	}
	return t
}
