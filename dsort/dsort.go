/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
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

	glog.Infof("starting dsort %s", m.ManagerUUID)

	// start watching memory
	if err := m.mw.watch(); err != nil {
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
		if err = m.distributeShardRecords(shardSize); err != nil {
			return err
		}
	}

	// In shard creation we should not expect memory increase (at least not from
	// dSort). Also it would be really hard to have concurrent sends and memory
	// cleanup.
	m.mw.stopWatchingExcess()
	runtime.GC()
	debug.FreeOSMemory()

	// After each target participates in the cluster-wide record distribution,
	// start listening for the signal to start creating shards locally.
	if err := m.createShardsLocally(); err != nil {
		return err
	}

	glog.Infof("finished dsort %s successfully", m.ManagerUUID)
	return nil
}

// extractLocalShards iterates through files local to the current target and
// calls ExtractShard on matching files based on the given ParsedRequestSpec.
func (m *Manager) extractLocalShards() (err error) {
	var (
		cfg                 = cmn.GCO.Get().DSort
		totalExtractedCount atomic.Uint64
	)

	// Metrics
	metrics := m.Metrics.Extraction
	metrics.begin()
	defer metrics.finish()

	metrics.Lock()
	metrics.ToSeenCnt = m.rs.InputFormat.Template.Count()
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

		m.acquireExtractGoroutineSema()
		extractShard := func(name string) func() error {
			return func() error {
				var (
					beforeExtraction time.Time
				)

				if m.Metrics.extended {
					beforeExtraction = time.Now()
				}

				metrics.Lock()
				metrics.SeenCnt++
				metrics.Unlock()

				defer m.releaseExtractGoroutineSema()

				shardName := name + m.rs.Extension
				si, errStr := cluster.HrwTarget(m.rs.Bucket, shardName, m.smap)
				if errStr != "" {
					return errors.New(errStr)
				}
				if si.DaemonID != m.ctx.node.DaemonID {
					return nil
				}

				lom, errMsg := cluster.LOM{T: m.ctx.t, Objname: shardName, Bucket: m.rs.Bucket, BucketProvider: m.rs.BckProvider}.Init()
				if errMsg != "" {
					return errors.New(errMsg)
				}
				_, errMsg = lom.Load(true)
				if errMsg != "" {
					return errors.New(errMsg)
				} else if !lom.Exists() {
					msg := fmt.Sprintf("shard %q does not exist (is missing)", shardName)
					return m.react(cfg.MissingShards, msg)
				}

				m.acquireExtractSema()
				if m.aborted() {
					m.releaseExtractSema()
					return newAbortError(m.ManagerUUID)
				}
				if _, oos := m.ctx.t.AvgCapUsed(nil); oos {
					m.releaseExtractSema()
					return errors.New("out of space")
				}

				m.ctx.nameLocker.Lock(lom.Uname(), false)
				f, err := os.Open(lom.FQN)
				if err != nil {
					m.releaseExtractSema()
					m.ctx.nameLocker.Unlock(lom.Uname(), false)
					return fmt.Errorf("unable to open local file, err: %v", err)
				}
				var compressedSize int64
				if m.extractCreator.UsingCompression() {
					compressedSize = lom.Size()
				}

				expectedUncompressedSize := uint64(float64(lom.Size()) / m.avgCompressionRatio())
				toDisk := m.mw.reserveMem(expectedUncompressedSize)

				reader := io.NewSectionReader(f, 0, lom.Size())
				extractedSize, extractedCount, err := m.extractCreator.ExtractShard(lom.FQN, reader, m.recManager, toDisk)

				// Make sure that compression rate is updated before releasing
				// next extractor goroutine.
				if m.extractCreator.UsingCompression() {
					m.addCompressionSizes(compressedSize, extractedSize)
				}

				m.releaseExtractSema()
				m.ctx.nameLocker.Unlock(lom.Uname(), false)

				m.mw.unreserveMem(expectedUncompressedSize) // schedule unreserving reserved memory on next memory update
				if err != nil {
					f.Close()
					return fmt.Errorf("error in ExtractShard, file: %s, err: %v", f.Name(), err)
				}
				f.Close()
				m.addToTotalInputShardsSeen(1)

				metrics.Lock()
				metrics.ExtractedRecordCnt += extractedCount
				metrics.ExtractedCnt++
				metrics.ExtractedSize += extractedSize
				if toDisk {
					metrics.ExtractedToDiskCnt++
					metrics.ExtractedToDiskSize += extractedSize
				}
				if m.Metrics.extended {
					metrics.ShardExtractionStats.update(time.Since(beforeExtraction))
				}
				metrics.Unlock()

				totalExtractedCount.Add(uint64(extractedCount))
				return nil
			}
		}(name)

		group.Go(extractShard)
	}
	if err := group.Wait(); err != nil {
		return err
	}

	// We will no longer reserve any memory
	m.mw.stopWatchingReserved()

	// FIXME: maybe there is a way to check this faster or earlier?
	//
	// Checking if all records have keys (keys are not nil). Algorithms other
	// than content kind should have keys, it is a bug if they don't.
	if m.rs.Algorithm.Kind == SortKindContent {
		if err := m.recManager.Records.EnsureKeys(); err != nil {
			return err
		}
	}

	m.incrementRef(int64(totalExtractedCount.Load()))
	return nil
}

func (m *Manager) createShard(s *extract.Shard) (err error) {
	var (
		beforeCreation time.Time
		loadContent    = m.loadContent()
		metrics        = m.Metrics.Creation

		// object related variables
		shardName   = s.Name
		bucket      = m.rs.OutputBucket
		bckProvider = m.rs.OutputBckProvider

		errCh = make(chan error, 2)
	)

	if m.Metrics.extended {
		beforeCreation = time.Now()
	}

	lom, errStr := cluster.LOM{T: m.ctx.t, Bucket: bucket, Objname: shardName, BucketProvider: bckProvider}.Init()
	if errStr != "" {
		return errors.New(errStr)
	}
	workFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, filetype.DSortWorkfileType, filetype.WorkfileCreateShard)

	// Check if aborted
	select {
	case <-m.listenAborted():
		return newAbortError(m.ManagerUUID)
	default:
	}

	m.acquireCreateSema()
	defer m.releaseCreateSema()

	if _, oos := m.ctx.t.AvgCapUsed(nil); oos {
		return errors.New("out of space")
	}

	wg := &sync.WaitGroup{}
	r, w := io.Pipe()
	wg.Add(2)
	go func() {
		err := m.ctx.t.Receive(workFQN, r, lom, cluster.WarmGet, nil)
		errCh <- err
		wg.Done()
	}()

	go func() {
		_, err := m.extractCreator.CreateShard(s, w, loadContent)
		errCh <- err
		w.CloseWithError(err) // if `nil`, the writer will close with EOF
		wg.Done()
	}()

	finishes := 0
	for finishes < 2 && err == nil {
		select {
		case err = <-errCh:
			if err != nil {
				r.CloseWithError(err)
				w.CloseWithError(err)
			}
			finishes++
		case <-m.listenAborted():
			err = newAbortError(m.ManagerUUID)
			r.CloseWithError(err)
			w.CloseWithError(err)
		}
	}

	wg.Wait()
	close(errCh)

	if err != nil {
		return err
	}

	si, errStr := cluster.HrwTarget(bucket, shardName, m.smap)
	if errStr != "" {
		return errors.New(errStr)
	}

	// If the newly created shard belongs on a different target
	// according to HRW, send it there. Since it doesn't really matter
	// if we have an extra copy of the object local to this target, we
	// optimize for performance by not removing the object now.
	if si.DaemonID != m.ctx.node.DaemonID {
		m.ctx.nameLocker.Lock(lom.Uname(), false)
		defer m.ctx.nameLocker.Unlock(lom.Uname(), false)

		file, err := cmn.NewFileHandle(lom.FQN)
		if err != nil {
			return err
		}

		stat, err := file.Stat()
		if err != nil {
			return err
		}

		if stat.Size() <= 0 {
			goto exit
		}

		cksumType, cksumValue := lom.Cksum().Get()
		hdr := transport.Header{
			Bucket:  bucket,
			Objname: shardName,
			IsLocal: bckProvider == cmn.LocalBs,
			ObjAttrs: transport.ObjectAttrs{
				Size:       stat.Size(),
				CksumType:  cksumType,
				CksumValue: cksumValue,
			},
		}

		// Make send synchronous
		streamWg := &sync.WaitGroup{}
		errCh := make(chan error, 1)
		streamWg.Add(1)
		err = m.streams.shards[si.DaemonID].Get().Send(hdr, file, func(_ transport.Header, _ io.ReadCloser, err error) {
			errCh <- err
			streamWg.Done()
		})
		if err != nil {
			return err
		}
		streamWg.Wait()
		if err = <-errCh; err != nil {
			return err
		}
	}

exit:
	if m.Metrics.extended {
		metrics.Lock()
		metrics.CreatedCnt++
		metrics.ShardCreationStats.update(time.Since(beforeCreation))
		if si.DaemonID != m.ctx.node.DaemonID {
			metrics.MovedShardCnt++
		}
		metrics.Unlock()
	}

	return nil
}

// createShardsLocally waits until it's given the signal to start creating
// shards, then creates shards in parallel.
func (m *Manager) createShardsLocally() (err error) {
	// Wait for signal to start shard creations. This will happen when manager
	// notice that specificion for shards to be created locally was received.
	select {
	case <-m.startShardCreation:
		break
	case <-m.listenAborted():
		return newAbortError(m.ManagerUUID)
	}

	metrics := m.Metrics.Creation
	metrics.begin()
	defer metrics.finish()
	metrics.Lock()
	metrics.ToCreate = len(m.shardManager.Shards)
	metrics.Unlock()

	group, ctx := errgroup.WithContext(context.Background())

CreateAllShards:
	for _, s := range m.shardManager.Shards {
		select {
		case <-m.listenAborted():
			group.Wait()
			return newAbortError(m.ManagerUUID)
		case <-ctx.Done():
			break CreateAllShards // context was canceled, therefore we have an error
		default:
		}

		m.acquireCreateGoroutineSema()
		group.Go(func(s *extract.Shard) func() error {
			return func() error {
				err := m.createShard(s)
				m.releaseCreateGoroutineSema()
				return err
			}
		}(s))
	}

	return group.Wait()
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
func (m *Manager) participateInRecordDistribution(targetOrder []*cluster.Snode) (currentTargetIsFinal bool, err error) {
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
			beforeSend := time.Now()
			body, e := js.Marshal(m.recManager.Records)
			if e != nil {
				err = fmt.Errorf("failed to marshal into JSON, err: %v", e)
				return
			}
			sendTo := targetOrder[i+1]
			u := sendTo.URL(cmn.NetworkIntraData) + fmt.Sprintf(
				"%s?%s=%d&%s=%d&%s=%d",
				cmn.URLPath(cmn.Version, cmn.Sort, cmn.Records, m.ManagerUUID),
				cmn.URLParamTotalCompressedSize, m.totalCompressedSize(),
				cmn.URLParamTotalUncompressedSize, m.totalUncompressedSize(),
				cmn.URLParamTotalInputShardsSeen, m.totalInputShardsSeen.Load(),
			)
			if _, e := m.doWithAbort(http.MethodPost, u, body, nil); e != nil {
				err = fmt.Errorf("failed to send SortedRecords to next target (%s), err: %v", sendTo.DaemonID, e)
				return
			}

			m.recManager.Records.Drain() // we do not need it anymore

			if m.Metrics.extended {
				metrics.Lock()
				metrics.SentStats.update(time.Since(beforeSend))
				metrics.Unlock()
			}
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

		if m.Metrics.extended {
			metrics.Lock()
			metrics.RecvStats.update(time.Since(beforeRecv))
			metrics.Unlock()
		}

		t := targetOrder[:0]
		for i, d = range targetOrder {
			if i%2 == 1 {
				t = append(t, d)
			}
		}
		targetOrder = t

		m.recManager.MergeEnqueuedRecords()
	}

	sortRecords(m.recManager.Records, m.rs.Algorithm)
	return true, nil
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
		n               = m.recManager.Records.Len()
		names           = m.rs.OutputFormat.Template.Iter()
		shardCount      = m.rs.OutputFormat.Template.Count()
		start           int
		curShardSize    int64
		wg              = &sync.WaitGroup{}
		errCh           = make(chan error, m.smap.CountTargets())
		shardsToTarget  = make(map[string][]*extract.Shard, m.smap.CountTargets())
		numLocalRecords = make(map[string]int, m.smap.CountTargets())
	)

	if maxSize <= 0 {
		// Heuristic: to count desired size of shard in case when maxSize is not
		// specified
		maxSize = int64(math.Ceil(float64(m.totalUncompressedSize()) / float64(shardCount)))
	}

	for _, d := range m.smap.Tmap {
		numLocalRecords[d.URL(cmn.NetworkIntraData)] = 0
		shardsToTarget[d.URL(cmn.NetworkIntraData)] = nil
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
			return fmt.Errorf("number of shards to be created exceeds number of expected shards (%d)", shardCount)
		}
		shard := &extract.Shard{
			Name: name + m.rs.Extension,
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

		si, errStr := cluster.HrwTarget(m.rs.OutputBucket, shard.Name, m.smap)
		if errStr != "" {
			return errors.New(errStr)
		}
		baseURL := si.URL(cmn.NetworkIntraData)

		shard.Size = curShardSize
		shard.Records = m.recManager.Records.Slice(start, i+1)
		shardsToTarget[baseURL] = append(shardsToTarget[baseURL], shard)

		start = i + 1
		curShardSize = 0
		for k := range numLocalRecords {
			numLocalRecords[k] = 0
		}
	}

	m.recManager.Records.Drain()

	for u, s := range shardsToTarget {
		wg.Add(1)
		go func(u string, s []*extract.Shard) {
			defer wg.Done()
			body, err := js.Marshal(s)
			if err != nil {
				errCh <- err
				return
			}
			u += fmt.Sprintf(
				"%s?%s=%s",
				cmn.URLPath(cmn.Version, cmn.Sort, cmn.Shards, m.ManagerUUID),
				cmn.URLParamBckProvider, m.rs.BckProvider,
			)
			if _, err = m.doWithAbort(http.MethodPost, u, body, nil); err != nil {
				errCh <- err
				return
			}
		}(u, s)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		return fmt.Errorf("error while sending shards, err: %v", err)
	}
	glog.Infof("finished sending all shards")
	return nil
}

// nodeForShardRequest returns the optimal daemon id for a shard
// creation request. The target chosen is determined based on:
//  1) Locality of shard source files, and in a tie situation,
//  2) Number of shard creation requests previously sent to the target.
//nolint:unused, deadcode
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
func randomTargetOrder(salt uint64, tmap map[string]*cluster.Snode) []*cluster.Snode {
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
