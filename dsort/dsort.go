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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dsort/extract"
	"github.com/NVIDIA/dfcpub/dsort/filetype"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/transport"
	"github.com/OneOfOne/xxhash"
	sigar "github.com/cloudfoundry/gosigar"
	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

const (
	memoryUpdateInterval      = 50 * time.Millisecond
	unreserveMemoryBufferSize = 100000
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

	// Phase 1.
	if err := m.extractLocalShards(); err != nil {
		return err
	}

	s := binary.BigEndian.Uint64(m.rs.TargetOrderSalt)
	targetOrder := randomTargetOrder(s, m.ctx.smap.Get().Tmap)
	glog.V(4).Infof("final target in targetOrder => URL: %s, Daemon ID: %s",
		targetOrder[len(targetOrder)-1].PublicNet.DirectURL, targetOrder[len(targetOrder)-1].DaemonID)

	// Phase 2.
	curTargetIsFinal, err := m.participateInRecordDistribution(targetOrder)
	if err != nil {
		return err
	}
	runtime.GC()
	debug.FreeOSMemory()

	// After each target participates in the cluster-wide record distribution, start listening for the signal
	// to start creating shards locally.
	shardCreation := &sync.WaitGroup{}
	shardCreation.Add(1)
	shardCreationErrCh := make(chan error, 1)
	go func() {
		shardCreationErrCh <- m.createShardsLocally()
		shardCreation.Done()
	}()

	// Run phase 3. only if you are final target (and actually have any sorted records)
	if curTargetIsFinal && m.recManager.Records.Len() > 0 {
		shardSize := m.rs.OutputShardSize
		if m.extractCreator.UsingCompression() {
			// By making the assumption that the input content is reasonably uniform across all shards,
			// the output shard size required (such that each gzip compressed output shard will have
			// a size close to rs.ShardSizeBytes) can be estimated.
			avgCompressRatio := m.avgCompressionRatio()
			shardSize = int64(float64(m.rs.OutputShardSize) / avgCompressRatio)
			glog.V(4).Infof("estimated output shard size required before gzip compression: %d", shardSize)
		}

		// Phase 3.
		if err = m.distributeShardRecords(shardSize); err != nil {
			return err
		}
	}

	shardCreation.Wait()
	if err := <-shardCreationErrCh; err != nil {
		return err
	}

	glog.Infof("finished dsort %s successfully", m.ManagerUUID)
	return nil
}

// extractLocalShards iterates through files local to the current target and
// calls ExtractShard on matching files based on the given ParsedRequestSpec.
func (m *Manager) extractLocalShards() (err error) {
	var (
		mem                 = sigar.Mem{}
		smap                = m.ctx.smap.Get()
		maxMemoryToUse      = uint64(0)
		totalExtractedCount = uint64(0)
	)

	// Metrics
	metrics := m.Metrics.Extraction
	metrics.begin()
	defer metrics.finish()

	if err := mem.Get(); err != nil {
		return err
	}
	if maxMemoryToUse, err = m.maxMemoryUsage(); err != nil {
		return err
	}
	memoryUsed := mem.ActualUsed
	reservedMemory := uint64(0)
	unreserveMemoryCh := make(chan uint64, unreserveMemoryBufferSize)

	// Starting memory updater. Since extraction phase is concurrent and we
	// cannot know how much memory will given compressed shard extract we need
	// to employ mechanism for updating memory. Just before extraction we
	// estimate how much memory shard will contain (by multiplying file size and
	// avg compress ratio). Then we update currently used memory to actual used
	// + reserved. After we finish extraction we put reserved memory for the
	// shard into the unreserve memory channel. Note that we cannot unreserve it
	// right away because actual used memory has not yet been updated (but it
	// surely changed). Once memory updater will update actually used memory we
	// can drain the channel and unreserve memory. This way it is almost
	// impossible exceed maximum memory to used - but unfortunately this can
	// happen when we underestimate the number of memory to be used when
	// extracting compressed files.
	ticker := time.NewTicker(memoryUpdateInterval)
	go func() {
		for range ticker.C {
			curMem := sigar.Mem{}
			if err := curMem.Get(); err == nil {
				atomic.SwapUint64(&memoryUsed, curMem.ActualUsed)

				unreserve := true
				for unreserve {
					select {
					case size := <-unreserveMemoryCh:
						atomic.AddUint64(&reservedMemory, ^uint64(size-1)) // decrement by size
					default:
						unreserve = false
					}
				}
			}
		}
	}()

	metrics.Lock()
	metrics.ToSeenCnt = m.rs.InputFormat.RangeCount
	metrics.Unlock()

	group, ctx := errgroup.WithContext(context.Background())
ExtractAllShards:
	for i := m.rs.InputFormat.Start; i <= m.rs.InputFormat.End; i += m.rs.InputFormat.Step {
		select {
		case <-m.listenAborted():
			group.Wait()
			return newAbortError(m.ManagerUUID)
		case <-ctx.Done():
			break ExtractAllShards // context was canceled, therefore we have an error
		default:
		}

		m.acquireExtractGoroutineSema()
		extractShard := func(i int) func() error {
			return func() error {
				metrics.Lock()
				metrics.SeenCnt++
				metrics.Unlock()

				defer m.releaseExtractGoroutineSema()

				shardName := m.rs.InputFormat.Prefix + fmt.Sprintf("%0*d", m.rs.InputFormat.DigitCount, i) + m.rs.InputFormat.Suffix + m.rs.Extension
				si, errStr := cluster.HrwTarget(m.rs.Bucket, shardName, smap)
				if errStr != "" {
					return errors.New(errStr)
				}
				if si.DaemonID != m.ctx.node.DaemonID {
					return nil
				}
				fqn, errStr := cluster.FQN(fs.ObjectType, m.rs.Bucket, shardName, m.rs.IsLocalBucket)
				if errStr != "" {
					return errors.New(errStr)
				}

				m.acquireExtractSema()
				if m.aborted() {
					m.releaseExtractSema()
					return newAbortError(m.ManagerUUID)
				}

				f, err := os.Open(fqn)
				if err != nil {
					m.releaseExtractSema()
					return fmt.Errorf("unable to open local file, err: %v", err)
				}
				var compressedSize int64
				fi, err := f.Stat()
				if err != nil {
					f.Close()
					m.releaseExtractSema()
					return err
				}

				if m.extractCreator.UsingCompression() {
					compressedSize = fi.Size()
				}

				expectedUncompressedSize := uint64(float64(fi.Size()) / m.avgCompressionRatio())
				reservedMemoryTmp := atomic.AddUint64(&reservedMemory, expectedUncompressedSize)

				// expected total memory after all objects will be extracted is equal
				// to: previously reserved memory + uncompressed size of shard + current memory used
				expectedTotalMemoryUsed := reservedMemoryTmp + atomic.LoadUint64(&memoryUsed)

				// Switch to extracting to disk if we hit this target's memory usage threshold.
				var toDisk bool
				if expectedTotalMemoryUsed >= maxMemoryToUse {
					toDisk = true
				}

				reader := io.NewSectionReader(f, 0, fi.Size())
				extractedSize, extractedCount, err := m.extractCreator.ExtractShard(fqn, reader, m.recManager, toDisk)
				m.releaseExtractSema()

				unreserveMemoryCh <- expectedUncompressedSize // schedule unreserving memory on next memory update
				if err != nil {
					f.Close()
					return fmt.Errorf("error in ExtractShard, file: %s, err: %v", f.Name(), err)
				}
				f.Close()
				m.addToTotalInputShardsSeen(1)
				if m.extractCreator.UsingCompression() {
					m.addCompressionSizes(compressedSize, extractedSize)
				}

				metrics.Lock()
				metrics.ExtractedRecordCnt += extractedCount
				metrics.ExtractedCnt++
				metrics.ExtractedSize += extractedSize
				if toDisk {
					metrics.ExtractedToDiskCnt++
					metrics.ExtractedToDiskSize += extractedSize
				}
				metrics.Unlock()

				atomic.AddUint64(&totalExtractedCount, uint64(extractedCount))
				return nil
			}
		}(i)

		group.Go(extractShard)
	}
	if err := group.Wait(); err != nil {
		return err
	}

	// FIXME: maybe there is a way to check this faster or earlier?
	//
	// Checking if all records have keys (keys are not nil). Algorithms other
	// than content kind should have keys, it is a bug if they don't.
	if m.rs.Algorithm.Kind == SortKindContent {
		if err := m.recManager.Records.EnsureKeys(); err != nil {
			return err
		}
	}

	m.incrementRef(int64(totalExtractedCount))
	return nil
}

func (m *Manager) createShard(s *extract.Shard) (err error) {
	loadContent := m.loadContent()
	metrics := m.Metrics.Creation

	var (
		shardFile *os.File
	)

	fqn, errStr := cluster.FQN(fs.ObjectType, s.Bucket, s.Name, s.IsLocal)
	if errStr != "" {
		return errors.New(errStr)
	}
	workFQN := fs.CSM.GenContentFQN(fqn, filetype.DSortWorkfileType, "")

	// Check if aborted
	select {
	case <-m.listenAborted():
		return newAbortError(m.ManagerUUID)
	default:
	}

	m.acquireCreateSema()
	shardFile, err = cmn.CreateFile(workFQN)
	if err != nil {
		return fmt.Errorf("failed to create new shard file, err: %v", err)
	}

	_, err = m.extractCreator.CreateShard(s, shardFile, loadContent)
	shardFile.Close()
	if err != nil {
		m.releaseCreateSema()
		return err
	}

	if err := cmn.MvFile(workFQN, fqn); err != nil {
		m.releaseCreateSema()
		return err
	}
	m.releaseCreateSema()

	metrics.Lock()
	metrics.CreatedCnt++
	metrics.Unlock()

	si, errStr := cluster.HrwTarget(s.Bucket, s.Name, m.ctx.smap.Get())
	if errStr != "" {
		return errors.New(errStr)
	}

	// If the newly created shard belongs on a different target
	// according to HRW, send it there. Since it doesn't really matter
	// if we have an extra copy of the object local to this target, we
	// optimize for performance by not removing the object now.
	if si.DaemonID != m.ctx.node.DaemonID {
		file, err := cmn.NewFileHandle(fqn)
		if err != nil {
			return err
		}

		stat, err := file.Stat()
		if err != nil {
			return err
		}

		if stat.Size() <= 0 {
			return nil
		}

		hdr := transport.Header{
			Bucket:  s.Bucket,
			Objname: s.Name,
			Dsize:   stat.Size(),
			Opaque:  strconv.AppendBool([]byte{}, s.IsLocal),
		}

		// Make send synchronous
		streamWg := &sync.WaitGroup{}
		errCh := make(chan error, 1)
		streamWg.Add(1)
		m.acquireCreateSema() // need to acquire sema because we will be reading file
		err = m.streams.shards[si.DaemonID].Get().Send(hdr, file, func(_ transport.Header, _ io.ReadCloser, err error) {
			errCh <- err
			streamWg.Done()
		})
		m.releaseCreateSema()
		if err != nil {
			return err
		}
		streamWg.Wait()
		if err = <-errCh; err != nil {
			return err
		}

		metrics.Lock()
		metrics.MovedShardCnt++
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
		dummyTarget *cluster.Snode = nil // dummy target is represented as nil value
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
			body, e := js.Marshal(m.recManager.Records)
			if e != nil {
				err = fmt.Errorf("failed to marshal into JSON, err: %v", e)
				return
			}
			sendTo := targetOrder[i+1]
			u := sendTo.IntraDataNet.DirectURL + fmt.Sprintf(
				"%s?%s=%d&%s=%d&%s=%d",
				cmn.URLPath(cmn.Version, cmn.Sort, cmn.Records, m.ManagerUUID),
				cmn.URLParamTotalCompressedSize, m.totalCompressedSize(),
				cmn.URLParamTotalUncompressedSize, m.totalUncompressedSize(),
				cmn.URLParamTotalInputShardsSeen, m.totalInputShardsSeen,
			)
			if _, e := m.doWithAbort(http.MethodPost, u, body, nil); e != nil {
				err = fmt.Errorf("failed to send SortedRecords to next target (%s), err: %v", sendTo.DaemonID, e)
				return
			}

			m.recManager.Records.Drain() // we do not need it anymore

			metrics.Lock()
			metrics.SentCnt++
			metrics.Unlock()
			return
		}

		// i%2 == 1
		receiveFrom := targetOrder[i-1]
		if receiveFrom == dummyTarget { // dummy target
			m.incrementReceived()
		}

		for atomic.LoadInt32(&m.received.count) < expectedReceived {
			select {
			case <-m.listenReceived():
			case <-m.listenAborted():
				err = newAbortError(m.ManagerUUID)
				return
			}
		}
		expectedReceived++

		metrics.Lock()
		metrics.RecvCnt++
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
		shardNum        = m.rs.OutputFormat.Start
		shardCount      = m.rs.OutputFormat.RangeCount
		start           int
		curShardSize    int64
		baseURL         string
		ext             = m.fileExtension
		wg              = &sync.WaitGroup{}
		smap            = m.ctx.smap.Get()
		errCh           = make(chan error, smap.CountTargets())
		shardsToTarget  = make(map[string][]*extract.Shard, smap.CountTargets())
		numLocalRecords = make(map[string]int, smap.CountTargets())
	)

	if maxSize <= 0 {
		// Heuristic: to count desired size of shard in case when maxSize is not
		// specified
		maxSize = int64(math.Ceil(float64(m.totalUncompressedSize()) / float64(shardCount)))
	}

	for _, d := range smap.Tmap {
		numLocalRecords[d.URL(cmn.NetworkIntraData)] = 0
		shardsToTarget[d.URL(cmn.NetworkIntraData)] = nil
	}
	for i, r := range m.recManager.Records.All() {
		numLocalRecords[r.DaemonID]++
		curShardSize += r.TotalSize() + m.extractCreator.MetadataSize()*int64(len(r.Objects))
		if curShardSize < maxSize && i < n-1 {
			continue
		}

		shard := &extract.Shard{
			Name:    m.rs.OutputFormat.Prefix + fmt.Sprintf("%0*d", m.rs.OutputFormat.DigitCount, shardNum) + m.rs.OutputFormat.Suffix + ext,
			Bucket:  m.rs.Bucket,
			IsLocal: m.rs.IsLocalBucket,
		}
		if m.extractCreator.UsingCompression() {
			daemonID := nodeForShardRequest(shardsToTarget, numLocalRecords)
			baseURL = m.ctx.smap.Get().Tmap[daemonID].URL(cmn.NetworkIntraData)
		} else {
			// If output shards are not compressed, there will always be less
			// data sent over the network if the shard is constructed on the
			// correct HRW target as opposed to constructing it on the target
			// with optimal file content locality and then sent to the correct
			// target.
			si, errStr := cluster.HrwTarget(m.rs.Bucket, shard.Name, smap)
			if errStr != "" {
				return errors.New(errStr)
			}
			baseURL = si.URL(cmn.NetworkIntraData)
		}
		shard.Size = curShardSize
		shard.Records = m.recManager.Records.Slice(start, i+1)
		shardsToTarget[baseURL] = append(shardsToTarget[baseURL], shard)

		start = i + 1
		shardNum += m.rs.OutputFormat.Step
		curShardSize = 0
		for k := range numLocalRecords {
			numLocalRecords[k] = 0
		}
	}

	m.recManager.Records.Drain()

	if shardNum > m.rs.OutputFormat.End {
		createdCount := shardNum / m.rs.OutputFormat.Step
		expectedCount := m.rs.OutputFormat.RangeCount
		return fmt.Errorf("number of shards to be created (%d) exceeds number of expected shards (%d)", createdCount, expectedCount)
	}

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
				"%s?%s=%t",
				cmn.URLPath(cmn.Version, cmn.Sort, cmn.Shards, m.ManagerUUID),
				cmn.URLParamLocal, m.rs.IsLocalBucket,
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
	glog.Infof("finished sending all %d shards", shardNum)
	return nil
}

// nodeForShardRequest returns the optimal daemon id for a shard
// creation request. The target chosen is determined based on:
//  1) Locality of shard source files, and in a tie situation,
//  2) Number of shard creation requests previously sent to the target.
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
	var keys []uint64
	for i, d := range tmap {
		c := xxhash.Checksum64S([]byte(i), salt)
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
