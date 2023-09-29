// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/tinylib/msgp/msgp"
	"golang.org/x/sync/errgroup"
)

const PrefixJobID = "srt-"

type (
	receiver interface {
		recvReq(hdr transport.ObjHdr, objReader io.Reader, err error) error // aka transport.RecvObj
	}
	dsorter interface {
		shard.ContentLoader
		receiver

		name() string
		init() error
		start() error
		postExtraction()
		postRecordDistribution()
		createShardsLocally() (err error)
		preShardCreation(shardName string, mi *fs.Mountpath) error
		postShardCreation(mi *fs.Mountpath)
		cleanup()
		finalCleanup() error
		preShardExtraction(expectedUncompressedSize uint64) (toDisk bool)
		postShardExtraction(expectedUncompressedSize uint64)
		onAbort()
	}
)

var js = jsoniter.ConfigFastest

func (m *Manager) finish() {
	if m.config.FastV(4, cos.SmoduleDsort) {
		nlog.Infof("%s: %s finished", g.t, m.ManagerUUID)
	}
	m.lock()
	m.setInProgressTo(false)
	m.unlock()

	// Trigger decrement reference counter. If it is already 0 it will
	// trigger cleanup because progress is set to false. Otherwise, the
	// cleanup will be triggered by decrementRef in load content handlers.
	m.decrementRef(0)
}

func (m *Manager) start() (err error) {
	defer m.finish()

	if err := m.startDSorter(); err != nil {
		return err
	}

	// Phase 1.
	nlog.Infof("%s: %s started extraction stage", g.t, m.ManagerUUID)
	if err := m.extractLocalShards(); err != nil {
		return err
	}

	s := binary.BigEndian.Uint64(m.Pars.TargetOrderSalt)
	targetOrder := _torder(s, m.smap.Tmap)
	if m.config.FastV(4, cos.SmoduleDsort) {
		nlog.Infof("%s: %s final target in targetOrder => URL: %s, tid %s", g.t, m.ManagerUUID,
			targetOrder[len(targetOrder)-1].PubNet.URL, targetOrder[len(targetOrder)-1].ID())
	}

	// Phase 2.
	nlog.Infof("%s: %s started sort stage", g.t, m.ManagerUUID)
	curTargetIsFinal, err := m.participateInRecordDistribution(targetOrder)
	if err != nil {
		return err
	}

	// Phase 3. - run only by the final target
	if curTargetIsFinal {
		// assuming uniform distribution estimate avg. output shard size
		ratio := m.compressionRatio()
		if m.config.FastV(4, cos.SmoduleDsort) {
			nlog.Infof("%s [dsort] %s phase3: ratio=%f", g.t, m.ManagerUUID, ratio)
		}
		debug.Assertf(shard.IsCompressed(m.Pars.InputExtension) || ratio == 1, "tar ratio=%f, ext=%q",
			ratio, m.Pars.InputExtension)

		shardSize := int64(float64(m.Pars.OutputShardSize) / ratio)
		nlog.Infof("%s: [dsort] %s started phase 3: ratio=%f, shard size (%d, %d)",
			g.t, m.ManagerUUID, shardSize, m.Pars.OutputShardSize)
		if err := m.phase3(shardSize); err != nil {
			nlog.Errorf("%s: [dsort] %s phase3 err: %v", g.t, m.ManagerUUID, err)
			return err
		}
	}

	// Wait for signal to start shard creations. This will happen when manager
	// notice that the specification for shards to be created locally was received.
	select {
	case <-m.startShardCreation:
		break
	case <-m.listenAborted():
		return m.newErrAborted()
	}

	// After each target participates in the cluster-wide record distribution,
	// start listening for the signal to start creating shards locally.
	nlog.Infof("%s: %s started creation stage", g.t, m.ManagerUUID)
	if err := m.dsorter.createShardsLocally(); err != nil {
		return err
	}

	nlog.Infof("%s: %s finished successfully", g.t, m.ManagerUUID)
	return nil
}

// returns a slice of targets in a pseudorandom order
func _torder(salt uint64, tmap meta.NodeMap) []*meta.Snode {
	var (
		targets = make(map[uint64]*meta.Snode, len(tmap))
		keys    = make([]uint64, 0, len(tmap))
	)
	for i, d := range tmap {
		if d.InMaintOrDecomm() {
			continue
		}
		c := xxhash.Checksum64S(cos.UnsafeB(i), salt)
		targets[c] = d
		keys = append(keys, c)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	t := make(meta.Nodes, len(keys))
	for i, k := range keys {
		t[i] = targets[k]
	}
	return t
}

func (m *Manager) startDSorter() error {
	defer m.markStarted()
	if err := m.initStreams(); err != nil {
		return err
	}
	nlog.Infof("%s: %s starting with dsorter: %q", g.t, m.ManagerUUID, m.dsorter.name())
	return m.dsorter.start()
}

func (m *Manager) extractLocalShards() (err error) {
	m.extractionPhase.adjuster.start()
	m.Metrics.Extraction.begin()

	// compare with xact/xs/multiobj.go
	group, ctx := errgroup.WithContext(context.Background())
	switch {
	case m.Pars.Pit.isRange():
		err = m.iterRange(ctx, group)
	case m.Pars.Pit.isList():
		err = m.iterList(ctx, group)
	default:
		debug.Assert(m.Pars.Pit.isPrefix())
		debug.Assert(false, "not implemented yet") // TODO -- FIXME
	}

	m.dsorter.postExtraction()
	m.Metrics.Extraction.finish()
	m.extractionPhase.adjuster.stop()
	if err == nil {
		m.incrementRef(int64(m.recm.Records.TotalObjectCount()))
	}
	return
}

func (m *Manager) iterRange(ctx context.Context, group *errgroup.Group) error {
	var (
		metrics = m.Metrics.Extraction
		pt      = m.Pars.Pit.Template
	)
	metrics.mu.Lock()
	metrics.TotalCnt = pt.Count()
	metrics.mu.Unlock()
	pt.InitIter()
outer:
	for name, hasNext := pt.Next(); hasNext; name, hasNext = pt.Next() {
		select {
		case <-m.listenAborted():
			group.Wait()
			return m.newErrAborted()
		case <-ctx.Done():
			break outer // context canceled: we have an error
		default:
		}

		m.extractionPhase.adjuster.acquireGoroutineSema()
		es := &extractShard{m, metrics, name, true /*is-range*/}
		group.Go(es.do)
	}
	return group.Wait()
}

func (m *Manager) iterList(ctx context.Context, group *errgroup.Group) error {
	metrics := m.Metrics.Extraction
	metrics.mu.Lock()
	metrics.TotalCnt = int64(len(m.Pars.Pit.ObjNames))
	metrics.mu.Unlock()
outer:
	for _, name := range m.Pars.Pit.ObjNames {
		select {
		case <-m.listenAborted():
			group.Wait()
			return m.newErrAborted()
		case <-ctx.Done():
			break outer // context canceled: we have an error
		default:
		}

		m.extractionPhase.adjuster.acquireGoroutineSema()
		es := &extractShard{m, metrics, name, false /*is-range*/}
		group.Go(es.do)
	}
	return group.Wait()
}

func (m *Manager) createShard(s *shard.Shard, lom *cluster.LOM) (err error) {
	var (
		metrics   = m.Metrics.Creation
		shardName = s.Name
		errCh     = make(chan error, 2)
	)
	if err = lom.InitBck(&m.Pars.OutputBck); err != nil {
		return
	}
	lom.SetAtimeUnix(time.Now().UnixNano())

	if m.aborted() {
		return m.newErrAborted()
	}

	if err := m.dsorter.preShardCreation(s.Name, lom.Mountpath()); err != nil {
		return err
	}
	defer m.dsorter.postShardCreation(lom.Mountpath())

	cs := fs.Cap()
	if err = cs.Err(); err != nil {
		m.abort(err)
		return
	}

	beforeCreation := time.Now()

	var (
		wg   = &sync.WaitGroup{}
		r, w = io.Pipe()
	)
	wg.Add(1)
	go func() {
		var err error
		if !m.Pars.DryRun {
			params := cluster.AllocPutObjParams()
			{
				params.WorkTag = "dsort"
				params.Cksum = nil
				params.Atime = beforeCreation

				// NOTE: cannot have `PutObject` closing the original reader
				// on error as it'll cause writer (below) to panic
				params.Reader = io.NopCloser(r)

				// TODO: params.Xact - in part, to count PUTs and bytes in a generic fashion
				// (vs metrics.ShardCreationStats.updateThroughput - see below)
			}
			err = g.t.PutObject(lom, params)
			cluster.FreePutObjParams(params)
		} else {
			_, err = io.Copy(io.Discard, r)
		}
		errCh <- err
		wg.Done()
	}()

	// may reshard into a different format
	shardRW := m.shardRW
	//
	// TODO -- FIXME: compare with extractShard._do()
	//
	if !m.Pars.DryRun && m.Pars.OutputExtension != m.Pars.InputExtension {
		debug.Assert(m.Pars.OutputExtension != "")
		shardRW = shard.RWs[m.Pars.OutputExtension]
		debug.Assert(shardRW != nil, m.Pars.OutputExtension)
	}

	_, err = shardRW.Create(s, w, m.dsorter)
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
		err = m.newErrAborted()
		r.CloseWithError(err)
		w.CloseWithError(err)
	}

	wg.Wait()
	close(errCh)

	if err != nil {
		return err
	}

	si, err := m.smap.HrwHash2T(lom.Digest(), true /*skip maint*/)
	if err != nil {
		return err
	}

	// If the newly created shard belongs on a different target
	// according to HRW, send it there. Since it doesn't really matter
	// if we have an extra copy of the object local to this target, we
	// optimize for performance by not removing the object now.
	if si.ID() != g.t.SID() && !m.Pars.DryRun {
		lom.Lock(false)
		defer lom.Unlock(false)

		// Need to make sure that the object is still there.
		if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			return err
		}

		if lom.SizeBytes() <= 0 {
			goto exit
		}

		file, err := cos.NewFileHandle(lom.FQN)
		if err != nil {
			return err
		}

		o := transport.AllocSend()
		o.Hdr = transport.ObjHdr{
			ObjName:  shardName,
			ObjAttrs: cmn.ObjAttrs{Size: lom.SizeBytes(), Cksum: lom.Checksum()},
		}
		o.Hdr.Bck.Copy(lom.Bucket())

		// Make send synchronous.
		streamWg := &sync.WaitGroup{}
		errCh := make(chan error, 1)
		o.Callback = func(_ transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
			errCh <- err
			streamWg.Done()
		}
		streamWg.Add(1)
		err = m.streams.shards.Send(o, file, si)
		if err != nil {
			return err
		}
		streamWg.Wait()
		if err := <-errCh; err != nil {
			return err
		}
	}

exit:
	metrics.mu.Lock()
	metrics.CreatedCnt++
	if si.ID() != g.t.SID() {
		metrics.MovedShardCnt++
	}
	metrics.mu.Unlock()

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
func (m *Manager) participateInRecordDistribution(targetOrder meta.Nodes) (currentTargetIsFinal bool, err error) {
	var (
		i           int
		d           *meta.Snode
		dummyTarget *meta.Snode // dummy target is represented as nil value
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
			if d != dummyTarget && d.ID() == g.t.SID() {
				break
			}
		}

		if i%2 == 0 {
			m.dsorter.postRecordDistribution()

			var (
				beforeSend = time.Now()
				group      = &errgroup.Group{}
				r, w       = io.Pipe()
			)
			group.Go(func() error {
				var (
					buf, slab = g.mm.AllocSize(serializationBufSize)
					msgpw     = msgp.NewWriterBuf(w, buf)
				)
				defer slab.Free(buf)

				if err := m.recm.Records.EncodeMsg(msgpw); err != nil {
					w.CloseWithError(err)
					return errors.Errorf("failed to marshal msgp: %v", err)
				}
				err := msgpw.Flush()
				w.CloseWithError(err)
				if err != nil {
					return errors.Errorf("failed to flush msgp: %v", err)
				}
				return nil
			})
			group.Go(func() error {
				var (
					query  = url.Values{}
					sendTo = targetOrder[i+1]
				)
				query.Add(apc.QparamTotalCompressedSize, strconv.FormatInt(m.totalShardSize(), 10))
				query.Add(apc.QparamTotalUncompressedSize, strconv.FormatInt(m.totalExtractedSize(), 10))
				query.Add(apc.QparamTotalInputShardsExtracted, strconv.Itoa(m.recm.Records.Len()))
				reqArgs := &cmn.HreqArgs{
					Method: http.MethodPost,
					Base:   sendTo.URL(cmn.NetIntraData),
					Path:   apc.URLPathdSortRecords.Join(m.ManagerUUID),
					Query:  query,
					BodyR:  r,
				}
				err := m._do(reqArgs, sendTo, "send sorted records")
				r.CloseWithError(err)
				return err
			})
			if err := group.Wait(); err != nil {
				return false, err
			}

			m.recm.Records.Drain() // we do not need it anymore

			metrics.mu.Lock()
			metrics.SentStats.updateTime(time.Since(beforeSend))
			metrics.mu.Unlock()
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
				err = m.newErrAborted()
				return
			}
		}
		expectedReceived++

		metrics.mu.Lock()
		metrics.RecvStats.updateTime(time.Since(beforeRecv))
		metrics.mu.Unlock()

		t := targetOrder[:0]
		for i, d = range targetOrder {
			if i%2 == 1 {
				t = append(t, d)
			}
		}
		targetOrder = t

		m.recm.MergeEnqueuedRecords()
	}

	err = sortRecords(m.recm.Records, m.Pars.Algorithm)
	m.dsorter.postRecordDistribution()
	return true, err
}

func (m *Manager) generateShardsWithTemplate(maxSize int64) ([]*shard.Shard, error) {
	var (
		start           int
		curShardSize    int64
		n               = m.recm.Records.Len()
		pt              = m.Pars.Pot.Template
		shardCount      = pt.Count()
		shards          = make([]*shard.Shard, 0)
		numLocalRecords = make(map[string]int, m.smap.CountActiveTs())
	)
	pt.InitIter()

	if maxSize <= 0 {
		// Heuristic: shard size when maxSize not specified.
		maxSize = int64(math.Ceil(float64(m.totalExtractedSize()) / float64(shardCount)))
	}

	for i, r := range m.recm.Records.All() {
		numLocalRecords[r.DaemonID]++
		curShardSize += r.TotalSize()
		if curShardSize < maxSize && i < n-1 {
			continue
		}

		name, hasNext := pt.Next()
		if !hasNext {
			// no more shard names are available
			return nil, errors.Errorf("number of shards to be created exceeds expected number of shards (%d)", shardCount)
		}
		shard := &shard.Shard{
			Name: name,
		}
		ext, err := archive.Mime("", name)
		if err == nil {
			debug.Assert(m.Pars.OutputExtension == ext)
		} else {
			shard.Name = name + m.Pars.OutputExtension
		}

		shard.Size = curShardSize
		shard.Records = m.recm.Records.Slice(start, i+1)
		shards = append(shards, shard)

		start = i + 1
		curShardSize = 0
		for k := range numLocalRecords {
			numLocalRecords[k] = 0
		}
	}

	return shards, nil
}

func (m *Manager) generateShardsWithOrderingFile(maxSize int64) ([]*shard.Shard, error) {
	var (
		shards         = make([]*shard.Shard, 0)
		externalKeyMap = make(map[string]string)
		shardsBuilder  = make(map[string][]*shard.Shard)
	)
	if maxSize <= 0 {
		return nil, fmt.Errorf(fmtErrInvalidMaxSize, maxSize)
	}
	parsedURL, err := url.Parse(m.Pars.OrderFileURL)
	if err != nil {
		return nil, fmt.Errorf(fmtErrOrderURL, m.Pars.OrderFileURL, err)
	}

	req, err := http.NewRequest(http.MethodGet, m.Pars.OrderFileURL, http.NoBody)
	if err != nil {
		return nil, err
	}
	// is intra-call
	tsi := g.t.Snode()
	req.Header.Set(apc.HdrCallerID, tsi.ID())
	req.Header.Set(apc.HdrCallerName, tsi.String())

	resp, err := m.client.Do(req) //nolint:bodyclose // closed by cos.Close below
	if err != nil {
		return nil, err
	}
	defer cos.Close(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"unexpected status code (%d) when requesting order file from %q",
			resp.StatusCode, m.Pars.OrderFileURL,
		)
	}

	// TODO: handle very large files > GB - in case the file is very big we
	//  need to save file to the disk and operate on the file directly rather
	//  than keeping everything in memory.

	switch filepath.Ext(parsedURL.Path) {
	case ".json":
		var ekm map[string][]string
		if err := jsoniter.NewDecoder(resp.Body).Decode(&ekm); err != nil {
			return nil, err
		}

		for shardNameFmt, recordKeys := range ekm {
			for _, recordKey := range recordKeys {
				externalKeyMap[recordKey] = shardNameFmt
			}
		}
	default:
		lineReader := bufio.NewReader(resp.Body)
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

			parts := strings.Split(line, m.Pars.OrderFileSep)
			if len(parts) != 2 {
				msg := fmt.Sprintf("malformed line (%d) in external key map: %s", idx, line)
				if err := m.react(m.Pars.EKMMalformedLine, msg); err != nil {
					return nil, err
				}
			}

			recordKey, shardNameFmt := parts[0], parts[1]
			externalKeyMap[recordKey] = shardNameFmt
		}
	}

	for _, r := range m.recm.Records.All() {
		key := fmt.Sprintf("%v", r.Key)
		shardNameFmt, ok := externalKeyMap[key]
		if !ok {
			msg := fmt.Sprintf("record %q doesn't belong in external key map", key)
			if err := m.react(m.Pars.EKMMissingKey, msg); err != nil {
				return nil, err
			}
		}

		shards := shardsBuilder[shardNameFmt]
		recordSize := r.TotalSize() + m.shardRW.MetadataSize()*int64(len(r.Objects))
		shardCount := len(shards)
		if shardCount == 0 || shards[shardCount-1].Size > maxSize {
			shard := &shard.Shard{
				Name:    fmt.Sprintf(shardNameFmt, shardCount),
				Size:    recordSize,
				Records: shard.NewRecords(1),
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

// Create `maxSize` output shard structures in the order defined by dsortManager.Records.
// Each output shard structure is "distributed" (via m._dist below)
// to one of the targets - to create the corresponding output shard.
// The logic to map output shard => target:
//  1. By HRW (not using compression)
//  2. By locality (using compression),
//     using two maps:
//     i) shardsToTarget - tracks the total number of shards creation requests sent to each target URL
//     ii) numLocalRecords - tracks the number of records in the current shardMeta each target has locally
//     The target is determined firstly by locality (i.e. the target with the most local records)
//     and secondly (if there is a tie), by least load
//     (i.e. the target with the least number of pending shard creation requests).
func (m *Manager) phase3(maxSize int64) error {
	var (
		shards         []*shard.Shard
		err            error
		shardsToTarget = make(map[*meta.Snode][]*shard.Shard, m.smap.CountActiveTs())
		sendOrder      = make(map[string]map[string]*shard.Shard, m.smap.CountActiveTs())
		errCh          = make(chan error, m.smap.CountActiveTs())
	)
	for _, d := range m.smap.Tmap {
		if m.smap.InMaintOrDecomm(d) {
			continue
		}
		shardsToTarget[d] = nil
		if m.dsorter.name() == DSorterMemType {
			sendOrder[d.ID()] = make(map[string]*shard.Shard, 100)
		}
	}
	if m.Pars.OrderFileURL != "" {
		shards, err = m.generateShardsWithOrderingFile(maxSize)
	} else {
		shards, err = m.generateShardsWithTemplate(maxSize)
	}
	if err != nil {
		return err
	}

	bck := meta.CloneBck(&m.Pars.OutputBck)
	if err := bck.Init(g.t.Bowner()); err != nil {
		return err
	}
	for _, s := range shards {
		si, err := m.smap.HrwName2T(bck.MakeUname(s.Name), true /*skip maint*/)
		if err != nil {
			return err
		}
		shardsToTarget[si] = append(shardsToTarget[si], s)

		if m.dsorter.name() == DSorterMemType {
			singleSendOrder := make(map[string]*shard.Shard)
			for _, record := range s.Records.All() {
				shrd, ok := singleSendOrder[record.DaemonID]
				if !ok {
					shrd = &shard.Shard{
						Name:    s.Name,
						Records: shard.NewRecords(100),
					}
					singleSendOrder[record.DaemonID] = shrd
				}
				shrd.Records.Insert(record)
			}

			for tid, shard := range singleSendOrder {
				sendOrder[tid][shard.Name] = shard
			}
		}
	}

	m.recm.Records.Drain()

	wg := cos.NewLimitedWaitGroup(cmn.MaxBcastParallel(), len(shardsToTarget))
	for si, s := range shardsToTarget {
		wg.Add(1)
		go m._dist(si, s, sendOrder[si.ID()], errCh, wg)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		nlog.Errorf("%s: [dsort] %s err while sending shards: %v", g.t, m.ManagerUUID, err)
		return err
	}
	nlog.Infof("%s: [dsort] %s finished sending shards", g.t, m.ManagerUUID)
	return nil
}

func (m *Manager) _dist(si *meta.Snode, s []*shard.Shard, order map[string]*shard.Shard, errCh chan error, wg cos.WG) {
	var (
		group = &errgroup.Group{}
		r, w  = io.Pipe()
	)
	group.Go(func() error {
		var (
			buf, slab = g.mm.AllocSize(serializationBufSize)
			msgpw     = msgp.NewWriterBuf(w, buf)
			md        = &CreationPhaseMetadata{Shards: s, SendOrder: order}
		)
		err := md.EncodeMsg(msgpw)
		if err == nil {
			err = msgpw.Flush()
		}
		w.CloseWithError(err)
		slab.Free(buf)
		return err
	})
	group.Go(func() error {
		query := m.Pars.InputBck.NewQuery()
		reqArgs := &cmn.HreqArgs{
			Method: http.MethodPost,
			Base:   si.URL(cmn.NetIntraData),
			Path:   apc.URLPathdSortShards.Join(m.ManagerUUID),
			Query:  query,
			BodyR:  r,
		}
		err := m._do(reqArgs, si, "distribute shards")
		r.CloseWithError(err)
		return err
	})

	if err := group.Wait(); err != nil {
		errCh <- err
	}
	wg.Done()
}

func (m *Manager) _do(reqArgs *cmn.HreqArgs, tsi *meta.Snode, act string) error {
	req, errV := reqArgs.Req()
	if errV != nil {
		return errV
	}
	resp, err := m.client.Do(req) //nolint:bodyclose // cos.Close below
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var b []byte
		b, err = io.ReadAll(resp.Body)
		if err == nil {
			err = fmt.Errorf("%s: %s failed to %s: %s", g.t, m.ManagerUUID, act, strings.TrimSuffix(string(b), "\n"))
		} else {
			err = fmt.Errorf("%s: %s failed to %s: got %v(%d) from %s", g.t, m.ManagerUUID, act, err,
				resp.StatusCode, tsi.StringEx())
		}
	}
	cos.Close(resp.Body)
	return err
}

//////////////////
// extractShard //
//////////////////

type extractShard struct {
	m       *Manager
	metrics *LocalExtraction
	name    string
	isRange bool
}

func (es *extractShard) do() (err error) {
	m := es.m
	shardName := es.name
	if es.isRange && m.Pars.InputExtension != "" {
		ext, errV := archive.Mime("", es.name) // from filename
		if errV == nil {
			if !archive.EqExt(ext, m.Pars.InputExtension) {
				if m.config.FastV(4, cos.SmoduleDsort) {
					nlog.Infof("%s: %s skipping %s: %q vs %q", g.t, m.ManagerUUID,
						es.name, ext, m.Pars.InputExtension)
				}
				return
			}
		} else {
			shardName = es.name + m.Pars.InputExtension
		}
	}
	lom := cluster.AllocLOM(shardName)

	err = es._do(lom)

	cluster.FreeLOM(lom)
	phaseInfo := &m.extractionPhase
	phaseInfo.adjuster.releaseGoroutineSema()
	return
}

func (es *extractShard) _do(lom *cluster.LOM) error {
	var (
		m                        = es.m
		estimateTotalRecordsSize uint64
		warnOOM                  bool
	)
	if err := lom.InitBck(&m.Pars.InputBck); err != nil {
		return err
	}
	if _, local, err := lom.HrwTarget(m.smap); err != nil || !local {
		return err
	}
	if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
		if cmn.IsErrObjNought(err) {
			msg := fmt.Sprintf("shard.do: %q does not exist", lom.Cname())
			return m.react(m.Pars.MissingShards, msg)
		}
		return err
	}

	shardRW := m.shardRW
	if shardRW == nil {
		debug.Assert(!m.Pars.DryRun)
		ext, err := archive.Mime("", lom.FQN)
		if err != nil {
			return nil // skip
		}
		shardRW = shard.RWs[ext]
		debug.Assert(shardRW != nil, ext)
	}

	phaseInfo := &m.extractionPhase
	phaseInfo.adjuster.acquireSema(lom.Mountpath())
	if m.aborted() {
		phaseInfo.adjuster.releaseSema(lom.Mountpath())
		return m.newErrAborted()
	}

	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		phaseInfo.adjuster.releaseSema(lom.Mountpath())
		m.abort(err)
		return err
	}

	lom.Lock(false)
	fh, err := os.Open(lom.FQN)
	if err != nil {
		phaseInfo.adjuster.releaseSema(lom.Mountpath())
		lom.Unlock(false)
		return errors.Errorf("unable to open %s: %v", lom.Cname(), err)
	}

	expectedExtractedSize := uint64(float64(lom.SizeBytes()) / m.compressionRatio())
	toDisk := m.dsorter.preShardExtraction(expectedExtractedSize)

	extractedSize, extractedCount, err := shardRW.Extract(lom, fh, m.recm, toDisk)
	cos.Close(fh)

	m.addSizes(lom.SizeBytes(), extractedSize) // update compression rate

	phaseInfo.adjuster.releaseSema(lom.Mountpath())
	lom.Unlock(false)

	m.dsorter.postShardExtraction(expectedExtractedSize) // schedule freeing reserved memory on next memory update
	if err != nil {
		return errors.Errorf("failed to extract shard %s: %v", lom.Cname(), err)
	}

	if toDisk {
		g.tstats.Add(stats.DSortExtractShardDskCnt, 1)
	} else {
		g.tstats.Add(stats.DSortExtractShardMemCnt, 1)
	}
	g.tstats.Add(stats.DSortExtractShardSize, extractedSize)

	//
	// update metrics, check OOM
	//

	metrics := es.metrics
	metrics.mu.Lock()
	metrics.ExtractedRecordCnt += int64(extractedCount)
	metrics.ExtractedCnt++
	if metrics.ExtractedCnt == 1 && extractedCount > 0 {
		// After extracting the _first_ shard estimate how much memory
		// will be required to keep all records in memory. One node
		// will eventually have all records from all shards so we
		// don't calculate estimates only for single node.
		recordSize := int(m.recm.Records.RecordMemorySize())
		estimateTotalRecordsSize = uint64(metrics.TotalCnt * int64(extractedCount*recordSize))
		if estimateTotalRecordsSize > m.freeMemory() {
			warnOOM = true
		}
	}
	metrics.ExtractedSize += extractedSize
	if toDisk {
		metrics.ExtractedToDiskCnt++
		metrics.ExtractedToDiskSize += extractedSize
	}
	metrics.mu.Unlock()

	if warnOOM {
		msg := fmt.Sprintf("(estimated) total size of records (%d) will possibly exceed available memory (%s) during sorting phase",
			estimateTotalRecordsSize, m.Pars.MaxMemUsage)
		return m.react(cmn.WarnReaction, msg)
	}
	return nil
}
