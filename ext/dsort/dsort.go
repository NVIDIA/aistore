// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"

	onexxh "github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/tinylib/msgp/msgp"
	"golang.org/x/sync/errgroup"
)

const PrefixJobID = "srt-"

type (
	dsorter interface {
		shard.ContentLoader

		name() string
		init(*cmn.Config) error
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
	if cmn.Rom.FastV(4, cos.SmoduleDsort) {
		nlog.Infof("%s: %s finished", core.T, m.ManagerUUID)
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

	if err := m.startDsorter(); err != nil {
		return err
	}

	// Phase 1.
	nlog.Infof("%s: %s started extraction stage", core.T, m.ManagerUUID)
	if err := m.extractLocalShards(); err != nil {
		return err
	}

	s := binary.BigEndian.Uint64(m.Pars.TargetOrderSalt)
	targetOrder := _torder(s, m.smap.Tmap)
	if cmn.Rom.FastV(4, cos.SmoduleDsort) {
		nlog.Infof("%s: %s final target in targetOrder => URL: %s, tid %s", core.T, m.ManagerUUID,
			targetOrder[len(targetOrder)-1].PubNet.URL, targetOrder[len(targetOrder)-1].ID())
	}

	// Phase 2.
	nlog.Infof("%s: %s started sort stage", core.T, m.ManagerUUID)
	curTargetIsFinal, err := m.participateInRecordDistribution(targetOrder)
	if err != nil {
		return err
	}

	// Phase 3. - run only by the final target
	if curTargetIsFinal {
		// assuming uniform distribution estimate avg. output shard size
		ratio := m.compressionRatio()
		if cmn.Rom.FastV(4, cos.SmoduleDsort) {
			nlog.Infof("%s [dsort] %s phase3: ratio=%f", core.T, m.ManagerUUID, ratio)
		}
		debug.Assertf(shard.IsCompressed(m.Pars.InputExtension) || ratio == 1, "tar ratio=%f, ext=%q",
			ratio, m.Pars.InputExtension)

		shardSize := int64(float64(m.Pars.OutputShardSize) / ratio)
		nlog.Infof("%s: [dsort] %s started phase 3: ratio=%f, shard size (%d, %d)",
			core.T, m.ManagerUUID, ratio, shardSize, m.Pars.OutputShardSize)
		if err := m.phase3(shardSize); err != nil {
			nlog.Errorf("%s: [dsort] %s phase3 err: %v", core.T, m.ManagerUUID, err)
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
	nlog.Infof("%s: %s started creation stage", core.T, m.ManagerUUID)
	if err := m.dsorter.createShardsLocally(); err != nil {
		return err
	}

	nlog.Infof("%s: %s finished successfully", core.T, m.ManagerUUID)
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
		c := onexxh.Checksum64S(cos.UnsafeB(i), salt)
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

func (m *Manager) startDsorter() error {
	defer m.markStarted()
	if err := m.initStreams(); err != nil {
		return err
	}
	nlog.Infof("%s: %s starting with dsorter: %q", core.T, m.ManagerUUID, m.dsorter.name())
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

func (m *Manager) createShard(s *shard.Shard, lom *core.LOM) error {
	var (
		metrics   = m.Metrics.Creation
		shardName = s.Name
		errCh     = make(chan error, 2)
	)
	if err := lom.InitBck(&m.Pars.OutputBck); err != nil {
		return err
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
	if err := cs.Err(); err != nil {
		m.abort(err)
		return err
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
			params := core.AllocPutParams()
			{
				params.WorkTag = "dsort"
				params.Cksum = nil
				params.Atime = beforeCreation

				// NOTE: cannot have `PutObject` closing the original reader
				// on error as it'll cause writer (below) to panic
				params.Reader = io.NopCloser(r)

				// TODO: params.Xact - in part, to count PUTs and bytes in a generic fashion
				// (vs metrics.ShardCreationStats.updateThroughput - see below)

				// TODO: add params.Size = (size resulting from shardRW.Create below)
			}
			err = core.T.PutObject(lom, params)
			core.FreePutParams(params)
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

	_, err := shardRW.Create(s, w, m.dsorter)
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

	si, errH := m.smap.HrwHash2T(lom.Digest())
	if errH != nil {
		return errH
	}

	// If the newly created shard belongs on a different target
	// according to HRW, send it there. Since it doesn't really matter
	// if we have an extra copy of the object local to this target, we
	// optimize for performance by not removing the object now.
	if si.ID() != core.T.SID() && !m.Pars.DryRun {
		lom.Lock(false)
		defer lom.Unlock(false)

		// Need to make sure that the object is still there.
		if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			return err
		}

		if lom.Lsize() <= 0 {
			goto exit
		}

		file, errO := cos.NewFileHandle(lom.FQN)
		if errO != nil {
			return errO
		}

		o := transport.AllocSend()
		o.Hdr = transport.ObjHdr{
			ObjName:  shardName,
			ObjAttrs: cmn.ObjAttrs{Size: lom.Lsize(), Cksum: lom.Checksum()},
		}
		o.Hdr.Bck.Copy(lom.Bucket())

		// Make send synchronous.
		streamWg := &sync.WaitGroup{}
		errCh := make(chan error, 1)
		o.Callback = func(_ *transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
			errCh <- err
			streamWg.Done()
		}
		streamWg.Add(1)
		if err := m.streams.shards.Send(o, file, si); err != nil {
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
	if si.ID() != core.T.SID() {
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
func (m *Manager) participateInRecordDistribution(targetOrder meta.Nodes) (currentTargetIsFinal bool, _ error) {
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
			if d != dummyTarget && d.ID() == core.T.SID() {
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
					buf, slab = g.mem.AllocSize(serializationBufSize)
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
			return false, nil
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
				return false, m.newErrAborted()
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

	err := sortRecords(m.recm.Records, m.Pars.Algorithm)
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

func (m *Manager) parseEKMFile() (shard.ExternalKeyMap, error) {
	ekm := shard.NewExternalKeyMap(64)
	parsedURL, err := url.Parse(m.Pars.EKMFileURL)
	if err != nil {
		return nil, fmt.Errorf(fmtErrOrderURL, m.Pars.EKMFileURL, err)
	}

	req, err := http.NewRequest(http.MethodGet, m.Pars.EKMFileURL, http.NoBody)
	if err != nil {
		return nil, err
	}
	// is intra-call
	tsi := core.T.Snode()
	req.Header.Set(apc.HdrCallerID, tsi.ID())
	req.Header.Set(apc.HdrCallerName, tsi.String())

	resp, err := m.client.Do(req) //nolint:bodyclose // closed by cos.Close below
	if err != nil {
		return nil, err
	}
	defer cos.Close(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"unexpected status code (%d) when requesting ekm file from %q",
			resp.StatusCode, m.Pars.EKMFileURL,
		)
	}

	// TODO: handle very large files > GB - in case the file is very big we
	//  need to save file to the disk and operate on the file directly rather
	//  than keeping everything in memory.

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Attempt to parse as JSON
	var content map[string][]string
	jsonErr := jsoniter.Unmarshal(bodyBytes, &content)
	if jsonErr != nil && filepath.Ext(parsedURL.Path) == ".json" {
		return nil, errors.New("EKM file parsing as JSON fails, but the file extension is .json which is not allowed")
	}

	if jsonErr == nil {
		// Add keys to the EKM
		for shardNameFmt, recordKeys := range content {
			for _, recordKey := range recordKeys {
				if err := ekm.Add(recordKey, shardNameFmt); err != nil {
					return nil, err
				}
			}
		}
		return ekm, nil
	}

	// Parse as normal EKM file
	lineReader := bufio.NewReader(bytes.NewReader(bodyBytes))
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

		parts := strings.Split(line, m.Pars.EKMFileSep)
		if len(parts) != 2 {
			msg := fmt.Sprintf("malformed line (%d) in external key map: %s", idx, line)
			if err := m.react(m.Pars.EKMMalformedLine, msg); err != nil {
				return nil, err
			}
		}

		recordKey, shardNameFmt := parts[0], parts[1]
		if err := ekm.Add(recordKey, shardNameFmt); err != nil {
			return nil, err
		}
	}

	return ekm, nil
}

func (m *Manager) generateShardsWithOrderingFile(maxSize int64) ([]*shard.Shard, error) {
	var (
		shards         = make([]*shard.Shard, 0)
		shardTemplates = make(map[string]*cos.ParsedTemplate, 8)
		shardsBuilder  = make(map[string][]*shard.Shard, 8)
	)
	if maxSize <= 0 {
		return nil, fmt.Errorf(fmtErrInvalidMaxSize, maxSize)
	}

	ekm, err := m.parseEKMFile()
	if err != nil {
		return nil, err
	}

	for _, shardNameFmt := range ekm.All() {
		tmpl, err := cos.NewParsedTemplate(shardNameFmt)
		if err != nil {
			return nil, err
		}
		if len(tmpl.Ranges) == 0 {
			return nil, fmt.Errorf("invalid output template %q: no ranges (prefix-only output is not supported)", shardNameFmt)
		}
		shardTemplates[shardNameFmt] = &tmpl
		shardTemplates[shardNameFmt].InitIter()
	}

	for _, r := range m.recm.Records.All() {
		key := fmt.Sprintf("%v", r.Key)
		shardNameFmt, err := ekm.Lookup(key)
		if err != nil {
			msg := fmt.Sprintf("error on lookup record %q in external key map: %s", key, err)
			if err := m.react(m.Pars.EKMMissingKey, msg); err != nil {
				return nil, err
			}
		}

		recordSize := r.TotalSize() + m.shardRW.MetadataSize()*int64(len(r.Objects))

		// retrieve all shards created using the current template format
		shards := shardsBuilder[shardNameFmt]
		// if no shards exist for this template, or the last shard exceeds the max size, create a new shard
		if len(shards) == 0 || shards[len(shards)-1].Size > maxSize {
			shardName, hasNext := shardTemplates[shardNameFmt].Next()
			if !hasNext {
				return nil, fmt.Errorf(
					"number of shards to be created using %s template exceeds expected number of shards (%d)",
					shardTemplates[shardNameFmt].Prefix, shardTemplates[shardNameFmt].Count(),
				)
			}
			shard := &shard.Shard{
				Name:    shardName,
				Size:    recordSize,
				Records: shard.NewRecords(1),
			}
			shard.Records.Insert(r)
			shardsBuilder[shardNameFmt] = append(shardsBuilder[shardNameFmt], shard)
		} else {
			// Append records
			lastShard := shards[len(shards)-1]
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
	for tid, tsi := range m.smap.Tmap {
		if m.smap.InMaintOrDecomm(tid) {
			continue
		}
		shardsToTarget[tsi] = nil
		if m.dsorter.name() == MemType {
			sendOrder[tid] = make(map[string]*shard.Shard, 100)
		}
	}
	if m.Pars.EKMFileURL != "" {
		shards, err = m.generateShardsWithOrderingFile(maxSize)
	} else {
		shards, err = m.generateShardsWithTemplate(maxSize)
	}
	if err != nil {
		return err
	}

	bck := meta.CloneBck(&m.Pars.OutputBck)
	if err := bck.Init(core.T.Bowner()); err != nil {
		return err
	}
	for _, s := range shards {
		si, err := m.smap.HrwName2T(bck.MakeUname(s.Name))
		if err != nil {
			return err
		}
		shardsToTarget[si] = append(shardsToTarget[si], s)

		if m.dsorter.name() == MemType {
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

	wg := cos.NewLimitedWaitGroup(sys.MaxParallelism(), len(shardsToTarget))
	for si, s := range shardsToTarget {
		wg.Add(1)
		go m._dist(si, s, sendOrder[si.ID()], errCh, wg)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		nlog.Errorf("%s: [dsort] %s err while sending shards: %v", core.T, m.ManagerUUID, err)
		return err
	}
	nlog.Infof("%s: [dsort] %s finished sending shards", core.T, m.ManagerUUID)
	return nil
}

func (m *Manager) _dist(si *meta.Snode, s []*shard.Shard, order map[string]*shard.Shard, errCh chan error, wg cos.WG) {
	var (
		group = &errgroup.Group{}
		r, w  = io.Pipe()
	)
	group.Go(func() error {
		var (
			buf, slab = g.mem.AllocSize(serializationBufSize)
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
		q := make(url.Values, 1)
		m.Pars.InputBck.SetQuery(q)
		reqArgs := &cmn.HreqArgs{
			Method: http.MethodPost,
			Base:   si.URL(cmn.NetIntraData),
			Path:   apc.URLPathdSortShards.Join(m.ManagerUUID),
			Query:  q,
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

	cmn.HreqFree(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var b []byte
		b, err = cos.ReadAll(resp.Body)
		if err == nil {
			err = fmt.Errorf("%s: %s failed to %s: %s", core.T, m.ManagerUUID, act, strings.TrimSuffix(string(b), "\n"))
		} else {
			err = fmt.Errorf("%s: %s failed to %s: got %v(%d) from %s", core.T, m.ManagerUUID, act, err,
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
				if cmn.Rom.FastV(4, cos.SmoduleDsort) {
					nlog.Infof("%s: %s skipping %s: %q vs %q", core.T, m.ManagerUUID,
						es.name, ext, m.Pars.InputExtension)
				}
				return
			}
		} else {
			shardName = es.name + m.Pars.InputExtension
		}
	}
	lom := core.AllocLOM(shardName)

	err = es._do(lom)

	core.FreeLOM(lom)
	phaseInfo := &m.extractionPhase
	phaseInfo.adjuster.releaseGoroutineSema()
	return
}

func (es *extractShard) _do(lom *core.LOM) error {
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
	fh, err := lom.Open()
	if err != nil {
		phaseInfo.adjuster.releaseSema(lom.Mountpath())
		lom.Unlock(false)
		return errors.Errorf("unable to open %s: %v", lom.Cname(), err)
	}

	expectedExtractedSize := uint64(float64(lom.Lsize()) / m.compressionRatio())
	toDisk := m.dsorter.preShardExtraction(expectedExtractedSize)

	extractedSize, extractedCount, err := shardRW.Extract(lom, fh, m.recm, toDisk)
	cos.Close(fh)

	m.addSizes(lom.Lsize(), extractedSize) // update compression rate

	phaseInfo.adjuster.releaseSema(lom.Mountpath())
	lom.Unlock(false)

	m.dsorter.postShardExtraction(expectedExtractedSize) // schedule freeing reserved memory on next memory update
	if err != nil {
		return errors.Errorf("failed to extract shard %s: %v", lom.Cname(), err)
	}

	if toDisk {
		core.T.StatsUpdater().Add(stats.DsortExtractShardDskCnt, 1)
	} else {
		core.T.StatsUpdater().Add(stats.DsortExtractShardMemCnt, 1)
	}
	core.T.StatsUpdater().Add(stats.DsortExtractShardSize, extractedSize)

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
