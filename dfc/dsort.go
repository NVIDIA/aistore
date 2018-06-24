package dfc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/dsort"
	"github.com/NVIDIA/dfcpub/iosgl"
	"github.com/OneOfOne/xxhash"
	"github.com/json-iterator/go"
)

const (
	dummyTargetID = "dummy"
	extractionDir = workfileprefix + "dsort-extracted"
)

var js = jsoniter.ConfigFastest

// sortHandler is the handler called for the HTTP endpoint /v1/sort.
// There are three major phases to this function:
//
// 1. extractLocalShards
// 2. participateInRecordDistribution
// 3. distributeShardRecords
func (t *targetrunner) sortHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		t.invalmsghdlr(w, r, "invalid method: %s", r.Method)
		return
	}
	if t.dsortManager.InProgress() {
		t.invalmsghdlr(w, r, fmt.Sprintf(
			"target %s is already participating in a sort - please try again later", t.si.DaemonID))
		return
	}
	t.dsortManager.SetInProgressTo(true)
	defer t.dsortManager.SetInProgressTo(false)
	glog.Infof("starting dsort")

	var rs dsort.RequestSpec
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("could not read request body, err: %v", err))
		return
	}
	if err = js.Unmarshal(b, &rs); err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err))
		return
	}
	if err = rs.Validate(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if err = t.dsortManager.Init(rs, t.httpclientLongTimeout, t.si.InternalNet.DirectURL); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	now := time.Now()
	// Phase 1.
	if err := t.extractLocalShards(rs); err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	metrics := fmt.Sprintf("local meta extract took: %v\n", time.Since(now))
	glog.Info(metrics)

	s := binary.BigEndian.Uint64(rs.TargetOrderSalt)
	targetOrder := randomTargetOrder(s, t.smapowner.get().Tmap)
	glog.Infof("final target in targetOrder => URL: %s, Daemon ID: %s",
		targetOrder[len(targetOrder)-1].PublicNet.DirectURL, targetOrder[len(targetOrder)-1].DaemonID)

	now = time.Now()
	// Phase 2.
	curTargetIsFinal, err := t.participateInRecordDistribution(targetOrder)
	if err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	metrics += fmt.Sprintf("time spent in record distribution phase: %v\n\n", time.Since(now))
	glog.Info(fmt.Sprintf("time spent in record distribution phase: %v\n", time.Since(now)))

	// After each target participates in the cluster-wide record distribution, start listening for the signal
	// to start creating shards locally.
	shardCreation := &sync.WaitGroup{}
	shardCreation.Add(1)
	shardCreationErrCh := make(chan error, 1)
	go func() {
		defer shardCreation.Done()
		now := time.Now()
		shardCreationErrCh <- t.createShardsLocally()
		metrics += fmt.Sprintf("shard creation took: %v\n", time.Since(now))
		glog.Infof(fmt.Sprintf("shard creation took: %v\n", time.Since(now)))
	}()

	if !curTargetIsFinal || len(t.dsortManager.SortedRecords) == 0 {
		shardCreation.Wait()
		if err := <-shardCreationErrCh; err != nil {
			glog.Error(err)
			t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		} else {
			w.Write([]byte(metrics))
			glog.Infof("finished dsort")
		}
		return
	}

	shardSize := rs.OutputShardSizeBytes
	if t.dsortManager.ExtractCreater.UsingCompression() {
		// By making the assumption that the input content is reasonably uniform across all shards,
		// the output shard size required (such that each gzip compressed output shard will have
		// a size close to rs.ShardSizeBytes) can be estimated.
		avgCompressRatio := t.dsortManager.SumCompressionRatios() /
			float64(t.dsortManager.TotalInputShardsSeen())
		shardSize = int64(float64(rs.OutputShardSizeBytes) / avgCompressRatio)
		glog.Infof("estimated output shard size required before gzip compression: %d", shardSize)
	}

	now = time.Now()
	// Phase 3.
	if err = t.distributeShardRecords(shardSize, rs); err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
	}

	shardCreation.Wait()
	if err := <-shardCreationErrCh; err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
	} else {
		w.Write([]byte(metrics))
	}
	t.dsortManager.SortedRecords = nil
}

// createShardsLocally waits until it's given the signal to start creating shards, then creates shards in parallel.
func (t *targetrunner) createShardsLocally() error {
	<-t.dsortManager.StartShardCreation
	errCh := make(chan error, len(t.dsortManager.Shards))
	wg := &sync.WaitGroup{}
	for _, s := range t.dsortManager.Shards {
		wg.Add(1)
		go func(s dsort.Shard) {
			defer wg.Done()
			fqn := t.fqn(s.Bucket, s.Name, s.IsLocal)
			written, err := t.dsortManager.ExtractCreater.CreateShard(s, fqn)
			if err != nil {
				errCh <- err
				return
			}

			si, errStr := HrwTarget(s.Bucket, s.Name, t.smapowner.get())
			if errStr != "" {
				errCh <- errors.New(errStr)
				return
			}

			// If the newly created shard belongs on a different target according to HRW, send it there.
			// Since it doesn't really matter if we have an extra copy of the object local to this target,
			// we optimize for performance by not removing the object now.
			if si.DaemonID != t.si.DaemonID {
				errStr = t.sendfile(http.MethodPut, s.Bucket, s.Name, si, written, "", "")
				if errStr != "" {
					errCh <- errors.New(errStr)
				}
			}
		}(s)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

// extractLocalShards iterates through files local to the current target and calls ExtractMeta on
// matching files based on the given RequestSpec.
func (t *targetrunner) extractLocalShards(rs dsort.RequestSpec) error {
	var (
		m     runtime.MemStats
		wg    = &sync.WaitGroup{}
		errCh = make(chan error, rs.End-rs.Start+1)
		smap  = t.smapowner.get()
	)
	runtime.ReadMemStats(&m)
	totalMemMB, err := TotalMemory()
	if err != nil {
		return err
	}
	totalMem := int64(totalMemMB) * MiB
	totalMemUsed := int64(m.Alloc)

	for i := rs.Start; i <= rs.End; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			shardName := rs.Prefix + fmt.Sprintf("%0*d", rs.DigitsToPrependTo, i) + rs.Extension
			si, errStr := HrwTarget(rs.Bucket, shardName, smap)
			if errStr != "" {
				errCh <- errors.New(errStr)
				return
			}
			if si.DaemonID != t.si.DaemonID {
				return
			}
			fqn := t.fqn(rs.Bucket, shardName, rs.IsLocalBucket)
			f, err := os.Open(fqn)
			if err != nil {
				errCh <- fmt.Errorf("unable to open local file, err: %v", err)
				return
			}
			var compressedSize int64
			fi, err := f.Stat()
			if err != nil {
				errCh <- err
				f.Close()
				return
			}

			if t.dsortManager.ExtractCreater.UsingCompression() {
				compressedSize = fi.Size()
			}

			// Switch to extracting to disk if we hit this target's memory usage threshold.
			var toDisk bool
			var extractionPath string
			if atomic.AddInt64(&totalMemUsed, fi.Size()) >= t.dsortManager.MaxMemUsagePercent()*(totalMem/100) {
				toDisk = true
				extractionPath = filepath.Join(filepath.Dir(fqn), extractionDir)
				t.dsortManager.AddExtractionPath(extractionPath)
			}

			records, n, err := t.dsortManager.ExtractCreater.ExtractShard(f, toDisk, extractionPath)
			if err != nil {
				errCh <- fmt.Errorf("error in ExtractMeta, file: %s, err: %v", f.Name(), err)
				f.Close()
				return
			}
			f.Close()
			dsort.SortRecords(&records, rs.Algorithm.Decreasing)
			t.dsortManager.Lock()
			t.dsortManager.AddToTotalInputShardsSeen(1)
			t.dsortManager.MergeSortedRecords(records)
			t.dsortManager.AddToSumCompressionRatios(float64(compressedSize) / float64(n))
			t.dsortManager.Unlock()
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

// participateInRecordDistribution coordinates the distributed merging and sorting of each target's SortedRecords
// based on the order defined by targetOrder. It returns a bool, currentTargetIsFinal, which is true iff
// the current target is the final target in targetOrder, which by construction of the algorithm, should
// contain the final, complete, sorted slice of Record structs.
//
// The algorithm uses the following premise: for a target T at index i in targetOrder, if i is even, then T will send
// its FileMeta slice to the target at index i+1 in targetOrder. If i is odd, then it will do a blocking receive on
// the FileMeta slice from the target at index i-1 in targetOrder, and will remove all even-indexed targets in
// targetOrder after receiving. This pattern repeats until len(targetOrder) == 1, in which case the single target
// in the slice is the final target with the final, complete, sorted slice of Record structs.
func (t *targetrunner) participateInRecordDistribution(
	targetOrder []*daemonInfo) (currentTargetIsFinal bool, err error) {

	expectedReceived := 1
	for len(targetOrder) > 1 {
		if len(targetOrder)%2 == 1 {
			// For simplicity, we always work with an even-length slice of targets. If len(targetOrder) is odd,
			// we put a "dummy target" into the slice at index len(targetOrder)-2 which simulates sending its
			// metadata to the next target in targetOrder (which is actually itself).
			targetOrder = append(
				targetOrder[:len(targetOrder)-1],
				&daemonInfo{DaemonID: dummyTargetID},
				targetOrder[len(targetOrder)-1])
		}
		for i, d := range targetOrder {
			if d.DaemonID != t.si.DaemonID {
				continue
			}
			if i%2 == 0 {
				b, e := js.Marshal(t.dsortManager.SortedRecords)
				if e != nil {
					err = fmt.Errorf("failed to marshal into JSON, SortedRecords: %+v err: %v",
						t.dsortManager.SortedRecords, e)
					return
				}
				sendTo := targetOrder[i+1]
				q := url.Values{}
				q.Add(api.URLParamTotalCompressionRatio,
					fmt.Sprintf("%f", t.dsortManager.SumCompressionRatios()))
				q.Add(api.URLParamTotalInputShardsSeen, strconv.Itoa(t.dsortManager.TotalInputShardsSeen()))
				if err = t.call(callArgs{
					req: reqArgs{
						method: http.MethodPost,
						base:   sendTo.InternalNet.DirectURL,
						path:   api.URLPath(api.Version, api.Records),
						query:  q,
						body:   b,
					},
					si: sendTo,
				}).err; err != nil {
					err = fmt.Errorf("failed to send SortedRecords to next target, err: %v", err)
				}
				return
			} else {
				if targetOrder[i-1].DaemonID == dummyTargetID {
					t.dsortManager.IncrementReceived()
				}
				var waited float64
				minsToWait := t.httpclientLongTimeout.Timeout.Minutes()
				for t.dsortManager.Received() < expectedReceived {
					if waited < minsToWait*1200 {
						time.Sleep(50 * time.Millisecond)
						waited++
					} else {
						err = fmt.Errorf(
							"timed out waiting for SortedRecords from target ID: %s", targetOrder[i-1].DaemonID)
						return
					}
				}
				expectedReceived++

				var t []*daemonInfo
				for j, d := range targetOrder {
					if j%2 == 1 {
						t = append(t, d)
					}
				}
				targetOrder = t
				break
			}
		}
	}
	return true, nil
}

// distributeShardRecords creates Shard structs in the order of t.dsortManager.SortedRecords corresponding
// to a maximum size maxSize. Each Shard is sent in an HTTP request to the appropriate target to create
// the actual file itself. The strategy used to determine the appropriate target differs depending on whether
// compression is used.
//
// 1) By HRW (not using compression)
// 2) By locality (using compression), using two maps:
//      i) totalShardsSentToTarget - tracks the total number of shards creation requests sent to each target URL
//      ii) numSourceFilesLocalToTarget - tracks the number of shard source files in the current shardMeta
// 										  each target has locally
//
//	    The appropriate target is determined firstly by locality (i.e. the target with the most local shard source files)
// 		and secondly (if there is a tie), by least load (i.e. the target with the least number of shard creation requests
// 		sent to it already).
func (t *targetrunner) distributeShardRecords(maxSize int64, rs dsort.RequestSpec) error {
	var (
		n                           = len(t.dsortManager.SortedRecords)
		shardNum                    int
		start                       int
		curShardSize                int64
		baseURL                     string
		ext                         = t.dsortManager.FileExtension()
		wg                          = &sync.WaitGroup{}
		smap                        = t.smapowner.get()
		errCh                       = make(chan error, n/int(maxSize)+1)
		shardsToTarget              = make(map[string][]dsort.Shard, smap.countTargets())
		numSourceFilesLocalToTarget = make(map[string]int, smap.countTargets())
	)
	for _, d := range smap.Tmap {
		numSourceFilesLocalToTarget[d.InternalNet.DirectURL] = 0
	}
	for i, r := range t.dsortManager.SortedRecords {
		numSourceFilesLocalToTarget[r.TargetURL]++
		curShardSize += r.Size
		if curShardSize < maxSize && i < n-1 {
			continue
		}

		shard := dsort.Shard{
			Name:    rs.OutputNamePrefix + fmt.Sprintf("%0*d", rs.DigitsToPrependTo, shardNum) + ext,
			Bucket:  rs.Bucket,
			IsLocal: rs.IsLocalBucket,
		}
		if t.dsortManager.ExtractCreater.UsingCompression() {
			baseURL = t.targetURLForShardRequest(shardsToTarget, numSourceFilesLocalToTarget)
		} else {
			// If output shards are not compressed, there will always be less data sent over the network
			// if the shard is constructed on the correct HRW target as opposed to constructing it on
			// the target with optimal file content locality and then sent to the correct target.
			si, errStr := HrwTarget(rs.Bucket, shard.Name, t.smapowner.get())
			if errStr != "" {
				return errors.New(errStr)
			}
			baseURL = si.InternalNet.DirectURL
		}
		shard.Size = curShardSize
		shard.Records = t.dsortManager.SortedRecords[start : i+1]
		shardsToTarget[baseURL] = append(shardsToTarget[baseURL], shard)

		start = i + 1
		shardNum++
		curShardSize = 0
		for k := range numSourceFilesLocalToTarget {
			numSourceFilesLocalToTarget[k] = 0
		}
	}

	for u, s := range shardsToTarget {
		wg.Add(1)
		go func(u string, s []dsort.Shard) {
			defer wg.Done()
			b, err := js.Marshal(s)
			if err != nil {
				errCh <- err
				return
			}
			// TODO: introduce query
			u += fmt.Sprintf("%s?%s=%s&%s=%t", api.URLPath(api.Version, api.Shards),
				api.URLParamBucket, rs.Bucket, api.URLParamLocal, rs.IsLocalBucket)
			resp, err := t.httpclientLongTimeout.Post(u, "application/json", bytes.NewBuffer(b))
			if err != nil {
				errCh <- err
				return
			} else if resp.StatusCode >= http.StatusBadRequest {
				b, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					errCh <- err
				} else {
					errCh <- errors.New(string(b))
				}
			}
			resp.Body.Close()
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

// recordsHandler is the handler called for the HTTP endpoint /v1/records.
// A valid POST to this endpoint updates this target's dsortManager.SortedRecords with the
// []dsort.Records from the request body, along with some related state variables.
func (t *targetrunner) recordsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s := fmt.Sprintf("invalid method: %s to %s, should be POST", r.Method, r.URL.String())
		glog.Error(s)
		t.invalmsghdlr(w, r, s, http.StatusInternalServerError)
		return
	}
	fStr := r.URL.Query().Get(api.URLParamTotalCompressionRatio)
	f, err := strconv.ParseFloat(fStr, 64)
	if err != nil {
		s := fmt.Sprintf("invalid %s in request to %s, err: %v", api.URLParamTotalCompressionRatio, r.URL.String(), err)
		glog.Error(s)
		t.invalmsghdlr(w, r, s)
		return
	}
	dStr := r.URL.Query().Get(api.URLParamTotalInputShardsSeen)
	d, err := strconv.Atoi(dStr)
	if err != nil {
		s := fmt.Sprintf("invalid %s in request to %s, err: %v", api.URLParamTotalInputShardsSeen, r.URL.String(), err)
		glog.Error(s)
		t.invalmsghdlr(w, r, s)
		return
	}

	records := make([]dsort.Record, 0, d)
	var bufSize int64
	if r.ContentLength > 0 {
		bufSize = r.ContentLength
	}
	bufReader := bytes.NewBuffer(make([]byte, 0, bufSize))
	if _, err := io.Copy(bufReader, r.Body); err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, fmt.Sprintf("could not read request body, err: %v", err))
		return
	}
	if err := js.Unmarshal(bufReader.Bytes(), &records); err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err))
		return
	}

	t.dsortManager.Lock()
	t.dsortManager.AddToSumCompressionRatios(f)
	t.dsortManager.AddToTotalInputShardsSeen(d)
	t.dsortManager.MergeSortedRecords(records)
	t.dsortManager.IncrementReceived()
	t.dsortManager.Unlock()
	glog.Infof("total times received records from another target: %d", t.dsortManager.Received())
}

// recordContentsHandler is the handler for the HTTP endpoint /v1/record-contents.
// This endpoint responds to a valid GET with the file contents of a record specified
// by query param `fqn`.
func (t *targetrunner) recordContentsHandler(w http.ResponseWriter, r *http.Request) {
	var (
		f   *os.File
		err error
	)
	if r.Method != http.MethodGet {
		s := fmt.Sprintf("invalid method: %s for %s, should be GET", r.Method, r.URL.String())
		glog.Error(s)
		t.invalmsghdlr(w, r, s, http.StatusInternalServerError)
		return
	}
	p := r.URL.Query().Get(api.URLParamPathToContents)
	v, ok := t.dsortManager.ExtractCreater.RecordContents().Load(p)
	if !ok {
		f, err = os.Open(p)
		if err != nil {
			glog.Error(err)
			t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = io.Copy(w, f)
		f.Close()
	} else {
		sgl := v.(*iosgl.SGL)
		_, err = io.Copy(w, sgl)
		sgl.Free()
		t.dsortManager.ExtractCreater.RecordContents().Delete(p)
	}
	if err != nil {
		e := fmt.Sprintf("error writing shard content to HTTP writer in %s, err: %v", err, r.URL.String())
		glog.Error(e)
		t.invalmsghdlr(w, r, e, http.StatusInternalServerError)
	}
}

// shardsHandler is the handler for the HTTP endpoint /v1/shards.
// A valid POST to this endpoint results in a new shard being created locally based on the contents
// of the incoming request body. The shard is then sent to the correct target in the cluster as per HRW.
func (t *targetrunner) shardsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s := fmt.Sprintf("invalid method: %s for %s, should be POST", r.Method, r.URL.String())
		glog.Error(s)
		t.invalmsghdlr(w, r, s)
		return
	}
	var bufSize int64
	if r.ContentLength > 0 {
		bufSize = r.ContentLength
	}
	bufReader := bytes.NewBuffer(make([]byte, 0, bufSize))
	if _, err := io.Copy(bufReader, r.Body); err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, fmt.Sprintf("could not read request body, err: %v", err))
		return
	}
	if err := js.Unmarshal(bufReader.Bytes(), &t.dsortManager.Shards); err != nil {
		glog.Error(err)
		t.invalmsghdlr(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err))
		return
	}
	t.dsortManager.StartShardCreation <- struct{}{}
}

// randomTargetOrder returns a daemonInfo slice for targets in a pseudorandom order.
func randomTargetOrder(salt uint64, tmap map[string]*daemonInfo) []*daemonInfo {
	targets := make(map[uint64]*daemonInfo, len(tmap))
	var keys []uint64
	for i, d := range tmap {
		c := xxhash.Checksum64S([]byte(i), salt)
		targets[c] = d
		keys = append(keys, c)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	var t []*daemonInfo
	for _, k := range keys {
		t = append(t, targets[k])
	}
	return t
}

// targetURLForShardRequest returns the optimal target's URL for a shard creation request.
// The target chosen is determined based on:
// 1) Locality of shard source files, and in a tie situation,
// 2) Number of shard creation requests previously sent to the target.
func (t *targetrunner) targetURLForShardRequest(
	shardsToTarget map[string][]dsort.Shard, numSourceFilesLocalToTarget map[string]int) string {
	var max int
	var u string
	var numSentToCur int

	for k, v := range numSourceFilesLocalToTarget {
		if v > max {
			numSentToCur = len(shardsToTarget[k])
			max = v
			u = k
		} else if v == max {
			// If a shard has equal number of source files in multiple targets, send request to the target
			// with the least requests sent to it so far.
			if len(shardsToTarget[k]) < numSentToCur {
				numSentToCur = len(shardsToTarget[k])
				u = k
			}
		}
	}
	return u
}
