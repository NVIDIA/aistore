// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/dfc/statsd"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const logsTotalSizeCheckTime = time.Hour * 3

//==============================
//
// types
//
//==============================
type fscapacity struct {
	Used    uint64 `json:"used"`    // bytes
	Avail   uint64 `json:"avail"`   // ditto
	Usedpct uint32 `json:"usedpct"` // reduntant ok
}

// implemented by the stats runners
type statslogger interface {
	log() (runlru bool)
	housekeep(bool)
}

// implemented by the ***CoreStats types
type statsif interface {
	add(name string, val int64)
	addMany(nameval ...interface{})
}

// TODO: use static map[string]int64
type proxyCoreStats struct {
	Numget      int64 `json:"numget"`
	Numput      int64 `json:"numput"`
	Numpost     int64 `json:"numpost"`
	Numdelete   int64 `json:"numdelete"`
	Numrename   int64 `json:"numrename"`
	Numlist     int64 `json:"numlist"`
	Getlatency  int64 `json:"getlatency"`  // microseconds
	Putlatency  int64 `json:"putlatency"`  // ---/---
	Listlatency int64 `json:"listlatency"` // ---/---
	Numerr      int64 `json:"numerr"`
	// omitempty
	ngets  int64
	nputs  int64
	nlists int64
	logged bool
}

type targetCoreStats struct {
	proxyCoreStats
	Numcoldget       int64 `json:"numcoldget"`
	Bytesloaded      int64 `json:"bytesloaded"`
	Bytesevicted     int64 `json:"bytesevicted"`
	Filesevicted     int64 `json:"filesevicted"`
	Numsentfiles     int64 `json:"numsentfiles"`
	Numsentbytes     int64 `json:"numsentbytes"`
	Numrecvfiles     int64 `json:"numrecvfiles"`
	Numrecvbytes     int64 `json:"numrecvbytes"`
	Numprefetch      int64 `json:"numprefetch"`
	Bytesprefetched  int64 `json:"bytesprefetched"`
	Numvchanged      int64 `json:"numvchanged"`
	Bytesvchanged    int64 `json:"bytesvchanged"`
	Numbadchecksum   int64 `json:"numbadchecksum"`
	Bytesbadchecksum int64 `json:"bytesbadchecksum"`
}

type statsrunner struct {
	sync.Mutex
	namedrunner
	statslogger
	chsts chan struct{}
}

type proxystatsrunner struct {
	statsrunner `json:"-"`
	Core        proxyCoreStats `json:"core"`
}

type storstatsrunner struct {
	statsrunner `json:"-"`
	Core        targetCoreStats        `json:"core"`
	Capacity    map[string]*fscapacity `json:"capacity"`
	// iostat
	CPUidle string               `json:"cpuidle"`
	Disk    map[string]simplekvs `json:"disk"`
	// omitempty
	timeUpdatedCapacity time.Time
	timeCheckedLogSizes time.Time
	fsmap               map[syscall.Fsid]string
}

type ClusterStats struct {
	Proxy  *proxyCoreStats             `json:"proxy"`
	Target map[string]*storstatsrunner `json:"target"`
}

type ClusterStatsRaw struct {
	Proxy  *proxyCoreStats            `json:"proxy"`
	Target map[string]json.RawMessage `json:"target"`
}

type iostatrunner struct {
	sync.Mutex
	namedrunner
	chsts       chan struct{}
	CPUidle     string
	metricnames []string
	Disk        map[string]simplekvs
	cmd         *exec.Cmd
}

type (
	XactionStatsRetriever interface {
		getStats([]XactionDetails) ([]byte, error)
	}

	XactionStats struct {
		Kind        string                     `json:"kind"`
		TargetStats map[string]json.RawMessage `json:"target"`
	}

	XactionDetails struct {
		Id        int64     `json:"id"`
		StartTime time.Time `json:"startTime"`
		EndTime   time.Time `json:"endTime"`
		Status    string    `json:"status"`
	}

	RebalanceTargetStats struct {
		Xactions     []XactionDetails `json:"xactionDetails"`
		NumSentFiles int64            `json:"numSentFiles"`
		NumSentBytes int64            `json:"numSentBytes"`
		NumRecvFiles int64            `json:"numRecvFiles"`
		NumRecvBytes int64            `json:"numRecvBytes"`
	}

	RebalanceStats struct {
		Kind        string                          `json:"kind"`
		TargetStats map[string]RebalanceTargetStats `json:"target"`
	}

	PrefetchTargetStats struct {
		Xactions           []XactionDetails `json:"xactionDetails"`
		NumFilesPrefetched int64            `json:"numFilesPrefetched"`
		NumBytesPrefetched int64            `json:"numBytesPrefetched"`
	}

	PrefetchStats struct {
		Kind        string                   `json:"kind"`
		TargetStats map[string]PrefetchStats `json:"target"`
	}
)

//==================
//
// common statsunner
//
//==================
func (r *statsrunner) runcommon(logger statslogger) error {
	r.chsts = make(chan struct{}, 4)

	glog.Infof("Starting %s", r.name)
	ticker := time.NewTicker(ctx.config.Periodic.StatsTime)
	for {
		select {
		case <-ticker.C:
			runlru := logger.log()
			logger.housekeep(runlru)
		case <-r.chsts:
			ticker.Stop()
			return nil
		}
	}
}

func (r *statsrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chsts <- v
	close(r.chsts)
}

// statslogger interface impl
func (r *statsrunner) log() (runlru bool) {
	assert(false)
	return false
}

func (r *statsrunner) housekeep(bool) {
}

//=================
//
// proxystatsrunner
//
//=================
func (r *proxystatsrunner) run() error {
	return r.runcommon(r)
}

// statslogger interface impl
func (r *proxystatsrunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	if r.Core.ngets > 0 {
		r.Core.Getlatency /= r.Core.ngets
	}
	if r.Core.nputs > 0 {
		r.Core.Putlatency /= r.Core.nputs
	}
	if r.Core.nlists > 0 {
		r.Core.Listlatency /= r.Core.nlists
	}
	b, err := json.Marshal(r.Core)
	r.Core.Getlatency, r.Core.Putlatency, r.Core.Listlatency = 0, 0, 0
	r.Core.ngets, r.Core.nputs, r.Core.nlists = 0, 0, 0
	r.Unlock()

	if err == nil {
		glog.Infoln(string(b))
		r.Core.logged = true
	}
	return
}

func (r *proxystatsrunner) add(name string, val int64) {
	r.Lock()
	r.addL(name, val)
	r.Unlock()
}

func (r *proxystatsrunner) addMany(nameval ...interface{}) {
	r.Lock()
	i := 0
	for i < len(nameval) {
		statsname, ok := nameval[i].(string)
		assert(ok, fmt.Sprintf("Invalid stats name: %v, %T", nameval[i], nameval[i]))
		i++
		statsval, ok := nameval[i].(int64)
		assert(ok, fmt.Sprintf("Invalid stats type: %v, %T", nameval[i], nameval[i]))
		i++
		r.addL(statsname, statsval)
	}
	r.Unlock()
}

func (r *proxystatsrunner) addL(name string, val int64) {
	var v *int64
	s := &r.Core
	switch name {
	case "numget":
		v = &s.Numget
	case "numput":
		v = &s.Numput
	case "numpost":
		v = &s.Numpost
	case "numdelete":
		v = &s.Numdelete
	case "numrename":
		v = &s.Numrename
	case "numlist":
		v = &s.Numlist
	case "getlatency":
		v = &s.Getlatency
		s.ngets++
	case "putlatency":
		v = &s.Putlatency
		s.nputs++
	case "listlatency":
		v = &s.Listlatency
		s.nlists++
	case "numerr":
		v = &s.Numerr
	default:
		assert(false, "Invalid stats name "+name)
	}
	*v += val
	// FIXME: This causes a race between this line and the 'logged = true' line in log().
	s.logged = false
}

//================
//
// storstatsrunner
//
//================
func (r *storstatsrunner) run() error {
	r.init()
	return r.runcommon(r)
}

func (r *storstatsrunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	lines := make([]string, 0, 16)
	// core stats
	if r.Core.ngets > 0 {
		r.Core.Getlatency /= r.Core.ngets
	}
	if r.Core.nputs > 0 {
		r.Core.Putlatency /= r.Core.nputs
	}
	if r.Core.nlists > 0 {
		r.Core.Listlatency /= r.Core.nlists
	}

	b, err := json.Marshal(r.Core)
	r.Core.Getlatency, r.Core.Putlatency, r.Core.Listlatency = 0, 0, 0
	r.Core.ngets, r.Core.nputs, r.Core.nlists = 0, 0, 0
	if err == nil {
		lines = append(lines, string(b))
	}
	// capacity
	if time.Since(r.timeUpdatedCapacity) >= ctx.config.LRU.CapacityUpdTime {
		runlru = r.updateCapacity()
		r.timeUpdatedCapacity = time.Now()
		for _, mpath := range r.fsmap {
			fscapacity := r.Capacity[mpath]
			b, err := json.Marshal(fscapacity)
			if err == nil {
				lines = append(lines, mpath+": "+string(b))
			}
		}
	}

	// disk
	riostat := getiostatrunner()
	if riostat != nil {
		riostat.Lock()
		r.CPUidle = riostat.CPUidle
		for dev, iometrics := range riostat.Disk {
			// FIXME (benign) storstatsrunner and iostatrunner share the same 'metrics'
			// e.g. when responding to http get stats, we do not currently take riostat.Lock
			r.Disk[dev] = iometrics
			if riostat.isZeroUtil(dev) {
				continue // skip zeros
			}
			b, err := json.Marshal(r.Disk[dev])
			if err == nil {
				lines = append(lines, dev+": "+string(b))
			}

			var stats []statsd.Metric
			for k, v := range iometrics {
				stats = append(stats, statsd.Metric{
					Type:  statsd.Gauge,
					Name:  k,
					Value: v,
				})
			}

			gettarget().statsdC.Send("iostat_"+dev, stats...)
		}

		lines = append(lines, fmt.Sprintf("CPU idle: %s%%", r.CPUidle))
		riostat.Unlock()
	}

	r.Core.logged = true
	r.Unlock()

	// log
	for _, ln := range lines {
		glog.Infoln(ln)
	}
	return
}

func (r *storstatsrunner) housekeep(runlru bool) {
	t := gettarget()

	if runlru && ctx.config.LRU.LRUEnabled {
		go t.runLRU()
	}

	// Run prefetch operation if there are items to be prefetched
	if len(t.prefetchQueue) > 0 {
		go t.doPrefetch()
	}

	// keep total log size below the configured max
	if time.Since(r.timeCheckedLogSizes) >= logsTotalSizeCheckTime {
		go r.removeLogs(ctx.config.Log.MaxTotal)
		r.timeCheckedLogSizes = time.Now()
	}
}

func (r *storstatsrunner) removeLogs(maxtotal uint64) {
	logfinfos, err := ioutil.ReadDir(ctx.config.Log.Dir)
	if err != nil {
		glog.Errorf("GC logs: cannot read log dir %s, err: %v", ctx.config.Log.Dir, err)
		return // ignore error
	}
	// sample name dfc.ip-10-0-2-19.root.log.INFO.20180404-031540.2249
	var logtypes = []string{".INFO.", ".WARNING.", ".ERROR."}
	for _, logtype := range logtypes {
		var (
			tot   = int64(0)
			infos = []os.FileInfo{}
		)
		for _, logfi := range logfinfos {
			if logfi.IsDir() {
				continue
			}
			if !strings.Contains(logfi.Name(), ".log.") {
				continue
			}
			if strings.Contains(logfi.Name(), logtype) {
				tot += logfi.Size()
				infos = append(infos, logfi)
			}
		}
		if tot > int64(maxtotal) {
			if len(infos) <= 1 {
				glog.Errorf("GC logs: %s, total %d for type %s, max %d", ctx.config.Log.Dir, tot, logtype, maxtotal)
				continue
			}
			r.removeOlderLogs(tot, int64(maxtotal), infos)
		}
	}
}

func (r *storstatsrunner) removeOlderLogs(tot, maxtotal int64, filteredInfos []os.FileInfo) {
	fiLess := func(i, j int) bool {
		return filteredInfos[i].ModTime().Before(filteredInfos[j].ModTime())
	}
	if glog.V(3) {
		glog.Infof("GC logs: started")
	}
	sort.Slice(filteredInfos, fiLess)
	for _, logfi := range filteredInfos[:len(filteredInfos)-1] { // except last = current
		logfqn := ctx.config.Log.Dir + "/" + logfi.Name()
		if err := os.Remove(logfqn); err == nil {
			tot -= logfi.Size()
			glog.Infof("GC logs: removed %s", logfqn)
			if tot < maxtotal {
				break
			}
		} else {
			glog.Errorf("GC logs: failed to remove %s", logfqn)
		}
	}
	if glog.V(3) {
		glog.Infof("GC logs: done")
	}
}

func (r *storstatsrunner) updateCapacity() (runlru bool) {
	for _, mpath := range r.fsmap {
		statfs := &syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		fscapacity := r.Capacity[mpath]
		r.fillfscap(fscapacity, statfs)
		if fscapacity.Usedpct >= ctx.config.LRU.HighWM {
			runlru = true
		}
	}
	return
}

func (r *storstatsrunner) fillfscap(fscapacity *fscapacity, statfs *syscall.Statfs_t) {
	fscapacity.Used = (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize)
	fscapacity.Avail = statfs.Bavail * uint64(statfs.Bsize)
	fscapacity.Usedpct = uint32((statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks)
}

func (r *storstatsrunner) init() {
	r.Disk = make(map[string]simplekvs, 8)
	// local filesystems and their cap-s
	r.Capacity = make(map[string]*fscapacity)
	r.fsmap = make(map[syscall.Fsid]string)
	for mpath, mountpath := range ctx.mountpaths.Available {
		mp1, ok := r.fsmap[mountpath.Fsid]
		if ok {
			// the same filesystem: usage cannot be different..
			assert(r.Capacity[mp1] != nil)
			r.Capacity[mpath] = r.Capacity[mp1]
			continue
		}
		statfs := &syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		r.fsmap[mountpath.Fsid] = mpath
		r.Capacity[mpath] = &fscapacity{}
		r.fillfscap(r.Capacity[mpath], statfs)
	}
}

func (r *storstatsrunner) add(name string, val int64) {
	r.Lock()
	r.addL(name, val)
	r.Unlock()
}

// FIXME: copy paste
func (r *storstatsrunner) addMany(nameval ...interface{}) {
	r.Lock()
	i := 0
	for i < len(nameval) {
		statsname, ok := nameval[i].(string)
		assert(ok, fmt.Sprintf("Invalid stats name: %v, %T", nameval[i], nameval[i]))
		i++
		statsval, ok := nameval[i].(int64)
		assert(ok, fmt.Sprintf("Invalid stats type: %v, %T", nameval[i], nameval[i]))
		i++
		r.addL(statsname, statsval)
	}
	r.Unlock()
}

func (r *storstatsrunner) addL(name string, val int64) {
	var v *int64
	s := &r.Core
	switch name {
	// common
	case "numget":
		v = &s.Numget
	case "numput":
		v = &s.Numput
	case "numpost":
		v = &s.Numpost
	case "numdelete":
		v = &s.Numdelete
	case "numrename":
		v = &s.Numrename
	case "numlist":
		v = &s.Numlist
	case "getlatency":
		v = &s.Getlatency
		s.ngets++
	case "putlatency":
		v = &s.Putlatency
		s.nputs++
	case "listlatency":
		v = &s.Listlatency
		s.nlists++
	case "numerr":
		v = &s.Numerr
	// target only
	case "numcoldget":
		v = &s.Numcoldget
	case "bytesloaded":
		v = &s.Bytesloaded
	case "bytesevicted":
		v = &s.Bytesevicted
	case "filesevicted":
		v = &s.Filesevicted
	case "numsentfiles":
		v = &s.Numsentfiles
	case "numsentbytes":
		v = &s.Numsentbytes
	case "numrecvfiles":
		v = &s.Numrecvfiles
	case "numrecvbytes":
		v = &s.Numrecvbytes
	case "numprefetch":
		v = &s.Numprefetch
	case "bytesprefetched":
		v = &s.Bytesprefetched
	case "numvchanged":
		v = &s.Numvchanged
	case "bytesvchanged":
		v = &s.Bytesvchanged
	case "numbadchecksum":
		v = &s.Numbadchecksum
	case "bytesbadchecksum":
		v = &s.Bytesbadchecksum
	default:
		assert(false, "Invalid stats name "+name)
	}
	*v += val
	s.logged = false
}

func (p PrefetchTargetStats) getStats(allXactionDetails []XactionDetails) (
	[]byte, error) {
	storageStatsRunner := getstorstatsrunner()
	storageStatsRunner.Lock()
	prefetchXactionStats := PrefetchTargetStats{
		Xactions:           allXactionDetails,
		NumBytesPrefetched: storageStatsRunner.Core.Numprefetch,
		NumFilesPrefetched: storageStatsRunner.Core.Bytesprefetched,
	}
	storageStatsRunner.Unlock()
	jsonBytes, err := json.Marshal(prefetchXactionStats)
	if err != nil {
		err = fmt.Errorf(
			"Unable to marshal prefetchXactionStats. Error: %v",
			err)
		return []byte{}, err
	}

	return jsonBytes, nil
}

func (r RebalanceTargetStats) getStats(allXactionDetails []XactionDetails) (
	[]byte, error) {
	storageStatsRunner := getstorstatsrunner()
	storageStatsRunner.Lock()
	rebalanceXactionStats := RebalanceTargetStats{
		Xactions:     allXactionDetails,
		NumRecvBytes: storageStatsRunner.Core.Numrecvbytes,
		NumRecvFiles: storageStatsRunner.Core.Numrecvfiles,
		NumSentBytes: storageStatsRunner.Core.Numsentbytes,
		NumSentFiles: storageStatsRunner.Core.Numsentfiles,
	}
	storageStatsRunner.Unlock()
	jsonBytes, err := json.Marshal(rebalanceXactionStats)
	if err != nil {
		err = fmt.Errorf(
			"Unable to marshal rebalanceXactionStats. Error: %v",
			err)
		return []byte{}, err
	}

	return jsonBytes, nil
}
