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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
)

const logsTotalSizeCheckTime = time.Hour * 3

//==============================
//
// types
//
//==============================
type metric = statsd.Metric // type alias

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
	Kalivemin   int64 `json:"kalivemin"`   // ---/---
	Kalivemax   int64 `json:"kalivemax"`   // ---/---
	Kalive      int64 `json:"kalive"`      // ---/---
	Uptime      int64 `json:"uptime"`      // proxy or cluster uptime: microseconds
	Numerr      int64 `json:"numerr"`
	// omitempty
	statsdC               *statsd.Client
	ngets                 int64
	nputs                 int64
	nlists                int64
	nkcalls, nkmin, nkmax int64
	logged                bool
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
	sync.RWMutex
	namedrunner
	chsts     chan struct{}
	starttime time.Time
}

type proxystatsrunner struct {
	statsrunner
	Core proxyCoreStats `json:"core"`
}

type storstatsrunner struct {
	statsrunner
	Core     targetCoreStats        `json:"core"`
	Capacity map[string]*fscapacity `json:"capacity"`
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
	sync.RWMutex
	namedrunner
	chsts       chan struct{}
	CPUidle     string
	metricnames []string
	Disk        map[string]simplekvs
	process     *os.Process // running iostat process. Required so it can be killed later
	fsdisks     map[string]StringSet
}

type (
	XactionStatsRetriever interface {
		getStats([]XactionDetails) []byte
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
	r.starttime = time.Now()

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
	if r.Core.nkcalls > 0 {
		r.Core.Kalive /= r.Core.nkcalls
	}
	if r.Core.nkmin > 0 {
		r.Core.Kalivemin /= r.Core.nkmin
	}
	if r.Core.nkmax > 0 {
		r.Core.Kalivemax /= r.Core.nkmax
	}
	b, err := json.Marshal(r.Core)
	r.Core.Getlatency, r.Core.Putlatency, r.Core.Listlatency = 0, 0, 0
	r.Core.Kalivemin, r.Core.Kalivemax, r.Core.Kalive = 0, 0, 0
	r.Core.ngets, r.Core.nputs, r.Core.nlists = 0, 0, 0
	r.Core.nkcalls, r.Core.nkmin, r.Core.nkmax = 0, 0, 0
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
	s := &r.Core
	s.addL(name, val)
}

func (s *proxyCoreStats) addL(name string, val int64) {
	var v *int64
	switch name {
	case "numget":
		v = &s.Numget
	case "numput":
		v = &s.Numput
	case "numpost":
		v = &s.Numpost
		s.statsdC.Send("cluster_post", metric{statsd.Counter, "count", val})
	case "numdelete":
		v = &s.Numdelete
		s.statsdC.Send("delete", metric{statsd.Counter, "count", val})
	case "numrename":
		v = &s.Numrename
		s.statsdC.Send("rename", metric{statsd.Counter, "count", val})
	case "numlist":
		v = &s.Numlist
	case "getlatency":
		v = &s.Getlatency
		s.ngets++
		s.statsdC.Send("get",
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	case "putlatency":
		v = &s.Putlatency
		s.nputs++
		s.statsdC.Send("put",
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	case "listlatency":
		v = &s.Listlatency
		s.nlists++
		s.statsdC.Send("list",
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	case "kalive":
		v = &s.Kalive
		s.nkcalls++
	case "kalivemax":
		v = &s.Kalivemax
		s.nkmax++
	case "kalivemin":
		v = &s.Kalivemin
		s.nkmin++
	case "numerr":
		v = &s.Numerr
	default:
		assert(false, "Invalid stats name "+name)
	}
	*v += val
	s.logged = false
}

//================
//
// storstatsrunner
//
//================
func newFSCapacity(statfs *syscall.Statfs_t) *fscapacity {
	return &fscapacity{
		Used:    (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize),
		Avail:   statfs.Bavail * uint64(statfs.Bsize),
		Usedpct: uint32((statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks),
	}
}

func (r *storstatsrunner) run() error {
	r.init()
	return r.runcommon(r)
}

func (r *storstatsrunner) init() {
	r.Disk = make(map[string]simplekvs, 8)
	r.updateCapacity()
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
	if r.Core.nkcalls > 0 {
		r.Core.Kalive /= r.Core.nkcalls
	}
	if r.Core.nkmin > 0 {
		r.Core.Kalivemin /= r.Core.nkmin
	}
	if r.Core.nkmax > 0 {
		r.Core.Kalivemax /= r.Core.nkmax
	}
	r.Core.Uptime = int64(time.Since(r.starttime) / time.Microsecond)

	b, err := json.Marshal(r.Core)
	r.Core.Getlatency, r.Core.Putlatency, r.Core.Listlatency = 0, 0, 0
	r.Core.Kalivemin, r.Core.Kalivemax, r.Core.Kalive = 0, 0, 0
	r.Core.ngets, r.Core.nputs, r.Core.nlists = 0, 0, 0
	r.Core.nkcalls, r.Core.nkmin, r.Core.nkmax = 0, 0, 0
	if err == nil {
		lines = append(lines, string(b))
	}
	// capacity
	if time.Since(r.timeUpdatedCapacity) >= ctx.config.LRU.CapacityUpdTime {
		runlru = r.updateCapacity()
		r.timeUpdatedCapacity = time.Now()
		for mpath, fsCapacity := range r.Capacity {
			b, err := json.Marshal(fsCapacity)
			if err == nil {
				lines = append(lines, mpath+": "+string(b))
			}
		}
	}

	// disk
	riostat := getiostatrunner()
	riostat.RLock()
	r.CPUidle = riostat.CPUidle
	for dev, iometrics := range riostat.Disk {
		r.Disk[dev] = iometrics
		if riostat.isZeroUtil(dev) {
			continue // skip zeros
		}
		b, err := json.Marshal(r.Disk[dev])
		if err == nil {
			lines = append(lines, dev+": "+string(b))
		}

		stats := make([]metric, len(iometrics))
		idx := 0
		for k, v := range iometrics {
			stats[idx] = metric{statsd.Gauge, k, v}
			idx++
		}
		gettarget().statsdC.Send("iostat_"+dev, stats...)
	}
	riostat.RUnlock()

	lines = append(lines, fmt.Sprintf("CPU idle: %s%%", r.CPUidle))

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
			infos = make([]os.FileInfo, 0, len(logfinfos))
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
	availableMountpaths, _ := ctx.mountpaths.Mountpaths()
	capacities := make(map[string]*fscapacity, len(availableMountpaths))

	for mpath := range availableMountpaths {
		statfs := &syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		fsCap := newFSCapacity(statfs)
		capacities[mpath] = fsCap
		if fsCap.Usedpct >= ctx.config.LRU.HighWM {
			runlru = true
		}
	}

	r.Capacity = capacities
	return
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
	s := &r.Core
	s.addL(name, val)
}

func (s *targetCoreStats) addL(name string, val int64) {
	var v *int64
	switch name {
	// common
	case "numget", "numput", "numpost", "numdelete", "numrename", "numlist",
		"getlatency", "putlatency", "listlatency",
		"kalive", "kalivemin", "kalivemax", "numerr":
		s.proxyCoreStats.addL(name, val)
		return
	// target only
	case "numcoldget":
		v = &s.Numcoldget
	case "bytesloaded":
		v = &s.Bytesloaded
		s.statsdC.Send("get.cold",
			metric{statsd.Counter, "count", 1},
			metric{statsd.Counter, "bytesloaded", val})
	case "bytesevicted":
		v = &s.Bytesevicted
		s.statsdC.Send("evict", metric{statsd.Counter, "bytes", val})
	case "filesevicted":
		v = &s.Filesevicted
		s.statsdC.Send("evict", metric{statsd.Counter, "files", val})
	case "numsentfiles":
		v = &s.Numsentfiles
		s.statsdC.Send("rebalance.send", metric{statsd.Counter, "files", val})
	case "numsentbytes":
		v = &s.Numsentbytes
		s.statsdC.Send("rebalance.send", metric{statsd.Counter, "bytes", val})
	case "numrecvfiles":
		v = &s.Numrecvfiles
		s.statsdC.Send("rebalance.receive", metric{statsd.Counter, "files", val})
	case "numrecvbytes":
		v = &s.Numrecvbytes
		s.statsdC.Send("rebalance.receive", metric{statsd.Counter, "bytes", val})
	case "numprefetch":
		v = &s.Numprefetch
	case "bytesprefetched":
		v = &s.Bytesprefetched
	case "numvchanged":
		v = &s.Numvchanged
	case "bytesvchanged":
		v = &s.Bytesvchanged
		s.statsdC.Send("get.cold",
			metric{statsd.Counter, "vchanged", 1},
			metric{statsd.Counter, "bytesvchanged", val})
	case "numbadchecksum":
		v = &s.Numbadchecksum
		s.statsdC.Send("error.badchecksum", metric{statsd.Counter, "count", val})
	case "bytesbadchecksum":
		v = &s.Bytesbadchecksum
		s.statsdC.Send("error.badchecksum", metric{statsd.Counter, "bytes", val})
	default:
		assert(false, "Invalid stats name "+name)
	}
	*v += val
	s.logged = false
}

func (p PrefetchTargetStats) getStats(allXactionDetails []XactionDetails) []byte {
	rstor := getstorstatsrunner()
	rstor.RLock()
	prefetchXactionStats := PrefetchTargetStats{
		Xactions:           allXactionDetails,
		NumBytesPrefetched: rstor.Core.Numprefetch,
		NumFilesPrefetched: rstor.Core.Bytesprefetched,
	}
	rstor.RUnlock()
	jsonBytes, err := json.Marshal(prefetchXactionStats)
	assert(err == nil, err)
	return jsonBytes
}

func (r RebalanceTargetStats) getStats(allXactionDetails []XactionDetails) []byte {
	rstor := getstorstatsrunner()
	rstor.RLock()
	rebalanceXactionStats := RebalanceTargetStats{
		Xactions:     allXactionDetails,
		NumRecvBytes: rstor.Core.Numrecvbytes,
		NumRecvFiles: rstor.Core.Numrecvfiles,
		NumSentBytes: rstor.Core.Numsentbytes,
		NumSentFiles: rstor.Core.Numsentfiles,
	}
	rstor.RUnlock()
	jsonBytes, err := json.Marshal(rebalanceXactionStats)
	assert(err == nil, err)
	return jsonBytes
}

func getToEvict(mpath string, hwm uint32, lwm uint32) (int64, error) {
	blocks, bavail, bsize, err := getFSStats(mpath)
	if err != nil {
		return -1, err
	}
	used := blocks - bavail
	usedpct := used * 100 / blocks
	glog.Infof("Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%", blocks, bavail, usedpct, hwm, lwm)
	if usedpct < uint64(hwm) {
		return 0, nil // 0 to evict
	}
	lwmblocks := blocks * uint64(lwm) / 100
	return int64(used-lwmblocks) * bsize, nil
}

func getFSUsedPercentage(path string) (usedPercentage uint64, ok bool) {
	totalBlocks, blocksAvailable, _, err := getFSStats(path)
	if err != nil {
		return
	}
	usedBlocks := totalBlocks - blocksAvailable
	return usedBlocks * 100 / totalBlocks, true
}
