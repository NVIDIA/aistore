// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

const (
	iostatMinVersion = 11
)

type (
	IostatRunner struct {
		cmn.NamedID
		stopCh      chan error    // terminate
		syncCh      chan struct{} // synchronize disks, maps, mountpaths
		metricNames []string
		process     *os.Process  // running iostat process. Required so it can be killed later
		ticker      *time.Ticker // logging ticker
		fs2disks    map[string]cmn.StringSet
		disks2mpath cmn.SimpleKVs
		stats       struct {
			dutil          map[string]float32           // disk utilizations (iostat's %util)
			availablePaths map[string]*fs.MountpathInfo // cached fs.Mountpaths.Get
		}
	}
	DevIOMetrics map[string]float32
)

func NewIostatRunner() *IostatRunner {
	return &IostatRunner{
		stopCh:      make(chan error, 2),
		syncCh:      make(chan struct{}, 1),
		metricNames: make([]string, 0, 16),
	}
}

var (
	_ fs.PathRunner      = &IostatRunner{} // as an fsprunner
	_ cmn.ConfigListener = &IostatRunner{}
)

func (r *IostatRunner) ReqEnableMountpath(mpath string)  { r.syncCh <- struct{}{} }
func (r *IostatRunner) ReqDisableMountpath(mpath string) { r.syncCh <- struct{}{} }
func (r *IostatRunner) ReqAddMountpath(mpath string)     { r.syncCh <- struct{}{} }
func (r *IostatRunner) ReqRemoveMountpath(mpath string)  { r.syncCh <- struct{}{} }

// subscribing to config changes
func (r *IostatRunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	if oldConf.Periodic.IostatTime != newConf.Periodic.IostatTime {
		r.ticker.Stop()
		r.ticker = time.NewTicker(newConf.Periodic.IostatTime)
	}
}

//
// public methods
//

// This code parses iostat output specifically looking for "Device", "%util",  "aqu-sz" and "avgqu-sz"
func (r *IostatRunner) Run() error {
	var (
		lines  cmn.SimpleKVs
		epoch  int64
		lm, lc int64 // time interval: multiplier and counter
	)
	glog.Infof("Starting %s", r.Getname())
	r.resyncMpathsDisks()
	d := cmn.GCO.Get().Periodic.IostatTime
	r.ticker = time.NewTicker(d) // epoch = one tick
	lm = cmn.DivCeil(int64(cmn.GCO.Get().Periodic.StatsTime), int64(d))
	lines = make(cmn.SimpleKVs, 16)

	cmn.GCO.Subscribe(r)

	// main loop
	for {
		select {
		case err := <-r.stopCh:
			r.cleanup()
			return err
		case <-r.syncCh:
			r.resyncMpathsDisks()
		case <-r.ticker.C:
			epoch++
			lc++

			fetchedDiskStats := GetDiskStats()
			for disk, mpath := range r.disks2mpath {
				stat, ok := fetchedDiskStats[disk]
				if !ok {
					continue
				}
				mpathInfo := r.stats.availablePaths[mpath]

				mpathInfo.SetIOstats(epoch, fs.StatDiskIOms, float32(stat.IOMs))
				if prev, cur := mpathInfo.GetIOstats(fs.StatDiskIOms); prev.Max != 0 {
					msElapsed := d.Nanoseconds() / (1000 * 1000) //convert to Milliseconds
					mpathInfo.SetIOstats(epoch, fs.StatDiskUtil, (cur.Max-prev.Max)*100/float32(msElapsed))
				}

				if lc >= lm {
					lines[disk] = stat.ToString()
				}
			}

			if lc >= lm {
				log(lines)
				lines = make(cmn.SimpleKVs, 16)
				lc = 0
			}
		}
	}
}

func (r *IostatRunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- err
	close(r.stopCh)
}

// CheckIostatVersion determines whether iostat is present and current
func CheckIostatVersion() error {
	const prefix = "[iostat] Error:"
	cmd := exec.Command("iostat", "-V")

	vbytes, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v", prefix, err)
	}

	vwords := strings.Split(string(vbytes), "\n")
	if vwords = strings.Split(vwords[0], " "); len(vwords) < 3 {
		return fmt.Errorf("%s unknown version format %v", prefix, vwords)
	}

	vss := strings.Split(vwords[2], ".")
	if len(vss) < 3 {
		return fmt.Errorf("%s unexpected version format %+v", prefix, vss)
	}

	version, err := strconv.ParseInt(vss[0], 10, 64)
	if err != nil {
		return fmt.Errorf("%s failed to parse version %+v, error %v", prefix, vss, err)
	}

	if version < iostatMinVersion {
		return fmt.Errorf("%s version %v is too old, expecting %v or later", prefix, version, iostatMinVersion)
	}

	return nil
}

//
// private methods
//

// "resync" gets triggered by mountpath changes which may or may not coincide with
// disk degraded/faulted/removed/added type changes - TODO
func (r *IostatRunner) resyncMpathsDisks() {
	availablePaths, _ := fs.Mountpaths.Get()
	l := len(availablePaths)
	r.stats.dutil = make(map[string]float32, l)
	r.stats.availablePaths, _ = fs.Mountpaths.Get()
	r.fs2disks = make(map[string]cmn.StringSet, len(availablePaths))
	r.disks2mpath = make(cmn.SimpleKVs, 16)
	for mpath, mpathInfo := range availablePaths {
		disks := fs2disks(mpathInfo.FileSystem)
		if len(disks) == 0 {
			glog.Errorf("filesystem (%+v) - no disks?", mpathInfo)
			continue
		}
		r.fs2disks[mpathInfo.FileSystem] = disks
		for dev := range disks {
			r.disks2mpath[dev] = mpath
		}
		r.stats.dutil[mpath] = -1
	}
}

func (r *IostatRunner) cleanup() {
	if r.ticker != nil {
		r.ticker.Stop()
	}
	// Kill process if started
	if r.process != nil {
		if err := r.process.Kill(); err != nil {
			glog.Errorf("Failed to kill iostat, err: %v", err)
		}
		r.process = nil
	}
}
