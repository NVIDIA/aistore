// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ios

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

const (
	iostatnumsys     = 6
	iostatnumdsk     = 14
	iostatMinVersion = 11
)

type (
	IostatRunner struct {
		cmn.NamedID
		stopCh      chan struct{} // terminate
		syncCh      chan struct{} // synchronize disks, maps, mountpaths
		metricNames []string
		reader      *bufio.Reader
		process     *os.Process  // running iostat process. Required so it can be killed later
		ticker      *time.Ticker // logging ticker
		fs2disks    map[string]cmn.StringSet
		disks2mpath cmn.SimpleKVs
		stats       struct {
			dutil          map[string]float32           // disk utilizations (iostat's %util)
			dquel          map[string]float32           // disk queue lengths ("avgqu-sz" or "aqu-sz")
			availablePaths map[string]*fs.MountpathInfo // cached fs.Mountpaths.Get
			lastUpdate     time.Time
		}
	}
	DevIOMetrics map[string]float32
)

func NewIostatRunner() *IostatRunner {
	return &IostatRunner{
		stopCh:      make(chan struct{}, 1),
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
		if err := r.execCmd(newConf.Periodic.IostatTime); err != nil {
			r.Stop(err)
		}
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
	if err := r.execCmd(d); err != nil {
		return err
	}
	r.ticker = time.NewTicker(d) // epoch = one tick
	lm = cmn.DivCeil(int64(cmn.GCO.Get().Periodic.StatsTime), int64(d))
	lines = make(cmn.SimpleKVs, 16)

	cmn.GCO.Subscribe(r)

	// main loop
	for {
		b, err := r.reader.ReadBytes('\n')
		if err == io.EOF {
			continue
		}
		if err != nil {
			if err = r.retry(2); err != nil {
				r.cleanup()
				return err
			}
		}
		line := string(b)
		fields := strings.Fields(line)
		if len(fields) < iostatnumdsk {
			continue
		}
		if strings.HasPrefix(fields[0], "Device") {
			if len(r.metricNames) == 0 {
				r.metricNames = append(r.metricNames, fields[1:]...)
			}
			continue
		}
		device := fields[0]
		if mpath, ok := r.disks2mpath[device]; ok {
			var (
				f64 float64
				val float32
			)
			mpathInfo, _ := r.stats.availablePaths[mpath]
			lines[device] = strings.Join(fields, ", ")
			for i := 1; i < len(fields); i++ {
				name := r.metricNames[i-1]
				f64, err = strconv.ParseFloat(fields[i], 32)
				if err != nil {
					continue
				}
				val = float32(f64)
				if name == "%util" {
					mpathInfo.SetIOstats(epoch, fs.StatDiskUtil, val) // FIXME: const
				} else if name == "aqu-sz" || name == "avgqu-sz" {
					mpathInfo.SetIOstats(epoch, fs.StatQueueLen, val)
				}
			}
		}

		select {
		case <-r.stopCh:
			r.cleanup()
			return nil
		case <-r.syncCh:
			r.resyncMpathsDisks()
		case <-r.ticker.C:
			epoch++
			lc++
			if lc >= lm {
				log(lines)
				lc = 0
			}
		default:
		}
	}
}

func (r *IostatRunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
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

func (r *IostatRunner) execCmd(period time.Duration) error {
	if r.process != nil {
		// kill previous process if running - can happen on config change
		if err := r.process.Kill(); err != nil {
			return err
		}
	}

	refreshPeriod := int(period / time.Second)
	cmd := exec.Command("iostat", "-dxm", strconv.Itoa(refreshPeriod)) // the iostat command
	stdout, err := cmd.StdoutPipe()
	r.reader = bufio.NewReader(stdout)
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}

	r.process = cmd.Process
	return nil
}

// "resync" gets triggered by mountpath changes which may or may not coincide with
// disk degraded/faulted/removed/added type changes - TODO
func (r *IostatRunner) resyncMpathsDisks() {
	availablePaths, _ := fs.Mountpaths.Get()
	l := len(availablePaths)
	r.stats.dutil = make(map[string]float32, l)
	r.stats.dquel = make(map[string]float32, l)
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
		r.stats.dquel[mpath] = -1
	}
}

func (r *IostatRunner) retry(cnt int) (err error) {
	for i := 0; i < cnt; i++ {
		glog.Errorf("Error reading StdoutPipe %v, retrying #%d", err, i+1)
		time.Sleep(time.Second)
		_, err = r.reader.ReadBytes('\n')
		if err == io.EOF {
			err = nil
			continue
		}
		if err == nil {
			return
		}
	}
	return
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
	}
}
