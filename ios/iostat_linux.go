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
	"sync"
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

type IostatRunner struct {
	sync.RWMutex
	cmn.NamedID
	// public
	Disk    map[string]cmn.SimpleKVs
	CPUidle string
	// private
	mountpaths  *fs.MountedFS
	stopCh      chan struct{}
	metricnames []string
	reader      *bufio.Reader
	process     *os.Process // running iostat process. Required so it can be killed later
	fsdisks     map[string]cmn.StringSet
}

func NewIostatRunner(mountpaths *fs.MountedFS) *IostatRunner {
	return &IostatRunner{
		mountpaths:  mountpaths,
		stopCh:      make(chan struct{}, 1),
		Disk:        make(map[string]cmn.SimpleKVs),
		metricnames: make([]string, 0),
	}
}

var (
	_ fs.PathRunner      = &IostatRunner{} // as an fsprunner
	_ cmn.ConfigListener = &IostatRunner{}
)

func (r *IostatRunner) ReqEnableMountpath(mpath string)  { r.updateFSDisks() }
func (r *IostatRunner) ReqDisableMountpath(mpath string) { r.updateFSDisks() }
func (r *IostatRunner) ReqAddMountpath(mpath string)     { r.updateFSDisks() }
func (r *IostatRunner) ReqRemoveMountpath(mpath string)  { r.updateFSDisks() }

func (r *IostatRunner) ConfigUpdate(config *cmn.Config) {
	if err := r.runIostat(config.Periodic.StatsTime); err != nil {
		r.Stop(err)
	}
}

//
// API
//

func (r *IostatRunner) runIostat(period time.Duration) error {
	if r.process != nil {
		// kill previous process if running, it can happen when config was updated
		if err := r.process.Kill(); err != nil {
			return err
		}
	}

	refreshPeriod := int(period / time.Second)
	cmd := exec.Command("iostat", "-cdxtm", strconv.Itoa(refreshPeriod))
	stdout, err := cmd.StdoutPipe()
	r.reader = bufio.NewReader(stdout)
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}

	// Assigning started process
	r.process = cmd.Process
	return nil
}

// iostat -cdxtm 10
func (r *IostatRunner) Run() error {
	r.updateFSDisks()

	// subscribe for config changes
	cmn.GCO.Subscribe(r)

	glog.Infof("Starting %s", r.Getname())
	if err := r.runIostat(cmn.GCO.Get().Periodic.StatsTime); err != nil {
		return err
	}

	for {
		b, err := r.reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return err
		}

		line := string(b)
		fields := strings.Fields(line)
		if len(fields) == iostatnumsys {
			r.Lock()
			r.CPUidle = fields[iostatnumsys-1]
			r.Unlock()
		} else if len(fields) >= iostatnumdsk {
			if strings.HasPrefix(fields[0], "Device") {
				if len(r.metricnames) == 0 {
					r.metricnames = append(r.metricnames, fields[1:]...)
				}
			} else {
				r.Lock()
				device := fields[0]
				var (
					iometrics cmn.SimpleKVs
					ok        bool
				)
				if iometrics, ok = r.Disk[device]; !ok {
					iometrics = make(cmn.SimpleKVs, len(fields)-1) // first time
				}
				for i := 1; i < len(fields); i++ {
					name := r.metricnames[i-1]
					iometrics[name] = fields[i]
				}
				r.Disk[device] = iometrics
				r.Unlock()
			}
		}
		select {
		case <-r.stopCh:
			return nil
		default:
		}
	}
}

func (r *IostatRunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)

	// Kill process if started
	if r.process != nil {
		if err := r.process.Kill(); err != nil {
			glog.Errorf("Failed to kill iostat, err: %v", err)
		}
	}
}

func (r *IostatRunner) MaxUtilFS(fs string) (util float32, ok bool) {
	r.RLock()
	disks, isOk := r.fsdisks[fs]
	if !isOk {
		r.RUnlock()
		return
	}
	util = float32(maxUtilDisks(r.Disk, disks))
	r.RUnlock()
	if util < 0 {
		return
	}
	return util, true
}

// CheckIostatVersion determines whether iostat is present and current
func CheckIostatVersion() error {
	cmd := exec.Command("iostat", "-V")

	vbytes, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("[iostat] Error: %v", err)
	}

	vwords := strings.Split(string(vbytes), "\n")
	if vwords = strings.Split(vwords[0], " "); len(vwords) < 3 {
		return fmt.Errorf("[iostat] Error: unknown iostat version format %v", vwords)
	}

	vss := strings.Split(vwords[2], ".")
	if len(vss) < 3 {
		return fmt.Errorf("[iostat] Error: unexpected version format: %v", vss)
	}

	version, err := strconv.ParseInt(vss[0], 10, 64)
	if err != nil {
		return fmt.Errorf("[iostat] Error: failed to parse version %v, error %v", vss, err)
	}

	if version < iostatMinVersion {
		return fmt.Errorf("[iostat] Error: version %v is too old. At least %v version is required", version, iostatMinVersion)
	}

	return nil
}

//
// private
//

func (r *IostatRunner) IsZeroUtil(dev string) bool {
	iometrics := r.Disk[dev]
	if utilstr, ok := iometrics["%util"]; ok {
		if util, err := strconv.ParseFloat(utilstr, 32); err == nil {
			if util == 0 {
				return true
			}
		}
	}
	return false
}

func (r *IostatRunner) updateFSDisks() {
	availablePaths, _ := fs.Mountpaths.Get()
	r.Lock()
	r.fsdisks = make(map[string]cmn.StringSet, len(availablePaths))
	for _, mpathInfo := range availablePaths {
		disks := fs2disks(mpathInfo.FileSystem)
		if len(disks) == 0 {
			glog.Errorf("filesystem (%+v) - no disks?", mpathInfo)
			continue
		}
		r.fsdisks[mpathInfo.FileSystem] = disks
	}
	r.Unlock()
}

func (r *IostatRunner) diskUtilFromFQN(fqn string) (util float32, ok bool) {
	mpathInfo, _ := r.mountpaths.Path2MpathInfo(fqn)
	if mpathInfo == nil {
		return
	}
	return r.MaxUtilFS(mpathInfo.FileSystem)
}
