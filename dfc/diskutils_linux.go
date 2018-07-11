// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

const (
	iostatnumsys     = 6
	iostatnumdsk     = 14
	iostatMinVersion = 11
)

// iostat -cdxtm 10
func (r *iostatrunner) run() (err error) {
	r.chsts = make(chan struct{}, 1)
	r.Disk = make(map[string]simplekvs, 0)
	r.metricnames = make([]string, 0)
	iostatival := strconv.Itoa(int(ctx.config.Periodic.StatsTime / time.Second))
	cmd := exec.Command("iostat", "-c", "-d", "-x", "-t", "-m", iostatival)
	stdout, err := cmd.StdoutPipe()
	reader := bufio.NewReader(stdout)
	if err != nil {
		return
	}
	if err = cmd.Start(); err != nil {
		return
	}

	// Assigning started process
	r.process = cmd.Process

	glog.Infof("Starting %s", r.name)
	for {
		b, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		line := string(b)
		fields := strings.Fields(line)
		if len(fields) == iostatnumsys {
			r.Lock()
			r.CPUidle = fields[iostatnumsys-1]
			r.Unlock()
		} else if len(fields) == iostatnumdsk {
			if strings.HasPrefix(fields[0], "Device") {
				if len(r.metricnames) == 0 {
					r.metricnames = append(r.metricnames, fields[1:]...)
					assert(len(r.metricnames) == iostatnumdsk-1)
				}
			} else {
				r.Lock()
				device := fields[0]
				var (
					iometrics simplekvs
					ok        bool
				)
				if iometrics, ok = r.Disk[device]; !ok {
					iometrics = make(simplekvs, iostatnumdsk-1) // first time
				}
				for i := 1; i < iostatnumdsk; i++ {
					name := r.metricnames[i-1]
					iometrics[name] = fields[i]
				}
				r.Disk[device] = iometrics
				r.Unlock()
			}
		}
		select {
		case <-r.chsts:
			return nil
		default:
		}
	}
	return
}

func (r *iostatrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.chsts <- v
	close(r.chsts)

	// Do nothing when process was not started
	if r.process == nil {
		return
	}

	if err := r.process.Kill(); err != nil {
		glog.Errorf("Failed to kill iostat, err: %v", err)
	}
}

func (r *iostatrunner) isZeroUtil(dev string) bool {
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

func (r *iostatrunner) getMaxUtil() (maxutil float64) {
	maxutil = -1
	r.Lock()
	for _, iometrics := range r.Disk {
		if utilstr, ok := iometrics["%util"]; ok {
			if util, err := strconv.ParseFloat(utilstr, 32); err == nil {
				if util > maxutil {
					maxutil = util
				}
			}
		}
	}
	r.Unlock()
	return
}

// Check determines whether `iostat` command is present and
// is not too old (at least version `iostatMinVersion` is required).
func (r *iostatrunner) Check() error {
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

	version := []int64{}
	for _, vs := range vss {
		v, err := strconv.ParseInt(vs, 10, 64)
		if err != nil {
			return fmt.Errorf("[iostat] Error: failed to parse version %v", vss)
		}
		version = append(version, v)
	}

	if version[0] < iostatMinVersion {
		return fmt.Errorf("[iostat] Error: version %v is too old. At least %v version is required", version, iostatMinVersion)
	}

	return nil
}
