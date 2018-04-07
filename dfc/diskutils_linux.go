// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

const (
	iostatnumsys = 6
	iostatnumdsk = 14
)

// iostat -cdxtm 10
func (r *iostatrunner) run() (err error) {
	r.chsts = make(chan struct{}, 1)
	r.Disk = make(map[string]deviometrics, 8)
	r.metricnames = make([]string, 0)
	iostatival := strconv.Itoa(int(ctx.config.StatsTime / time.Second))
	r.cmd = exec.Command("iostat", "-c", "-d", "-x", "-t", "-m", iostatival)
	stdout, err := r.cmd.StdoutPipe()
	reader := bufio.NewReader(stdout)
	if err != nil {
		return
	}
	if err = r.cmd.Start(); err != nil {
		return
	}
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
				iometrics := make(map[string]string, iostatnumdsk-1)
				for i := 1; i < iostatnumdsk; i++ {
					iometrics[r.metricnames[i-1]] = fields[i]
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
	if err := r.cmd.Process.Kill(); err != nil {
		glog.Errorf("Failed to kill iostat, err: %v", err)
	}
}

//===========================
//
// check presence and version
//
//===========================
func iostatverok() (ok bool) {
	version := []int64{}
	cmd := exec.Command("iostat", "-V")

	vbytes, err := cmd.CombinedOutput()
	if err != nil {
		glog.Errorf("iostat err: %v", err)
		return
	}
	vwords := strings.Split(string(vbytes), "\n")
	if vwords = strings.Split(vwords[0], " "); len(vwords) < 3 {
		glog.Errorf("iostat: unknown iostat version format %v", vwords)
		return
	}
	vss := strings.Split(vwords[2], ".")
	if len(vss) < 3 {
		glog.Errorf("iostat: unexpected version format: %v", vss)
	}
	for _, vs := range vss {
		v, err := strconv.ParseInt(vs, 10, 64)
		if err != nil {
			glog.Errorf("iostat: failed to parse version %v", vss)
			return
		}
		version = append(version, v)
	}
	if version[0] < 11 {
		glog.Errorf("iostat version %v is too old", version)
		return
	}
	return true
}
