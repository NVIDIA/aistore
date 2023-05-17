// Package main
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
)

const iostatColumnCnt = 14

var (
	devices = make(map[string]struct{}) // Replace with the device name to track

	pctUtil = make(map[string]string)
	avgQuSz = make(map[string]string)

	oldIOMs        map[string]int64
	oldIOQueueMs   map[string]int64
	oldMeasureTime time.Time
)

func main() {
	getDiskCommand := exec.Command("lsblk", "-no", "name", "-J")
	outputBytes, err := getDiskCommand.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to lsblk, err %v\n", err)
		os.Exit(1)
	}
	if len(outputBytes) == 0 {
		fmt.Fprintf(os.Stderr, "Failed to lsblk - no disks?\n")
		os.Exit(1)
	}
	disks := lsblkOutput2disks(outputBytes)

	if len(os.Args[1:]) == 0 {
		fmt.Fprintln(os.Stderr, "available disks (pass as args to run):")
		for k := range disks {
			fmt.Fprintln(os.Stderr, k)
		}
		os.Exit(0)
	}

	for _, x := range os.Args[1:] {
		if _, ok := disks[x]; !ok {
			fmt.Fprintf(os.Stderr, "%s is not a disk\n", x)
			os.Exit(1)
		}
		devices[x] = struct{}{}
	}

	responseCh := make(chan string)

	go pollIostat(responseCh)
	metricNames := make([]string, 0)

	for {
		line := <-responseCh
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < iostatColumnCnt {
			continue
		}
		if strings.HasPrefix(fields[0], "Device") {
			if len(metricNames) == 0 {
				metricNames = append(metricNames, fields[1:]...)
			}
			continue
		}
		device := fields[0]
		if _, ok := devices[device]; ok {
			for i := 1; i < len(fields); i++ {
				name := metricNames[i-1]
				if name == "%util" {
					pctUtil[device] = fields[i]
				} else if name == "aqu-sz" || name == "avgqu-sz" {
					avgQuSz[device] = fields[i]
				}
			}
		}
		if len(pctUtil) == len(devices) {
			generateComparison()

			pctUtil = make(map[string]string)
			avgQuSz = make(map[string]string)
		}
	}
}

func pollIostat(responseCh chan string) {
	refreshPeriod := "1"                                 // seconds
	cmd := exec.Command("iostat", "-dxm", refreshPeriod) // the iostat command
	stdout, _ := cmd.StdoutPipe()
	reader := bufio.NewReader(stdout)
	cmd.Start()
	process := cmd.Process

	for {
		b, err := reader.ReadBytes('\n')
		if process == nil {
			return
		}
		if err == io.EOF {
			continue
		}
		responseCh <- string(b)
	}
}

func generateComparison() { // to diskstats
	newIOMs := make(map[string]int64)
	newIOQueueMs := make(map[string]int64)
	diskstats := GetDiskstats()
	newMeasureTime := time.Now()

	for k, v := range diskstats {
		if _, ok := devices[k]; !ok {
			continue
		}
		newIOMs[k] = v.IOMs
		newIOQueueMs[k] = v.IOMsWeighted
	}

	if !oldMeasureTime.IsZero() {
		for k := range devices {
			dsPctUtil := fmt.Sprintf("%v", 100*float64(newIOMs[k]-oldIOMs[k])/timeGetMs(newMeasureTime.Sub(oldMeasureTime)))
			dsAvgQuSz := fmt.Sprintf("%v", float64(newIOQueueMs[k]-oldIOQueueMs[k])/timeGetMs(newMeasureTime.Sub(oldMeasureTime)))

			fmt.Println(strings.Join([]string{time.Now().Format(time.RFC3339Nano), "Pct Util Compare", k, pctUtil[k], dsPctUtil}, ","))
			fmt.Println(strings.Join([]string{time.Now().Format(time.RFC3339Nano), "Avg Queue Size Compare", k, avgQuSz[k], dsAvgQuSz}, ","))
		}
	}

	oldMeasureTime = newMeasureTime
	oldIOMs = newIOMs
	oldIOQueueMs = newIOQueueMs
}

func timeGetMs(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / (1000 * 1000)
}

// Code for querying for disks

type BlockDevice struct {
	Name         string        `json:"name"`
	BlockDevices []BlockDevice `json:"children"`
}

type LsBlk struct {
	BlockDevices []BlockDevice `json:"blockdevices"`
}

func lsblkOutput2disks(lsblkOutputBytes []byte) (disks cos.StrSet) {
	disks = make(cos.StrSet)
	var lsBlkOutput LsBlk
	err := jsoniter.Unmarshal(lsblkOutputBytes, &lsBlkOutput)
	if err != nil {
		glog.Errorf("Unable to unmarshal lsblk output [%s]. Error: [%v]", string(lsblkOutputBytes), err)
		return
	}

	findDevs(lsBlkOutput.BlockDevices, disks)

	return disks
}

func findDevs(devList []BlockDevice, disks cos.StrSet) {
	for _, bd := range devList {
		if !strings.HasPrefix(bd.Name, "loop") {
			disks[bd.Name] = struct{}{}
			findDevs(bd.BlockDevices, disks)
		}
	}
}
