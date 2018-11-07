/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"errors"

	"github.com/NVIDIA/dfcpub/cmn"
)

//
// empty stubs to have it running on Mac with no iostats
//
func newIostatRunner() *iostatrunner {
	return &iostatrunner{
		chsts:       make(chan struct{}, 1),
		CPUidle:     "(error: iostat unavailable)",
		Disk:        make(map[string]cmn.SimpleKVs, 0),
		metricnames: make([]string, 0),
	}
}

func (r *iostatrunner) run() (err error) {
	cmn.Assert(false, "niy")
	return nil
}

func (r *iostatrunner) stop(err error) {
	cmn.Assert(false, "niy")
}

func (r *iostatrunner) isZeroUtil(dev string) bool {
	return true
}

func checkIostatVersion() error {
	return errors.New("Not yet implemented")
}

func (r *iostatrunner) maxUtilFS(path string) (utilization float32, ok bool) {
	return float32(-1), false
}

func (r *iostatrunner) diskUtilFromFQN(path string) (utilization float32, ok bool) {
	return float32(-1), false
}

func fs2disks(fileSystem string) (disks cmn.StringSet) {
	return make(cmn.StringSet)
}

func lsblkOutput2disks(lsblkOutputBytes []byte, fileSystem string) (disks cmn.StringSet) {
	return make(cmn.StringSet)
}
