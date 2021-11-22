// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/3rdparty/glog"

var loghdr string

func SetNodeName(sname string) {
	thisNodeName = sname
	glog.FileHeaderCB = glocb
}

func AppGloghdr(hdr string) {
	loghdr += hdr + "\n"
}

func glocb() string { return loghdr }
