// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

type NodeStateFlags BitFlags

const (
	VoteInProgress       = NodeStateFlags(1 << iota) // warning
	ClusterStarted                                   // info
	NodeStarted                                      // info
	Rebalancing                                      // warning
	RebalanceInterrupted                             // warning
	Resilvering                                      // warning
	ResilverInterrupted                              // warning
	Restarted                                        // warning
	OOS                                              // red alert
	OOM                                              // red alert
	MaintenanceMode                                  // warning
	LowCapacity                                      // (used > high); warning: OOS possible soon..
	LowMemory                                        // ditto OOM
	DiskFault                                        // red & critical // TODO -- FIXME: NIY
)

func (f NodeStateFlags) IsSet(flag NodeStateFlags) bool { return BitFlags(f).IsSet(BitFlags(flag)) }

func (f NodeStateFlags) Set(flags NodeStateFlags) NodeStateFlags {
	return NodeStateFlags(BitFlags(f).Set(BitFlags(flags)))
}

func (f NodeStateFlags) Clear(flags NodeStateFlags) NodeStateFlags {
	return NodeStateFlags(BitFlags(f).Clear(BitFlags(flags)))
}

func (f NodeStateFlags) String() string {
	var sb strings.Builder
	if f == 0 {
		return ""
	}
	if f&VoteInProgress == VoteInProgress {
		sb.WriteString("vote-in-progress,")
	}
	if f&ClusterStarted == 0 {
		sb.WriteString("(primary:cluster-not-started-yet; other:not-joined-yet),")
	}
	if f&NodeStarted == 0 {
		sb.WriteString("starting-up,")
	}
	if f&Rebalancing == Rebalancing {
		sb.WriteString("rebalancing,")
	}
	if f&RebalanceInterrupted == RebalanceInterrupted {
		sb.WriteString("rebalance-interrupted,")
	}
	if f&Resilvering == Resilvering {
		sb.WriteString("resilvering,")
	}
	if f&ResilverInterrupted == ResilverInterrupted {
		sb.WriteString("resilver-interrupted,")
	}
	if f&Restarted == Restarted {
		sb.WriteString("restarted,")
	}
	if f&OOS == OOS {
		sb.WriteString("OOS,")
	}
	if f&OOM == OOM {
		sb.WriteString("OOM,")
	}
	if f&MaintenanceMode == MaintenanceMode {
		sb.WriteString("in-maintenance-mode,")
	}
	if f&LowCapacity == LowCapacity {
		sb.WriteString("low-usable-capacity,")
	}
	if f&LowMemory == LowMemory {
		sb.WriteString("low-memory,")
	}
	if f&DiskFault == DiskFault {
		sb.WriteString("disk-fault,")
	}
	s := sb.String()
	if s == "" {
		err := fmt.Errorf("unknown flag %x", int64(f))
		nlog.Errorln(err)
		debug.Assert(false, err)
		return ""
	}
	return s[:len(s)-1]
}

//
// NodeStateInfo
//

type (
	NodeStateInfo struct {
		Smap struct {
			Primary struct {
				PubURL  string `json:"pub_url"`
				CtrlURL string `json:"control_url"`
				ID      string `json:"id"`
			}
			Version int64  `json:"version,string"`
			UUID    string `json:"uuid"`
		} `json:"smap"`
		BMD struct {
			UUID    string `json:"uuid"`
			Version int64  `json:"version,string"`
		} `json:"bmd"`
		RMD struct {
			Version int64 `json:"version,string"`
		} `json:"rmd"`
		Config struct {
			Version int64 `json:"version,string"`
		} `json:"config"`
		EtlMD struct {
			Version int64 `json:"version,string"`
		} `json:"etlmd"`
		Flags NodeStateFlags `json:"flags"`
	}
)

func (nsti *NodeStateInfo) String() string {
	s, flags := fmt.Sprintf("%+v", *nsti), nsti.Flags.String()
	if flags != "" {
		s += ", flags: " + flags
	}
	return s
}

func (nsti *NodeStateInfo) SmapEqual(other *NodeStateInfo) (ok bool) {
	if nsti == nil || other == nil {
		return false
	}
	return nsti.Smap.Version == other.Smap.Version && nsti.Smap.Primary.ID == other.Smap.Primary.ID
}
