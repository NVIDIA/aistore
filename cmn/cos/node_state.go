// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn/debug"
)

type NodeStateFlags BitFlags

const NodeAlerts = "state.flags"

const (
	VoteInProgress       = NodeStateFlags(1 << iota) // warning
	ClusterStarted                                   // info: (primary: cluster-started | all other nodes: joined-cluster)
	NodeStarted                                      // info: (started; possibly, not joined yet)
	Rebalancing                                      // warning
	RebalanceInterrupted                             // warning
	Resilvering                                      // warning
	ResilverInterrupted                              // warning
	NodeRestarted                                    // warning (powercycle, crash)
	OOS                                              // out of space; red alert (see IsRed below)
	OOM                                              // out of memory; red alert
	MaintenanceMode                                  // warning
	LowCapacity                                      // (used > high); warning: OOS possible soon..
	LowMemory                                        // ditto OOM
	DiskFault                                        // red
	NoMountpaths                                     // red: (reserved, not used)
	NumGoroutines                                    // red
	CertWillSoonExpire                               // warning X.509
	CertificateExpired                               // red --/--
	CertificateInvalid                               // red --/--
	KeepAliveErrors                                  // warning (new keep-alive errors during the last 5m)
	OOCPU                                            // out of CPU; red
	LowCPU                                           // warning
)

func (f NodeStateFlags) IsOK() bool { return f == NodeStarted|ClusterStarted }

func (f NodeStateFlags) IsRed() bool {
	return f.IsSet(OOS) || f.IsSet(OOM) || f.IsSet(OOCPU) || f.IsSet(DiskFault) || f.IsSet(NoMountpaths) || f.IsSet(NumGoroutines) ||
		f.IsSet(CertificateExpired)
}

func (f NodeStateFlags) IsWarn() bool {
	return f.IsSet(Rebalancing) || f.IsSet(RebalanceInterrupted) ||
		f.IsSet(Resilvering) || f.IsSet(ResilverInterrupted) ||
		f.IsSet(NodeRestarted) || f.IsSet(MaintenanceMode) ||
		f.IsSet(LowCapacity) || f.IsSet(LowMemory) || f.IsSet(LowCPU) ||
		f.IsSet(CertWillSoonExpire)
}

func (f NodeStateFlags) IsSet(flag NodeStateFlags) bool { return BitFlags(f).IsSet(BitFlags(flag)) }

func (f NodeStateFlags) Set(flags NodeStateFlags) NodeStateFlags {
	return NodeStateFlags(BitFlags(f).Set(BitFlags(flags)))
}

func (f NodeStateFlags) Clear(flags NodeStateFlags) NodeStateFlags {
	return NodeStateFlags(BitFlags(f).Clear(BitFlags(flags)))
}

func (f NodeStateFlags) String() string {
	if f.IsOK() {
		return "ok"
	}

	sb := make([]string, 0, 4)
	if f&VoteInProgress == VoteInProgress {
		sb = append(sb, "vote-in-progress")
	}
	if f&ClusterStarted == 0 {
		// NOTE not set when:
		// - primary:         cluster-started
		// - all other nodes: joined-cluster
		// See also: IsOK() above
		sb = append(sb, "cluster-not-started-yet")
	}
	if f&NodeStarted == 0 {
		sb = append(sb, "node-not-started-yet")
	}
	if f&Rebalancing == Rebalancing {
		sb = append(sb, "rebalancing")
	}
	if f&RebalanceInterrupted == RebalanceInterrupted {
		sb = append(sb, "rebalance-interrupted")
	}
	if f&Resilvering == Resilvering {
		sb = append(sb, "resilvering")
	}
	if f&ResilverInterrupted == ResilverInterrupted {
		sb = append(sb, "resilver-interrupted")
	}
	if f&NodeRestarted == NodeRestarted {
		sb = append(sb, "restarted")
	}
	if f&OOS == OOS {
		sb = append(sb, "OOS")
	}
	if f&OOM == OOM {
		sb = append(sb, "OOM")
	}
	if f&MaintenanceMode == MaintenanceMode {
		sb = append(sb, "in-maintenance-mode")
	}
	if f&LowCapacity == LowCapacity {
		sb = append(sb, "low-usable-capacity")
	}
	if f&LowMemory == LowMemory {
		sb = append(sb, "low-memory")
	}
	if f&DiskFault == DiskFault {
		sb = append(sb, "disk-fault")
	}
	if f&NoMountpaths == NoMountpaths {
		sb = append(sb, "no-mountpaths")
	}
	if f&NumGoroutines == NumGoroutines {
		sb = append(sb, "high-number-of-goroutines")
	}
	if f&CertWillSoonExpire == CertWillSoonExpire {
		sb = append(sb, "tls-cert-will-soon-expire")
	}
	if f&CertificateExpired == CertificateExpired {
		sb = append(sb, "tls-cert-expired")
	}
	if f&CertificateInvalid == CertificateInvalid {
		sb = append(sb, "tls-cert-invalid")
	}
	if f&KeepAliveErrors == KeepAliveErrors {
		sb = append(sb, "keep-alive-errors")
	}
	if f&OOCPU == OOCPU {
		sb = append(sb, "out-of-cpu")
	}
	if f&LowCPU == LowCPU {
		sb = append(sb, "low-cpu")
	}

	l := len(sb)
	switch l {
	case 0:
		v := int64(f)
		err := fmt.Errorf("node state alerts: unknown flag %x (%b)", v, v)
		fmt.Fprintln(os.Stderr, err)
		debug.Assert(false, err)
		return "-" // (teb.unknownVal)
	case 1:
		return sb[0]
	default:
		return fmt.Sprint(sb)
	}
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
			UUID    string `json:"uuid"`
			Version int64  `json:"version,string"`
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
