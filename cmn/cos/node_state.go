// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
)

type NodeStateFlags BitFlags

const NodeAlerts = "state.flags"

const noAlerts = "ok"

const (
	VoteInProgress       = NodeStateFlags(1 << iota) // warning
	ClusterStarted                                   // info: (primary: cluster-started | all other nodes: joined-cluster)
	NodeStarted                                      // info: (started; possibly, not joined yet)
	Rebalancing                                      // warning
	RebalanceInterrupted                             // warning
	Resilvering                                      // warning
	ResilverInterrupted                              // warning
	NodeRestarted                                    // warning (powercycle, crash)
	OOS                                              // node out of space; red alert (see IsRed below)
	OOM                                              // out of memory; red alert
	MaintenanceMode                                  // warning
	LowCapacity                                      // node (used > high); warning: OOS possible soon..
	LowMemory                                        // ditto OOM
	DiskFault                                        // red
	NumGoroutines                                    // yellow
	HighNumGoroutines                                // red
	CertWillSoonExpire                               // warning X.509
	CertificateExpired                               // red --/--
	CertificateInvalid                               // red --/--
	KeepAliveErrors                                  // warning (new keep-alive errors during the last 5m)
	OOCPU                                            // out of CPU; red
	LowCPU                                           // warning
	DiskOOS                                          // disk out of space
	DiskLowCapacity                                  // warning
)

const (
	isRed = OOS | OOM | OOCPU | DiskFault | HighNumGoroutines | CertificateExpired | DiskOOS

	isWarn = Rebalancing | RebalanceInterrupted | Resilvering | ResilverInterrupted | NodeRestarted | MaintenanceMode |
		LowCapacity | LowMemory | LowCPU | CertWillSoonExpire | DiskLowCapacity | NumGoroutines
)

func (f NodeStateFlags) IsOK() bool   { return f == NodeStarted|ClusterStarted }
func (f NodeStateFlags) IsRed() bool  { return f.IsAnySet(isRed) }
func (f NodeStateFlags) IsWarn() bool { return f.IsAnySet(isWarn) }

func (f NodeStateFlags) IsSet(flag NodeStateFlags) bool { return BitFlags(f).IsSet(BitFlags(flag)) }

func (f NodeStateFlags) IsAnySet(flag NodeStateFlags) bool {
	return BitFlags(f).IsAnySet(BitFlags(flag))
}

func (f NodeStateFlags) Set(flags NodeStateFlags) NodeStateFlags {
	return NodeStateFlags(BitFlags(f).Set(BitFlags(flags)))
}

func (f NodeStateFlags) Clear(flags NodeStateFlags) NodeStateFlags {
	return NodeStateFlags(BitFlags(f).Clear(BitFlags(flags)))
}

// NOTE: call it only with complete (non-masked) flags
func (f NodeStateFlags) String() string {
	if f.IsOK() {
		return noAlerts
	}

	sb := make([]string, 0, 4)
	if f&ClusterStarted == 0 {
		// not set when:
		// - primary:         cluster-started
		// - all other nodes: joined-cluster
		// see also IsOK() above
		sb = append(sb, "cluster-not-started-yet")
	}
	if f&NodeStarted == 0 {
		sb = append(sb, "node-not-started-yet")
	}

	return f._str(sb)
}

// use it with masked (partial) bits
func (f NodeStateFlags) Str() string {
	if f == 0 {
		return noAlerts
	}
	sb := make([]string, 0, 4)
	return f._str(sb)
}

func (f NodeStateFlags) _str(sb []string) string {
	if f&VoteInProgress == VoteInProgress {
		sb = append(sb, "vote-in-progress")
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
		sb = append(sb, "OOS") // node
	}
	if f&OOM == OOM {
		sb = append(sb, "OOM")
	}
	if f&MaintenanceMode == MaintenanceMode {
		sb = append(sb, "in-maintenance-mode")
	}
	if f&LowCapacity == LowCapacity {
		sb = append(sb, "low-usable-capacity") // node
	}
	if f&LowMemory == LowMemory {
		sb = append(sb, "low-memory")
	}
	if f&DiskFault == DiskFault {
		sb = append(sb, "disk-fault")
	}
	if f&NumGoroutines == NumGoroutines {
		sb = append(sb, "number-of-goroutines")
	}
	if f&HighNumGoroutines == HighNumGoroutines {
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
	if f&DiskOOS == DiskOOS {
		sb = append(sb, "disk-OOS") // disk
	}
	if f&DiskLowCapacity == DiskLowCapacity {
		sb = append(sb, "disk-low-capacity") // disk
	}

	l := len(sb)
	switch l {
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
