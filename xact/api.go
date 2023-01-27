// Package xact provides core functionality for the AIStore eXtended Actions (xactions).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xact

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	// either xaction ID or Kind must be specified
	// is getting passed via ActMsg.Value w/ MorphMarshal extraction
	ArgsMsg struct {
		ID   string // xaction UUID
		Kind string // xaction kind _or_ name (see `xact.Table`)

		// optional parameters to further narrow down or filter-out xactions in question
		DaemonID    string        // node that runs this xaction
		Bck         cmn.Bck       // bucket
		Buckets     []cmn.Bck     // list of buckets (e.g., copy-bucket, lru-evict, etc.)
		Timeout     time.Duration // max time to wait and other "non-filters"
		Force       bool          // force
		OnlyRunning bool          // look only for running xactions
	}

	// simplified JSON-tagged version of the above
	QueryMsg struct {
		OnlyRunning *bool     `json:"show_active"`
		Bck         cmn.Bck   `json:"bck"`
		ID          string    `json:"id"`
		Kind        string    `json:"kind"`
		DaemonID    string    `json:"node,omitempty"`
		Buckets     []cmn.Bck `json:"buckets,omitempty"`
	}

	// further simplified, non-JSON, internal AIS use
	Flt struct {
		Bck         *cluster.Bck
		OnlyRunning *bool
		ID          string
		Kind        string
	}
)

/////////////
// ArgsMsg //
/////////////

func (args *ArgsMsg) String() (s string) {
	if args.ID == "" {
		s = fmt.Sprintf("x-%s", args.Kind)
	} else {
		s = fmt.Sprintf("x-%s[%s]", args.Kind, args.ID)
	}
	if !args.Bck.IsEmpty() {
		s += "-" + args.Bck.String()
	}
	if args.Timeout > 0 {
		s += "-" + args.Timeout.String()
	}
	if args.DaemonID != "" {
		s += "-node[" + args.DaemonID + "]"
	}
	return
}

//////////////
// QueryMsg //
//////////////

func (msg *QueryMsg) String() (s string) {
	if msg.ID == "" {
		s = fmt.Sprintf("x-%s", msg.Kind)
	} else {
		s = fmt.Sprintf("x-%s[%s]", msg.Kind, msg.ID)
	}
	if !msg.Bck.IsEmpty() {
		s += "-" + msg.Bck.String()
	}
	if msg.DaemonID != "" {
		s += "-node[" + msg.DaemonID + "]"
	}
	if msg.OnlyRunning != nil && *msg.OnlyRunning {
		s += "-only-running"
	}
	return
}

/////////
// Flt //
/////////

func (flt *Flt) String() string {
	msg := QueryMsg{OnlyRunning: flt.OnlyRunning, Bck: flt.Bck.Clone(), ID: flt.ID, Kind: flt.Kind}
	return msg.String()
}

func (flt Flt) Matches(xctn cluster.Xact) (yes bool) {
	debug.Assert(IsValidKind(xctn.Kind()), xctn.String())
	// running?
	if flt.OnlyRunning != nil {
		if *flt.OnlyRunning != xctn.Running() {
			return false
		}
	}
	// same ID?
	if flt.ID != "" {
		debug.Assert(cos.IsValidUUID(flt.ID) || IsValidRebID(flt.ID), flt.ID)
		if yes = xctn.ID() == flt.ID; yes {
			debug.Assert(xctn.Kind() == flt.Kind, xctn.String()+" vs same ID "+flt.String())
		}
		return
	}
	// kind?
	if flt.Kind != "" {
		debug.Assert(IsValidKind(flt.Kind), flt.Kind)
		if xctn.Kind() != flt.Kind {
			return false
		}
	}
	// bucket?
	if Table[xctn.Kind()].Scope != ScopeB {
		return true // non single-bucket x
	}
	if flt.Bck == nil {
		return true // the filter's not filtering out
	}

	if xctn.Bck() == nil {
		return false // ambiguity (cannot really compare)
	}
	return xctn.Bck().Equal(flt.Bck, true, true)
}
