// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"

	"github.com/NVIDIA/aistore/core/meta"
)

// common error formats
const (
	fmtErrInsuffMpaths1 = "%s: not enough mountpaths (%d) to configure %s as %d-way mirror"
	fmtErrInsuffMpaths2 = "%s: not enough mountpaths (%d) to replicate %s (configured) %d times"
	fmtErrInvaldAction  = "invalid action %q (expected one of %v)"
	fmtUnknownQue       = "unexpected query [what=%s]"
	fmtNested           = "%s: nested (%v): failed to %s %q: %v"
	fmtOutside          = "%s is present (vs requested 'flt-outside'(%d))"
	fmtFailedRejoin     = "%s failed to rejoin cluster: %v(%d)"

	fmtNodeNotPresent = "node %s not present in %s"   // compare w/ errNodeNotFound
	fmtSelfNotPresent = "%s (self) not present in %s" // compare w/ errSelfNotFound
)

// error types
type (
	errTgtBmdUUIDDiffer struct{ detail string } // BMD & its uuid
	errPrxBmdUUIDDiffer struct{ detail string }
	errBmdUUIDSplit     struct{ detail string }
	errSmapUUIDDiffer   struct{ detail string } // ditto Smap
	errNodeNotFound     struct {
		si   *meta.Snode // self
		smap *smapX
		msg  string
		id   string
	}
	errSelfNotFound struct {
		si   *meta.Snode
		smap *smapX
		act  string
	}
	errNotEnoughTargets struct {
		si       *meta.Snode
		smap     *smapX
		required int // should at least contain
	}
	errDowngrade struct {
		si       *meta.Snode
		from, to string
	}
	errNotPrimary struct {
		si     *meta.Snode
		smap   *smapX
		detail string
	}
	errNoUnregister struct {
		action string
	}
	errInvIntraControl struct {
		si         *meta.Snode // self
		sname, sid string      // sender
	}
)

var (
	errRebalanceDisabled = errors.New("rebalance is disabled")
	errForwarded         = errors.New("forwarded")
	errFastKalive        = errors.New("cannot fast-keepalive")

	// sentinel - do not wrap; compare w/ errInvIntraControl
	errNotIntraControl = errors.New("not an intra-control request")
)

// BMD uuid errs
var errNoBMD = errors.New("no bucket metadata")

func (e *errTgtBmdUUIDDiffer) Error() string { return e.detail }
func (e *errBmdUUIDSplit) Error() string     { return e.detail }
func (e *errPrxBmdUUIDDiffer) Error() string { return e.detail }
func (e *errSmapUUIDDiffer) Error() string   { return e.detail }

func (e *errNodeNotFound) Error() string {
	err := fmt.Errorf(fmtNodeNotPresent, e.id, e.smap)
	return fmt.Sprintf("%s: %s %v", e.si, e.msg, err)
}

func (e *errSelfNotFound) Error() string {
	err := fmt.Errorf(fmtSelfNotPresent, e.si, e.smap)
	return fmt.Sprintf("failed to %s: %v", e.act, err)
}

/////////////////////
// errNoUnregister //
/////////////////////

func (e *errNoUnregister) Error() string { return e.action }

func isErrNoUnregister(err error) (ok bool) {
	_, ok = err.(*errNoUnregister)
	return
}

//////////////////
// errDowngrade //
//////////////////

func newErrDowngrade(si *meta.Snode, from, to string) *errDowngrade {
	return &errDowngrade{si, from, to}
}

func (e *errDowngrade) Error() string {
	return fmt.Sprintf("%s: attempt to downgrade %s to %s", e.si, e.from, e.to)
}

func isErrDowngrade(err error) bool {
	if _, ok := err.(*errDowngrade); ok {
		return true
	}
	var erd *errDowngrade
	return errors.As(err, &erd)
}

/////////////////////////
// errNotEnoughTargets //
/////////////////////////

func (e *errNotEnoughTargets) Error() string {
	return fmt.Sprintf("%s: not enough targets: %s, need %d, have %d",
		e.si, e.smap, e.required, e.smap.CountActiveTs())
}

///////////////////
// errNotPrimary //
///////////////////

func newErrNotPrimary(si *meta.Snode, smap *smapX, detail ...string) *errNotPrimary {
	if len(detail) == 0 {
		return &errNotPrimary{si, smap, ""}
	}
	return &errNotPrimary{si, smap, detail[0]}
}

func (e *errNotPrimary) Error() string {
	var present, detail string
	if !e.smap.isPresent(e.si) {
		present = "not present in the "
	}
	if e.detail != "" {
		detail = ": " + e.detail
	}
	return fmt.Sprintf("%s is not primary [%s%s]%s", e.si, present, e.smap.StringEx(), detail)
}

////////////////////////
// errInvIntraControl //
////////////////////////

func (e *errInvIntraControl) Error() string {
	return fmt.Sprintf("%s: invalid intra-cluster request from [name=%q, id=%q]", e.si, e.sname, e.sid)
}
