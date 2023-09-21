// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// Rx demux -------------------------------

const (
	numHmaps = 16
	mskHmaps = numHmaps - 1

	numOld = 32
)

type hmap map[string]handler

type (
	errTrname struct {
		name string
	}
	errDuplicateTrname      struct{ errTrname }
	errUnknownTrname        struct{ errTrname }
	errAlreadyClosedTrname  struct{ errTrname }
	errAlreadyRemovedTrname struct{ errTrname }
)

var (
	hmaps [numHmaps]hmap // current (active) Rx endpoints
	hmtxs [numHmaps]sync.Mutex

	old    [numOld]string // a limited pool of the most recently closed Rx endpoints
	oldIdx int
	oldMtx sync.Mutex
)

func _idx(trname string) byte {
	l := len(trname)
	b := trname[l-1]
	if l >= cos.LenShortID {
		return (-b ^ trname[l-2]) & mskHmaps
	}
	return (b ^ trname[0]) & mskHmaps
}

func oget(trname string) (h handler, err error) {
	i := _idx(trname)
	hmtxs[i].Lock()
	hmap := hmaps[i]
	h, ok := hmap[trname]
	hmtxs[i].Unlock()
	if ok {
		return
	}

	oldMtx.Lock()
	err = _lookup(trname)
	oldMtx.Unlock()
	return
}

func _lookup(trname string) error {
	for j := 0; j < numOld; j++ {
		if old[j] == trname {
			return &errAlreadyClosedTrname{errTrname{trname}}
		}
	}
	return &errUnknownTrname{errTrname{trname}}
}

func oput(trname string, h handler) (err error) {
	i := _idx(trname)
	hmtxs[i].Lock()
	hmap := hmaps[i]
	if _, ok := hmap[trname]; ok {
		err = &errDuplicateTrname{errTrname{trname}}
	} else {
		hmap[trname] = h
	}
	hmtxs[i].Unlock()
	return
}

// plus, hk.Unreg
func odel(trname string) (err error) {
	i := _idx(trname)
	hmtxs[i].Lock()
	hmap := hmaps[i]
	h, ok := hmap[trname]
	if !ok {
		hmtxs[i].Unlock()
		return &errAlreadyRemovedTrname{errTrname{trname}}
	}

	delete(hmap, trname)
	hmtxs[i].Unlock()

	h.unreg()

	oldMtx.Lock()
	old[oldIdx] = trname
	oldIdx++
	if oldIdx >= numOld {
		oldIdx = 0
	}
	oldMtx.Unlock()
	return
}

//
// Rx errors
//

func IsErrDuplicateTrname(e error) bool {
	_, ok := e.(*errDuplicateTrname)
	return ok
}

const fmtep = " transport endpoint %q"

func (e *errDuplicateTrname) Error() string      { return fmt.Sprintf("duplicate"+fmtep, e.name) }
func (e *errUnknownTrname) Error() string        { return fmt.Sprintf("unknown"+fmtep, e.name) }
func (e *errAlreadyClosedTrname) Error() string  { return fmt.Sprintf("already closed"+fmtep, e.name) }
func (e *errAlreadyRemovedTrname) Error() string { return fmt.Sprintf("already removed"+fmtep, e.name) } // unexpected
