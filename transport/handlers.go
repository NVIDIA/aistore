// Package transport provides long-lived http/tcp connections for
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	numHmaps = 16
	mskHmaps = numHmaps - 1
)

type hmap map[string]handler

var (
	hmaps [numHmaps]hmap
	hmtxs [numHmaps]sync.Mutex
)

func _idx(trname string) byte {
	l := len(trname)
	b := trname[l-1]
	if l > 8 {
		return (-b ^ trname[l-2]) & mskHmaps
	}
	return (b ^ trname[0]) & mskHmaps
}

func oget(trname string) (h handler, err error) {
	var (
		i  = _idx(trname)
		ok bool
	)
	hmtxs[i].Lock()
	hmap := hmaps[i]
	if h, ok = hmap[trname]; !ok {
		err = cos.NewErrNotFound("unknown transport endpoint %q", trname)
	}
	hmtxs[i].Unlock()
	return
}

func oput(trname string, h handler) (err error) {
	i := _idx(trname)
	hmtxs[i].Lock()
	hmap := hmaps[i]
	if _, ok := hmap[trname]; ok {
		err = &ErrDuplicateTrname{trname}
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
	if h, ok := hmap[trname]; ok {
		delete(hmap, trname)
		h.unreg()
	} else {
		err = &ErrUnknownTrname{trname}
	}
	hmtxs[i].Unlock()
	return
}
