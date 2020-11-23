// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"sync"
)

/////////////////////
// Obj pool (send) //
/////////////////////

var sendPool sync.Pool

func AllocSend() (obj *Obj) {
	if v := sendPool.Get(); v != nil {
		obj = v.(*Obj)
		*obj = Obj{}
	} else {
		obj = &Obj{}
	}
	return
}

func FreeSend(obj *Obj) { sendPool.Put(obj) }
