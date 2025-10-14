// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/debug"
)

var (
	lomPool = sync.Pool{
		New: func() any { return new(LOM) },
	}
	lom0 LOM

	putObjPool = sync.Pool{
		New: func() any { return new(PutParams) },
	}
	putObj0 PutParams
)

/////////////
// lomPool //
/////////////

func AllocLOM(objName string) *LOM {
	v := lomPool.Get()
	lom := v.(*LOM)
	debug.Assert(lom.ObjName == "" && lom.FQN == "")
	lom.ObjName = objName
	return lom
}

func FreeLOM(lom *LOM) {
	debug.Assert(lom.ObjName != "" || lom.FQN != "", "FreeLOM: empty LOM")
	*lom = lom0
	lomPool.Put(lom)
}

//
// PutParams pool
//

func AllocPutParams() (a *PutParams) {
	v := putObjPool.Get()
	a = v.(*PutParams)
	return
}

func FreePutParams(a *PutParams) {
	*a = putObj0
	putObjPool.Put(a)
}
