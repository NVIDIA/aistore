// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/debug"
)

var (
	lomPool sync.Pool
	lom0    LOM

	putObjPool sync.Pool
	putObj0    PutParams

	coiPool sync.Pool
	coi0    CopyParams
)

/////////////
// lomPool //
/////////////

func AllocLOM(objName string) *LOM {
	v := lomPool.Get()
	if v == nil {
		return &LOM{ObjName: objName}
	}
	lom := v.(*LOM)
	debug.Assert(lom.ObjName == "" && lom.FQN == "")
	lom.ObjName = objName
	return lom
}

func FreeLOM(lom *LOM) {
	debug.Assertf(lom.ObjName != "" || lom.FQN != "", "%q, %q", lom.ObjName, lom.FQN)
	*lom = lom0
	lomPool.Put(lom)
}

//
// PutParams pool
//

func AllocPutParams() (a *PutParams) {
	if v := putObjPool.Get(); v != nil {
		a = v.(*PutParams)
		return
	}
	return &PutParams{}
}

func FreePutParams(a *PutParams) {
	*a = putObj0
	putObjPool.Put(a)
}

//
// CopyParams pool
//

func AllocCOI() (a *CopyParams) {
	if v := coiPool.Get(); v != nil {
		a = v.(*CopyParams)
		return
	}
	return &CopyParams{}
}

func FreeCOI(a *CopyParams) {
	*a = coi0
	coiPool.Put(a)
}
