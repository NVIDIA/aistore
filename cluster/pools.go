// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/debug"
)

var (
	lomPool sync.Pool
	lom0    LOM

	nodesPool sync.Pool

	cpObjPool sync.Pool
	cpObj0    CopyObjectParams

	putObjPool sync.Pool
	putObj0    PutObjectParams
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
	debug.AssertMsg(lom.ObjName == "" && lom.mpathInfo == nil, lom.ObjName)
	lom.ObjName = objName
	return lom
}

func FreeLOM(lom *LOM) {
	*lom = lom0
	debug.AssertMsg(lom.ObjName == "" && lom.mpathInfo == nil, lom.ObjName)
	lomPool.Put(lom)
}

///////////////
// nodesPool //
///////////////

func AllocNodes(capacity int) (nodes Nodes) {
	if v := nodesPool.Get(); v != nil {
		pnodes := v.(*Nodes)
		nodes = *pnodes
		debug.Assert(nodes != nil && len(nodes) == 0)
	} else {
		debug.Assert(capacity > 0)
		nodes = make(Nodes, 0, capacity)
	}
	return
}

func FreeNodes(nodes Nodes) {
	nodes = nodes[:0]
	nodesPool.Put(&nodes)
}

//////////////////////
// CopyObjectParams pool
//////////////////////

func AllocCpObjParams() (a *CopyObjectParams) {
	if v := cpObjPool.Get(); v != nil {
		a = v.(*CopyObjectParams)
		return
	}
	return &CopyObjectParams{}
}

func FreeCpObjParams(a *CopyObjectParams) {
	*a = cpObj0
	cpObjPool.Put(a)
}

//////////////////////
// PutObjectParams pool
//////////////////////

func AllocPutObjParams() (a *PutObjectParams) {
	if v := putObjPool.Get(); v != nil {
		a = v.(*PutObjectParams)
		return
	}
	return &PutObjectParams{}
}

func FreePutObjParams(a *PutObjectParams) {
	*a = putObj0
	putObjPool.Put(a)
}
