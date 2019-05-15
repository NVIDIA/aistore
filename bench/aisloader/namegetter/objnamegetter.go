/*
* Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package namegetter

import (
	"math/rand"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/atomic"

	"github.com/NVIDIA/aistore/cmn"
)

type ObjectNameGetter interface {
	ObjName() string
	AddObjName(objName string)
	Init(names []string, rnd *rand.Rand)
	Names() []string
	Len() int
}

type RandomNameGetter struct {
	rnd *rand.Rand
	BaseNameGetter
}

func (rung *RandomNameGetter) Init(names []string, rnd *rand.Rand) {
	rung.names = names
	rung.rnd = rnd
}

func (rung *RandomNameGetter) AddObjName(objName string) {
	rung.names = append(rung.names, objName)
}

func (rung *RandomNameGetter) ObjName() string {
	idx := rung.rnd.Intn(len(rung.names))
	return rung.names[idx]
}

type RandomUniqueNameGetter struct {
	BaseNameGetter
	rnd     *rand.Rand
	bitmask []uint64
	used    int
}

func (rung *RandomUniqueNameGetter) Init(names []string, rnd *rand.Rand) {
	rung.names = names
	rung.rnd = rnd

	lenBitmask := len(names) / 64
	if len(names)%64 != 0 {
		lenBitmask++
	}

	rung.bitmask = make([]uint64, lenBitmask)
}

func (rung *RandomUniqueNameGetter) AddObjName(objName string) {
	if len(rung.names)%64 == 0 {
		rung.bitmask = append(rung.bitmask, uint64(0))
	}
	rung.names = append(rung.names, objName)
}

func (rung *RandomUniqueNameGetter) ObjName() string {
	if rung.used == len(rung.names) {
		for i := range rung.bitmask {
			rung.bitmask[i] = 0
		}
		rung.used = 0
	}

	for {
		idx := rung.rnd.Intn(len(rung.names))
		if rung.bitmask[idx/64]&(1<<uint64(idx%64)) == 0 {
			rung.used++
			return rung.names[idx]
		}
	}
}

type RandomUniqueIterNameGetter struct {
	BaseNameGetter
	rnd     *rand.Rand
	bitmask []uint64
	used    int
}

func (ruing *RandomUniqueIterNameGetter) Init(names []string, rnd *rand.Rand) {
	ruing.names = names
	ruing.rnd = rnd

	lenBitmask := len(names) / 64
	if len(names)%64 != 0 {
		lenBitmask++
	}

	ruing.bitmask = make([]uint64, lenBitmask)
}

func (ruing *RandomUniqueIterNameGetter) AddObjName(objName string) {
	if len(ruing.names)%64 == 0 {
		ruing.bitmask = append(ruing.bitmask, uint64(0))
	}
	ruing.names = append(ruing.names, objName)
}

func (ruing *RandomUniqueIterNameGetter) ObjName() string {
	if ruing.used == len(ruing.names) {
		for i := range ruing.bitmask {
			ruing.bitmask[i] = 0
		}
		ruing.used = 0
	}

	idx := ruing.rnd.Intn(len(ruing.names))

	for {
		if ruing.bitmask[idx/64]&(1<<uint64(idx%64)) == 0 {
			return ruing.names[idx]
		}
		idx = (idx + 1) % len(ruing.names)
	}
}

type PermutationUniqueNameGetter struct {
	BaseNameGetter
	rnd     *rand.Rand
	perm    []int
	permidx atomic.Int32
}

func (pung *PermutationUniqueNameGetter) Init(names []string, rnd *rand.Rand) {
	pung.names = names
	pung.rnd = rnd
	pung.perm = pung.rnd.Perm(len(names))
}

func (pung *PermutationUniqueNameGetter) AddObjName(objName string) {
	cmn.AssertMsg(false, "can't add object once PermutationUniqueNameGetter is initialized")
}

func (pung *PermutationUniqueNameGetter) ObjName() string {
	if pung.permidx.Load() == int32(len(pung.names)) {
		pung.permidx.Store(0)
		pung.perm = pung.rnd.Perm(len(pung.names))
	}
	objName := pung.names[pung.permidx.Load()]
	pung.permidx.Inc()
	return objName
}

type PermutationUniqueImprovedNameGetter struct {
	BaseNameGetter
	rnd       *rand.Rand
	perm      []int
	permNext  []int
	permidx   atomic.Int32
	nextReady sync.WaitGroup
}

func (pung *PermutationUniqueImprovedNameGetter) Init(names []string, rnd *rand.Rand) {
	pung.names = names
	pung.rnd = rnd
	pung.perm = pung.rnd.Perm(len(names))
	pung.permNext = pung.rnd.Perm(len(names))
}

func (pung *PermutationUniqueImprovedNameGetter) AddObjName(objName string) {
	cmn.AssertMsg(false, "can't add object once PermutationUniqueImprovedNameGetter is initialized")
}

func (pung *PermutationUniqueImprovedNameGetter) ObjName() string {
	if pung.permidx.Load() == int32(len(pung.names)) {
		pung.nextReady.Wait()
		pung.permNext, pung.perm = pung.perm, pung.permNext
		pung.permidx.Store(0)

		pung.nextReady.Add(1)
		go func() {
			pung.perm = pung.rnd.Perm(len(pung.names))
			pung.nextReady.Done()
		}()
	}
	objName := pung.names[pung.permidx.Load()]
	pung.permidx.Inc()
	return objName
}

type BaseNameGetter struct {
	names []string
}

func (bng *BaseNameGetter) Names() []string {
	return bng.names
}

func (bng *BaseNameGetter) Len() int {
	return len(bng.names)
}
