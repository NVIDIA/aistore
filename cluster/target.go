// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"io"

	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

var (
	_ Target = &TargetMock{}
)

// For implementations, please refer to ais/target.go
type Target interface {
	OOS(oos ...bool) bool
	IsRebalancing() bool
	RunLRU()
	PrefetchQueueLen() int
	Prefetch()
	GetBowner() Bowner
	FSHC(err error, path string)
	GetAtimeRunner() *atime.Runner
	GetMem2() *memsys.Mem2
	Receive(workFQN string, reader io.ReadCloser, lom *LOM) error
	GetFSPRG() fs.PathRunGroup
}

// TargetMock implements Target interface with mocked return values.
type TargetMock struct {
	Atime *atime.Runner
	BO    Bowner
}

func NewTargetMock(bo Bowner) *TargetMock {
	return &TargetMock{
		Atime: nil,
		BO:    bo,
	}
}

func (t *TargetMock) OOS(oos ...bool) bool                                         { return false }
func (t *TargetMock) IsRebalancing() bool                                          { return false }
func (t *TargetMock) RunLRU()                                                      {}
func (t *TargetMock) PrefetchQueueLen() int                                        { return 0 }
func (t *TargetMock) Prefetch()                                                    {}
func (t *TargetMock) GetBowner() Bowner                                            { return t.BO }
func (t *TargetMock) FSHC(err error, path string)                                  {}
func (t *TargetMock) GetAtimeRunner() *atime.Runner                                { return t.Atime }
func (t *TargetMock) GetMem2() *memsys.Mem2                                        { return memsys.Init() }
func (t *TargetMock) Receive(workFQN string, reader io.ReadCloser, lom *LOM) error { return nil }
func (t *TargetMock) GetFSPRG() fs.PathRunGroup                                    { return nil }
