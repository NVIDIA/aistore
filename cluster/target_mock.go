// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// interface guard
var _ Target = (*TargetMock)(nil)

// TargetMock implements Target interface with mocked return values.
type TargetMock struct {
	BO Bowner
}

func NewTargetMock(bo Bowner) *TargetMock {
	t := &TargetMock{BO: bo}
	T = t
	initLomLocker()
	return t
}

func (t *TargetMock) Bowner() Bowner         { return t.BO }
func (*TargetMock) Sname() string            { return "" }
func (*TargetMock) SID() string              { return "" }
func (*TargetMock) Snode() *Snode            { return nil }
func (*TargetMock) ClusterStarted() bool     { return true }
func (*TargetMock) NodeStarted() bool        { return true }
func (*TargetMock) DataClient() *http.Client { return http.DefaultClient }
func (*TargetMock) Sowner() Sowner           { return nil }
func (*TargetMock) MMSA() *memsys.MMSA       { return memsys.DefaultPageMM() }
func (*TargetMock) SmallMMSA() *memsys.MMSA  { return memsys.DefaultSmallMM() }

func (*TargetMock) PutObject(*LOM, PutObjectParams) error                   { return nil }
func (*TargetMock) FinalizeObj(*LOM, string) (int, error)                   { return 0, nil }
func (*TargetMock) EvictObject(*LOM) (int, error)                           { return 0, nil }
func (*TargetMock) DeleteObject(*LOM, bool) (int, error)                    { return 0, nil }
func (*TargetMock) PromoteFile(PromoteFileParams) (*LOM, error)             { return nil, nil }
func (*TargetMock) DB() dbdriver.Driver                                     { return nil }
func (*TargetMock) Backend(*Bck) BackendProvider                            { return nil }
func (*TargetMock) LookupRemoteSingle(*LOM, *Snode) bool                    { return false }
func (*TargetMock) RebalanceNamespace(*Snode) ([]byte, int, error)          { return nil, 0, nil }
func (*TargetMock) BMDVersionFixup(*http.Request, ...cmn.Bck)               {}
func (*TargetMock) CopyObject(*LOM, *CopyObjectParams, bool) (int64, error) { return 0, nil }
func (*TargetMock) CompareObjects(context.Context, *LOM) (bool, int, error) { return true, 0, nil }

func (*TargetMock) GetCold(context.Context, *LOM, GetColdType) (int, error) {
	return http.StatusOK, nil
}

func (*TargetMock) Health(*Snode, time.Duration, url.Values) ([]byte, int, error) {
	return nil, 0, nil
}

func (*TargetMock) FSHC(error, string)             {}
func (*TargetMock) OOS(*fs.CapStatus) fs.CapStatus { return fs.CapStatus{} }
func (*TargetMock) TrashNonExistingBucket(cmn.Bck) {}
