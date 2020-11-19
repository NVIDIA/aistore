// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/memsys"
)

// interface guard
var _ Target = (*TargetMock)(nil)

// TargetMock implements Target interface with mocked return values.
type TargetMock struct {
	BO Bowner
}

func NewTargetMock(bo Bowner) *TargetMock {
	InitLomLocker()
	return &TargetMock{
		BO: bo,
	}
}

func (*TargetMock) Snode() *Snode                                       { return nil }
func (*TargetMock) ClusterStarted() bool                                { return true }
func (*TargetMock) NodeStarted() bool                                   { return true }
func (*TargetMock) Client() *http.Client                                { return http.DefaultClient }
func (*TargetMock) NodeStartedTime() time.Time                          { return time.Now() }
func (*TargetMock) RunLRU(_ string, _ bool, _ ...cmn.Bck)               {}
func (t *TargetMock) Bowner() Bowner                                    { return t.BO }
func (*TargetMock) Sowner() Sowner                                      { return nil }
func (*TargetMock) FSHC(_ error, _ string)                              {}
func (*TargetMock) MMSA() *memsys.MMSA                                  { return memsys.DefaultPageMM() }
func (*TargetMock) SmallMMSA() *memsys.MMSA                             { return memsys.DefaultSmallMM() }
func (*TargetMock) PutObject(_ *LOM, _ PutObjectParams) (string, error) { return "", nil }
func (*TargetMock) GetObject(_ io.Writer, _ *LOM, _ time.Time) error    { return nil }
func (*TargetMock) EvictObject(_ *LOM) error                            { return nil }
func (*TargetMock) DeleteObject(ctx context.Context, lom *LOM, evict bool) (int, error) {
	return 0, nil
}

func (*TargetMock) CopyObject(_ *LOM, _ CopyObjectParams, _ bool) (bool, int64, error) {
	return false, 0, nil
}

func (*TargetMock) GetCold(ctx context.Context, lom *LOM, prefetch bool) (int, error) {
	return http.StatusOK, nil
}
func (*TargetMock) PromoteFile(_ PromoteFileParams) (*LOM, error)      { return nil, nil }
func (*TargetMock) DB() dbdriver.Driver                                { return nil }
func (*TargetMock) Cloud(_ *Bck) CloudProvider                         { return nil }
func (*TargetMock) StartTime() time.Time                               { return time.Now() }
func (*TargetMock) GFN(_ GFNType) GFN                                  { return nil }
func (*TargetMock) LookupRemoteSingle(_ *LOM, _ *Snode) bool           { return false }
func (*TargetMock) RebalanceNamespace(_ *Snode) ([]byte, int, error)   { return nil, 0, nil }
func (*TargetMock) BMDVersionFixup(_ *http.Request, _ cmn.Bck, _ bool) {}
func (*TargetMock) Health(si *Snode, timeout time.Duration, query url.Values) ([]byte, int, error) {
	return nil, 0, nil
}

func (*TargetMock) CheckCloudVersion(ctx context.Context, lom *LOM) (bool, int, error) {
	return false, 0, nil
}
