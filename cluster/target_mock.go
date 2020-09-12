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
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

var (
	_ Target = &TargetMock{}
)

// TargetMock implements Target interface with mocked return values.
type TargetMock struct {
	BO Bowner
}

func NewTargetMock(bo Bowner) *TargetMock {
	return &TargetMock{
		BO: bo,
	}
}

func (*TargetMock) Snode() *Snode                                          { return nil }
func (*TargetMock) ClusterStarted() bool                                   { return true }
func (*TargetMock) NodeStarted() bool                                      { return true }
func (*TargetMock) Client() *http.Client                                   { return http.DefaultClient }
func (*TargetMock) NodeStartedTime() time.Time                             { return time.Now() }
func (*TargetMock) RunLRU(_ string, _ bool, _ ...cmn.Bck)                  {}
func (t *TargetMock) GetBowner() Bowner                                    { return t.BO }
func (*TargetMock) GetSowner() Sowner                                      { return nil }
func (*TargetMock) FSHC(_ error, _ string)                                 {}
func (*TargetMock) GetMMSA() *memsys.MMSA                                  { return memsys.DefaultPageMM() }
func (*TargetMock) GetSmallMMSA() *memsys.MMSA                             { return memsys.DefaultSmallMM() }
func (*TargetMock) PutObject(_ PutObjectParams) error                      { return nil }
func (*TargetMock) GetObject(_ io.Writer, _ *LOM, _ time.Time) error       { return nil }
func (*TargetMock) EvictObject(_ *LOM) error                               { return nil }
func (*TargetMock) CopyObject(_ CopyObjectParams) (bool, error)            { return false, nil }
func (*TargetMock) GetCold(_ context.Context, _ *LOM, _ bool) (error, int) { return nil, http.StatusOK }
func (*TargetMock) PromoteFile(_ PromoteFileParams) (*LOM, error)          { return nil, nil }

func (*TargetMock) PutObjectToTarget(destTarget *Snode, r io.ReadCloser, bckTo *Bck, objNameTo string, header http.Header) error {
	return nil
}
func (*TargetMock) GetDB() dbdriver.Driver                             { return nil }
func (*TargetMock) GetFSPRG() fs.PathRunGroup                          { return nil }
func (*TargetMock) Cloud(_ *Bck) CloudProvider                         { return nil }
func (*TargetMock) StartTime() time.Time                               { return time.Now() }
func (*TargetMock) GetGFN(_ GFNType) GFN                               { return nil }
func (*TargetMock) LookupRemoteSingle(_ *LOM, _ *Snode) bool           { return false }
func (*TargetMock) RebalanceNamespace(_ *Snode) ([]byte, int, error)   { return nil, 0, nil }
func (*TargetMock) BMDVersionFixup(_ *http.Request, _ cmn.Bck, _ bool) {}
func (*TargetMock) K8sNodeName() string                                { return "" }
func (*TargetMock) GetXactRegistry() XactRegistry                      { return nil }

func (*TargetMock) Health(_ *Snode, _ time.Duration, _ url.Values) ([]byte, error, int) {
	return nil, nil, 0
}
func (*TargetMock) CheckCloudVersion(_ context.Context, _ *LOM) (bool, error, int) {
	return false, nil, 0
}
