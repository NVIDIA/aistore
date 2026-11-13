// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
)

// interface guard
var _ ios.IOS = (*IOS)(nil)

type IOS struct {
	Utils ios.MpathUtil
}

func NewIOS() *IOS                              { return &IOS{} }
func (m *IOS) GetAllMpathUtils() *ios.MpathUtil { return &m.Utils }
func (m *IOS) GetMpathUtil(mpath string) int64  { return m.Utils.Get(mpath) }

func (*IOS) AddMpath(_, _ string, _ cos.MountpathLabel, _ *cmn.Config, _ ios.BlockDevs, prepare func() error) (ios.FsDisks, error) {
	if prepare != nil {
		return nil, prepare()
	}
	return nil, nil
}

func (*IOS) RescanDisks(string, string, []string) ios.RescanDisksResult {
	return ios.RescanDisksResult{}
}

func (*IOS) RemoveMpath(string, bool)      {}
func (*IOS) LogAppend(l []string) []string { return l }
func (*IOS) DiskStats(cos.AllDiskStats)    {}
