// Package mock provides a variety of mock implementations used for testing.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import "github.com/NVIDIA/aistore/ios"

// interface guard
var _ ios.IOStater = (*IOStater)(nil)

type (
	IOStater struct {
		Utils ios.MpathUtil
	}
)

func NewIOStater() *IOStater                                   { return &IOStater{} }
func (m *IOStater) GetAllMpathUtils() *ios.MpathUtil           { return &m.Utils }
func (m *IOStater) GetMpathUtil(mpath string) int64            { return m.Utils.Get(mpath) }
func (*IOStater) AddMpath(string, string) (ios.FsDisks, error) { return nil, nil }
func (*IOStater) RemoveMpath(string)                           {}
func (*IOStater) LogAppend(l []string) []string                { return l }
func (*IOStater) FillDiskStats(ios.AllDiskStats)               {}
