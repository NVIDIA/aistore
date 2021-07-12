// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ios

// interface guard
var _ IOStater = (*IOStaterMock)(nil)

type (
	IOStaterMock struct {
		Utils MpathsUtils
	}
)

func NewIOStaterMock() *IOStaterMock                           { return &IOStaterMock{} }
func (m *IOStaterMock) GetAllMpathUtils() *MpathsUtils         { return &m.Utils }
func (m *IOStaterMock) GetMpathUtil(mpath string) int64        { return m.Utils.Util(mpath) }
func (*IOStaterMock) AddMpath(string, string) (FsDisks, error) { return nil, nil }
func (*IOStaterMock) RemoveMpath(string)                       {}
func (*IOStaterMock) LogAppend(l []string) []string            { return l }
func (*IOStaterMock) FillDiskStats(AllDiskStats)               {}
