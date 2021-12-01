// Package ios is a collection of interfaces to the local storage subsystem;
// the package includes OS-dependent implementations for those interfaces.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package mock

import "github.com/NVIDIA/aistore/ios"

// interface guard
var _ ios.IOStater = (*IOStater)(nil)

type (
	IOStater struct {
		Utils ios.MpathsUtils
	}
)

func NewIOStater() *IOStater                                   { return &IOStater{} }
func (m *IOStater) GetAllMpathUtils() *ios.MpathsUtils         { return &m.Utils }
func (m *IOStater) GetMpathUtil(mpath string) int64            { return m.Utils.Util(mpath) }
func (*IOStater) AddMpath(string, string) (ios.FsDisks, error) { return nil, nil }
func (*IOStater) RemoveMpath(string)                           {}
func (*IOStater) LogAppend(l []string) []string                { return l }
func (*IOStater) FillDiskStats(ios.AllDiskStats)               {}
