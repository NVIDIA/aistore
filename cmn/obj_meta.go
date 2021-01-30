// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	ObjHeaderMetaProvider interface {
		Size(special ...bool) int64
		Cksum() *Cksum
		Version() string
		AtimeUnix() int64
		CustomMD() SimpleKVs
	}

	HdrMetaCustomVersion struct {
		provider ObjHeaderMetaProvider
		version  string
	}
)

func (m *HdrMetaCustomVersion) Size(_ ...bool) int64 { return m.provider.Size() }
func (m *HdrMetaCustomVersion) Cksum() *Cksum        { return m.provider.Cksum() }
func (m *HdrMetaCustomVersion) AtimeUnix() int64     { return m.provider.AtimeUnix() }
func (m *HdrMetaCustomVersion) CustomMD() SimpleKVs  { return m.provider.CustomMD() }
func (m *HdrMetaCustomVersion) Version() string      { return m.version }

func NewHdrMetaCustomVersion(provider ObjHeaderMetaProvider, version string) *HdrMetaCustomVersion {
	return &HdrMetaCustomVersion{
		provider: provider,
		version:  version,
	}
}
