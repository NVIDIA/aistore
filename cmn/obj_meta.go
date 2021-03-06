// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/cos"

type (
	ObjHeaderMetaProvider interface {
		Size(special ...bool) int64
		Version(special ...bool) string
		Cksum() *cos.Cksum
		AtimeUnix() int64
		CustomMD() cos.SimpleKVs
	}

	HdrMetaCustomVersion struct {
		provider ObjHeaderMetaProvider
		version  string
	}
)

func (m *HdrMetaCustomVersion) Size(_ ...bool) int64     { return m.provider.Size() }
func (m *HdrMetaCustomVersion) Version(_ ...bool) string { return m.version }
func (m *HdrMetaCustomVersion) Cksum() *cos.Cksum        { return m.provider.Cksum() }
func (m *HdrMetaCustomVersion) AtimeUnix() int64         { return m.provider.AtimeUnix() }
func (m *HdrMetaCustomVersion) CustomMD() cos.SimpleKVs  { return m.provider.CustomMD() }

func NewHdrMetaCustomVersion(provider ObjHeaderMetaProvider, version string) *HdrMetaCustomVersion {
	return &HdrMetaCustomVersion{
		provider: provider,
		version:  version,
	}
}
