// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	objMeta struct {
		size    int64
		atime   int64
		cksum   *cmn.Cksum
		version string
	}

	OfflineDataProvider struct {
		bckMsg *cmn.Bck2BckMsg
		comm   Communicator
	}
)

// interface guard
var (
	_ cluster.LomReaderProvider = &OfflineDataProvider{}
	_ cmn.ObjHeaderMetaProvider = &objMeta{}
)

func (om *objMeta) Size() int64          { return om.size }
func (om *objMeta) Cksum() *cmn.Cksum    { return om.cksum }
func (om *objMeta) Version() string      { return om.version }
func (om *objMeta) AtimeUnix() int64     { return om.atime }
func (*objMeta) CustomMD() cmn.SimpleKVs { return nil }

func NewOfflineDataProvider(msg *cmn.Bck2BckMsg) (*OfflineDataProvider, error) {
	comm, err := GetCommunicator(msg.ID)
	if err != nil {
		return nil, err
	}

	return &OfflineDataProvider{
		bckMsg: msg,
		comm:   comm,
	}, nil
}

// Returns reader resulting from lom ETL transformation.
func (dp *OfflineDataProvider) Reader(lom *cluster.LOM) (cmn.ReadOpenCloser, cmn.ObjHeaderMetaProvider, func(), error) {
	body, length, err := dp.comm.Get(lom.Bck(), lom.ObjName)
	if err != nil {
		return nil, nil, nil, err
	}

	om := &objMeta{
		size:    length,
		version: lom.Version(),
		cksum:   cmn.NewCksum(cmn.ChecksumNone, ""),
		atime:   lom.AtimeUnix(),
	}
	return cmn.NopOpener(body), om, func() {}, nil
}
