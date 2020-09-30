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
	OfflineDataProvider struct {
		bckMsg *cmn.Bck2BckMsg
		comm   Communicator
	}
)

var (
	_ cluster.LomReaderProvider = &OfflineDataProvider{}
)

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

// Returns reader resulting from lom ETL transformation. Fills hdrLOM with lom metadata required for http.Header.
func (dp *OfflineDataProvider) Reader(lom *cluster.LOM) (cmn.ReadOpenCloser, *cluster.LOM, func(), error) {
	body, length, err := dp.comm.Get(lom.Bck(), lom.ObjName)
	if err != nil {
		return nil, nil, nil, err
	}

	hdrLOM := &cluster.LOM{}
	hdrLOM.SetCksum(cmn.NewCksum(cmn.ChecksumNone, ""))
	hdrLOM.SetAtimeUnix(lom.AtimeUnix())
	hdrLOM.SetVersion(lom.Version())
	hdrLOM.SetSize(length)

	return cmn.NopOpener(body), hdrLOM, func() {}, nil
}
