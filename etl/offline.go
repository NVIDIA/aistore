// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	OfflineDataProvider struct {
		bckMsg *OfflineMsg
		comm   Communicator
	}
)

var (
	_ cluster.SendDataProvider = &OfflineDataProvider{}
)

func NewOfflineDataProvider(msg *OfflineBckMsg) (*OfflineDataProvider, error) {
	comm, err := GetCommunicator(msg.ID)
	if err != nil {
		return nil, err
	}

	return &OfflineDataProvider{
		bckMsg: &msg.OfflineMsg,
		comm:   comm,
	}, nil
}

// Returns reader resulting from lom ETL transformation. Fills hdrLOM with lom metadata required for http.Header.
func (dp *OfflineDataProvider) Reader(lom *cluster.LOM) (cmn.ReadOpenCloser, *cluster.LOM, error) {
	body, length, err := dp.comm.Get(lom.Bck(), lom.ObjName)
	if err != nil {
		return nil, nil, err
	}

	hdrLOM := &cluster.LOM{}
	hdrLOM.SetCksum(cmn.NewCksum(cmn.ChecksumNone, ""))
	hdrLOM.SetAtimeUnix(lom.AtimeUnix())
	hdrLOM.SetVersion(lom.Version())
	hdrLOM.SetSize(length)

	return cmn.NopOpener(body), hdrLOM, nil
}

func (dp *OfflineDataProvider) ObjNameTo(objName string) string {
	return newETLObjName(objName, dp.bckMsg)
}

// Replace extension and add suffix if provided.
func newETLObjName(name string, msg *OfflineMsg) string {
	if msg.Ext != "" {
		if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
			name = name[:idx+1] + msg.Ext
		}
	}
	if msg.Prefix != "" {
		name = msg.Prefix + name
	}

	return name
}
