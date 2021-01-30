// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"io"
	"time"

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
	_ cluster.LomReaderProvider = (*OfflineDataProvider)(nil)
	_ cmn.ObjHeaderMetaProvider = (*objMeta)(nil)
)

func (om *objMeta) Size(_ ...bool) int64 { return om.size }
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
	var (
		body   io.ReadCloser
		length int64
		err    error
	)

	call := func() (int, error) {
		body, length, err = dp.comm.Get(lom.Bck(), lom.ObjName)
		return 0, err
	}

	// Try repeating relatively many times, as ETL bucket is long operation, and should not be disturbed by possible
	// network issues.
	//
	// TODO: We should check if ETL pod is healthy. If not, maybe we should wait some more time for it to become healthy
	//  again.
	err = cmn.NetworkCallWithRetry(&cmn.CallWithRetryArgs{
		Call:    call,
		Action:  "etl-obj-" + lom.Uname(),
		SoftErr: 5,
		HardErr: 2,
		Sleep:   50 * time.Millisecond,
		BackOff: true,
	})

	if err != nil {
		return nil, nil, nil, err
	}

	om := &objMeta{
		size:    length,
		version: "",            // Object after ETL is a new object with a new version.
		cksum:   cmn.NoneCksum, // TODO: Revisit and check if possible to have a checksum.
		atime:   lom.AtimeUnix(),
	}
	return cmn.NopOpener(body), om, func() {}, nil
}
