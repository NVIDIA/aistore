// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
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
		bckMsg         *cmn.Bck2BckMsg
		comm           Communicator
		requestTimeout time.Duration
	}
)

// interface guard
var (
	_ cluster.LomReaderProvider = (*OfflineDataProvider)(nil)
	_ cmn.ObjHeaderMetaProvider = (*objMeta)(nil)
)

func (om *objMeta) Size(_ ...bool) int64     { return om.size }
func (om *objMeta) Version(_ ...bool) string { return om.version }
func (om *objMeta) Cksum() *cmn.Cksum        { return om.cksum }
func (om *objMeta) AtimeUnix() int64         { return om.atime }
func (*objMeta) CustomMD() cmn.SimpleKVs     { return nil }

func NewOfflineDataProvider(msg *cmn.Bck2BckMsg) (*OfflineDataProvider, error) {
	comm, err := GetCommunicator(msg.ID)
	if err != nil {
		return nil, err
	}
	pr := &OfflineDataProvider{
		bckMsg: msg,
		comm:   comm,
	}

	if msg.RequestTimeoutStr != "" {
		pr.requestTimeout, err = time.ParseDuration(msg.RequestTimeoutStr)
	}

	return pr, err
}

// Returns reader resulting from lom ETL transformation.
func (dp *OfflineDataProvider) Reader(lom *cluster.LOM) (cmn.ReadOpenCloser, cmn.ObjHeaderMetaProvider, error) {
	var (
		r   cmn.ReadCloseSizer
		err error
	)

	call := func() (int, error) {
		r, err = dp.comm.Get(lom.Bck(), lom.ObjName, dp.requestTimeout)
		return 0, err
	}

	// TODO: Check if ETL pod is healthy and wait some more if not (yet).
	err = cmn.NetworkCallWithRetry(&cmn.CallWithRetryArgs{
		Call:      call,
		Action:    "etl-obj-" + lom.Uname(),
		SoftErr:   5,
		HardErr:   2,
		Sleep:     50 * time.Millisecond,
		BackOff:   true,
		Verbosity: cmn.CallWithRetryLogQuiet,
	})
	if err != nil {
		return nil, nil, err
	}

	om := &objMeta{
		size:    r.Size(),
		version: "",            // Object after ETL is a new object with a new version.
		cksum:   cmn.NoneCksum, // TODO: Revisit and check if possible to have a checksum.
		atime:   lom.AtimeUnix(),
	}
	return cmn.NopOpener(r), om, nil
}
