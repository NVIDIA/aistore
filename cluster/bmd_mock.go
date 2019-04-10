/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package cluster

import "github.com/NVIDIA/aistore/cmn"

var (
	_ Bowner = &BownerMock{}
)

type BownerMock struct {
	BMD
}

func (r BownerMock) Get() *BMD { return &r.BMD }

func NewBaseBownerMock(bckName string) *BownerMock {
	return &BownerMock{BMD: BMD{
		LBmap: map[string]*cmn.BucketProps{
			bckName: {
				Cksum: cmn.CksumConf{Type: cmn.ChecksumXXHash},
			},
		},
		CBmap:   map[string]*cmn.BucketProps{},
		Version: 1,
	}}
}
