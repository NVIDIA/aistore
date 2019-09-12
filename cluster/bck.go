// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
)

type Bck struct {
	Name     string
	Provider string
	Props    *cmn.BucketProps
}

func (b *Bck) IsAIS() bool { return b.Provider == cmn.AIS || b.Provider == cmn.ProviderAIS }
func (b *Bck) IsCloud() bool {
	return b.Provider == cmn.Cloud || b.Provider == cmn.ProviderAmazon || b.Provider == cmn.ProviderGoogle
}

func (newbck Bck) Init(bowner Bowner, bmds ...*BMD) (*Bck, *BMD, error) {
	var (
		bmd *BMD
		b   = &newbck
	)
	if len(bmds) == 0 || bmds[0] == nil {
		bmd = bowner.Get()
	} else {
		bmd = bmds[0]
	}
	if b.Provider == "" {
		if bmd.IsAIS(b.Name) {
			b.Provider = cmn.AIS
		} else if bmd.IsCloud(b.Name) {
			b.Provider = cmn.Cloud
		} else {
			return nil, bmd, cmn.NewErrorCloudBucketDoesNotExist(b.Name)
		}
	} else {
		if b.IsAIS() && !bmd.IsAIS(b.Name) {
			return nil, bmd, cmn.NewErrorBucketDoesNotExist(b.Name)
		}
		if b.IsCloud() && !bmd.IsCloud(b.Name) {
			return nil, bmd, cmn.NewErrorCloudBucketDoesNotExist(b.Name)
		}
	}
	if b.IsCloud() && b.Provider != cmn.Cloud {
		config := cmn.GCO.Get()
		if b.Provider != config.CloudProvider {
			err := fmt.Errorf("provider mismatch: %q vs bucket (%s, %s)", config.CloudProvider, b.Name, b.Provider)
			return nil, bmd, err
		}
	}
	b.Props, _ = bmd.Get(b.Name, b.IsAIS())
	return b, bmd, nil
}
