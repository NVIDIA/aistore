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

func (b *Bck) String() string {
	var (
		bid        uint64
		inProgress bool
	)
	if b.Props != nil {
		bid = b.Props.BID
		inProgress = b.Props.InProgress
	}
	return fmt.Sprintf("%s(%x, %s, %v)", b.Name, bid, b.Provider, inProgress)
}
func (b *Bck) IsAIS() bool { return b.Provider == cmn.AIS || b.Provider == cmn.ProviderAIS }
func (b *Bck) IsCloud() bool {
	return b.Provider == cmn.Cloud || b.Provider == cmn.ProviderAmazon || b.Provider == cmn.ProviderGoogle
}
func (b *Bck) Equal(other *Bck) bool {
	if b.Name != other.Name {
		return false
	}
	if b.Props != nil && other.Props != nil {
		if b.Props.BID != other.Props.BID {
			return false
		}
		if b.Props.InProgress != other.Props.InProgress {
			return false
		}
	}
	if b.IsAIS() && other.IsAIS() {
		return true
	}
	return b.IsCloud() && other.IsCloud()
}

// NOTE: when the specified bucket is not present in the BMD:
//       - always returns the corresponding *DoesNotExist error
//       - for Cloud bucket - fills in the props with defaults from config
//       - for AIS bucket - sets the props to nil
//       - for Cloud bucket, the caller can type-cast err.(*cmn.ErrorCloudBucketDoesNotExist) and proceed
func (b *Bck) Init(bowner Bowner) (err error) {
	bmd := bowner.Get()
	if b.Provider == "" {
		if bmd.IsAIS(b.Name) {
			b.Provider = cmn.AIS
		} else if bmd.IsCloud(b.Name) {
			b.Provider = cmn.Cloud
		} else {
			b.Provider = cmn.Cloud
			err = cmn.NewErrorCloudBucketDoesNotExist(b.Name)
		}
	} else {
		if b.IsAIS() && !bmd.IsAIS(b.Name) {
			return cmn.NewErrorBucketDoesNotExist(b.Name)
		}
		if b.IsCloud() && !bmd.IsCloud(b.Name) {
			err = cmn.NewErrorCloudBucketDoesNotExist(b.Name)
		}
	}
	if b.IsCloud() && b.Provider != cmn.Cloud {
		config := cmn.GCO.Get()
		if b.Provider != config.CloudProvider {
			err = fmt.Errorf("provider mismatch: %q vs bucket (%s, %s)", config.CloudProvider, b.Name, b.Provider)
		}
	}
	b.Props, _ = bmd.Get(b)
	return
}

//
// access perms
//

func (b *Bck) AllowGET() error     { return b.allow("GET", cmn.AccessGET) }
func (b *Bck) AllowHEAD() error    { return b.allow("HEAD", cmn.AccessHEAD) }
func (b *Bck) AllowPUT() error     { return b.allow("PUT", cmn.AccessPUT) }
func (b *Bck) AllowColdGET() error { return b.allow("cold-GET", cmn.AccessColdGET) }
func (b *Bck) AllowDELETE() error  { return b.allow("DELETE", cmn.AccessDELETE) }
func (b *Bck) AllowRENAME() error  { return b.allow("RENAME", cmn.AccessRENAME) }

func (b *Bck) allow(oper string, bits uint64) (err error) {
	if b.Props.AccessAttrs == cmn.AllowAnyAccess {
		return
	}
	if (b.Props.AccessAttrs & bits) != 0 {
		return
	}
	err = cmn.NewBucketAccessDenied(b.String(), oper, b.Props.AccessAttrs)
	return
}
