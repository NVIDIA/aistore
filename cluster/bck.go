// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"path/filepath"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn"
)

type Bck struct {
	Name     string
	Provider string
	Ns       string
	Props    *cmn.BucketProps
}

func (b *Bck) MakeUname(objName string) string {
	var (
		l   = len(b.Provider) + 1 + len(b.Ns) + 1 + len(b.Name) + 1 + len(objName)
		buf = make([]byte, 0, l)
	)
	buf = append(buf, b.Provider...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, b.Ns...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, b.Name...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return *(*string)(unsafe.Pointer(&buf))
}

func ParseUname(uname string) (b Bck, objName string) {
	var (
		prev, itemIdx int
	)
	for i := 0; i < len(uname); i++ {
		if uname[i] != filepath.Separator {
			continue
		}

		item := uname[prev:i]
		switch itemIdx {
		case 0:
			b.Provider = item
		case 1:
			b.Ns = item
		case 2:
			b.Name = item
			objName = uname[i+1:]
			return
		}

		itemIdx++
		prev = i + 1
	}
	return
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
	return fmt.Sprintf("%s(%x, %s, %s, %v)", b.Name, bid, b.Provider, b.Ns, inProgress)
}

func (b *Bck) IsAIS() bool         { return cmn.IsProviderAIS(b.Provider) }
func (b *Bck) IsCloud() bool       { return cmn.IsProviderCloud(b.Provider, false /*acceptAnon*/) }
func (b *Bck) IsInitialized() bool { return b.Props != nil }
func (b *Bck) HasProvider() bool   { return b.IsAIS() || b.IsCloud() }

func (b *Bck) Equal(other *Bck) bool {
	if b.Name != other.Name {
		return false
	}
	if b.Ns != other.Ns {
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
			b.Provider = cmn.ProviderAIS
		} else if bmd.IsCloud(b.Name) {
			b.Provider = cmn.GCO.Get().Cloud.Provider
		} else {
			err = cmn.NewErrorCloudBucketDoesNotExist(b.Name)
		}
	} else {
		if b.IsAIS() && !bmd.IsAIS(b.Name) {
			return cmn.NewErrorBucketDoesNotExist(b.Name)
		}
		if !b.IsAIS() {
			b.Provider = cmn.GCO.Get().Cloud.Provider
			if !bmd.IsCloud(b.Name) {
				err = cmn.NewErrorCloudBucketDoesNotExist(b.Name)
			}
		}
	}

	// NOTE: At this point we should be sure that we no longer use `cloud` but
	//  rather we have explicit cloud provider name.
	cmn.Assert(b.Provider != cmn.Cloud)

	if b.IsCloud() {
		if cloudProvider := cmn.GCO.Get().Cloud.Provider; b.Provider != cloudProvider {
			err = fmt.Errorf("provider mismatch: %q vs bucket (%s, %s)", cloudProvider, b.Name, b.Provider)
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
func (b *Bck) AllowPATCH() error   { return b.allow("PATCH", cmn.AccessPATCH) }
func (b *Bck) AllowAPPEND() error  { return b.allow("APPEND", cmn.AccessAPPEND) }
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
