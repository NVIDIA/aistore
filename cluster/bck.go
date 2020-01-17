// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn"
)

type Bck struct {
	Name     string
	Provider string
	Props    *cmn.BucketProps
}

func (b *Bck) MakeUname(objName string) string {
	cmn.Assert(b.Provider != "" && b.Name != "" && objName != "") // FIXME: this should be removed
	var (
		l   = len(b.Provider) + 2 + len(b.Name) + len(objName)
		buf = make([]byte, 0, l)
	)
	buf = append(buf, b.Provider...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, b.Name...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, objName...)
	return *(*string)(unsafe.Pointer(&buf))
}

func ParseUname(uname string) (b Bck, objName string) {
	i := strings.IndexByte(uname, filepath.Separator)
	b.Provider = uname[:i]
	j := strings.IndexByte(uname[i+1:], filepath.Separator)
	b.Name, objName = uname[i+1:i+j+1], uname[i+j+2:]
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
	return fmt.Sprintf("%s(%x, %s, %v)", b.Name, bid, b.Provider, inProgress)
}

func (b *Bck) IsAIS() bool { return b.Provider == cmn.ProviderAIS }
func (b *Bck) IsCloud() bool {
	return b.Provider == cmn.ProviderAmazon || b.Provider == cmn.ProviderGoogle
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
			b.Provider = cmn.ProviderAIS
		} else if bmd.IsCloud(b.Name) {
			b.Provider = cmn.GCO.Get().CloudProvider
		} else {
			err = cmn.NewErrorCloudBucketDoesNotExist(b.Name)
		}
	} else {
		if b.IsAIS() && !bmd.IsAIS(b.Name) {
			return cmn.NewErrorBucketDoesNotExist(b.Name)
		}
		if !b.IsAIS() {
			b.Provider = cmn.GCO.Get().CloudProvider
			if !bmd.IsCloud(b.Name) {
				err = cmn.NewErrorCloudBucketDoesNotExist(b.Name)
			}
		}
	}

	// NOTE: At this point we should be sure that we no longer use `cloud` but
	//  rather we have explicit cloud provider name.
	cmn.Assert(b.Provider != cmn.Cloud)

	if b.IsCloud() {
		if cloudProvider := cmn.GCO.Get().CloudProvider; b.Provider != cloudProvider {
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
