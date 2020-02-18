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
	cmn.Bck
	Props *cmn.BucketProps
}

func NewBck(name, provider string, ns cmn.Ns, optProps ...*cmn.BucketProps) *Bck {
	var props *cmn.BucketProps
	if len(optProps) > 0 {
		props = optProps[0]
	}
	if !cmn.IsValidProvider(provider) {
		cmn.Assert(provider == "" || provider == cmn.Cloud)
	}
	b := &Bck{Bck: cmn.Bck{Name: name, Provider: provider, Ns: ns}, Props: props}
	return b
}

func NewBckEmbed(bck cmn.Bck) *Bck { return &Bck{Bck: bck} }

func (b *Bck) MakeUname(objName string) string {
	var (
		nsUname = b.Ns.Uname()
		l       = len(b.Provider) + 1 + len(nsUname) + 1 + len(b.Name) + 1 + len(objName)
		buf     = make([]byte, 0, l)
	)
	buf = append(buf, b.Provider...)
	buf = append(buf, filepath.Separator)
	buf = append(buf, nsUname...)
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
			b.Ns = cmn.ParseNsUname(item)
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

func (b *Bck) IsAIS() bool         { return cmn.IsProviderAIS(b.Bck) }
func (b *Bck) IsCloud() bool       { return cmn.IsProviderCloud(b.Bck, false /*acceptAnon*/) }
func (b *Bck) IsInitialized() bool { return b.Props != nil }
func (b *Bck) HasProvider() bool   { return b.IsAIS() || b.IsCloud() }

func (b *Bck) Equal(other *Bck, sameID bool) bool {
	if b.Name != other.Name {
		return false
	}
	if b.Ns != other.Ns {
		return false
	}
	if sameID {
		if b.Props != nil && other.Props != nil {
			if b.Props.BID != other.Props.BID {
				return false
			}
			if b.Props.InProgress != other.Props.InProgress {
				return false
			}
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
func (b *Bck) Init(bowner Bowner, si *Snode) (err error) {
	bmd := bowner.Get()
	if b.Provider == "" {
		bmd.initBckAnyProvider(b)
	} else if b.Provider == cmn.Cloud {
		cloudConf := cmn.GCO.Get().Cloud
		b.Provider = cloudConf.Provider
		b.Ns = cloudConf.Ns
		bmd.initBckCloudProvider(b)
	} else {
		b.Props, _ = bmd.Get(b)
	}
	if b.Props == nil {
		var name string
		if si != nil {
			name = si.Name()
		}
		if cmn.IsProviderAIS(b.Bck) {
			return cmn.NewErrorBucketDoesNotExist(b.Bck, name)
		}
		return cmn.NewErrorCloudBucketDoesNotExist(b.Bck, name)
	}
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
