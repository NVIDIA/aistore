// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// interface to Get current BMD instance
// (for implementation, see ais/bucketmeta.go)
type (
	Bowner interface {
		Get() (bmd *BMD)
	}

	Buckets    map[string]*cmn.BucketProps
	Namespaces map[string]Buckets
	Providers  map[string]Namespaces

	// - BMD is the root of the (providers, namespaces, buckets) hierarchy
	// - BMD (instance) can be obtained via Bowner.Get()
	// - BMD is immutable and versioned
	// - BMD versioning is monotonic and incremental
	BMD struct {
		Version   int64       `json:"version,string"` // version - gets incremented on every update
		UUID      string      `json:"uuid"`           // immutable
		Providers Providers   `json:"providers"`      // (provider, namespace, bucket) hierarchy
		Ext       interface{} `json:"ext,omitempty"`  // within meta-version extensions
	}
)

var errBucketIDMismatch = errors.New("bucket ID mismatch")

func (m *BMD) String() string {
	if m == nil {
		return "BMD <nil>"
	}
	return "BMD v" + strconv.FormatInt(m.Version, 10)
}

func (m *BMD) numBuckets() (na, nc, nr int) {
	for provider, namespaces := range m.Providers {
		for nsUname, buckets := range namespaces {
			ns := cmn.ParseNsUname(nsUname)
			if provider == apc.ProviderAIS {
				if ns.IsRemote() {
					nr += len(buckets)
				} else {
					na += len(buckets)
				}
			} else {
				nc += len(buckets)
			}
		}
	}
	return
}

func (m *BMD) StringEx() string {
	if m == nil {
		return "BMD <nil>"
	}
	var (
		sna, snc, snr string
		na, nc, nr    = m.numBuckets()
	)
	if na > 0 {
		sna = fmt.Sprintf(", num ais=%d", na)
	}
	if nc > 0 {
		snc = fmt.Sprintf(", num cloud=%d", nc)
	}
	if nr > 0 {
		snr = fmt.Sprintf(", num remote=%d", nr)
	}
	return fmt.Sprintf("BMD v%d[%s%s%s%s]", m.Version, m.UUID, sna, snc, snr)
}

func (m *BMD) Get(bck *Bck) (p *cmn.BucketProps, present bool) {
	buckets := m.getBuckets(bck)
	if buckets != nil {
		p, present = buckets[bck.Name]
	}
	return
}

func (m *BMD) Check(bck *Bck, bckID uint64) error {
	debug.Assert(bckID != 0)
	bprops, present := m.Get(bck)
	if !present {
		if bck.IsRemote() {
			return cmn.NewErrRemoteBckNotFound(bck.Bucket())
		}
		return cmn.NewErrBckNotFound(bck.Bucket())
	}
	if bckID == bprops.BID {
		return nil // ok
	}
	return errBucketIDMismatch
}

func (m *BMD) Del(bck *Bck) (deleted bool) {
	buckets := m.getBuckets(bck)
	if buckets == nil {
		return
	}
	if _, present := buckets[bck.Name]; !present {
		return
	}
	delete(buckets, bck.Name)
	return true
}

func (m *BMD) Set(bck *Bck, p *cmn.BucketProps) {
	buckets := m.getBuckets(bck)
	buckets[bck.Name] = p
}

func (m *BMD) Exists(bck *Bck, bckID uint64) (exists bool) {
	p, present := m.Get(bck)
	if present {
		bck.Props = p
		if bck.IsAIS() {
			exists = bckID != 0 && p.BID == bckID
		} else {
			exists = p.BID == bckID
		}
	}
	return
}

func (m *BMD) Add(bck *Bck) {
	var (
		namespaces Namespaces
		buckets    Buckets
		ok         bool
	)
	if namespaces, ok = m.Providers[bck.Provider]; !ok {
		namespaces = make(Namespaces)
		m.Providers[bck.Provider] = namespaces
	}
	if buckets, ok = namespaces[bck.Ns.Uname()]; !ok {
		buckets = make(Buckets)
		namespaces[bck.Ns.Uname()] = buckets
	}
	buckets[bck.Name] = bck.Props
}

func (m *BMD) IsECUsed() (yes bool) {
	m.Range(nil, nil, func(bck *Bck) (stop bool) {
		if bck.Props.EC.Enabled {
			yes, stop = true, true
		}
		return
	})
	return
}

// providerQuery == nil: all providers; nsQuery == nil: all namespaces
func (m *BMD) Range(providerQuery *string, nsQuery *cmn.Ns, callback func(*Bck) bool) {
	for provider, namespaces := range m.Providers {
		if providerQuery != nil && provider != *providerQuery {
			continue
		}
		for nsUname, buckets := range namespaces {
			if nsQuery != nil && nsUname != nsQuery.Uname() {
				continue
			}
			for name, props := range buckets {
				ns := cmn.ParseNsUname(nsUname)
				bck := NewBck(name, provider, ns, props)
				if callback(bck) { // break?
					return
				}
			}
		}
	}
}

/////////////////////
// private methods //
/////////////////////

func (m *BMD) getBuckets(bck *Bck) (buckets Buckets) {
	if namespaces, ok := m.Providers[bck.Provider]; ok {
		buckets = namespaces[bck.Ns.Uname()]
	}
	return
}

func (m *BMD) initBckAnyProvider(bck *Bck) (present bool) {
	if namespaces, ok := m.Providers[apc.ProviderAIS]; ok {
		if buckets, ok := namespaces[cmn.NsGlobal.Uname()]; ok {
			if p, present := buckets[bck.Name]; present {
				bck.Provider = apc.ProviderAIS
				bck.Ns = cmn.NsGlobal
				bck.Props = p
				return true
			}
		}
	}
	if m.initBckCloudProvider(bck) {
		return true
	}

	bck.Provider = apc.ProviderAIS
	return false
}

func (m *BMD) initBckCloudProvider(bck *Bck) (present bool) {
	for provider, namespaces := range m.Providers {
		for nsUname, buckets := range namespaces {
			var (
				p  *cmn.BucketProps
				ns = cmn.ParseNsUname(nsUname)
				b  = cmn.Bck{Provider: provider, Ns: ns}
			)
			if b.IsAIS() { // looking for Cloud bucket
				continue
			}
			if p, present = buckets[bck.Name]; present {
				bck.Provider = provider
				bck.Ns = ns
				bck.Props = p
				return
			}
		}
	}
	return
}
