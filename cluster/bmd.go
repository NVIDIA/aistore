// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"fmt"
	"strconv"

	"github.com/NVIDIA/aistore/cmn"
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
		Version   int64     `json:"version,string"` // version - gets incremented on every update
		Origin    uint64    `json:"origin,string"`  // (unique) origin stays the same for the lifetime
		Providers Providers `json:"providers"`      // (provider, namespace, bucket) hierarchy
	}
)

func (m *BMD) String() string {
	if m == nil {
		return "BMD <nil>"
	}
	return "BMD v" + strconv.FormatInt(m.Version, 10)
}

// nsQuery == nil: all namespaces

func (m *BMD) NumAIS(nsQuery *string) (na int) {
	if namespaces, ok := m.Providers[cmn.ProviderAIS]; ok {
		for ns, buckets := range namespaces {
			if nsQuery != nil && ns != *nsQuery {
				continue
			}
			na += len(buckets)
		}
	}
	return
}

func (m *BMD) NumCloud(nsQuery *string) (nc int) {
	for provider, namespaces := range m.Providers {
		if cmn.IsProviderAIS(provider) {
			continue
		}
		for ns, buckets := range namespaces {
			if nsQuery != nil && ns != *nsQuery {
				continue
			}
			nc += len(buckets)
		}
	}
	return
}

func (m *BMD) StringEx() string {
	if m == nil {
		return "BMD <nil>"
	}
	na, nc := m.NumAIS(nil), m.NumCloud(nil)
	return fmt.Sprintf("BMD v%d[...%d, ais=%d, cloud=%d]", m.Version, m.Origin%1000, na, nc)
}

func (m *BMD) Get(bck *Bck) (p *cmn.BucketProps, present bool) {
	buckets := m.getBuckets(bck)
	if buckets != nil {
		p, present = buckets[bck.Name]
	}
	if bck.IsAIS() {
		return
	}
	if !present {
		p = cmn.DefaultBucketProps()
	}
	return
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
				ns := cmn.MakeNs(nsUname)
				bck := NewBck(name, provider, ns, props)
				if callback(bck) { // break?
					return
				}
			}
		}
	}
}

func (m *BMD) DeepCopy(dst *BMD) {
	*dst = *m
	dst.Providers = make(Providers, len(m.Providers))
	for provider, namespaces := range m.Providers {
		dstNamespaces := make(Namespaces, len(namespaces))
		for ns, buckets := range namespaces {
			dstBuckets := make(Buckets, len(buckets))
			for name, p := range buckets {
				dstProps := &cmn.BucketProps{}
				*dstProps = *p
				dstBuckets[name] = dstProps
			}
			dstNamespaces[ns] = dstBuckets
		}
		dst.Providers[provider] = dstNamespaces
	}
}

/////////////////////
// private methods //
/////////////////////

func (m *BMD) getBuckets(bck *Bck) (buckets Buckets) {
	cmn.Assert(bck.HasProvider()) // TODO -- FIXME: remove
	if namespaces, ok := m.Providers[bck.Provider]; ok {
		buckets = namespaces[bck.Ns.Uname()]
	}
	return
}

func (m *BMD) initBckAnyProvider(bck *Bck) (provider string, p *cmn.BucketProps) {
	var present bool
	if namespaces, ok := m.Providers[cmn.ProviderAIS]; ok {
		if buckets, ok := namespaces[bck.Ns.Uname()]; ok {
			if p, present = buckets[bck.Name]; present {
				provider = cmn.ProviderAIS
				return
			}
		}
	}
	return m.initBckCloudProvider(bck)
}

func (m *BMD) initBckCloudProvider(bck *Bck) (provider string, p *cmn.BucketProps) {
	var present bool
	for prov, namespaces := range m.Providers {
		if prov == cmn.ProviderAIS {
			continue
		}
		if buckets, ok2 := namespaces[bck.Ns.Uname()]; ok2 {
			if p, present = buckets[bck.Name]; present {
				provider = prov
				return
			}
		}
	}
	return
}
