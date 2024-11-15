// Package meta: cluster-level metadata
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package meta

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
)

type (
	// interface to Get current (versioned, immutable) BMD instance
	// (for implementation, see ais/bucketmeta.go)
	Bowner interface {
		Get() (bmd *BMD)
	}

	Buckets    map[string]*cmn.Bprops
	Namespaces map[string]Buckets
	Providers  map[string]Namespaces

	// - BMD is the root of the (providers, namespaces, buckets) hierarchy
	// - BMD (instance) can be obtained via Bowner.Get()
	// - BMD is immutable and versioned
	// - BMD versioning is monotonic and incremental
	BMD struct {
		Ext       any       `json:"ext,omitempty"`  // within meta-version extensions
		Providers Providers `json:"providers"`      // (provider, namespace, bucket) hierarchy
		UUID      string    `json:"uuid"`           // unique & immutable
		Version   int64     `json:"version,string"` // gets incremented on every update
	}
)

func (m *BMD) String() string {
	if m == nil {
		return "BMD <nil>"
	}
	return "BMD v" + strconv.FormatInt(m.Version, 10)
}

func (m *BMD) StringEx() string {
	if m == nil {
		return "BMD <nil>"
	}
	na, nar, nc, no := m.numBuckets(false /*check empty*/)
	if na+nar+nc+no == 0 {
		return fmt.Sprintf("BMD v%d[%s (no buckets)]", m.Version, m.UUID)
	}
	if nar == 0 && no == 0 {
		return fmt.Sprintf("BMD v%d[%s, buckets: ais(%d), cloud(%d)]", m.Version, m.UUID, na, nc)
	}
	return fmt.Sprintf("BMD v%d[%s, buckets: ais(%d), cloud(%d), remote-ais(%d), remote-other(%d)]",
		m.Version, m.UUID, na, nc, nar, no)
}

func (m *BMD) Get(bck *Bck) (p *cmn.Bprops, present bool) {
	buckets := m.getBuckets(bck)
	if buckets != nil {
		p, present = buckets[bck.Name]
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

func (m *BMD) Set(bck *Bck, p *cmn.Bprops) {
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
		namespaces = make(Namespaces, 1)
		m.Providers[bck.Provider] = namespaces
	}
	nsUname := bck.Ns.Uname()
	if buckets, ok = namespaces[nsUname]; !ok {
		buckets = make(Buckets)
		namespaces[nsUname] = buckets
	}
	buckets[bck.Name] = bck.Props
}

func (m *BMD) IsEmpty() bool {
	na, nar, nc, no := m.numBuckets(true)
	return na+nar+nc+no == 0
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
	var qname string
	if nsQuery != nil {
		qname = nsQuery.Uname()
	}
	for provider, namespaces := range m.Providers {
		if providerQuery != nil && provider != *providerQuery {
			continue
		}
		for nsUname, buckets := range namespaces {
			if nsQuery != nil && nsUname != qname {
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

func (m *BMD) Select(qbck *cmn.QueryBcks) cmn.Bcks {
	var (
		cp   *string
		bcks = cmn.Bcks{} // (json representation: nil slice != empty slice)
	)
	if qbck.Provider != "" {
		cp = &qbck.Provider
	}
	m.Range(cp, nil, func(bck *Bck) bool {
		b := bck.Bucket()
		if qbck.Equal(b) || qbck.Contains(b) {
			if len(bcks) == 0 {
				bcks = make(cmn.Bcks, 0, 8)
			}
			bcks = append(bcks, bck.Clone())
		}
		return false
	})
	sort.Sort(bcks)
	return bcks
}

//
// private methods
//

func (m *BMD) getBuckets(bck *Bck) (buckets Buckets) {
	if namespaces, ok := m.Providers[bck.Provider]; ok {
		buckets = namespaces[bck.Ns.Uname()]
	}
	return
}

func (m *BMD) numBuckets(checkEmpty bool) (na, nar, nc, no int) {
	for provider, namespaces := range m.Providers {
		for nsUname, buckets := range namespaces {
			ns := cmn.ParseNsUname(nsUname)
			switch {
			case provider == apc.AIS:
				if ns.IsRemote() {
					nar += len(buckets)
				} else {
					na += len(buckets)
				}
			case apc.IsCloudProvider(provider):
				nc += len(buckets)
			default:
				no += len(buckets)
			}

			if checkEmpty && (na+nar+nc+no > 0) {
				return na, nar, nc, no
			}
		}
	}
	return na, nar, nc, no
}

func (m *BMD) initBckGlobalNs(bck *Bck) bool {
	namespaces, ok := m.Providers[bck.Provider]
	if !ok {
		return false
	}
	buckets, ok := namespaces[cmn.NsGlobal.Uname()]
	if !ok {
		return false
	}
	p, present := buckets[bck.Name]
	if present {
		debug.Assert(bck.Ns.IsGlobal())
		bck.Props = p
	}
	return present
}

func (m *BMD) initBck(bck *Bck) {
	namespaces, ok := m.Providers[bck.Provider]
	if !ok {
		return
	}
	for nsUname, buckets := range namespaces {
		if p, present := buckets[bck.Name]; present {
			bck.Props = p
			bck.Ns = cmn.ParseNsUname(nsUname)
			return
		}
	}
}

func (m *BMD) getAllByName(bckName string) (all []Bck) {
	for provider, namespace := range m.Providers {
		for nsUname, buckets := range namespace {
			if props, present := buckets[bckName]; present {
				bck := Bck{Name: bckName, Provider: provider, Props: props}
				bck.Ns = cmn.ParseNsUname(nsUname)
				all = append(all, bck)
			}
		}
	}
	return
}
