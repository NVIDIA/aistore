// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"errors"
	"fmt"
	"sort"
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
		Ext       interface{} `json:"ext,omitempty"`  // within meta-version extensions
		Providers Providers   `json:"providers"`      // (provider, namespace, bucket) hierarchy
		UUID      string      `json:"uuid"`           // unique & immutable
		Version   int64       `json:"version,string"` // gets incremented on every update
	}
)

var errBucketIDMismatch = errors.New("bucket ID mismatch")

func (m *BMD) String() string {
	if m == nil {
		return "BMD <nil>"
	}
	return "BMD v" + strconv.FormatInt(m.Version, 10)
}

func (m *BMD) numBuckets() (na, nar, nc, no int) {
	for provider, namespaces := range m.Providers {
		for nsUname, buckets := range namespaces {
			ns := cmn.ParseNsUname(nsUname)
			if provider == apc.ProviderAIS {
				if ns.IsRemote() {
					nar += len(buckets)
				} else {
					na += len(buckets)
				}
			} else if cmn.IsCloudProvider(provider) {
				nc += len(buckets)
			} else {
				no += len(buckets)
			}
		}
	}
	return
}

func (m *BMD) StringEx() string {
	if m == nil {
		return "BMD <nil>"
	}
	na, nar, nc, no := m.numBuckets()
	if na == 0 && nc == 0 && nar == 0 && no == 0 {
		return fmt.Sprintf("BMD v%d[%s (no buckets)]", m.Version, m.UUID)
	}
	if nar == 0 && no == 0 {
		return fmt.Sprintf("BMD v%d[%s, buckets: ais(%d), cloud(%d)]", m.Version, m.UUID, na, nc)
	}
	return fmt.Sprintf("BMD v%d[%s, buckets: ais(%d), cloud(%d), remote-ais(%d), remote-other(%d)]",
		m.Version, m.UUID, na, nc, nar, no)
}

func (m *BMD) Get(bck *Bck) (p *cmn.BucketProps, present bool) {
	buckets := m.getBuckets(bck)
	if buckets != nil {
		p, present = buckets[bck.Name]
	}
	return
}

func (m *BMD) eqBID(bck *Bck, bckID uint64) error {
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

func (m *BMD) Select(qbck *cmn.QueryBcks) cmn.Bcks {
	var (
		names = make(cmn.Bcks, 0, 10)
		cp    = &qbck.Provider
	)
	if qbck.Provider == "" {
		cp = nil
	}
	m.Range(cp, nil, func(bck *Bck) bool {
		if qbck.Equal(bck.Bucket()) || qbck.Contains(bck.Bucket()) {
			names = append(names, bck.Clone())
		}
		return false
	})
	sort.Sort(names)
	return names
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

func (m *BMD) initBck(bck *Bck) bool {
	namespaces, ok := m.Providers[bck.Provider]
	if !ok {
		return false
	}
	for nsUname, buckets := range namespaces {
		if p, present := buckets[bck.Name]; present {
			bck.Props = p
			bck.Ns = cmn.ParseNsUname(nsUname)
			return true
		}
	}
	return false
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
