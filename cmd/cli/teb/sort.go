// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"sort"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

type StstMap map[string]*stats.NodeStatus // by node ID (SID)

func (psts StstMap) sortSIDs() (ids []string) {
	ids = make([]string, 0, len(psts))
	for sid := range psts {
		ids = append(ids, sid)
	}
	sort.Strings(ids)
	return ids
}

// NOTE: using stats.NodeStatus reserved field
func (psts StstMap) sortPODs(smap *meta.Smap, proxies bool) (ids []string) {
	var (
		pods = make([]*stats.NodeStatus, 0, len(psts))
		pid  = smap.Primary.ID()
		hasp bool
	)
	if len(psts) == 0 {
		return nil
	}
	ids = make([]string, 0, len(psts))
	for sid, ds := range psts {
		debug.Assert(sid == ds.Node.Snode.ID(), sid, " vs ", ds.Node.Snode.ID()) // (unlikely)
		ids = append(ids, sid)
		if l := len(ds.K8sPodName); l > 3 && ds.K8sPodName[l-1] >= '0' && ds.K8sPodName[l-1] <= '9' {
			pods = append(pods, ds)
		}
		if proxies && pid == sid {
			hasp = true
		}
	}
	if len(ids) == 1 {
		return ids
	}
	if len(ids) != len(pods) {
		sort.Strings(ids)
		return ids
	}

	for _, ds := range pods {
		var (
			s   = ds.K8sPodName
			ten = 1
			n   int
		)
		for i := len(s) - 1; i >= 0; i-- {
			if s[i] < '0' || s[i] > '9' {
				break
			}
			n += ten * int(s[i]-'0')
			ten *= 10
		}
		ds.Reserved4 = int64(n)
	}
	less := func(i, j int) bool { return pods[i].Reserved4 < pods[j].Reserved4 }
	sort.Slice(pods, less)

	// ids in the pod-names order
	for i, ds := range pods {
		ids[i] = ds.Node.Snode.ID()
	}

	// finally: primary on top
	if proxies && hasp && ids[0] != pid {
		for i := range ids {
			if ids[i] != pid {
				continue
			}
			copy(ids[1:], ids[0:i])
			ids[0] = pid
			break
		}
	}

	return ids
}
