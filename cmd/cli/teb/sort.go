// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"sort"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
)

func (psts NodeStatusMap) sortSIDs() (ids []string) {
	ids = make([]string, 0, len(psts))
	for sid := range psts {
		ids = append(ids, sid)
	}
	sort.Strings(ids)
	return ids
}

func (psts NodeStatusMap) sortPODs(smap *meta.Smap, proxies bool) (ids []string) {
	if len(psts) == 0 {
		return nil
	}

	type podInfo struct {
		id  string
		ord int
	}

	var (
		pods    = make([]podInfo, 0, len(psts))
		pid     string
		hasp    bool
		allPods = true
	)

	if proxies {
		pid = smap.Primary.ID()
	}

	ids = make([]string, 0, len(psts))
	for sid, ds := range psts {
		debug.Assert(sid == ds.Node.Snode.ID(), sid, " vs ", ds.Node.Snode.ID()) // unlikely
		ids = append(ids, sid)

		if allPods {
			if n, ok := podOrdinal(ds.K8sPodName); ok {
				pods = append(pods, podInfo{id: sid, ord: n})
			} else {
				allPods = false
			}
		}
		if proxies && sid == pid {
			hasp = true
		}
	}

	if len(ids) == 1 {
		return ids
	}
	if !allPods {
		sort.Strings(ids)
		return ids
	}

	sort.Slice(pods, func(i, j int) bool {
		if pods[i].ord != pods[j].ord {
			return pods[i].ord < pods[j].ord
		}
		return pods[i].id < pods[j].id
	})

	for i, pod := range pods {
		ids[i] = pod.id
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

func podOrdinal(name string) (int, bool) {
	if len(name) < 4 {
		return 0, false
	}

	i := len(name) - 1
	if name[i] < '0' || name[i] > '9' {
		return 0, false
	}

	var (
		n   int
		ten = 1
	)
	for ; i >= 0; i-- {
		c := name[i]
		if c < '0' || c > '9' {
			break
		}
		n += ten * int(c-'0')
		ten *= 10
	}
	return n, true
}
