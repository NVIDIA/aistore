// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"errors"
	"fmt"
	"sort"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xact"
)

type (
	// TODO: add more, like target finished, query Aborted by user etc.
	NotifListenerQuery struct {
		xact.NotifXactListener
		Targets    []*cluster.Snode
		WorkersCnt uint
	}
)

func NewQueryListener(uuid string, smap *cluster.Smap, msg *InitMsg) (*NotifListenerQuery, error) {
	cos.Assert(uuid != "")
	numNodes := len(smap.Tmap)
	if msg.WorkersCnt != 0 && msg.WorkersCnt < uint(numNodes) {
		// FIXME: this should not be necessary. Proxy could know that if worker's
		//  target is done, worker should be redirected to the next not-done target.
		//  However, it should be done once query is able to keep more detailed
		//  information about targets.
		return nil, fmt.Errorf("expected WorkersCnt to be at least %d", numNodes)
	}

	// Ensure same order on all nodes
	targets := smap.Tmap.ActiveNodes()
	sort.SliceStable(targets, func(i, j int) bool {
		return targets[i].DaemonID < targets[j].DaemonID
	})
	nl := &NotifListenerQuery{
		NotifXactListener: *xact.NewXactNL(uuid,
			cmn.ActQueryObjects, smap, nil, msg.QueryMsg.From.Bck),
		WorkersCnt: msg.WorkersCnt,
		Targets:    targets,
	}
	return nl, nil
}

func (q *NotifListenerQuery) WorkersTarget(workerID uint) (*cluster.Snode, error) {
	if q.WorkersCnt == 0 {
		return nil, errors.New("query registered with 0 workers")
	}
	if workerID == 0 {
		return nil, errors.New("workerID cannot be empty")
	}
	return q.Targets[workerID%uint(len(q.Targets))], nil
}
