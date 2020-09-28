// Package runners provides implementation for the AIStore extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runners

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	getMarked = func() xaction.XactMarked
	RebBase   struct {
		xaction.XactBase
		wg *sync.WaitGroup
	}

	Rebalance struct {
		RebBase
		statsRunner  *stats.Trunner // extended stats
		getRebMarked getMarked
	}

	Resilver struct {
		RebBase
	}

	Election struct {
		xaction.XactBase
	}
)

func (xact *RebBase) MarkDone()      { xact.wg.Done() }
func (xact *RebBase) WaitForFinish() { xact.wg.Wait() }

func (xact *RebBase) String() string {
	s := xact.XactBase.String()
	if xact.Bck().Name != "" {
		s += ", bucket " + xact.Bck().String()
	}
	return s
}

//
// resilver|rebalance helper
//
func makeXactRebBase(id cluster.XactID, kind string) RebBase {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return RebBase{
		XactBase: *xaction.NewXactBase(id, kind),
		wg:       wg,
	}
}

// Rebalance

func NewRebalance(id cluster.XactID, kind string, statsRunner *stats.Trunner, getMarked getMarked) *Rebalance {
	return &Rebalance{
		RebBase:      makeXactRebBase(id, kind),
		statsRunner:  statsRunner,
		getRebMarked: getMarked,
	}
}

func (xact *Rebalance) IsMountpathXact() bool { return false }

func (xact *Rebalance) String() string {
	return fmt.Sprintf("%s, %s", xact.RebBase.String(), xact.ID())
}

// override/extend cmn.XactBase.Stats()
func (xact *Rebalance) Stats() cluster.XactStats {
	var (
		baseStats   = xact.XactBase.Stats().(*xaction.BaseXactStats)
		rebStats    = stats.RebalanceTargetStats{BaseXactStats: *baseStats}
		statsRunner = xact.statsRunner
	)
	rebStats.Ext.RebTxCount = statsRunner.Get(stats.RebTxCount)
	rebStats.Ext.RebTxSize = statsRunner.Get(stats.RebTxSize)
	rebStats.Ext.RebRxCount = statsRunner.Get(stats.RebRxCount)
	rebStats.Ext.RebRxSize = statsRunner.Get(stats.RebRxSize)
	if marked := xact.getRebMarked(); marked.Xact != nil {
		rebStats.Ext.RebID = marked.Xact.ID().Int()
	} else {
		rebStats.Ext.RebID = 0
	}
	rebStats.ObjCountX = rebStats.Ext.RebTxCount + rebStats.Ext.RebRxCount
	rebStats.BytesCountX = rebStats.Ext.RebTxSize + rebStats.Ext.RebRxSize
	return &rebStats
}

// Resilver

func NewResilver(uuid, kind string) *Resilver {
	return &Resilver{
		RebBase: makeXactRebBase(xaction.XactBaseID(uuid), kind),
	}
}

func (xact *Resilver) IsMountpathXact() bool { return true }

func (xact *Resilver) String() string {
	return xact.RebBase.String()
}

// Election
func (e *Election) IsMountpathXact() bool { return false }
