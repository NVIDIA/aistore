// Package notifications provides interfaces for AIStore notifications
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package nl

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
)

type (
	Base struct {
		F func(n cluster.Notif, err error) // notification callback
		P func(n cluster.Notif)            // on progress notification callback

		Dsts []string // node IDs to notify

		When     cluster.Upon  // see the enum below
		Interval time.Duration // interval at which progress needs to be updated

		lastNotified atomic.Int64 // time when last notified
	}
)

//////////
// Base //
//////////

func (base *Base) OnFinishedCB() func(cluster.Notif, error) { return base.F }
func (base *Base) OnProgressCB() func(cluster.Notif)        { return base.P }
func (base *Base) Upon(u cluster.Upon) bool                 { return base != nil && base.When&u != 0 }
func (base *Base) Subscribers() []string                    { return base.Dsts }
func (base *Base) LastNotifTime() int64                     { return base.lastNotified.Load() }
func (base *Base) SetLastNotified(now int64)                { base.lastNotified.Store(now) }
func (base *Base) NotifyInterval() time.Duration {
	if base.Interval == 0 {
		return cmn.GCO.Get().Periodic.NotifTime.D()
	}
	return base.Interval
}

//
// common callbacks
//

func shouldNotify(n cluster.Notif) bool {
	lastTime := n.LastNotifTime()
	return lastTime == 0 || mono.Since(lastTime) > n.NotifyInterval()
}

func OnProgress(n cluster.Notif) {
	if n == nil {
		return
	}
	if cb := n.OnProgressCB(); cb != nil && shouldNotify(n) {
		n.SetLastNotified(mono.NanoTime())
		cb(n)
	}
}

func OnFinished(n cluster.Notif, err error) {
	if n == nil {
		return
	}
	if cb := n.OnFinishedCB(); cb != nil {
		cb(n, err)
	}
}
