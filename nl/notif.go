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
	NotifBase struct {
		When     cluster.Upon  // see the enum below
		Interval time.Duration // interval at which progress needs to be updated

		Dsts []string                         // node IDs to notify
		F    func(n cluster.Notif, err error) // notification callback
		P    func(n cluster.Notif)            // on progress notification callback

		lastNotified atomic.Int64 // time when last notified
	}
)

func (notif *NotifBase) OnFinishedCB() func(cluster.Notif, error) { return notif.F }
func (notif *NotifBase) OnProgressCB() func(cluster.Notif)        { return notif.P }
func (notif *NotifBase) Upon(u cluster.Upon) bool                 { return notif != nil && notif.When&u != 0 }
func (notif *NotifBase) Subscribers() []string                    { return notif.Dsts }
func (notif *NotifBase) LastNotifTime() int64                     { return notif.lastNotified.Load() }
func (notif *NotifBase) SetLastNotified(now int64)                { notif.lastNotified.Store(now) }
func (notif *NotifBase) NotifyInterval() time.Duration {
	if notif.Interval == 0 {
		return cmn.GCO.Get().Periodic.NotifTime.D()
	}
	return notif.Interval
}

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
