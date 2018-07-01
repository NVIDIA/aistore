/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

// Keep track average latency of call between proxies and targets
// Currently it filters the calls by request (configurable), other filters (for example,
// by http method) can be added if need to
// A warning is logged if a call's latency exceeds the average by a predefined factor/threshhold
// APIs provided by this package are concurrent access safe

import (
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/dfc/statsd"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
)

type (
	callInfo struct {
		url     string
		latency time.Duration
		failed  bool
	}

	latency struct {
		cnt          int64
		totalLatency time.Duration // accumulated latency
		errCnt       int64
	}

	// CallStatsServer stores parameters and call stats collected from each call
	CallStatsServer struct {
		wg              sync.WaitGroup
		ch              chan callInfo
		stats           map[string]*latency
		factor          float32
		requestIncluded []string
		statsdC         *statsd.Client
	}
)

func (l *latency) avg() int64 {
	if l.cnt == 0 {
		return 0
	}

	return int64(l.totalLatency) / l.cnt
}

// NewCallStatsServer returns a CallStatsServer
// Note: the channel size is picked as 100, just a number, even 1 works but Call() will become blocking.
//       another place can be config file.
func NewCallStatsServer(requestsIncluded []string, factor float32, statsdC *statsd.Client) *CallStatsServer {
	return &CallStatsServer{
		ch:              make(chan callInfo, 100),
		stats:           make(map[string]*latency),
		requestIncluded: requestsIncluded,
		factor:          factor,
		statsdC:         statsdC,
	}
}

// Start starts the worker that does the stats collection
func (c *CallStatsServer) Start() {
	c.wg.Add(1)
	go c.worker()
}

// Stop stops the worker
func (c *CallStatsServer) Stop() {
	close(c.ch)
	c.wg.Wait()
}

// Call sends a call's info to the channel
// FIXME: when httprunner is stopped, it doesn't mean there are no more intra-cluster calls anymore, those
// calls will come to here and can trigger a write to closed channel error.
// Unlikely but can happen.
func (c *CallStatsServer) Call(url string, latency time.Duration, failed bool) {
	// c.ch <- callInfo{url, latency, failed}
}

func (c *CallStatsServer) worker() {
	for ci := range c.ch {
		// check with filters
		var include bool
		for _, v := range c.requestIncluded {
			if strings.Contains(ci.url, v) {
				include = true
				break
			}
		}

		if !include {
			continue
		}

		var errCnt int64
		if ci.failed {
			errCnt = 1
		}

		s, ok := c.stats[ci.url]
		if ok {
			s.cnt++
			s.totalLatency += ci.latency
			s.errCnt += errCnt

			if float64(ci.latency) > float64(c.factor)*float64(s.avg()) {
				glog.Warningf("call %v latency %v too high, avg = %v", ci.url, ci.latency, s.avg())
			}
		} else {
			c.stats[ci.url] = &latency{1, ci.latency, errCnt}
		}

		// from http://10.112.76.11:8080/v1/cluster/keepalive to 10_112_76_11:8080/v1/cluster/keepalive
		// ideally 'url' is passed in as url.URL, which is not the case right now because
		// proxy/target.call() has url not url.URL.
		tag := strings.Replace(strings.TrimPrefix(ci.url, "http://"), ".", "_", -1 /* replace all */)
		c.statsdC.Send("call."+tag,
			statsd.Metric{
				Type:  statsd.Gauge,
				Name:  "latency",
				Value: ci.latency,
			},
			statsd.Metric{
				Type:  statsd.Counter,
				Name:  "count",
				Value: 1,
			},
		)

		if ci.failed {
			c.statsdC.Send("call."+tag,
				statsd.Metric{
					Type:  statsd.Counter,
					Name:  "error",
					Value: 1,
				},
			)
		}
	}

	c.wg.Done()
}
