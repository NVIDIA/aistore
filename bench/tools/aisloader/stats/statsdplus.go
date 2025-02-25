// Package stats provides various structs for collecting stats
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/stats/statsd"
)

const (
	get              = "get"
	put              = "put"
	getConfig        = "getconfig"
	throughput       = "throughput"
	latency          = "latency"
	latencyProxy     = "latency.proxy"
	latencyProxyConn = "latency.proxyconn"
	minLatency       = "minlatency"
	maxLatency       = "maxlatency"
	pending          = "pending"
	count            = "count"
)

type (
	BaseMetricAgg struct {
		name  string
		start time.Time // time current stats started
		cnt   int64     // total # of requests
	}
	MetricAgg struct {
		MetricLatAgg
		bytes   int64 // total bytes by all requests
		errs    int64 // number of failed requests
		pending int64
	}
	MetricLatAgg struct {
		BaseMetricAgg
		latency time.Duration // Accumulated request latency

		// self maintained fields
		minLatency time.Duration
		maxLatency time.Duration
	}
	MetricConfigAgg struct {
		BaseMetricAgg
		latency             time.Duration
		minLatency          time.Duration
		maxLatency          time.Duration
		proxyLatency        time.Duration
		minProxyLatency     time.Duration
		maxProxyLatency     time.Duration
		proxyConnLatency    time.Duration
		minProxyConnLatency time.Duration
		maxProxyConnLatency time.Duration
	}
	MetricLatsAgg struct {
		metrics map[string]*MetricLatAgg
	}
	Metrics struct {
		Put    MetricAgg
		Get    MetricAgg
		Config MetricConfigAgg
		PutLat MetricLatsAgg
		GetLat MetricLatsAgg
	}
)

func NewStatsdMetrics(start time.Time) Metrics {
	m := Metrics{}
	m.Get.start = start
	m.Put.start = start
	m.Config.start = start
	return m
}

func (ma *MetricAgg) Add(size int64, lat time.Duration) {
	ma.cnt++
	ma.latency += lat
	ma.bytes += size
	ma.minLatency = min(ma.minLatency, lat)
	ma.maxLatency = max(ma.maxLatency, lat)
}

func (ma *MetricAgg) AddPending(pending int64) {
	ma.pending += pending
}

func (ma *MetricAgg) AddErr() {
	ma.errs++
}

func (ma *MetricAgg) AvgLatency() float64 {
	if ma.cnt == 0 {
		return 0
	}
	return float64(ma.latency/time.Millisecond) / float64(ma.cnt)
}

func (ma *MetricAgg) Throughput(end time.Time) int64 {
	if ma.cnt == 0 {
		return 0
	}
	if end == ma.start {
		return 0
	}

	return int64(float64(ma.bytes) / end.Sub(ma.start).Seconds())
}

func (mgs *MetricLatsAgg) Add(name string, lat time.Duration) {
	if mgs.metrics == nil {
		mgs.metrics = make(map[string]*MetricLatAgg)
	}

	if val, ok := mgs.metrics[name]; !ok {
		mgs.metrics[name] = &MetricLatAgg{
			BaseMetricAgg: BaseMetricAgg{
				start: time.Now(),
				cnt:   1,
				name:  name,
			},

			latency:    lat,
			minLatency: lat,
			maxLatency: lat,
		}
	} else {
		val.cnt++
		val.latency += lat
		val.maxLatency = max(val.maxLatency, lat)
		val.minLatency = min(val.minLatency, lat)
	}
}

func (mcg *MetricConfigAgg) Add(lat, _, _ time.Duration) {
	mcg.cnt++

	mcg.latency += lat
	mcg.minLatency = min(mcg.minLatency, lat)
	mcg.maxLatency = max(mcg.maxLatency, lat)

	mcg.proxyLatency += lat
	mcg.minProxyLatency = min(mcg.minProxyLatency, lat)
	mcg.maxProxyLatency = max(mcg.maxProxyLatency, lat)

	mcg.proxyConnLatency += lat
	mcg.minProxyConnLatency = min(mcg.minProxyConnLatency, lat)
	mcg.maxProxyConnLatency = max(mcg.maxProxyConnLatency, lat)
}

func (ma *MetricAgg) Send(c *statsd.Client, mType string, general []statsd.Metric, genAggCnt int64) {
	endTime := time.Now()
	c.Send(mType, 1, statsd.Metric{
		Type:  statsd.Counter,
		Name:  count,
		Value: ma.cnt,
	})
	// don't send anything when cnt == 0 -> no data aggregated
	if ma.cnt > 0 {
		c.Send(mType, ma.cnt, statsd.Metric{
			Type:  statsd.Gauge,
			Name:  pending,
			Value: ma.pending / ma.cnt,
		})
		c.Send(mType, ma.cnt,
			statsd.Metric{
				Type:  statsd.Timer,
				Name:  latency,
				Value: ma.AvgLatency(),
			},
			statsd.Metric{
				Type:  statsd.Timer,
				Name:  minLatency,
				Value: float64(ma.minLatency / time.Millisecond),
			},
			statsd.Metric{
				Type:  statsd.Timer,
				Name:  maxLatency,
				Value: float64(ma.maxLatency / time.Millisecond),
			},
		)
		c.Send(mType, ma.cnt, statsd.Metric{
			Type:  statsd.Gauge,
			Name:  throughput,
			Value: ma.Throughput(endTime),
		})
	}
	if len(general) != 0 && genAggCnt > 0 {
		c.Send(mType, genAggCnt, general...)
	}
}

func (mcg *MetricConfigAgg) Send(c *statsd.Client) {
	// don't send anything when cnt == 0 -> no data aggregated
	if mcg.cnt == 0 {
		return
	}
	c.Send(getConfig, 1,
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  count,
			Value: mcg.cnt,
		})
	c.Send(getConfig, mcg.cnt,
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  latency,
			Value: float64(mcg.latency/time.Millisecond) / float64(mcg.cnt),
		},
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  latencyProxyConn,
			Value: float64(mcg.proxyConnLatency/time.Millisecond) / float64(mcg.cnt),
		},
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  latencyProxy,
			Value: float64(mcg.proxyLatency/time.Millisecond) / float64(mcg.cnt),
		},
	)
}

func (m *Metrics) SendAll(c *statsd.Client) {
	var (
		aggCntGet, aggCntPut int64
		generalMetricsGet    = make([]statsd.Metric, 0, len(m.GetLat.metrics))
		generalMetricsPut    = make([]statsd.Metric, 0, len(m.PutLat.metrics))
	)
	for _, m := range m.GetLat.metrics {
		generalMetricsGet = append(generalMetricsGet, statsd.Metric{
			Type:  statsd.Timer,
			Name:  m.name,
			Value: float64(m.latency/time.Millisecond) / float64(m.cnt),
		})
		// m.cnt is the same for all aggregated metrics
		aggCntGet = m.cnt
	}
	for _, m := range m.PutLat.metrics {
		generalMetricsPut = append(generalMetricsPut, statsd.Metric{
			Type:  statsd.Timer,
			Name:  m.name,
			Value: float64(m.latency/time.Millisecond) / float64(m.cnt),
		})
		// m.cnt is the same for all aggregated metrics
		aggCntPut = m.cnt
	}

	m.Get.Send(c, get, generalMetricsGet, aggCntGet)
	m.Put.Send(c, put, generalMetricsPut, aggCntPut)

	m.Config.Send(c)
}

func ResetMetricsGauges(c *statsd.Client) {
	c.Send(get, 1,
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  throughput,
			Value: 0,
		}, statsd.Metric{
			Type:  statsd.Gauge,
			Name:  pending,
			Value: 0,
		})

	c.Send(put, 1,
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  throughput,
			Value: 0,
		}, statsd.Metric{
			Type:  statsd.Gauge,
			Name:  pending,
			Value: 0,
		})
}
