// Package stats provides various structs for collecting stats
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats/statsd"
)

const (
	get              = "get"
	put              = "put"
	getBatch         = "getbatch"
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
		latency time.Duration // accumulated request latency

		// self-maintained fields
		minLatency time.Duration
		maxLatency time.Duration
	}
	MetricLatsAgg struct {
		metrics map[string]*MetricLatAgg
	}
	Metrics struct {
		Put      MetricAgg
		Get      MetricAgg
		GetBatch MetricAgg
		PutLat   MetricLatsAgg
		GetLat   MetricLatsAgg
	}
)

func NewStatsdMetrics(start time.Time) Metrics {
	m := Metrics{}
	m.Get.start = start
	m.Put.start = start
	m.GetBatch.start = start
	return m
}

func (ma *MetricAgg) Add(size int64, lat time.Duration) {
	ma.cnt++
	ma.latency += lat
	ma.bytes += size
	ma.minLatency = min(ma.minLatency, lat)
	ma.maxLatency = max(ma.maxLatency, lat)
}

// Note: this aggregates pending values (to be averaged at send time).
// If you ever want "last pending" semantics, change this to assignment.
func (ma *MetricAgg) AddPending(p int64) {
	ma.pending += p
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
	if end == ma.start { //nolint:staticcheck // equal times can have different locations
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

func (ma *MetricAgg) Send(c *statsd.Client, mType string, general []statsd.Metric, genAggCnt int64) {
	endTime := time.Now()

	// Always send count (even when zero) to make downstream dashboards stable.
	c.Send(mType, 1, statsd.Metric{
		Type:  statsd.Counter,
		Name:  count,
		Value: ma.cnt,
	})

	// Don't send the rest if no activity.
	if ma.cnt > 0 {
		c.Send(mType, ma.cnt, statsd.Metric{
			Type:  statsd.Gauge,
			Name:  pending,
			Value: ma.pending / ma.cnt, // average pending across events
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

	// Optional per-name latency aggregates (GET/PUT have them today).
	if len(general) != 0 && genAggCnt > 0 {
		c.Send(mType, genAggCnt, general...)
	}
}

func (m *Metrics) SendAll(c *statsd.Client) {
	var (
		aggCntGet, aggCntPut int64
		generalMetricsGet    = make([]statsd.Metric, 0, len(m.GetLat.metrics))
		generalMetricsPut    = make([]statsd.Metric, 0, len(m.PutLat.metrics))
	)

	for _, mm := range m.GetLat.metrics {
		generalMetricsGet = append(generalMetricsGet, statsd.Metric{
			Type:  statsd.Timer,
			Name:  mm.name,
			Value: float64(mm.latency/time.Millisecond) / float64(mm.cnt),
		})
		// mm.cnt is the same for all aggregated metrics
		aggCntGet = mm.cnt
	}

	for _, mm := range m.PutLat.metrics {
		generalMetricsPut = append(generalMetricsPut, statsd.Metric{
			Type:  statsd.Timer,
			Name:  mm.name,
			Value: float64(mm.latency/time.Millisecond) / float64(mm.cnt),
		})
		// mm.cnt is the same for all aggregated metrics
		aggCntPut = mm.cnt
	}

	// Emit series
	m.Get.Send(c, get, generalMetricsGet, aggCntGet)
	m.Put.Send(c, put, generalMetricsPut, aggCntPut)

	// GetBatch has its own series; no per-name latencies yet (TODO)
	m.GetBatch.Send(c, getBatch, nil, 0)
}

func ResetMetricsGauges(c *statsd.Client) {
	c.Send(get, 1,
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  throughput,
			Value: 0,
		},
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  pending,
			Value: 0,
		},
	)

	c.Send(put, 1,
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  throughput,
			Value: 0,
		},
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  pending,
			Value: 0,
		},
	)

	c.Send(getBatch, 1,
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  throughput,
			Value: 0,
		},
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  pending,
			Value: 0,
		},
	)
}
