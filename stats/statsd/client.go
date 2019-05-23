/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// A StatsD client that sends basic statd metrics(timer, counter and gauge) to a
// listening UDP StatsD server

package statsd

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

// MetricType is the type of StatsD metric
type MetricType int

const (
	// Timer is StatsD's timer type
	Timer MetricType = iota
	// Counter is StatsD's counter type
	Counter
	// Gauge is StatsD's gauge type
	Gauge
	// PersistentCounter is StatsD's gauge type which is increased every time by the value
	PersistentCounter

	get       = "get"
	put       = "put"
	getConfig = "getconfig"

	throughput = "throughput"

	latency          = "latency"
	latencyProxy     = "latency.proxy"
	latencyProxyConn = "latency.proxyconn"
	minLatency       = "minlatency"
	maxLatency       = "maxlatency"

	pending = "pending"
	count   = "count"
)

type (
	// Client implements a StatsD client
	Client struct {
		conn   *net.UDPConn
		prefix string
		opened bool // true if the connection with StatsD is successfully opened
	}

	// Metric is a generic structure for all type of StatsD metrics
	Metric struct {
		Type  MetricType // time, counter or gauge
		Name  string     // Name for this particular metric
		Value interface{}
	}

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
		latency    time.Duration
		minLatency time.Duration
		maxLatency time.Duration

		proxyLatency    time.Duration
		minProxyLatency time.Duration
		maxProxyLatency time.Duration

		proxyConnLatency    time.Duration
		minProxyConnLatency time.Duration
		maxProxyConnLatency time.Duration
	}

	MetricLatsAgg struct {
		metrics map[string]*MetricLatAgg
	}

	Metrics struct {
		Put     MetricAgg
		Get     MetricAgg
		Config  MetricConfigAgg
		General MetricLatsAgg
	}
)

// New returns a client after resolving server and self's address and dialed the server
// Caller needs to call close
func New(ip string, port int, prefix string) (Client, error) {
	server, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return Client{}, err
	}

	self, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		return Client{}, err
	}

	conn, err := net.DialUDP("udp", self, server)
	if err != nil {
		return Client{}, err
	}

	return Client{conn, prefix, true}, nil
}

// Close closes the UDP connection
func (c Client) Close() error {
	if c.opened {
		return c.conn.Close()
	}

	return nil
}

// Send sends metrics to StatsD server
// Note: Sending error is ignored
// aggCnt - if stats were aggregated - number of stats which were aggregated, 1 otherwise
// 1/ratio - how many samples are aggregated into single metric
// see: https://github.com/statsd/statsd/blob/master/docs/metric_types.md#sampling
func (c Client) Send(bucket string, aggCnt int64, metrics ...Metric) {
	if !c.opened {
		return
	}

	if aggCnt == 0 {
		// there was no data aggregated, don't send anything to StatsD
		return
	}

	var (
		t, prefix string
		packet    bytes.Buffer
	)

	// NOTE: ":" is not allowed since it will be treated as value eg. in case daemonID is in form NUMBER:NUMBER
	bucket = strings.Replace(bucket, ":", "_", -1)

	for _, m := range metrics {
		switch m.Type {
		case Timer:
			t = "ms"
		case Counter:
			t = "c"
		case Gauge:
			t = "g"
		case PersistentCounter:
			prefix = "+"
			t = "g"
		default:
			cmn.AssertMsg(false, fmt.Sprintf("Unknown type %+v", m.Type))
		}

		if packet.Len() > 0 {
			packet.WriteRune('\n')
		}

		var err error
		if aggCnt != 1 {
			_, err = fmt.Fprintf(&packet, "%s.%s.%s:%s%v|%s|@%f", c.prefix, bucket, m.Name, prefix, m.Value, t, float64(1)/float64(aggCnt))
		} else {
			_, err = fmt.Fprintf(&packet, "%s.%s.%s:%s%v|%s", c.prefix, bucket, m.Name, prefix, m.Value, t)
		}
		cmn.AssertNoErr(err)
	}

	if packet.Len() > 0 {
		_, err := c.conn.Write(packet.Bytes())
		if err != nil && glog.V(5) {
			glog.Infof("Sending to StatsD failed: %s\n", err.Error())
		}
	}
}

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
	ma.minLatency = cmn.MinDuration(ma.minLatency, lat)
	ma.maxLatency = cmn.MaxDuration(ma.maxLatency, lat)
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
		val.maxLatency = cmn.MaxDuration(val.maxLatency, lat)
		val.minLatency = cmn.MinDuration(val.minLatency, lat)
	}
}

func (mcg *MetricConfigAgg) Add(lat, latProxy, latProxyConn time.Duration) {
	mcg.cnt++

	mcg.latency += lat
	mcg.minLatency = cmn.MinDuration(mcg.minLatency, lat)
	mcg.maxLatency = cmn.MaxDuration(mcg.maxLatency, lat)

	mcg.proxyLatency += lat
	mcg.minProxyLatency = cmn.MinDuration(mcg.minProxyLatency, lat)
	mcg.maxProxyLatency = cmn.MaxDuration(mcg.maxProxyLatency, lat)

	mcg.proxyConnLatency += lat
	mcg.minProxyConnLatency = cmn.MinDuration(mcg.minProxyConnLatency, lat)
	mcg.maxProxyConnLatency = cmn.MaxDuration(mcg.maxProxyConnLatency, lat)
}

func (ma *MetricAgg) Send(c *Client, mType string, general []Metric, genAggCnt int64) {
	endTime := time.Now()
	c.Send(mType, 1, Metric{
		Type:  Counter,
		Name:  count,
		Value: ma.cnt,
	})

	// don't send anything when cnt == 0 -> no data aggregated
	if ma.cnt != 0 {
		c.Send(mType, ma.cnt, Metric{
			Type:  Gauge,
			Name:  pending,
			Value: ma.pending / ma.cnt,
		})
		c.Send(mType, ma.cnt,
			Metric{
				Type:  Timer,
				Name:  latency,
				Value: ma.AvgLatency(),
			},
			Metric{
				Type:  Timer,
				Name:  minLatency,
				Value: float64(ma.minLatency / time.Millisecond),
			},
			Metric{
				Type:  Timer,
				Name:  maxLatency,
				Value: float64(ma.maxLatency / time.Millisecond),
			},
		)
		c.Send(mType, ma.cnt, Metric{
			Type:  Gauge,
			Name:  throughput,
			Value: ma.Throughput(endTime),
		})
	}

	if len(general) != 0 {
		c.Send(mType, genAggCnt, general...)
	}
}

func (mcg *MetricConfigAgg) Send(c *Client) {
	// don't send anything when cnt == 0 -> no data aggregated
	if mcg.cnt == 0 {
		return
	}

	c.Send(getConfig, 1,
		Metric{
			Type:  Counter,
			Name:  count,
			Value: mcg.cnt,
		})
	c.Send(getConfig, mcg.cnt,
		Metric{
			Type:  Timer,
			Name:  latency,
			Value: float64(mcg.latency/time.Millisecond) / float64(mcg.cnt),
		},
		Metric{
			Type:  Timer,
			Name:  latencyProxyConn,
			Value: float64(mcg.proxyConnLatency/time.Millisecond) / float64(mcg.cnt),
		},
		Metric{
			Type:  Timer,
			Name:  latencyProxy,
			Value: float64(mcg.proxyLatency/time.Millisecond) / float64(mcg.cnt),
		},
	)
}

func (m *Metrics) SendAll(c *Client) {
	generalMetrics := make([]Metric, 0, len(m.General.metrics))
	var aggCnt int64
	for _, m := range m.General.metrics {
		generalMetrics = append(generalMetrics, Metric{
			Type:  Timer,
			Name:  m.name,
			Value: float64(m.latency/time.Millisecond) / float64(m.cnt),
		})
		// m.cnt is the same for all aggregated metrics
		aggCnt = m.cnt
	}

	m.Get.Send(c, get, generalMetrics, aggCnt)
	m.Put.Send(c, put, generalMetrics, aggCnt)
	m.Config.Send(c)
}

func ResetMetricsGauges(c *Client) {
	c.Send(get, 1,
		Metric{
			Type:  Gauge,
			Name:  throughput,
			Value: 0,
		}, Metric{
			Type:  Gauge,
			Name:  pending,
			Value: 0,
		})

	c.Send(put, 1,
		Metric{
			Type:  Gauge,
			Name:  throughput,
			Value: 0,
		}, Metric{
			Type:  Gauge,
			Name:  pending,
			Value: 0,
		})
}
