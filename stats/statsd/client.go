// Package statsd provides a client to send basic statd metrics (timer, counter and gauge) to listening UDP StatsD server.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package statsd

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/memsys"
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
	// PersistentCounter is StatsD's gauge type which is increased every time by its value
	PersistentCounter
)

const (
	numErrsLog    = 100 // log one every so many
	numTestProbes = 10  // num UDP probes at startup (optional)
)

type (
	// Client implements a StatsD client
	Client struct {
		conn   *net.UDPConn
		server *net.UDPAddr // resolved StatsD server addr
		prefix string       // e.g. aistarget<ID>
		opened bool         // true if the connection with StatsD is successfully opened
	}

	// Metric is a generic structure for all type of StatsD metrics
	Metric struct {
		Type  MetricType // time, counter or gauge
		Name  string     // Name for this particular metric
		Value any
	}
)

var (
	smm           *memsys.MMSA
	errcnt, msize int64
)

// New returns a UDP client that we then use to send metrics to the specified IP:port
func New(ip string, port int, prefix string, probe bool) (*Client, error) {
	c, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return &Client{}, err
	}
	conn, ok := c.(*net.UDPConn)
	debug.Assert(ok)
	server, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		conn.Close()
		return &Client{}, err
	}
	smm = memsys.ByteMM()
	client := &Client{conn, server, prefix, true /*opened*/}
	if !probe {
		return client, nil
	}
	if err := client.probeUDP(); err != nil {
		conn.Close()
		return &Client{}, err
	}
	return client, nil
}

// NOTE: a single failed probe disables StatsD for the entire runtime
func (c *Client) probeUDP() (err error) {
	var (
		sgl = smm.NewSGL(0)
		m   = Metric{Type: Counter, Name: "counter", Value: 1}
		cnt int
	)
	defer sgl.Free()
	for i := 1; i <= numTestProbes; i++ {
		sgl.Reset()
		m.Value = i
		c.appendM(m, sgl, "probe", 1)
		bytes := sgl.Bytes()
		n, erw := c.conn.WriteToUDP(bytes, c.server)
		if erw != nil {
			err = erw
			cnt++
		} else if n == 0 {
			cnt++
		}
		time.Sleep(100 * time.Millisecond)
	}
	if cnt == 0 {
		return
	}
	err = fmt.Errorf("failed to probe %v (%d/%d)", err, cnt, numTestProbes)
	return
}

// Close closes the UDP connection
func (c *Client) Close() error {
	if c.opened {
		c.opened = false
		return c.conn.Close()
	}
	return nil
}

// Send sends metrics to StatsD server
// bucket - StatsD "bucket" - not to confuse with storage
// aggCnt - if stats were aggregated - number of stats which were aggregated, 1 otherwise
// 1/ratio - how many samples are aggregated into single metric
// see: https://github.com/statsd/statsd/blob/master/docs/metric_types.md#sampling
func (c *Client) Send(bucket string, aggCnt int64, metrics ...Metric) {
	if !c.opened {
		return
	}
	sgl := smm.NewSGL(msize)
	defer sgl.Free()

	bucket = strings.ReplaceAll(bucket, ":", "_")
	for _, m := range metrics {
		c.appendM(m, sgl, bucket, aggCnt)
	}
	l := sgl.Len()
	msize = max(msize, l)
	bytes := sgl.Bytes()
	c.write(bytes, int(l))
}

// NOTE: ignoring potential race vs client.Close() - disregarding write errors, if any
func (c *Client) SendSGL(sgl *memsys.SGL) {
	l := sgl.Len()
	debug.Assert(l < sgl.Slab().Size(), l, " vs slab ", sgl.Slab().Size())
	if !c.opened || l == 0 {
		return
	}
	bytes := sgl.Bytes()
	c.write(bytes, int(l))
}

func (c Client) write(bytes []byte, l int) {
	n, err := c.conn.WriteToUDP(bytes, c.server)
	if err != nil || n < l {
		errcnt++
		if errcnt%numErrsLog == 0 {
			nlog.Errorf("StatsD: %v(%d/%d) %d", err, n, l, errcnt)
		}
	}
}

func (c Client) AppMetric(m Metric, sgl *memsys.SGL) {
	var (
		t, prefix string
		err       error
	)
	if !c.opened {
		return
	}
	if sgl.Len() > 0 {
		sgl.WriteByte('\n')
	}
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
		debug.Assertf(false, "unknown type %+v", m.Type)
	}
	_, err = fmt.Fprintf(sgl, "%s:%s%v|%s", m.Name /*slabel*/, prefix, m.Value, t)
	debug.AssertNoErr(err)
}

func (c Client) appendM(m Metric, sgl *memsys.SGL, bucket string, aggCnt int64) {
	var (
		t, prefix string
		err       error
	)
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
		debug.Assertf(false, "unknown type %+v", m.Type)
	}
	if sgl.Len() > 0 {
		sgl.WriteByte('\n')
	}
	if aggCnt != 1 {
		_, err = fmt.Fprintf(sgl, "%s.%s.%s:%s%v|%s|@%f",
			c.prefix, bucket, m.Name, prefix, m.Value, t, float64(1)/float64(aggCnt))
	} else {
		_, err = fmt.Fprintf(sgl, "%s.%s.%s:%s%v|%s", c.prefix, bucket, m.Name, prefix, m.Value, t)
	}
	debug.AssertNoErr(err)
}
