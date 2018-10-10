/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// A statsd client that sends basic statd metrics(timer, counter and gauge) to a
// listening UDP statsd server

package statsd

import (
	"fmt"
	"net"
)

// MetricType is the type of statsd metric
type MetricType int

const (
	// Timer is statd's timer type
	Timer MetricType = iota
	// Counter is statd's counter type
	Counter
	// Gauge is statd's gauge type
	Gauge
)

type (
	// Client implements a statd client
	Client struct {
		conn   *net.UDPConn
		prefix string
		opened bool // true if the connection with statsd is successfully opened
	}

	// Metric is a generic structure for all type of statsd metrics
	Metric struct {
		Type  MetricType // time, counter or gauge
		Name  string     // Name for this particular metric
		Value interface{}
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

// Send sends metrics to statsd server
// Note: Sending error is ignored
func (c Client) Send(bucket string, metrics ...Metric) {
	if !c.opened {
		return
	}

	var t string

	for _, m := range metrics {
		switch m.Type {
		case Timer:
			t = "ms"
		case Counter:
			t = "c"
		case Gauge:
			t = "g"
		default:
			t = ""
			// Do nothing
			// Hopefully the caller will notice he/she's stats won't show up in Graphite or Datadog, etc
		}
		if t != "" {
			c.conn.Write([]byte(fmt.Sprintf("%s.%s.%s:%v|%s", c.prefix, bucket, m.Name, m.Value, t)))
		}
	}
}
