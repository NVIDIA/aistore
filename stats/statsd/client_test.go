/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package statsd_test

import (
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/stats/statsd"
)

var (
	port     = 10001
	protocol = "udp"
	prefix   = "test"
	self     = "localhost"
)

// checkMsg reads one UDP message and verifies it matches the expected string
func checkMsg(t *testing.T, s *net.UDPConn, exp string) {
	err := s.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err != nil {
		t.Fatal("Failed to set server deadline", err)
	}

	buf := make([]byte, 256)
	n, _, err := s.ReadFromUDP(buf)
	if err != nil {
		t.Fatal("Failed to receive", err)
	}

	if exp != string(buf[:n]) {
		t.Fatal(fmt.Sprintf("Wrong data, exp = %s, act = %s", exp, string(buf[:n])), nil)
	}
}

// startServer resolves the UDP address for server and starts listening on the port
func startServer() (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr(protocol, fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	s, err := net.ListenUDP(protocol, addr)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func TestClient(t *testing.T) {
	s, err := startServer()
	if err != nil {
		t.Fatal("Failed to start server", err)
	}
	defer s.Close()

	c, err := statsd.New(self, port, prefix)
	if err != nil {
		t.Fatal("Failed to create client", err)
	}
	defer c.Close()

	c.Send("timer",
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  "timer",
			Value: 123,
		})
	checkMsg(t, s, "test.timer.timer:123|ms")

	c.Send("three",
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  "timer",
			Value: 123,
		},
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "counter",
			Value: 456,
		},
		statsd.Metric{
			Type:  statsd.Gauge,
			Name:  "gauge.onemore",
			Value: 789,
		},
	)
	checkMsg(t, s, "test.three.timer:123|ms")
	checkMsg(t, s, "test.three.counter:456|c")
	checkMsg(t, s, "test.three.gauge.onemore:789|g")
}

// server is the UDP server routine used for testing
// it receives UDP requests and throw them away
// stops when a message is received from the stop channel
func server(c *net.UDPConn, stop <-chan bool) {
	buf := make([]byte, 256)
	for {
		err := c.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err != nil {
			fmt.Printf("Failed to set read deadline: %v", err)
		}
		_, addr, err := c.ReadFromUDP(buf)
		if err, ok := err.(net.Error); ok && !err.Timeout() {
			fmt.Println("Server receive failed", addr, err, ok)
		}

		select {
		case <-stop:
			return
		default:
		}
	}
}

func BenchmarkSend(b *testing.B) {
	s, err := startServer()
	if err != nil {
		b.Fatal("Failed to start server", err)
	}
	defer s.Close()

	stop := make(chan bool)
	go server(s, stop)

	c, err := statsd.New(self, port, prefix)
	if err != nil {
		b.Fatal("Failed to create client", err)
	}
	defer c.Close()

	for i := 0; i < b.N; i++ {
		c.Send("timer",
			statsd.Metric{
				Type:  statsd.Timer,
				Name:  "test",
				Value: rand.Float64(),
			})
	}

	stop <- true
}

func BenchmarkSendParallel(b *testing.B) {
	s, err := startServer()
	if err != nil {
		b.Fatal("Failed to start server", err)
	}
	defer s.Close()

	stop := make(chan bool)
	go server(s, stop)

	c, err := statsd.New(self, port, prefix)
	if err != nil {
		b.Fatal("Failed to create client", err)
	}
	defer c.Close()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Send("timer",
				statsd.Metric{
					Type:  statsd.Timer,
					Name:  "test",
					Value: rand.Float64(),
				})
		}
	})

	stop <- true
}
