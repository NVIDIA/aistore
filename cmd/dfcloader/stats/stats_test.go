package stats_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmd/dfcloader/stats"
)

func verify(t *testing.T, msg string, exp, act int64) {
	if exp != act {
		t.Fatalf("Error: %s, expected = %d, actual = %d", msg, exp, act)
	}
}

func TestStats(t *testing.T) {
	start := time.Now()
	s := stats.NewStats(start)

	// basic put
	s.AddPut(100, time.Duration(100*time.Millisecond))
	s.AddPut(200, time.Duration(20*time.Millisecond))
	s.AddErrPut()
	s.AddPut(50, time.Duration(30*time.Millisecond))

	verify(t, "Total puts", 3, s.TotalPuts())
	verify(t, "Total put bytes", 350, s.TotalPutBytes())
	verify(t, "Min put latency", 20000000, s.MinPutLatency())
	verify(t, "Avg put latency", 50000000, int64(s.AvgPutLatency()))
	verify(t, "Max put latency", 100000000, s.MaxPutLatency())
	verify(t, "Put throughput", 5, s.PutThroughput(start.Add(70*time.Second)))
	verify(t, "Failed puts", 1, s.TotalErrPuts())

	// basic get
	s.AddGet(100, time.Duration(100*time.Millisecond))
	s.AddGet(200, time.Duration(20*time.Millisecond))
	s.AddErrGet()
	s.AddGet(50, time.Duration(10*time.Millisecond))
	s.AddErrGet()
	s.AddGet(200, time.Duration(190*time.Millisecond))

	verify(t, "Total gets", 4, s.TotalGets())
	verify(t, "Total get bytes", 550, s.TotalGetBytes())
	verify(t, "Min get latency", 10000000, int64(s.MinGetLatency()))
	verify(t, "Avg get latency", 80000000, int64(s.AvgGetLatency()))
	verify(t, "Max get latency", 190000000, int64(s.MaxGetLatency()))
	verify(t, "Get throughput", 5, s.GetThroughput(start.Add(110*time.Second)))
	verify(t, "Failed gets", 2, s.TotalErrGets())

	// accumulate non empty stats on top of empty stats
	total := stats.NewStats(start)
	total.Aggregate(s)
	verify(t, "Total puts", 3, total.TotalPuts())
	verify(t, "Total put bytes", 350, total.TotalPutBytes())
	verify(t, "Min put latency", 20000000, total.MinPutLatency())
	verify(t, "Avg put latency", 50000000, int64(total.AvgPutLatency()))
	verify(t, "Max put latency", 100000000, total.MaxPutLatency())
	verify(t, "Put throughput", 5, total.PutThroughput(start.Add(70*time.Second)))
	verify(t, "Failed puts", 1, total.TotalErrPuts())

	// accumulate empty stats on top of non empty stats
	s = stats.NewStats(start)
	total.Aggregate(s)
	verify(t, "Min get latency", 10000000, int64(total.MinGetLatency()))
	verify(t, "Avg get latency", 80000000, int64(total.AvgGetLatency()))
	verify(t, "Max get latency", 190000000, int64(total.MaxGetLatency()))

	// accumulate non empty stats on top of non empty stats
	s.AddErrGet()
	s.AddErrPut()
	total.Aggregate(s)
	verify(t, "Failed puts", 2, total.TotalErrPuts())
	verify(t, "Failed gets", 3, total.TotalErrGets())

	s.AddGet(1000, time.Duration(5*time.Millisecond))
	s.AddPut(1, time.Duration(1000*time.Millisecond))
	total.Aggregate(s)
	verify(t, "Total puts", 4, total.TotalPuts())
	verify(t, "Total put bytes", 351, total.TotalPutBytes())
	verify(t, "Min put latency", 20000000, total.MinPutLatency())
	verify(t, "Avg put latency", 287500000, int64(total.AvgPutLatency()))
	verify(t, "Max put latency", 1000000000, total.MaxPutLatency())
	verify(t, "Put throughput", 5, total.PutThroughput(start.Add(70*time.Second)))
	verify(t, "Total gets", 5, total.TotalGets())
	verify(t, "Total get bytes", 1550, total.TotalGetBytes())
	verify(t, "Min get latency", 5000000, int64(total.MinGetLatency()))
	verify(t, "Avg get latency", 65000000, int64(total.AvgGetLatency()))
	verify(t, "Max get latency", 190000000, int64(total.MaxGetLatency()))
	verify(t, "Get throughput", 14, total.GetThroughput(start.Add(110*time.Second)))
}
