package stats

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type PrimitiveStat struct {
	AISLoaderStat
	Fatal  bool   `json:"fatal"`
	OpType string `json:"op_type"`
	ID     string `json:"id"` //Soakprim assumes responsibility for filling

	RecipeName string `json:"recipe_name"` //Report package assumes responsibility for filling
	RecipeNum  int    `json:"recipe_num"`  //Report package assumes responsibility for filling
}

// AISLoaderStat is a response from AISLoader, keep json consistent with the `jsonStats` struct in AISLoader
type AISLoaderStat struct {
	LatencyMin   time.Duration `json:"min_latency"`
	Latency      time.Duration `json:"latency"` //Average
	LatencyMax   time.Duration `json:"max_latency"`
	Throughput   int64         `json:"throughput"` // bytes
	TotalSize    int64         `json:"bytes"`
	RequestCount int64         `json:"count"`
	ErrorsCount  int64         `json:"errors"`
	StartTime    time.Time     `json:"start_time"`
	Duration     time.Duration `json:"duration"`
}

func (ps *PrimitiveStat) writeHeadings(f *os.File) {
	f.WriteString(
		strings.Join(
			[]string{
				"Timestamp",
				"Recipe Name",
				"Recipe Num",
				"Primitive ID",
				"Get/Put",
				"Avg Latency (s)",
				"Throughput (byte/s)",
				"Error Count",
				"Fatal",
			},
			","))

	f.WriteString("\n")
}

func (ps *PrimitiveStat) writeStat(f *os.File) {
	if ps.Fatal {
		f.WriteString(
			strings.Join(
				[]string{
					ps.StartTime.Format(time.RFC3339Nano),
					ps.RecipeName,
					strconv.Itoa(ps.RecipeNum),
					ps.ID,
					"",
					"",
					"",
					"",
					"TRUE",
				},
				","))
		f.WriteString("\n")
		return
	}

	safeLatency := ""
	if ps.RequestCount > 0 {
		safeLatency = fmt.Sprintf("%f", ps.Latency.Seconds())
	}
	safeThroughput := ""
	if ps.Throughput > 0 {
		safeThroughput = fmt.Sprintf("%d", ps.Throughput)
	}
	safeErrorCount := ""
	if ps.ErrorsCount > 0 {
		safeErrorCount = fmt.Sprintf("%d", ps.ErrorsCount)
	}

	f.WriteString(
		strings.Join(
			[]string{
				ps.StartTime.Format(time.RFC3339Nano),
				ps.RecipeName,
				strconv.Itoa(ps.RecipeNum),
				ps.ID,

				ps.OpType,
				safeLatency,
				safeThroughput,

				safeErrorCount,

				"FALSE",
			},
			","))

	f.WriteString("\n")
}
