package stats

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

//This is intended to summarize primitives within a recipe
type RecipeStats struct {
	//Report package assumes responsibility for filling these
	RecipeName string    `json:"recipe_name"`
	RecipeNum  int       `json:"recipe_num"`
	OpType     string    `json:"type"`
	BeginTime  time.Time `json:"begin_time"`
	EndTime    time.Time `json:"end_time"`

	latencyTotal time.Duration
	requestCount int64
	totalSize    int64 // bytes
	duration     time.Duration
	errorsCount  int64

	numFatal int
}

func (rs *RecipeStats) Add(p *PrimitiveStat) {
	if p.Fatal {
		rs.numFatal++
		return
	}
	rs.latencyTotal += time.Duration(int64(p.RequestCount) * int64(p.Latency))
	rs.requestCount += p.RequestCount
	rs.totalSize += p.TotalSize
	rs.errorsCount += p.ErrorsCount

	rs.duration += p.Duration
}

func (rs *RecipeStats) HasData() bool {
	return rs.requestCount > 0
}

func (rs RecipeStats) writeHeadings(f *os.File) {
	f.WriteString(
		strings.Join(
			[]string{
				"Begin Time",
				"End Time",
				"Recipe Name",
				"Recipe Num",
				"OpType",

				"Avg Latency (s)",
				"Throughput (byte/s)",

				"Num Errors",
				"Num Fatal",
			},
			","))

	f.WriteString("\n")
}

func (rs RecipeStats) writeStat(f *os.File) {

	safeLatency := ""
	if rs.requestCount > 0 {
		safeLatency = fmt.Sprintf("%f", rs.latencyTotal.Seconds()/float64(rs.requestCount))
	}
	safeThroughput := ""
	if rs.duration.Seconds() > 0 {
		safeThroughput = fmt.Sprintf("%f", float64(rs.totalSize)/rs.duration.Seconds())
	}
	safeErrorCount := ""
	if rs.errorsCount > 0 {
		safeErrorCount = fmt.Sprintf("%d", rs.errorsCount)
	}
	safeNumFatal := ""
	if rs.numFatal > 0 {
		safeErrorCount = strconv.Itoa(rs.numFatal)
	}

	f.WriteString(
		strings.Join(
			[]string{
				rs.BeginTime.Format(csvTimeFormat),
				rs.EndTime.Format(csvTimeFormat),
				rs.RecipeName,
				strconv.Itoa(rs.RecipeNum),
				rs.OpType,

				safeLatency,
				safeThroughput,

				safeErrorCount,
				safeNumFatal,
			},
			","))

	f.WriteString("\n")
}
