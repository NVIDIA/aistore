// Package aisloader
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */

package aisloader

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

var examples = `# 1. Cleanup (i.e., destroy) an existing bucket:
     $ aisloader -bucket=ais://abc -duration 0s -totalputsize=0 -cleanup=true
     $ aisloader -bucket=mybucket -provider=aws -cleanup=true -duration 0s -totalputsize=0
# 2. Timed 100% PUT via 8 parallel workers into an ais bucket (that may or may not exists) with
     complete cleanup upon termination (NOTE: cleanup involves emptying the specified bucket):
     $ aisloader -bucket=ais://abc -duration 30s -numworkers=8 -minsize=1K -maxsize=1K -pctput=100 --cleanup=true
# 3. Timed (for 1h) 100% GET from an existing AWS S3 bucket, no cleanup:
     $ aisloader -bucket=nvaws -duration 1h -numworkers=30 -pctput=0 -provider=aws -cleanup=false
# or, same:
     $ aisloader -bucket=s3://nvaws -duration 1h -numworkers=30 -pctput=0 -cleanup=false

# 4. Mixed 30%/70% PUT and GET of variable-size objects to/from an AWS S3 bucket.
#    PUT will generate random object names and the duration is limited only by the total size (10GB).
#    Cleanup enabled - upon completion all generated objects and the bucket itself will be deleted:
     $ aisloader -bucket=s3://nvaws -duration 0s -cleanup=true -numworkers=3 -minsize=1024 -maxsize=1MB -pctput=30 -totalputsize=10G
# 5. PUT 1GB total into an ais bucket with cleanup disabled, object size = 1MB, duration unlimited:
     $ aisloader -bucket=ais://abc -cleanup=false -totalputsize=1G -duration=0 -minsize=1MB -maxsize=1MB -numworkers=8 -pctput=100
# 6. 100% GET from an ais bucket (no cleanup):
     $ aisloader -bucket=ais://abc -duration 15s -numworkers=3 -pctput=0 -cleanup=false
# or, same:
     $ aisloader -bucket=abc -provider=ais -duration 5s -numworkers=3 -pctput=0 -cleanup=false

# 7. PUT 2000 objects named as 'aisloader/hex({0..2000}{loaderid})', cleanup upon exit:
     $ aisloader -bucket=ais://abc -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -objNamePrefix="aisloader" -cleanup=true
# 8. Use random object names and loaderID to report statistics:
     $ aisloader -loaderid=10
# 9. PUT objects with random name generation being based on the specified loaderID and the total number of concurrent aisloaders:
     $ aisloader -loaderid=10 -loadernum=20
# 10. Same as above except that loaderID is computed by the aisloader as hash(loaderstring) & 0xff:
     $ aisloader -loaderid=loaderstring -loaderidhashlen=8
# 11. Print loaderID and exit (all 3 examples below) with the resulting loaderID commented on the right:",
     $ aisloader -getloaderid 			# 0x0
     $ aisloader -loaderid=10 -getloaderid	# 0xa
     $ aisloader -loaderid=loaderstring -loaderidhashlen=8 -getloaderid	# 0xdb
# 12. Timed 100% GET _directly_ from S3 bucket (notice '-s3endpoint' command line):
     $ aisloader -bucket=s3://xyz -cleanup=false -numworkers=8 -pctput=0 -duration=10m -s3endpoint=https://s3.amazonaws.com
# 13. PUT approx. 8000 files into s3 bucket directly, skip printing usage and defaults (NOTE: aistore is not being used):
     $ aisloader -bucket=s3://xyz -cleanup=false -minsize=16B -maxsize=16B -numworkers=8 -pctput=100 -totalputsize=128k -s3endpoint=https://s3.amazonaws.com -quiet
`

const readme = cmn.GitHubHome + "/blob/main/docs/howto_benchmark.md"

func printUsage(f *flag.FlagSet) {
	fmt.Printf("aisloader v%s (build %s)\n", _version, _buildtime)
	fmt.Println("\nAbout")
	fmt.Println("=====")
	fmt.Println("AIS Loader (aisloader) is a benchmarking tool to measure AIStore performance.")
	fmt.Println("It's a load generator that has been developed to benchmark and stress-test AIStore")
	fmt.Println("but can be easily extended to benchmark any S3-compatible backend.")
	fmt.Println("For usage, run: `aisloader`, or `aisloader usage`, or `aisloader --help`.")
	fmt.Println("Further details at " + readme)

	fmt.Println("\nCommand-line options")
	fmt.Println("====================")
	f.PrintDefaults()
	fmt.Println()

	fmt.Println("Examples")
	fmt.Println("========")
	fmt.Print(examples)
}

// prettyNumber converts a number to format like 1,234,567
func prettyNumber(n int64) string {
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}
	return fmt.Sprintf("%s,%03d", prettyNumber(n/1000), n%1000)
}

// prettyBytes converts number of bytes to something like 4.7G, 2.8K, etc
func prettyBytes(n int64) string {
	if n <= 0 { // process special case that B2S do not cover
		return "-"
	}
	return cos.ToSizeIEC(n, 1)
}

func prettySpeed(n int64) string {
	if n <= 0 {
		return "-"
	}
	return cos.ToSizeIEC(n, 2) + "/s"
}

// prettyDuration converts an integer representing a time in nano second to a string
func prettyDuration(t int64) string {
	d := time.Duration(t).String()
	i := strings.Index(d, ".")
	if i < 0 {
		return d
	}
	out := make([]byte, i+1, 32)
	copy(out, d[0:i+1])
	for j := i + 1; j < len(d); j++ {
		if d[j] > '9' || d[j] < '0' {
			out = append(out, d[j])
		} else if j < i+4 {
			out = append(out, d[j])
		}
	}
	return string(out)
}

// prettyLatency combines three latency min, avg and max into a string
func prettyLatency(min, avg, max int64) string {
	return fmt.Sprintf("%-11s%-11s%-11s", prettyDuration(min), prettyDuration(avg), prettyDuration(max))
}

func now() string {
	return time.Now().Format(cos.StampSec)
}

func preWriteStats(to io.Writer, jsonFormat bool) {
	if !jsonFormat {
		fmt.Fprintln(to)
		fmt.Fprintf(to, statsPrintHeader,
			"Time", "OP", "Count", "Size (Total)", "Latency (min, avg, max)", "Throughput (Avg)", "Errors (Total)")
	} else {
		fmt.Fprint(to, "[")
	}
}

func postWriteStats(to io.Writer, jsonFormat bool) {
	if jsonFormat {
		fmt.Fprintln(to)
		fmt.Fprintln(to, "]")
	}
}

func finalizeStats(to io.Writer) {
	accumulatedStats.aggregate(&intervalStats)
	writeStats(to, runParams.jsonFormat, true /* final */, &intervalStats, &accumulatedStats)
	postWriteStats(to, runParams.jsonFormat)

	// reset gauges, otherwise they would stay at last send value
	stats.ResetMetricsGauges(statsdC)
}

func writeFinalStats(to io.Writer, jsonFormat bool, s *sts) {
	if !jsonFormat {
		writeHumanReadibleFinalStats(to, s)
	} else {
		writeStatsJSON(to, s, false)
	}
}

func writeIntervalStats(to io.Writer, jsonFormat bool, s, t *sts) {
	if !jsonFormat {
		writeHumanReadibleIntervalStats(to, s, t)
	} else {
		writeStatsJSON(to, s)
	}
}

func jsonStatsFromReq(r stats.HTTPReq) *jsonStats {
	jStats := &jsonStats{
		Cnt:        r.Total(),
		Bytes:      r.TotalBytes(),
		Start:      r.Start(),
		Duration:   time.Since(r.Start()),
		Errs:       r.TotalErrs(),
		Latency:    r.AvgLatency(),
		MinLatency: r.MinLatency(),
		MaxLatency: r.MaxLatency(),
		Throughput: r.Throughput(r.Start(), time.Now()),
	}

	return jStats
}

func writeStatsJSON(to io.Writer, s *sts, withcomma ...bool) {
	jStats := struct {
		Get *jsonStats `json:"get"`
		Put *jsonStats `json:"put"`
		Cfg *jsonStats `json:"cfg"`
	}{
		Get: jsonStatsFromReq(s.get),
		Put: jsonStatsFromReq(s.put),
		Cfg: jsonStatsFromReq(s.getConfig),
	}

	jsonOutput, err := json.MarshalIndent(jStats, "", "  ")
	cos.AssertNoErr(err)
	fmt.Fprintf(to, "\n%s", string(jsonOutput))
	// print comma by default
	if len(withcomma) == 0 || withcomma[0] {
		fmt.Fprint(to, ",")
	}
}

func fprintf(w io.Writer, format string, a ...any) {
	_, err := fmt.Fprintf(w, format, a...)
	debug.AssertNoErr(err)
}

func writeHumanReadibleIntervalStats(to io.Writer, s, t *sts) {
	p := fprintf
	pn := prettyNumber
	pb := prettyBytes
	ps := prettySpeed
	pl := prettyLatency
	pt := now

	workOrderResLen := int64(len(resCh))
	// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
	errs := "-"
	if t.put.TotalErrs() != 0 {
		errs = pn(s.put.TotalErrs()) + " (" + pn(t.put.TotalErrs()) + ")"
	}
	if s.put.Total() != 0 {
		p(to, statsPrintHeader, pt(), "PUT",
			pn(s.put.Total())+" ("+pn(t.put.Total())+" "+pn(putPending)+" "+pn(workOrderResLen)+")",
			pb(s.put.TotalBytes())+" ("+pb(t.put.TotalBytes())+")",
			pl(s.put.MinLatency(), s.put.AvgLatency(), s.put.MaxLatency()),
			ps(s.put.Throughput(s.put.Start(), time.Now()))+" ("+ps(t.put.Throughput(t.put.Start(), time.Now()))+")",
			errs)
	}
	errs = "-"
	if t.get.TotalErrs() != 0 {
		errs = pn(s.get.TotalErrs()) + " (" + pn(t.get.TotalErrs()) + ")"
	}
	if s.get.Total() != 0 {
		p(to, statsPrintHeader, pt(), "GET",
			pn(s.get.Total())+" ("+pn(t.get.Total())+" "+pn(getPending)+" "+pn(workOrderResLen)+")",
			pb(s.get.TotalBytes())+" ("+pb(t.get.TotalBytes())+")",
			pl(s.get.MinLatency(), s.get.AvgLatency(), s.get.MaxLatency()),
			ps(s.get.Throughput(s.get.Start(), time.Now()))+" ("+ps(t.get.Throughput(t.get.Start(), time.Now()))+")",
			errs)
	}
	if s.getConfig.Total() != 0 {
		p(to, statsPrintHeader, pt(), "CFG",
			pn(s.getConfig.Total())+" ("+pn(t.getConfig.Total())+")",
			pb(s.getConfig.TotalBytes())+" ("+pb(t.getConfig.TotalBytes())+")",
			pl(s.getConfig.MinLatency(), s.getConfig.AvgLatency(), s.getConfig.MaxLatency()),
			ps(s.getConfig.Throughput(s.getConfig.Start(), time.Now()))+" ("+ps(t.getConfig.Throughput(t.getConfig.Start(), time.Now()))+")",
			pn(s.getConfig.TotalErrs())+" ("+pn(t.getConfig.TotalErrs())+")")
	}
}

func writeHumanReadibleFinalStats(to io.Writer, t *sts) {
	p := fprintf
	pn := prettyNumber
	pb := prettyBytes
	ps := prettySpeed
	pl := prettyLatency
	pt := now
	preWriteStats(to, false)

	sput := &t.put
	if sput.Total() > 0 {
		p(to, statsPrintHeader, pt(), "PUT",
			pn(sput.Total()),
			pb(sput.TotalBytes()),
			pl(sput.MinLatency(), sput.AvgLatency(), sput.MaxLatency()),
			ps(sput.Throughput(sput.Start(), time.Now())),
			pn(sput.TotalErrs()))
	}
	sget := &t.get
	if sget.Total() > 0 {
		p(to, statsPrintHeader, pt(), "GET",
			pn(sget.Total()),
			pb(sget.TotalBytes()),
			pl(sget.MinLatency(), sget.AvgLatency(), sget.MaxLatency()),
			ps(sget.Throughput(sget.Start(), time.Now())),
			pn(sget.TotalErrs()))
	}
	sconfig := &t.getConfig
	if sconfig.Total() > 0 {
		p(to, statsPrintHeader, pt(), "CFG",
			pn(sconfig.Total()),
			pb(sconfig.TotalBytes()),
			pl(sconfig.MinLatency(), sconfig.AvgLatency(), sconfig.MaxLatency()),
			pb(sconfig.Throughput(sconfig.Start(), time.Now())),
			pn(sconfig.TotalErrs()))
	}
}

// writeStatus writes stats to the writter.
// if final = true, writes the total; otherwise writes the interval stats
func writeStats(to io.Writer, jsonFormat, final bool, s, t *sts) {
	if final {
		writeFinalStats(to, jsonFormat, t)
	} else {
		// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
		writeIntervalStats(to, jsonFormat, s, t)
	}
}

// printRunParams show run parameters in json format
func printRunParams(p *params) {
	var d = p.duration.String()
	if p.duration.Val == time.Duration(math.MaxInt64) {
		d = "-"
	}
	b, err := jsoniter.MarshalIndent(struct {
		Seed          int64  `json:"seed,string"`
		URL           string `json:"proxy"`
		Bucket        string `json:"bucket"`
		Provider      string `json:"provider"`
		Namespace     string `json:"namespace"`
		Duration      string `json:"duration"`
		MaxPutBytes   int64  `json:"PUT upper bound,string"`
		PutPct        int    `json:"% PUT"`
		MinSize       int64  `json:"minimum object size (bytes)"`
		MaxSize       int64  `json:"maximum object size (bytes)"`
		NumWorkers    int    `json:"# workers"`
		StatsInterval string `json:"stats interval"`
		Backing       string `json:"backed by"`
		Cleanup       bool   `json:"cleanup"`
	}{
		Seed:          p.seed,
		URL:           p.proxyURL,
		Bucket:        p.bck.Name,
		Provider:      p.bck.Provider,
		Namespace:     p.bck.Ns.String(),
		Duration:      d,
		MaxPutBytes:   p.putSizeUpperBound,
		PutPct:        p.putPct,
		MinSize:       p.minSize,
		MaxSize:       p.maxSize,
		NumWorkers:    p.numWorkers,
		StatsInterval: (time.Duration(runParams.statsShowInterval) * time.Second).String(),
		Backing:       p.readerType,
		Cleanup:       p.cleanUp.Val,
	}, "", "   ")
	cos.AssertNoErr(err)

	fmt.Printf("Runtime configuration:\n%s\n\n", string(b))
}
