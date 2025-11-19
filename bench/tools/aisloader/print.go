// Package aisloader
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

	"github.com/NVIDIA/aistore/bench/tools/aisloader/namegetter"
	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/tools/readers"

	jsoniter "github.com/json-iterator/go"
)

const (
	opLabelPut = "PUT"
	opLabelGet = "GET"
	opLabelMPU = "MPU"
	opLabelGBT = "GBT"
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
# 13. PUT approx. 8000 files into s3 bucket directly, skip printing usage and defaults (NOTE: AIStore is not being used):
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
	return cos.IEC(n, 1)
}

func prettySpeed(n int64) string {
	if n <= 0 {
		return "-"
	}
	return cos.IEC(n, 2) + "/s"
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
func prettyLatency(minL, avgL, maxL int64) string {
	return fmt.Sprintf("%-11s%-11s%-11s", prettyDuration(minL), prettyDuration(avgL), prettyDuration(maxL))
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
		Get      *jsonStats `json:"get"`
		Put      *jsonStats `json:"put"`
		GetBatch *jsonStats `json:"get_batch"`
	}{
		Get:      jsonStatsFromReq(s.get),
		Put:      jsonStatsFromReq(s.put),
		GetBatch: jsonStatsFromReq(s.getBatch),
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

//nolint:dupl // format PUT and GET stats: minor field differences justify seemingly duplicated code blocks
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
		p(to, statsPrintHeader, pt(), opLabelPut,
			pn(s.put.Total())+" ("+pn(t.put.Total())+" "+pn(putPending)+" "+pn(workOrderResLen)+")",
			pb(s.put.TotalBytes())+" ("+pb(t.put.TotalBytes())+")",
			pl(s.put.MinLatency(), s.put.AvgLatency(), s.put.MaxLatency()),
			ps(s.put.Throughput(s.put.Start(), time.Now()))+" ("+ps(t.put.Throughput(t.put.Start(), time.Now()))+")",
			errs)
	}
	errs = "-"
	if t.putMPU.TotalErrs() != 0 {
		errs = pn(s.putMPU.TotalErrs()) + " (" + pn(t.putMPU.TotalErrs()) + ")"
	}
	if s.putMPU.Total() != 0 {
		p(to, statsPrintHeader, pt(), opLabelMPU,
			pn(s.putMPU.Total())+" ("+pn(t.putMPU.Total())+")",
			pb(s.putMPU.TotalBytes())+" ("+pb(t.putMPU.TotalBytes())+")",
			pl(s.putMPU.MinLatency(), s.putMPU.AvgLatency(), s.putMPU.MaxLatency()),
			ps(s.putMPU.Throughput(s.putMPU.Start(), time.Now()))+" ("+ps(t.putMPU.Throughput(t.putMPU.Start(), time.Now()))+")",
			errs)
	}
	errs = "-"
	if t.get.TotalErrs() != 0 {
		errs = pn(s.get.TotalErrs()) + " (" + pn(t.get.TotalErrs()) + ")"
	}
	if s.get.Total() != 0 {
		p(to, statsPrintHeader, pt(), opLabelGet,
			pn(s.get.Total())+" ("+pn(t.get.Total())+" "+pn(getPending)+" "+pn(workOrderResLen)+")",
			pb(s.get.TotalBytes())+" ("+pb(t.get.TotalBytes())+")",
			pl(s.get.MinLatency(), s.get.AvgLatency(), s.get.MaxLatency()),
			ps(s.get.Throughput(s.get.Start(), time.Now()))+" ("+ps(t.get.Throughput(t.get.Start(), time.Now()))+")",
			errs)
	}
	errs = "-"
	if t.getBatch.TotalErrs() != 0 {
		errs = pn(s.getBatch.TotalErrs()) + " (" + pn(t.getBatch.TotalErrs()) + ")"
	}
	if s.getBatch.Total() != 0 {
		p(to, statsPrintHeader, pt(), opLabelGBT,
			pn(s.getBatch.Total())+" ("+pn(t.getBatch.Total())+")",
			pb(s.getBatch.TotalBytes())+" ("+pb(t.getBatch.TotalBytes())+")",
			pl(s.getBatch.MinLatency(), s.getBatch.AvgLatency(), s.getBatch.MaxLatency()),
			ps(s.getBatch.Throughput(s.getBatch.Start(), time.Now()))+" ("+ps(t.getBatch.Throughput(t.getBatch.Start(), time.Now()))+")",
			errs)
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
		p(to, statsPrintHeader, pt(), opLabelPut,
			pn(sput.Total()),
			pb(sput.TotalBytes()),
			pl(sput.MinLatency(), sput.AvgLatency(), sput.MaxLatency()),
			ps(sput.Throughput(sput.Start(), time.Now())),
			pn(sput.TotalErrs()))
	}
	smpu := &t.putMPU
	if smpu.Total() > 0 {
		p(to, statsPrintHeader, pt(), opLabelMPU,
			pn(smpu.Total()),
			pb(smpu.TotalBytes()),
			pl(smpu.MinLatency(), smpu.AvgLatency(), smpu.MaxLatency()),
			ps(smpu.Throughput(smpu.Start(), time.Now())),
			pn(smpu.TotalErrs()))
	}
	sget := &t.get
	if sget.Total() > 0 {
		p(to, statsPrintHeader, pt(), opLabelGet,
			pn(sget.Total()),
			pb(sget.TotalBytes()),
			pl(sget.MinLatency(), sget.AvgLatency(), sget.MaxLatency()),
			ps(sget.Throughput(sget.Start(), time.Now())),
			pn(sget.TotalErrs()))
	}
	sgbatch := &t.getBatch
	if sgbatch.Total() > 0 {
		p(to, statsPrintHeader, pt(), opLabelGBT,
			pn(sgbatch.Total()),
			pb(sgbatch.TotalBytes()),
			pl(sgbatch.MinLatency(), sgbatch.AvgLatency(), sgbatch.MaxLatency()),
			ps(sgbatch.Throughput(sgbatch.Start(), time.Now())),
			pn(sgbatch.TotalErrs()))
	}
}

// writeStatus writes stats to the specified io.Writer.
// if final = true, writes the total; otherwise writes the interval stats
func writeStats(to io.Writer, jsonFormat, final bool, s, t *sts) {
	if final {
		writeFinalStats(to, jsonFormat, t)
	} else {
		// show interval stats; some fields are shown of both interval and total, for example, gets, puts, etc
		writeIntervalStats(to, jsonFormat, s, t)
	}
}

// when starting to run show essential parameters in JSON format
type (
	ppEssential struct {
		URL           string  `json:"proxy"`
		Bucket        string  `json:"bucket"`
		Duration      string  `json:"duration"`
		NumWorkers    int     `json:"# workers"`
		StatsInterval string  `json:"stats interval"`
		PutPct        int     `json:"% PUT,omitempty"`
		UpdatePct     int     `json:"% Update Existing,omitempty"`
		MultipartPct  int     `json:"% Multipart PUT,omitempty"`
		GetBatchSize  int     `json:"GET(batch): batch size,omitempty"`
		MinSize       int64   `json:"minimum object size (bytes),omitempty"`
		MaxSize       int64   `json:"maximum object size (bytes),omitempty"`
		MaxPutBytes   int64   `json:"PUT upper bound,string,omitempty"`
		Arch          *ppArch `json:"archive (shards),omitempty"`
		NameGetter    string  `json:"name-getter"`
		ReaderType    string  `json:"reader-type,omitempty"`
		Cleanup       bool    `json:"cleanup"`
	}
	ppArch struct {
		Pct      int    `json:"% workload"`
		Format   string `json:"format"`
		Prefix   string `json:"prefix,omitempty"`
		NumFiles int    `json:"files per shard,omitempty"`
		MinSize  int64  `json:"minimum file size"`
		MaxSize  int64  `json:"maximum file size"`
	}
)

func printRunParams(p *params) {
	var arch *ppArch
	if p.archParams.pct != 0 {
		// temp readers.Arch to run Init() and get computed values
		// aisloader's default format: TAR
		mime := cos.NonZero(p.archParams.format, archive.ExtTar)
		tmpArch := &readers.Arch{
			Mime:    mime,
			Prefix:  p.archParams.prefix,
			MinSize: p.archParams.minSz,
			MaxSize: p.archParams.maxSz,
			Num:     p.archParams.numFiles,
		}
		err := tmpArch.Init(p.maxSize)
		cos.AssertNoErr(err)

		numf := cos.Ternary(tmpArch.Num == readers.DynamicNumFiles, 0, p.archParams.numFiles)
		arch = &ppArch{
			Pct:      p.archParams.pct,
			Format:   tmpArch.Mime,
			Prefix:   tmpArch.Prefix,
			NumFiles: numf,
			MinSize:  tmpArch.MinSize,
			MaxSize:  tmpArch.MaxSize,
		}
	}

	// omit PUT-only
	var (
		minsize, maxsize int64
		readerType       string
	)
	if p.putPct > 0 {
		minsize, maxsize = p.minSize, p.maxSize
		readerType = p.readerType
	}
	b, err := jsoniter.MarshalIndent(ppEssential{
		URL:           p.proxyURL,
		Bucket:        p.bck.Cname(""),
		Duration:      cos.Ternary(p.duration.Val == time.Duration(math.MaxInt64), "-", p.duration.String()),
		NumWorkers:    p.numWorkers,
		StatsInterval: (time.Duration(runParams.statsShowInterval) * time.Second).String(),
		PutPct:        p.putPct,
		UpdatePct:     p.updateExistingPct,
		MultipartPct:  p.multipartPct,
		GetBatchSize:  p.getBatchSize,
		MinSize:       minsize,
		MaxSize:       maxsize,
		MaxPutBytes:   p.putSizeUpperBound,
		Arch:          arch,
		NameGetter:    ngLabel(),
		ReaderType:    readerType,
		Cleanup:       p.cleanUp.Val,
	}, "", "   ")

	cos.AssertNoErr(err)
	fmt.Printf("Runtime configuration:\n%s\n\n", string(b))
}

func ngLabel() string {
	switch objnameGetter.(type) {
	case *namegetter.Random:
		return "random non-unique"
	case *namegetter.RandomUnique:
		return "random unique"
	case *namegetter.PermShuffle:
		return "unique sequential"
	case *namegetter.PermAffinePrime:
		return "unique epoch-based"
	default:
		s := fmt.Sprintf("%T", objnameGetter)
		debug.Assert(false, s)
		return s
	}
}
