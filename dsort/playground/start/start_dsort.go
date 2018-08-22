package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/dsort"
	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	ext               string
	bucket            string
	proxyURL          string
	inputTemplate     string
	outputTemplate    string
	outputShardSize   int64
	extractConcLimit  int
	createConcLimit   int
	memUsage          string
	metricRefreshTime int
)

func init() {
	flag.StringVar(&ext, "ext", ".tar", "extension for output shards (either `.tar`, `.tgz` or `.zip`)")
	flag.StringVar(&bucket, "bucket", "dsort-testing", "bucket where shards objects are stored")
	flag.StringVar(&proxyURL, "url", "http://localhost:8080", "proxy url to which requests will be made")
	flag.StringVar(&inputTemplate, "input", "shard-{0..10}", "name template for input shard")
	flag.StringVar(&outputTemplate, "output", "new-shard-{0000..1000}", "name template for output shard")
	flag.Int64Var(&outputShardSize, "size", 1024*1024*10, "size of output of shard")
	flag.IntVar(&extractConcLimit, "elimit", 20, "limits number of concurrent shards extracted")
	flag.IntVar(&createConcLimit, "climit", 20, "limits number of concurrent shards created")
	flag.StringVar(&memUsage, "mem", "60%", "limits maximum of total memory until extraction starts spilling data to the disk, can be expressed in format: 60% or 10GB")
	flag.IntVar(&metricRefreshTime, "refresh", 5, "metric refresh time (in seconds)")
	flag.Parse()
}

func main() {
	rs := dsort.RequestSpec{
		Bucket:           bucket,
		Extension:        ext,
		IntputFormat:     inputTemplate,
		OutputFormat:     outputTemplate,
		OutputShardSize:  outputShardSize,
		IsLocalBucket:    true,
		ExtractConcLimit: extractConcLimit,
		CreateConcLimit:  createConcLimit,
		MaxMemUsage:      memUsage,
	}
	dsortUUID, err := tutils.StartDSort(proxyURL, rs)
	if err != nil {
		glog.Fatal(err)
	}

	for {
		fmt.Print("\n---------------------------------------------------------\n")
		fmt.Print("---------------------------------------------------------\n")
		fmt.Print("---------------------------------------------------------\n")

		allMetrics, err := tutils.MetricsDSort(proxyURL, dsortUUID)
		if err != nil {
			glog.Fatal(err)
		}

		allFinished := true
		for _, metrics := range allMetrics {
			allFinished = allFinished && metrics.Creation.Finished

			if metrics.Aborted {
				glog.Fatal("dsort was aborted")
			}
		}

		b, err := json.MarshalIndent(allMetrics, "", "\t")
		if err != nil {
			glog.Fatal(err)
		}
		fmt.Printf("%s", string(b))
		if allFinished {
			break
		}

		time.Sleep(time.Second * time.Duration(metricRefreshTime))
	}

	fmt.Println("Distributed sort has finished!")
}
