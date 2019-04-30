package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/tutils"
)

var (
	ext               string
	bucket            string
	outputBucket      string
	description       string
	proxyURL          string
	inputTemplate     string
	outputTemplate    string
	outputShardSize   int64
	algoKind          string
	algoDesc          bool
	algoSeed          string
	extractConcLimit  int
	createConcLimit   int
	memUsage          string
	metricRefreshTime int
	logPath           string

	err       error
	dsortUUID string
	sigCh     = make(chan os.Signal, 1)
)

func init() {
	flag.StringVar(&ext, "ext", ".tar", "extension for input and output shards (either `.tar`, `.tgz` or `.zip`)")
	flag.StringVar(&bucket, "bucket", "dsort-testing", "bucket where shards objects are stored")
	flag.StringVar(&description, "description", "", "description of dsort job")
	flag.StringVar(&outputBucket, "obucket", "", "bucket where new output shards will be saved")
	flag.StringVar(&proxyURL, "url", "http://localhost:8080", "proxy url to which requests will be made")
	flag.StringVar(&inputTemplate, "input", "shard-{0..9}", "name template for input shard")
	flag.StringVar(&outputTemplate, "output", "new-shard-{0000..1000}", "name template for output shard")
	flag.Int64Var(&outputShardSize, "size", 1024*1024*10, "size of output of shard")
	flag.StringVar(&algoKind, "akind", "alphanumeric", "kind of algorithm used to sort data")
	flag.BoolVar(&algoDesc, "adesc", false, "determines whether data should be sorted by algorithm in descending or ascending")
	flag.StringVar(&algoSeed, "aseed", "", "seed used for random shuffling algorithm")
	flag.IntVar(&extractConcLimit, "elimit", 0, "limits number of concurrent shards extracted")
	flag.IntVar(&createConcLimit, "climit", 0, "limits number of concurrent shards created")
	flag.StringVar(&memUsage, "mem", "60%", "limits maximum of total memory until extraction starts spilling data to the disk, can be expressed in format: 60% or 10GB")
	flag.IntVar(&metricRefreshTime, "refresh", 5, "metric refresh time (in seconds)")
	flag.StringVar(&logPath, "log", "", "path to file where the metrics will be stored")
	flag.Parse()
}

func handleKillSignal(bp *api.BaseParams) {
	_, ok := <-sigCh
	if ok {
		api.AbortDSort(bp, dsortUUID)
	}
}

func main() {
	var (
		w = os.Stdout
	)

	if logPath != "" {
		file, err := cmn.CreateFile(logPath)
		if err != nil {
			glog.Fatal(err)
		}
		defer file.Close()
		w = file
	}

	baseParams := tutils.BaseAPIParams(proxyURL)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go handleKillSignal(baseParams)

	rs := dsort.RequestSpec{
		ProcDescription: description,
		Bucket:          bucket,
		OutputBucket:    outputBucket,
		Extension:       ext,
		IntputFormat:    inputTemplate,
		OutputFormat:    outputTemplate,
		OutputShardSize: outputShardSize,
		BckProvider:     cmn.LocalBs,
		Algorithm: dsort.SortAlgorithm{
			Kind:       algoKind,
			Decreasing: algoDesc,
			Seed:       algoSeed,
		},
		ExtractConcLimit: extractConcLimit,
		CreateConcLimit:  createConcLimit,
		MaxMemUsage:      memUsage,
		ExtendedMetrics:  true,
	}
	dsortUUID, err = api.StartDSort(baseParams, rs)
	if err != nil {
		glog.Fatal(err)
	}

	fmt.Printf("DSort job id: %s\n", dsortUUID)

	for {
		allMetrics, err := api.MetricsDSort(baseParams, dsortUUID)
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
		fmt.Fprintf(w, "%s\n", string(b))
		if allFinished {
			break
		}

		time.Sleep(time.Second * time.Duration(metricRefreshTime))
	}

	fmt.Println("Distributed sort has finished!")
	close(sigCh)
}
