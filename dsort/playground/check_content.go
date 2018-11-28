package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"flag"
	"fmt"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	bucket   string
	ext      string
	proxyURL string
	shardCnt int
	prefix   string
)

func init() {
	flag.StringVar(&ext, "ext", ".tar", "extension for tarballs (either `.tar` or `.tgz`)")
	flag.StringVar(&bucket, "bucket", "dsort-testing", "bucket where data is located")
	flag.StringVar(&proxyURL, "url", "http://localhost:8080", "proxy url to which requests will be made")
	flag.IntVar(&shardCnt, "shards", 10, "number of shards to show")
	flag.StringVar(&prefix, "prefix", "shard-", "prefix of shard")
	flag.Parse()
}

func main() {
	for i := 0; i < shardCnt; i++ {
		b := bytes.NewBuffer(nil)
		w := bufio.NewWriter(b)
		s := fmt.Sprintf("%s%d%s", prefix, i, ext)
		baseParams := tutils.BaseAPIParams(proxyURL)
		options := api.GetObjectInput{
			Writer: w,
		}
		if _, err := api.GetObject(baseParams, bucket, s, options); err != nil {
			fmt.Printf("shard (%s) wasn't be able to be read, err: %v", s, err)
		}

		tr := tar.NewReader(b)
		fmt.Printf("shard: %s\n", s)
		i := 1
		for {
			h, err := tr.Next()
			if err != nil {
				break
			}

			fmt.Printf("\t%d:\tfilename: %s; size: %d\n", i, h.Name, h.Size)
			i++
		}

		fmt.Printf("\n\n")
	}
}
