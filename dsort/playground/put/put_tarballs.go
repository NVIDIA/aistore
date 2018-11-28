package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/dsort/playground/gen/util"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	bucket           string
	ext              string
	proxyURL         string
	concurrencyLimit int
	cleanup          bool

	dir string

	inMem             bool
	tarballCnt        int
	inputPrefix       string
	fileInTarballSize int
	fileInTarballCnt  int
	tarballSize       int

	client = &http.Client{}
)

func init() {
	flag.StringVar(&ext, "ext", ".tar", "extension for tarballs (either `.tar` or `.tgz`)")
	flag.StringVar(&bucket, "bucket", "dsort-testing", "bucket where shards will be put")
	flag.StringVar(&proxyURL, "url", "http://localhost:8080", "proxy url to which requests will be made")
	flag.IntVar(&concurrencyLimit, "conc", 100, "limits number of concurrent put requests (in-memory mode it also limits number of concurrent shards created)")
	flag.BoolVar(&cleanup, "cleanup", false, "when true the bucket will be deleted and created so all objects will be fresh")

	flag.BoolVar(&inMem, "inmem", true, "determines if tarballs should be created on the fly in the memory or are will be read from the folder")

	flag.StringVar(&dir, "dir", "data", "directory where the data will be created")

	flag.IntVar(&tarballCnt, "shards", 10, "number of shards to create")
	flag.StringVar(&inputPrefix, "iprefix", "shard-", "prefix of input shard")
	flag.IntVar(&fileInTarballSize, "fsize", 1024*1024, "single file size (in bytes) inside the shard")
	flag.IntVar(&fileInTarballCnt, "fcount", 10, "number of files inside single shard")

	flag.Parse()

	tarballSize = fileInTarballCnt * fileInTarballSize
}

func main() {
	baseParams := &api.BaseParams{
		Client: client,
		URL:    proxyURL,
	}
	exists, err := tutils.DoesLocalBucketExist(proxyURL, bucket)
	if err != nil {
		glog.Fatal("failed to check if local bucket exists")
	}

	if exists && cleanup {
		err := api.DestroyLocalBucket(baseParams, bucket)
		if err != nil {
			glog.Fatal("failed to remove local bucket")
		}
	}

	if !exists || cleanup {
		if err = api.CreateLocalBucket(baseParams, bucket); err != nil {
			glog.Fatal("failed to create local bucket")
		}
	}

	wg := &sync.WaitGroup{}
	sema := make(chan struct{}, concurrencyLimit)
	baseParams = tutils.BaseAPIParams(proxyURL)
	if inMem {
		numFilesDigits := len(strconv.Itoa(fileInTarballCnt))
		mem := &memsys.Mem2{}
		if err := mem.Init(false); err != nil {
			glog.Fatal(err)
		}

		for shardNum := 0; shardNum < tarballCnt; shardNum++ {
			sema <- struct{}{}
			wg.Add(1)
			go func(i int) {
				defer func() {
					<-sema
					wg.Done()
				}()
				name := fmt.Sprintf("%s%d%s", inputPrefix, i, ext)
				sgl := mem.NewSGL(int64(tarballSize))
				util.CreateTar(sgl, i*fileInTarballCnt, (i+1)*fileInTarballCnt, fileInTarballSize, numFilesDigits)

				reader, err := tutils.NewSGReader(sgl, 0, false)
				if err != nil {
					glog.Error(err)
				}
				if err := api.PutObject(baseParams, bucket, name, reader.XXHash(), reader); err != nil {
					glog.Error(err)
				}
				fmt.Printf("PUT: %s/%s\n", bucket, name)
				sgl.Free()
			}(shardNum)
		}
	} else {
		err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			if !strings.HasSuffix(path, ext) {
				return nil
			}

			reader, err := tutils.NewFileReaderFromFile(path, false)
			if err != nil {
				return err
			}

			wg.Add(1)
			go func() {
				sema <- struct{}{}
				if err := api.PutObject(baseParams, bucket, filepath.Base(path), reader.XXHash(), reader); err != nil {
					glog.Error(err)
				}
				<-sema
				wg.Done()
			}()
			return nil
		})

		if err != nil {
			glog.Error(err)
		}
	}

	wg.Wait()
}
