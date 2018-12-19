package dfc_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/dsort"
	"github.com/NVIDIA/dfcpub/tutils"
)

type dsortFramework struct {
	m *metadata

	inputPrefix  string
	outputPrefix string

	inputTempl        string
	outputTempl       string
	tarballCnt        int
	fileInTarballCnt  int
	fileInTarballSize int
	tarballSize       int
	outputShardCnt    int

	extension       string
	algorithm       string
	outputShardSize int64
	maxMemUsage     string
}

func (df *dsortFramework) init() {
	// Assumption is that all prefixes end with dash: "-"
	df.inputPrefix = df.inputTempl[:strings.Index(df.inputTempl, "-")+1]
	df.outputPrefix = df.outputTempl[:strings.Index(df.outputTempl, "-")+1]

	df.tarballSize = df.fileInTarballCnt * df.fileInTarballSize
	df.outputShardSize = int64(10 * df.fileInTarballCnt * df.fileInTarballSize)
	df.outputShardCnt = (df.tarballCnt * df.tarballSize) / int(df.outputShardSize)
}

func (df *dsortFramework) gen() dsort.RequestSpec {
	algorithm := dsort.SortAlgorithm{}
	if df.algorithm != "" {
		algorithm.Kind = df.algorithm
	}

	return dsort.RequestSpec{
		Bucket:           df.m.bucket,
		Extension:        df.extension,
		IntputFormat:     df.inputTempl,
		OutputFormat:     df.outputTempl,
		OutputShardSize:  df.outputShardSize,
		Algorithm:        algorithm,
		IsLocalBucket:    true,
		ExtractConcLimit: 10,
		CreateConcLimit:  10,
		MaxMemUsage:      df.maxMemUsage,
	}
}

func (df *dsortFramework) createInputShards() {
	const tmpDir = "/tmp"
	var (
		err error
	)
	tutils.Logln("creating tarballs...")
	wg := &sync.WaitGroup{}
	errCh := make(chan error, df.tarballCnt)
	for i := 0; i < df.tarballCnt; i++ {
		path := fmt.Sprintf("%s/%s%d", tmpDir, df.inputPrefix, i)
		if df.extension == ".tar" {
			err = tutils.CreateTarWithRandomFiles(path, false, df.fileInTarballCnt, df.fileInTarballSize)
		} else if df.extension == ".tar.gz" {
			err = tutils.CreateTarWithRandomFiles(path, true, df.fileInTarballCnt, df.fileInTarballSize)
		} else if df.extension == ".zip" {
			err = tutils.CreateZipWithRandomFiles(path, df.fileInTarballCnt, df.fileInTarballSize)
		}
		tutils.CheckFatal(err, df.m.t)

		fqn := path + df.extension
		defer os.Remove(fqn)

		reader, err := tutils.NewFileReaderFromFile(fqn, false)
		tutils.CheckFatal(err, df.m.t)

		wg.Add(1)
		go tutils.PutAsync(wg, df.m.proxyURL, df.m.bucket, filepath.Base(fqn), reader, errCh)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		tutils.CheckFatal(err, df.m.t)
	}
	tutils.Logln("done creating tarballs")
}

func (df *dsortFramework) checkOutputShards(zeros int) {
	var (
		files []os.FileInfo
	)

	tutils.Logln("checking if files are sorted...")
	lastName := ""

	gzipped := false
	if df.extension != ".tar" {
		gzipped = true
	}

	inversions := 0
	for i := 0; i < df.outputShardCnt; i++ {
		shardName := fmt.Sprintf("%s%0*d%s", df.outputPrefix, zeros, i, df.extension)
		var buffer bytes.Buffer
		getOptions := api.GetObjectInput{
			Writer: &buffer,
		}
		_, err := api.GetObject(tutils.HTTPClient, df.m.proxyURL, df.m.bucket, shardName, getOptions)
		if err != nil && df.extension == ".zip" && i > df.outputShardCnt/2 {
			// We estimated too much output shards to be produced - zip compression
			// was so good that we could fit more files inside the shard.
			//
			// Sanity check to make sure that error did not occur before half
			// of the shards estimated (zip compression should be THAT good).
			break
		}
		tutils.CheckFatal(err, df.m.t)
		if df.extension == ".tar" || df.extension == ".tar.gz" {
			files, err = tutils.GetFileInfosFromTarBuffer(buffer, gzipped)
		} else if df.extension == ".zip" {
			files, err = tutils.GetFileInfosFromZipBuffer(buffer)
		}
		tutils.CheckFatal(err, df.m.t)
		for _, file := range files {
			if df.algorithm == "" {
				if lastName > file.Name() {
					df.m.t.Fatalf("names are not in correct order (shard: %s, lastName: %s, curName: %s)", shardName, lastName, file.Name())
				}
			} else if df.algorithm == dsort.SortKindShuffle {
				if lastName > file.Name() {
					inversions++
				}
			}
			if file.Size() != int64(df.fileInTarballSize) {
				df.m.t.Fatalf("file sizes has changed (expected: %d, got: %d)", df.fileInTarballSize, file.Size())
			}
			lastName = file.Name()
		}
	}

	if df.algorithm == dsort.SortKindShuffle {
		if inversions == 0 {
			df.m.t.Fatal("shuffle sorting did not create any inversions")
		}
	}
}

// helper for dispatching i-th dSort job
func dispatchDSortJob(m *metadata, i int) {
	dsortFW := &dsortFramework{
		m:                 m,
		inputTempl:        fmt.Sprintf("input%d-{0..999}", i),
		outputTempl:       fmt.Sprintf("output%d-{00000..01000}", i),
		tarballCnt:        1000,
		fileInTarballCnt:  50,
		fileInTarballSize: 1024,
		extension:         ".tar",
		maxMemUsage:       "99%",
	}
	dsortFW.init()

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, m.t)

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, m.t)
	tutils.Logln("finished distributed sort")

	metrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, m.t)
	if len(metrics) != m.originalTargetCount {
		m.t.Errorf("number of metrics %d is not same as number of targets %d", len(metrics), m.originalTargetCount)
	}

	dsortFW.checkOutputShards(5)
}

func TestDistributedSort(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{00000..01000}",
			tarballCnt:        1000,
			fileInTarballCnt:  100,
			fileInTarballSize: 1024,
			extension:         ".tar",
			maxMemUsage:       "99%",
		}
	)
	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	tutils.Logln("finished distributed sort")

	metrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(metrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(metrics), m.originalTargetCount)
	}

	dsortFW.checkOutputShards(5)
}

// TestDistributedSortParallel runs multiple dSorts in parallel
func TestDistributedSortParallel(t *testing.T) {
	var (
		m = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dSortsCount = 5
	)
	if testing.Short() {
		t.Skip(skipping)
	}

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	wg := &sync.WaitGroup{}
	for i := 0; i < dSortsCount; i++ {
		wg.Add(1)
		go func(i int) {
			dispatchDSortJob(m, i)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

// TestDistributedSortChain runs multiple dSorts one after another
func TestDistributedSortChain(t *testing.T) {
	var (
		m = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dSortsCount = 5
	)
	if testing.Short() {
		t.Skip(skipping)
	}

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	for i := 0; i < dSortsCount; i++ {
		dispatchDSortJob(m, i)
	}
}

func TestDistributedSortShuffle(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			algorithm:         dsort.SortKindShuffle,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{00000..01000}",
			tarballCnt:        1000,
			fileInTarballCnt:  10,
			fileInTarballSize: 1024,
			extension:         ".tar",
			maxMemUsage:       "99%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	tutils.Logln("finished distributed sort")

	metrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(metrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(metrics), m.originalTargetCount)
	}

	dsortFW.checkOutputShards(5)
}

func TestDistributedSortWithDisk(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..99}",
			outputTempl:       "output-{0..1000}",
			tarballCnt:        100,
			fileInTarballCnt:  10,
			fileInTarballSize: 1024,
			extension:         ".tar",
			maxMemUsage:       "1KB",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort with spilling to disk...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	tutils.Logln("finished distributed sort")

	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	for target, metrics := range allMetrics {
		if metrics.Extraction.ExtractedToDiskCnt == 0 && metrics.Extraction.ExtractedCnt > 0 {
			t.Errorf("target %s did not extract any files do disk", target)
		}
	}

	dsortFW.checkOutputShards(0)
}

func TestDistributedSortZip(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{00000..01000}",
			tarballCnt:        1000,
			fileInTarballCnt:  100,
			fileInTarballSize: 1024,
			extension:         ".zip",
			maxMemUsage:       "60%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort with compression (.zip)...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	tutils.Logln("finished distributed sort")

	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	dsortFW.checkOutputShards(5)
}

func TestDistributedSortWithCompression(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{00000..01000}",
			tarballCnt:        1000,
			fileInTarballCnt:  50,
			fileInTarballSize: 1024,
			extension:         ".tar.gz",
			maxMemUsage:       "60%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort with compression (.tar.gz)...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	tutils.Logln("finished distributed sort")

	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	dsortFW.checkOutputShards(5)
}

func TestDistributedSortAbort(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{0..10000}",
			tarballCnt:        1000,
			fileInTarballCnt:  10,
			fileInTarballSize: 1024,
			extension:         ".tar",
			maxMemUsage:       "60%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	tutils.Logln("aborting distributed sort...")
	err = tutils.AbortDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)

	tutils.Logln("waiting for distributed sort to finish up...")
	aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if !aborted {
		t.Error("dsort was not aborted")
	}

	tutils.Logln("checking metrics...")
	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	for target, metrics := range allMetrics {
		if metrics.Aborted != true {
			t.Errorf("dsort was not aborted by target: %s", target)
		}
	}
}

func TestDistributedSortAbortExtractionPhase(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{0..1000}",
			tarballCnt:        1000,
			fileInTarballCnt:  100,
			fileInTarballSize: 1024,
			extension:         ".tar",
			maxMemUsage:       "60%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	tutils.Logln("waiting for extraction phase...")
	for {
		allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
		tutils.CheckFatal(err, t)

		extractionPhase := true
		for _, metrics := range allMetrics {
			extractionPhase = extractionPhase && metrics.Extraction.Running
		}

		if extractionPhase {
			tutils.Logln("aborting distributed sort...")
			err = tutils.AbortDSort(m.proxyURL, managerUUID)
			tutils.CheckFatal(err, t)
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	tutils.Logln("waiting for distributed sort to finish up...")
	aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if !aborted {
		t.Error("dsort was not aborted")
	}

	tutils.Logln("checking metrics...")
	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	for target, metrics := range allMetrics {
		if metrics.Aborted != true {
			t.Errorf("dsort was not aborted by target: %s", target)
		}
	}
}

func TestDistributedSortAbortSortingPhase(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{0..100000}",
			tarballCnt:        1000,
			fileInTarballCnt:  500,
			fileInTarballSize: 2,
			extension:         ".tar",
			maxMemUsage:       "40%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	tutils.Logln("waiting for sorting phase...")
	for {
		allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
		tutils.CheckFatal(err, t)

		sortingPhase := true
		for _, metrics := range allMetrics {
			sortingPhase = sortingPhase && (metrics.Sorting.Running || metrics.Sorting.Finished)
		}

		if sortingPhase {
			tutils.Logln("aborting distributed sort...")
			err = tutils.AbortDSort(m.proxyURL, managerUUID)
			tutils.CheckFatal(err, t)
			break
		}

		time.Sleep(time.Millisecond)
	}

	tutils.Logln("waiting for distributed sort to finish up...")
	aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if !aborted {
		t.Error("dsort was not aborted")
	}

	tutils.Logln("checking metrics...")
	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	for target, metrics := range allMetrics {
		if metrics.Aborted != true {
			t.Errorf("dsort was not aborted by target: %s", target)
		}
	}
}

func TestDistributedSortAbortCreationPhase(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..999}",
			outputTempl:       "output-{0..1000}",
			tarballCnt:        1000,
			fileInTarballCnt:  100,
			fileInTarballSize: 128,
			extension:         ".tar",
			maxMemUsage:       "50%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	tutils.Logln("waiting for creation phase...")
	for {
		allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
		tutils.CheckFatal(err, t)

		creationPhase := true
		for _, metrics := range allMetrics {
			creationPhase = creationPhase && (metrics.Creation.Running || metrics.Creation.Finished)
		}

		if creationPhase {
			tutils.Logln("aborting distributed sort...")
			err = tutils.AbortDSort(m.proxyURL, managerUUID)
			tutils.CheckFatal(err, t)
			break
		}

		time.Sleep(time.Millisecond * 10)
	}

	tutils.Logln("waiting for distributed sort to finish up...")
	aborted, err := tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if !aborted {
		t.Error("dsort was not aborted")
	}

	tutils.Logln("checking metrics...")
	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	for target, metrics := range allMetrics {
		if metrics.Aborted != true {
			t.Errorf("dsort was not aborted by target: %s", target)
		}
	}
}

func TestDistributedSortMetricsAfterFinish(t *testing.T) {
	var (
		err error
		m   = &metadata{
			t:      t,
			bucket: TestLocalBucketName,
		}
		dsortFW = &dsortFramework{
			m:                 m,
			inputTempl:        "input-{0..99}",
			outputTempl:       "output-{0..1000}",
			tarballCnt:        100,
			fileInTarballCnt:  10,
			fileInTarballSize: 1024,
			extension:         ".tar",
			maxMemUsage:       "40%",
		}
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	dsortFW.init()

	// Initialize metadata
	saveClusterState(m)
	if m.originalTargetCount < 3 {
		t.Fatalf("Must have 3 or more targets in the cluster, have only %d", m.originalTargetCount)
	}

	// Create local bucket
	createFreshLocalBucket(t, m.proxyURL, m.bucket)
	defer destroyLocalBucket(t, m.proxyURL, m.bucket)

	dsortFW.createInputShards()

	tutils.Logln("started distributed sort...")
	rs := dsortFW.gen()
	managerUUID, err := tutils.StartDSort(m.proxyURL, rs)
	tutils.CheckFatal(err, t)

	_, err = tutils.WaitForDSortToFinish(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	tutils.Logln("finished distributed sort")

	allMetrics, err := tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}

	dsortFW.checkOutputShards(0)

	tutils.Logln("checking if metrics are still accessible after some time..")
	time.Sleep(time.Second * 2)

	// Check if metrics can be fetched after some time
	allMetrics, err = tutils.MetricsDSort(m.proxyURL, managerUUID)
	tutils.CheckFatal(err, t)
	if len(allMetrics) != m.originalTargetCount {
		t.Errorf("number of metrics %d is not same as number of targets %d", len(allMetrics), m.originalTargetCount)
	}
}
