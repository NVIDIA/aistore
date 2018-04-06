/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // profile
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/pkg/client/readers"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
	"github.com/OneOfOne/xxhash"
)

// usage examples:
// # go test ./tests -v -run=regression
// # go test ./tests -v -run=down -args -bucket=mybucket
// # go test ./tests -v -run=list -bucket=otherbucket -prefix=smoke/obj -props=atime,ctime,iscached,checksum,version,size
// # go test ./tests -v -run=smoke -numworkers=4
// # go test ./tests -v -run=xxx -bench . -count 10

const (
	baseDir        = "/tmp/dfc"
	LocalDestDir   = "/tmp/dfc/dest"         // client-side download destination
	LocalSrcDir    = "/tmp/dfc/src"          // client-side src directory for upload
	ProxyURL       = "http://localhost:8080" // assuming local proxy is listening on 8080
	ColdValidStr   = "coldmd5"
	ChksumValidStr = "chksum"
	ColdMD5str     = "coldmd5"
	DeleteDir      = "/tmp/dfc/delete"
	DeleteStr      = "delete"
	largefilesize  = 4 // in MB
)

// globals
var (
	clibucket   string
	numfiles    int
	numworkers  int
	match       string
	clichecksum string
	totalio     int64
	proxyurl    string
	props       string
	pagesize    int64
)

// worker's result
type workres struct {
	totfiles int
	totbytes int64
}

type reqError struct {
	code    int
	message string
}

func (err reqError) Error() string {
	return err.message
}

func newReqError(msg string, code int) reqError {
	return reqError{
		code:    code,
		message: msg,
	}
}

func init() {
	flag.StringVar(&proxyurl, "proxyurl", ProxyURL, "Proxy URL")
	flag.StringVar(&clibucket, "bucket", "shri-new", "AWS or GCP bucket")
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
	flag.IntVar(&numworkers, "numworkers", 10, "Number of the workers")
	flag.StringVar(&match, "match", ".*", "object name regex")
	flag.StringVar(&clichecksum, "checksum", "all", "all | xxhash | coldmd5")
	flag.Int64Var(&totalio, "totalio", 80, "Total IO Size in MB")
	flag.StringVar(&props, "props", "", "List of object properties to return. Empty value means default set of properties")
	flag.Int64Var(&pagesize, "pagesize", 1000, "The maximum number of object returned by one list bucket call")
}

func checkMemory() {
	if readerType == readers.ReaderTypeSG || readerType == readers.ReaderTypeInMem {
		megabytes, _ := dfc.TotalMemory()
		if megabytes < PhysMemSizeWarn {
			fmt.Fprintf(os.Stderr, "Warning: host memory size = %dMB may be insufficient, consider use other reader type\n", megabytes)
		}
	}
}

func parse() {
	flag.Parse()
	usingSG = readerType == readers.ReaderTypeSG
	usingFile = readerType == readers.ReaderTypeFile
	checkMemory()
}

func Test_download(t *testing.T) {
	parse()

	if err := client.Tcping(proxyurl); err != nil {
		tlogf("%s: %v\n", proxyurl, err)
		os.Exit(1)
	}

	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	resultChans := make([]chan workres, numworkers)
	filesCreated := make(chan string, numfiles)

	defer func() {
		close(filesCreated)
		var err error
		for file := range filesCreated {
			e := os.Remove(LocalDestDir + "/" + file)
			if e != nil {
				err = e
			}
		}
		if err != nil {
			t.Error(err)
		}
	}()

	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keynameChans[i] = make(chan string, 100)

		// Initialize number of files downloaded
		resultChans[i] = make(chan workres, 100)
	}

	// Start the worker pools
	errch := make(chan error, 100)

	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		// Read the response and write it to a file
		go getAndCopyTmp(i, keynameChans[i], t, wg, errch, resultChans[i], clibucket)
	}

	num := getMatchingKeys(match, clibucket, keynameChans, filesCreated, t)

	t.Logf("Expecting to get %d keys\n", num)

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
	}

	wg.Wait()

	// Now find the total number of files and data downloaed
	var sumtotfiles int
	var sumtotbytes int64
	for i := 0; i < numworkers; i++ {
		res := <-resultChans[i]
		sumtotbytes += res.totbytes
		sumtotfiles += res.totfiles
		t.Logf("Worker #%d: %d files, size %.2f MB (%d B)",
			i, res.totfiles, float64(res.totbytes/1000/1000), res.totbytes)
	}
	t.Logf("\nSummary: %d workers, %d files, total size %.2f MB (%d B)",
		numworkers, sumtotfiles, float64(sumtotbytes/1000/1000), sumtotbytes)

	if sumtotfiles != num {
		s := fmt.Sprintf("Not all files downloaded. Expected: %d, Downloaded:%d", num, sumtotfiles)
		t.Error(s)
		if errch != nil {
			errch <- errors.New(s)
		}
	}
	select {
	case <-errch:
		t.Fail()
	default:
	}
}

// delete existing objects that match the regex
func Test_matchdelete(t *testing.T) {
	parse()

	// Declare one channel per worker to pass the keyname
	keyname_chans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keyname_chans[i] = make(chan string, 100)
	}
	// Start the worker pools
	errch := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(keyname_chans[i], t, wg, errch, clibucket)
	}

	// list the bucket
	var msg = &dfc.GetMsg{}
	reslist, err := client.ListBucket(proxyurl, clibucket, msg)
	if err != nil {
		t.Error(err)
		t.Fail()
		return
	}
	re, rerr := regexp.Compile(match)
	if testfail(rerr, fmt.Sprintf("Invalid match expression %s", match), nil, nil, t) {
		return
	}
	// match
	var num int
	for _, entry := range reslist.Entries {
		name := entry.Name
		if !re.MatchString(name) {
			continue
		}
		keyname_chans[num%numworkers] <- name
		if num++; num >= numfiles {
			break
		}
	}
	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keyname_chans[i])
	}
	wg.Wait()
	select {
	case <-errch:
		t.Fail()
	default:
	}
}

func Test_putdeleteRange(t *testing.T) {
	parse()

	const (
		numFiles     = 100
		commonPrefix = "tst" // object full name: <bucket>/<commonPrefix>/<generated_name:a-####|b-####>
	)
	var sgl *dfc.SGLIO

	if err := dfc.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	errch := make(chan error, numfiles*5)
	filesput := make(chan string, numfiles)
	filesize := uint64(16 * 1024)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	filenameList := make([]string, 0, numfiles)
	for i := 0; i < numfiles/2; i++ {
		fname := fmt.Sprintf("a-%04d", i)
		filenameList = append(filenameList, fname)
		fname = fmt.Sprintf("b-%04d", i)
		filenameList = append(filenameList, fname)
	}
	fillWithRandomData(baseseed, filesize, filenameList, clibucket, t, errch, filesput, DeleteDir, commonPrefix, false, sgl)
	selectErr(errch, "put", t, true /* fatal - if PUT does not work then it makes no sense to continue */)
	close(filesput)

	type testParams struct {
		// title to print out while testing
		name string
		// prefix for object name
		prefix string
		// regular expression object name must match
		regexStr string
		// a range of file IDs
		rangeStr string
		// total number of files expected to delete
		delta int
	}
	tests := []testParams{
		{
			"Trying to delete files with invalid prefix",
			"file/a-", "\\d+", "0:10",
			0,
		},
		{
			"Trying to delete files out of range",
			commonPrefix + "/a-", "\\d+", fmt.Sprintf("%d:%d", numFiles+10, numFiles+110),
			0,
		},
		{
			"Deleting 10 files with prefix 'a-'",
			commonPrefix + "/a-", "\\d+", "10:19",
			10,
		},
		{
			"Deleting 20 files (short range)",
			commonPrefix + "/", "\\d+", "30:39",
			20,
		},
		{
			"Deleting 20 more files (wide range)",
			commonPrefix + "/", "2\\d+", "10:90",
			20,
		},
		{
			"Deleting files with empty range",
			commonPrefix + "/b-", "", "",
			30,
		},
	}

	totalFiles := numFiles
	for idx, test := range tests {
		msg := &dfc.GetMsg{GetPrefix: commonPrefix + "/"}
		tlogf("%d. %s\n    Prefix: [%s], range: [%s], regexp: [%s]\n", idx+1, test.name, test.prefix, test.rangeStr, test.regexStr)

		err := client.DeleteRange(proxyurl, clibucket, test.prefix, test.regexStr, test.rangeStr, true, 0)
		if err != nil {
			t.Error(err)
		}

		totalFiles -= test.delta
		bktlst, err := client.ListBucket(proxyurl, clibucket, msg)
		if err != nil {
			t.Error(err)
		}
		if len(bktlst.Entries) != totalFiles {
			t.Errorf("Incorrect number of remaining files: %d, should be %d", len(bktlst.Entries), totalFiles)
		} else {
			tlogf("  %d files have been deleted\n", test.delta)
		}
	}

	tlogf("Cleaning up remained objects...\n")
	msg := &dfc.GetMsg{GetPrefix: commonPrefix + "/"}
	bktlst, err := client.ListBucket(proxyurl, clibucket, msg)
	if err != nil {
		t.Errorf("Failed to get the list of remained files, err: %v\n", err)
	}
	// cleanup everything at the end
	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keynameChans[i] = make(chan string, 100)
	}

	// Start the worker pools
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(keynameChans[i], t, wg, errch, clibucket)
	}

	if usingFile {
		for name := range filesput {
			os.Remove(DeleteDir + "/" + name)
		}
	}
	num := 0
	for _, entry := range bktlst.Entries {
		keynameChans[num%numworkers] <- entry.Name
		num++
	}

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
	}

	wg.Wait()
	selectErr(errch, "delete", t, false)
}

// PUT, then delete
func Test_putdelete(t *testing.T) {
	parse()

	var sgl *dfc.SGLIO
	if err := dfc.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	errch := make(chan error, numfiles)
	filesput := make(chan string, numfiles)
	filesize := uint64(512 * 1024)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(0, baseseed, filesize, numfiles, clibucket, t, nil, errch, filesput, DeleteDir, DeleteStr, "", false, sgl)
	close(filesput)

	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keynameChans[i] = make(chan string, 100)
	}

	// Start the worker pools
	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		go deleteFiles(keynameChans[i], t, wg, errch, clibucket)
	}

	num := 0
	for name := range filesput {
		if usingFile {
			os.Remove(DeleteDir + "/" + name)
		}

		keynameChans[num%numworkers] <- DeleteStr + "/" + name
		num++
	}

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keynameChans[i])
	}

	wg.Wait()
	selectErr(errch, "delete", t, false)
}

func Test_list(t *testing.T) {
	parse()

	var (
		copy     bool
		reslist  *dfc.BucketList
		file     *os.File
		err      error
		pageSize = int(pagesize)
	)

	// list the names, sizes, creation times and MD5 checksums
	var msg *dfc.GetMsg
	if props == "" {
		msg = &dfc.GetMsg{GetProps: dfc.GetPropsSize + ", " + dfc.GetPropsCtime + ", " + dfc.GetPropsChecksum + ", " + dfc.GetPropsVersion, GetPageSize: pageSize}
	} else {
		msg = &dfc.GetMsg{GetProps: props, GetPageSize: pageSize}
	}
	if prefix != "" {
		msg.GetPrefix = prefix
	}
	tlogf("Displaying properties: %s\n", msg.GetProps)

	bucket := clibucket
	fname := LocalDestDir + "/" + bucket
	if copy {
		// Write list to a local filename = bucket
		if err = dfc.CreateDir(LocalDestDir); err != nil {
			t.Errorf("Failed to create dir %s, err: %v", LocalDestDir, err)
			return
		}
		file, err = os.Create(fname)
		if err != nil {
			t.Errorf("Failed to create file %s, err: %v", fname, err)
			return
		}
	}

	totalObjs := 0
	for {
		reslist = testListBucket(t, bucket, msg)
		if reslist == nil {
			return
		}
		if pageSize != 0 && len(reslist.Entries) > pageSize {
			t.Errorf("Exceeded: %d entries\n", len(reslist.Entries))
		}
		if copy {
			for _, m := range reslist.Entries {
				fmt.Fprintln(file, m)
			}
			t.Logf("ls bucket %s written to %s", bucket, fname)
		} else {
			for _, m := range reslist.Entries {
				if len(m.Checksum) > 8 {
					tlogf("%s %d %s [%s] %s [%v - %s]\n", m.Name, m.Size, m.Ctime, m.Version, m.Checksum[:8]+"...", m.IsCached, m.Atime)
				} else {
					tlogf("%s %d %s [%s] %s [%v - %s]\n", m.Name, m.Size, m.Ctime, m.Version, m.Checksum, m.IsCached, m.Atime)
				}
			}
			totalObjs += len(reslist.Entries)
		}

		if reslist.PageMarker == "" {
			break
		}

		msg.GetPageMarker = reslist.PageMarker
		tlogf("PageMarker for the next page: %s\n", reslist.PageMarker)
	}
	tlogf("-----------------\nTotal objects listed: %v\n", totalObjs)
}

func Test_coldgetmd5(t *testing.T) {
	var (
		numPuts   = 5
		filesput  = make(chan string, numPuts)
		fileslist = make([]string, 0, 100)
		errch     = make(chan error, 100)
		wg        = &sync.WaitGroup{}
		bucket    = clibucket
		totalsize = numPuts * largefilesize
		filesize  = uint64(largefilesize * 1024 * 1024)
		sgl       *dfc.SGLIO
	)

	ldir := LocalSrcDir + "/" + ColdValidStr
	if err := dfc.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	config := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	cksumconfig := config["cksum_config"].(map[string]interface{})
	bcoldget := cksumconfig["validate_cold_get"].(bool)

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(0, baseseed, filesize, numPuts, bucket, t, nil, errch, filesput, ldir, ColdValidStr, "", true, sgl)
	selectErr(errch, "put", t, false)
	close(filesput) // to exit for-range
	for fname := range filesput {
		fileslist = append(fileslist, ColdValidStr+"/"+fname)
	}
	evictobjects(t, fileslist)
	// Disable Cold Get Validation
	if bcoldget {
		setConfig("validate_cold_get", strconv.FormatBool(false), proxyurl+"/v1/cluster", httpclient, t)
	}
	start := time.Now()
	getfromfilelist(t, bucket, errch, fileslist, false)
	curr := time.Now()
	duration := curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tlogf("GET %d MB without MD5 validation: %v\n", totalsize, duration)
	selectErr(errch, "get", t, false)
	evictobjects(t, fileslist)
	// Enable Cold Get Validation
	setConfig("validate_cold_get", strconv.FormatBool(true), proxyurl+"/v1/cluster", httpclient, t)
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, bucket, errch, fileslist, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tlogf("GET %d MB with MD5 validation:    %v\n", totalsize, duration)
	selectErr(errch, "get", t, false)
cleanup:
	setConfig("validate_cold_get", strconv.FormatBool(bcoldget), proxyurl+"/v1/cluster", httpclient, t)
	for _, fn := range fileslist {
		if usingFile {
			_ = os.Remove(LocalSrcDir + "/" + fn)
		}

		wg.Add(1)
		go client.Del(proxyurl, bucket, fn, wg, errch, false)
	}
	wg.Wait()
	selectErr(errch, "delete", t, false)
	close(errch)
}

func Test_headbucket(t *testing.T) {
	// Test that a local bucket returns Server:"DFC"
	createLocalBucket(httpclient, t, TestLocalBucketName)
	time.Sleep(time.Second * 2) // FIXME
	props, err := client.HeadBucket(proxyurl, TestLocalBucketName)
	if err != nil {
		t.Errorf("Failed to execute HeadBucket: %v", err)
	} else if props.CloudProvider != dfc.ProviderDfc {
		t.Errorf("Received incorrect Server from HeadBucket: \"%v\", expecting \"DFC\"", props.CloudProvider)
	}
	destroyLocalBucket(httpclient, t, TestLocalBucketName)
}

func Benchmark_get(b *testing.B) {
	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for j := 0; j < b.N; j++ {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			keyname := "dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
			go client.Get(proxyurl, clibucket, keyname, wg, errch, false, false)
		}
		wg.Wait()
		select {
		case err := <-errch:
			b.Error(err)
		default:
		}
	}
}

func getAndCopyTmp(id int, keynames <-chan string, t *testing.T, wg *sync.WaitGroup,
	errch chan error, resch chan workres, bucket string) {
	geturl := proxyurl + "/v1/files"
	res := workres{0, 0}
	defer wg.Done()

	for keyname := range keynames {
		url := geturl + "/" + bucket + "/" + keyname
		written, failed := getAndCopyOne(id, t, errch, bucket, keyname, url)
		if failed {
			t.Fail()
			return
		}
		res.totfiles++
		res.totbytes += written
	}
	resch <- res
	close(resch)
}

func getAndCopyOne(id int, t *testing.T, errch chan error, bucket, keyname, url string) (written int64, failed bool) {
	var errstr string
	t.Logf("Worker %2d: GET %q", id, url)
	r, err := http.Get(url)
	hdhash := r.Header.Get(dfc.HeaderDfcChecksumVal)
	hdhashtype := r.Header.Get(dfc.HeaderDfcChecksumType)
	if testfail(err, fmt.Sprintf("Worker %2d: get key %s from bucket %s", id, keyname, bucket), r, errch, t) {
		t.Errorf("Failing test")
		failed = true
		return
	}
	defer func(r *http.Response) {
		r.Body.Close()
	}(r)
	// Create a local copy
	fname := LocalDestDir + "/" + keyname
	file, err := dfc.CreateFile(fname)
	if err != nil {
		t.Errorf("Worker %2d: Failed to create file, err: %v", id, err)
		failed = true
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			errstr = fmt.Sprintf("Failed to close file, err: %s", err)
			t.Errorf("Worker %2d: %s", id, errstr)
		}
	}()
	if hdhashtype == dfc.ChecksumXXHash {
		xx := xxhash.New64()
		written, errstr = dfc.ReceiveAndChecksum(file, r.Body, nil, xx)
		if errstr != "" {
			t.Errorf("Worker %2d: failed to write file, err: %s", id, errstr)
			failed = true
			return
		}
		hashIn64 := xx.Sum64()
		hashInBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(hashInBytes, uint64(hashIn64))
		hash := hex.EncodeToString(hashInBytes)
		if hdhash != hash {
			t.Errorf("Worker %2d: header's %s %s doesn't match the file's %s", id, dfc.ChecksumXXHash, hdhash, hash)
			failed = true
			return
		}
		tlogf("Worker %2d: header's %s checksum %s matches the file's %s\n", id, dfc.ChecksumXXHash, hdhash, hash)
	} else if hdhashtype == dfc.ChecksumMD5 {
		md5 := md5.New()
		written, errstr = dfc.ReceiveAndChecksum(file, r.Body, nil, md5)
		if errstr != "" {
			t.Errorf("Worker %2d: failed to write file, err: %s", id, errstr)
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash := hex.EncodeToString(hashInBytes)
		if errstr != "" {
			t.Errorf("Worker %2d: failed to compute %s, err: %s", id, dfc.ChecksumMD5, errstr)
			failed = true
			return
		}
		if hdhash != md5hash {
			t.Errorf("Worker %2d: header's %s %s doesn't match the file's %s", id, dfc.ChecksumMD5, hdhash, md5hash)
			failed = true
			return
		}
		tlogf("Worker %2d: header's %s checksum %s matches the file's %s\n", id, dfc.ChecksumMD5, hdhash, md5hash)
	} else {
		written, errstr = dfc.ReceiveAndChecksum(file, r.Body, nil)
		if errstr != "" {
			t.Errorf("Worker %2d: failed to write file, err: %s", id, errstr)
			failed = true
			return
		}
	}
	return
}

func deleteFiles(keynames <-chan string, t *testing.T, wg *sync.WaitGroup, errch chan error, bucket string) {
	defer wg.Done()
	dwg := &sync.WaitGroup{}
	for keyname := range keynames {
		dwg.Add(1)
		go client.Del(proxyurl, bucket, keyname, dwg, errch, false)
	}
	dwg.Wait()
}

func getMatchingKeys(regexmatch, bucket string, keynameChans []chan string, outputChan chan string, t *testing.T) int {
	// list the bucket
	var msg = &dfc.GetMsg{}
	reslist := testListBucket(t, bucket, msg)
	if reslist == nil {
		return 0
	}
	re, rerr := regexp.Compile(regexmatch)
	if testfail(rerr, fmt.Sprintf("Invalid match expression %s", match), nil, nil, t) {
		return 0
	}
	// match
	num := 0
	numchans := len(keynameChans)
	for _, entry := range reslist.Entries {
		name := entry.Name
		if !re.MatchString(name) {
			continue
		}
		keynameChans[num%numchans] <- name
		if outputChan != nil {
			outputChan <- name
		}
		if num++; num >= numfiles {
			break
		}
	}

	return num
}

func testfail(err error, str string, r *http.Response, errch chan error, t *testing.T) bool {
	if err != nil {
		if dfc.IsErrConnectionRefused(err) {
			t.Fatalf("http connection refused - terminating")
		}
		s := fmt.Sprintf("%s, err: %v", str, err)
		t.Error(s)
		if errch != nil {
			errch <- errors.New(s)
		}
		t.Fail()
		return true
	}
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		s := fmt.Sprintf("%s, http status %d", str, r.StatusCode)
		t.Error(s)
		if errch != nil {
			errch <- errors.New(s)
		}
		return true
	}
	return false
}

func testListBucketAll(t *testing.T, bucket string, msg dfc.GetMsg) *dfc.BucketList {
	var (
		url = proxyurl + "/v1/files/" + bucket
	)
	tlogf("LIST ALL %q\n", url)
	fullbucketlist := &dfc.BucketList{Entries: make([]*dfc.BucketEntry, 0)}
	for {
		bucketlist := testListBucket(t, bucket, &msg)
		if bucketlist == nil {
			return nil
		}
		fullbucketlist.Entries = append(fullbucketlist.Entries, bucketlist.Entries...)
		if bucketlist.PageMarker == "" {
			break
		}
		msg.GetPageMarker = bucketlist.PageMarker
	}
	return fullbucketlist
}

func testListBucket(t *testing.T, bucket string, msg *dfc.GetMsg) *dfc.BucketList {
	var (
		url = proxyurl + "/v1/files/" + bucket
	)
	tlogf("LIST %q\n", url)
	reslist, err := client.ListBucket(proxyurl, bucket, msg)
	if testfail(err, fmt.Sprintf("List bucket %s failed", bucket), nil, nil, t) {
		return nil
	}

	return reslist
}

func emitError(r *http.Response, err error, errch chan error) {
	if err == nil || errch == nil {
		return
	}

	if r != nil {
		errObj := newReqError(err.Error(), r.StatusCode)
		errch <- errObj
	} else {
		errch <- err
	}
}

func Test_checksum(t *testing.T) {
	var (
		filesput    = make(chan string, 100)
		fileslist   = make([]string, 0, 100)
		errch       = make(chan error, 100)
		bucket      = clibucket
		start, curr time.Time
		duration    time.Duration
		htype       string
		numPuts     = 5
		filesize    = uint64(largefilesize * 1024 * 1024)
		sgl         *dfc.SGLIO
	)
	totalio := (numPuts * largefilesize)

	ldir := LocalSrcDir + "/" + ChksumValidStr
	if err := dfc.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}
	// Get Current Config
	config := getConfig(proxyurl+"/v1/daemon", httpclient, t)
	cksumconfig := config["cksum_config"].(map[string]interface{})
	ocoldget := cksumconfig["validate_cold_get"].(bool)
	ochksum := cksumconfig["checksum"].(string)
	if ochksum == dfc.ChecksumXXHash {
		htype = ochksum
	}

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	putRandomFiles(0, 0, filesize, int(numPuts), bucket, t, nil, errch, filesput, ldir, ChksumValidStr, htype, true, sgl)
	selectErr(errch, "put", t, false)
	close(filesput) // to exit for-range
	for fname := range filesput {
		if fname != "" {
			fileslist = append(fileslist, ChksumValidStr+"/"+fname)
		}
	}
	// Delete it from cache.
	evictobjects(t, fileslist)
	// Disable checkum
	if ochksum != dfc.ChecksumNone {
		setConfig("checksum", dfc.ChecksumNone, proxyurl+"/v1/cluster", httpclient, t)
	}
	if t.Failed() {
		goto cleanup
	}
	// Disable Cold Get Validation
	if ocoldget {
		setConfig("validate_cold_get", fmt.Sprint("false"), proxyurl+"/v1/cluster", httpclient, t)
	}
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, bucket, errch, fileslist, false)
	curr = time.Now()
	duration = curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tlogf("GET %d MB without any checksum validation: %v\n", totalio, duration)
	selectErr(errch, "get", t, false)
	evictobjects(t, fileslist)
	switch clichecksum {
	case "all":
		setConfig("checksum", dfc.ChecksumXXHash, proxyurl+"/v1/cluster", httpclient, t)
		setConfig("validate_cold_get", fmt.Sprint("true"), proxyurl+"/v1/cluster", httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	case dfc.ChecksumXXHash:
		setConfig("checksum", dfc.ChecksumXXHash, proxyurl+"/v1/cluster", httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	case ColdMD5str:
		setConfig("validate_cold_get", fmt.Sprint("true"), proxyurl+"/v1/cluster", httpclient, t)
		if t.Failed() {
			goto cleanup
		}
	case dfc.ChecksumNone:
		// do nothing
		tlogf("Checksum validation has been disabled \n")
		goto cleanup
	default:
		fmt.Fprintf(os.Stdout, "Checksum is either not set or invalid\n")
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, bucket, errch, fileslist, true)
	curr = time.Now()
	duration = curr.Sub(start)
	tlogf("GET %d MB and validate checksum (%s): %v\n", totalio, clichecksum, duration)
	selectErr(errch, "get", t, false)
cleanup:
	deletefromfilelist(t, bucket, errch, fileslist)
	// restore old config
	setConfig("checksum", fmt.Sprint(ochksum), proxyurl+"/v1/cluster", httpclient, t)
	setConfig("validate_cold_get", fmt.Sprint(ocoldget), proxyurl+"/v1/cluster", httpclient, t)
	return
}

func deletefromfilelist(t *testing.T, bucket string, errch chan error, fileslist []string) {
	wg := &sync.WaitGroup{}
	// Delete local file and objects from bucket
	for _, fn := range fileslist {
		if usingFile {
			err := os.Remove(LocalSrcDir + "/" + fn)
			if err != nil {
				t.Error(err)
			}
		}
		wg.Add(1)
		go client.Del(proxyurl, bucket, fn, wg, errch, true)
	}
	wg.Wait()
	selectErr(errch, "delete", t, false)
	close(errch)
}

func getfromfilelist(t *testing.T, bucket string, errch chan error, fileslist []string, validate bool) {
	getsGroup := &sync.WaitGroup{}
	for i := 0; i < len(fileslist); i++ {
		if fileslist[i] != "" {
			getsGroup.Add(1)
			go client.Get(proxyurl, bucket, fileslist[i], getsGroup, errch, false, validate)
		}
	}
	getsGroup.Wait()
}

func evictobjects(t *testing.T, fileslist []string) {
	var (
		bucket = clibucket
	)
	err := client.EvictList(proxyurl, bucket, fileslist, true, 0)
	if testfail(err, bucket, nil, nil, t) {
		return
	}
}
