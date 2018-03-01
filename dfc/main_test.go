// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends._test
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof" // profile
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
)

// commandline examples:
// # go test -v -run=down -args -numfiles=10
// # go test -v -run=down -args -bucket=mybucket
// # go test -v -run=down -args -bucket=mybucket -numworkers 5
// # go test -v -run=list
// # go test -v -run=xxx -bench . -count 10

const (
	baseDir         = "/tmp/dfc"
	LocalDestDir    = "/tmp/dfc/dest"         // client-side download destination
	LocalSrcDir     = "/tmp/dfc/src"          // client-side src directory for upload
	ProxyURL        = "http://localhost:8080" // assuming local proxy is listening on 8080
	RestAPIGet      = ProxyURL + "/v1/files"  // version = 1, resource = files
	RestAPIProxyPut = ProxyURL + "/v1/files"  // version = 1, resource = files
	RestAPIProxyDel = ProxyURL + "/v1/files"  // version = 1, resource = files
	ColdValidStr    = "coldmd5"
	DeleteDir       = "/tmp/dfc/delete"
	DeleteStr       = "delete"
)

// globals
var (
	clibucket  string
	numfiles   int
	numworkers int
	match      string
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
	flag.StringVar(&clibucket, "bucket", "shri-new", "AWS or GCP bucket")
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
	flag.IntVar(&numworkers, "numworkers", 10, "Number of the workers")
	flag.StringVar(&match, "match", ".*", "object name regex")
}

func Test_download(t *testing.T) {
	flag.Parse()

	// Declare one channel per worker to pass the keyname
	keynameChans := make([]chan string, numworkers)
	resultChans := make([]chan workres, numworkers)
	filesCreated := make(chan string, numfiles)

	defer func() {
		//Delete files created by getAndCopyTmp
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

func Test_delete(t *testing.T) {
	flag.Parse()

	if err := dfc.CreateDir(DeleteDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", DeleteDir, err)
	}

	errch := make(chan error, numfiles)
	filesput := make(chan string, numfiles)
	putRandomFiles(0, baseseed, 512*1024, numfiles, clibucket, t, nil, errch, filesput, DeleteDir, DeleteStr)
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
		os.Remove(DeleteDir + "/" + name)
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
	flag.Parse()

	// list the names, sizes, creation times and MD5 checksums
	var msg = &dfc.GetMsg{GetProps: dfc.GetPropsSize + ", " + dfc.GetPropsCtime + ", " + dfc.GetPropsChecksum}
	jsbytes, err := json.Marshal(msg)
	if err != nil {
		t.Errorf("Unexpected json-marshal failure, err: %v", err)
		return
	}
	bucket := clibucket
	var copy bool
	// copy = true

	reslist := listbucket(t, bucket, jsbytes)
	if reslist == nil {
		return
	}
	if !copy {
		for _, m := range reslist.Entries {
			if len(m.Checksum) > 8 {
				tlogf("%s %d %s %s\n", m.Name, m.Size, m.Ctime, m.Checksum[:8]+"...")
			} else {
				tlogf("%s %d %s %s\n", m.Name, m.Size, m.Ctime, m.Checksum)
			}
		}
		return
	}
	// alternatively, write to a local filename = bucket
	fname := LocalDestDir + "/" + bucket
	if err = dfc.CreateDir(LocalDestDir); err != nil {
		t.Errorf("Failed to create dir %s, err: %v", LocalDestDir, err)
		return
	}
	file, err := os.Create(fname)
	if err != nil {
		t.Errorf("Failed to create file %s, err: %v", fname, err)
		return
	}
	for _, m := range reslist.Entries {
		fmt.Fprintln(file, m)
	}
	t.Logf("ls bucket %s written to %s", bucket, fname)
}

func Test_coldgetmd5(t *testing.T) {
	var (
		numPuts   = 5
		filesput  = make(chan string, numPuts)
		fileslist = make([]string, 0, 100)
		errch     = make(chan error, 100)
		wg        = &sync.WaitGroup{}
		bucket    = clibucket
		totalsize = (numPuts * largefilesize) / (1024 * 1024)
	)
	ldir := LocalSrcDir + "/" + ColdValidStr
	if err := dfc.CreateDir(ldir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", ldir, err)
	}

	config := getConfig(RestAPIDaemonPath, client, t)
	cksumconfig := config["cksum_config"].(map[string]interface{})
	bcoldget := cksumconfig["validate_cold_get"].(bool)

	putRandomFiles(0, baseseed, uint64(largefilesize), numPuts, bucket, t, nil, errch, filesput, ldir, ColdValidStr)
	selectErr(errch, "put", t, false)
	close(filesput) // to exit for-range
	for fname := range filesput {
		fileslist = append(fileslist, ColdValidStr+"/"+fname)
	}
	evictobjects(t, fileslist)
	// Disable Cold Get Validation
	if bcoldget {
		setConfig("validate_cold_get", strconv.FormatBool(false), RestAPIClusterPath, client, t)
	}
	start := time.Now()
	getfromfilelist(t, bucket, errch, fileslist)
	curr := time.Now()
	duration := curr.Sub(start)
	if t.Failed() {
		goto cleanup
	}
	tlogf("GET %d MB without MD5 validation: %v\n", totalsize, duration)
	selectErr(errch, "get", t, false)
	evictobjects(t, fileslist)
	// Enable Cold Get Validation
	setConfig("validate_cold_get", strconv.FormatBool(true), RestAPIClusterPath, client, t)
	if t.Failed() {
		goto cleanup
	}
	start = time.Now()
	getfromfilelist(t, bucket, errch, fileslist)
	curr = time.Now()
	duration = curr.Sub(start)
	tlogf("GET %d MB with MD5 validation:    %v\n", totalsize, duration)
	selectErr(errch, "get", t, false)
cleanup:
	setConfig("validate_cold_get", strconv.FormatBool(bcoldget), RestAPIClusterPath, client, t)
	for _, fn := range fileslist {
		_ = os.Remove(LocalSrcDir + "/" + fn)
		wg.Add(1)
		go del(bucket, fn, wg, errch, false)
	}
	wg.Wait()
	selectErr(errch, "delete", t, false)
	close(errch)
}

func Benchmark_get(b *testing.B) {
	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for j := 0; j < b.N; j++ {
		for i := 0; i < 10; i++ {
			wg.Add(1)
			keyname := "dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
			go get(clibucket, keyname, wg, errch, false)
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
	var md5hash string
	var written int64
	var errstr string
	res := workres{0, 0}
	defer wg.Done()

	for keyname := range keynames {
		url := RestAPIGet + "/" + bucket + "/" + keyname
		t.Logf("Worker %2d: GET %q", id, url)
		r, err := http.Get(url)
		if testfail(err, fmt.Sprintf("Worker %2d: get key %s from bucket %s", id, keyname, bucket), r, errch, t) {
			t.Errorf("Failing test")
			return
		}
		defer func(r *http.Response) {
			r.Body.Close()
		}(r)
		hdhash := r.Header.Get("Content-HASH")
		// Create a local copy
		fname := LocalDestDir + "/" + keyname
		file, err := dfc.Createfile(fname)
		if err != nil {
			t.Errorf("Worker %2d: Failed to create file, err: %v", id, err)
			return
		}
		defer func() {
			if err := file.Close(); err != nil {
				errstr = fmt.Sprintf("Failed to close file, err: %s", err)
				t.Errorf("Worker %2d: %s", id, errstr)
			}
		}()
		config := getConfig(RestAPIDaemonPath, client, t)
		cksumconfig := config["cksum_config"].(map[string]interface{})
		bcoldget := cksumconfig["validate_cold_get"].(bool)
		if bcoldget {
			md5 := md5.New()
			written, errstr = dfc.ReceiveFile(file, r.Body, nil, md5)
			if errstr != "" {
				t.Errorf("Worker %2d: failed to write file, err: %s", id, errstr)
				return
			}
			hashInBytes := md5.Sum(nil)[:16]
			md5hash = hex.EncodeToString(hashInBytes)
			if errstr != "" {
				t.Errorf("Worker %2d: failed to compute xxhash, err: %s", id, errstr)
				return
			}

			if hdhash != md5hash {
				t.Errorf("Worker %2d: header's md5 %s doesn't match the file's %s", id, hdhash, md5hash)
				resch <- res
				close(resch)
				return
			}
		} else {
			written, errstr = dfc.ReceiveFile(file, r.Body, nil)
			if errstr != "" {
				t.Errorf("Worker %2d: failed to write file, err: %s", id, errstr)
				return
			}

		}
		res.totfiles++
		res.totbytes += written
	}
	// Send information back
	resch <- res
	close(resch)
}

func deleteFiles(keynames <-chan string, t *testing.T, wg *sync.WaitGroup, errch chan error, bucket string) {
	defer wg.Done()
	dwg := &sync.WaitGroup{}
	for keyname := range keynames {
		dwg.Add(1)
		go del(bucket, keyname, dwg, errch, false)
	}
	dwg.Wait()
}

func getMatchingKeys(regexmatch, bucket string, keynameChans []chan string, outputChan chan string, t *testing.T) int {
	// list the bucket
	var msg = &dfc.GetMsg{}
	jsbytes, err := json.Marshal(msg)
	if err != nil {
		t.Errorf("Unexpected json-marshal failure, err: %v", err)
		return 0
	}
	reslist := listbucket(t, bucket, jsbytes)
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

func get(bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) {
	if wg != nil {
		defer wg.Done()
	}
	url := RestAPIGet + "/" + bucket + "/" + keyname
	if !silent {
		tlogf("GET: object %s\n", keyname)
	}
	r, err := http.Get(url)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	err = discardResponse(r, err, fmt.Sprintf("object %s from bucket %s", keyname, bucket))
	emitError(r, err, errch)
}

func listbucket(t *testing.T, bucket string, injson []byte) *dfc.BucketList {
	var (
		url     = RestAPIGet + "/" + bucket
		err     error
		request *http.Request
		r       *http.Response
	)
	tlogf("LIST %q\n", url)
	if len(injson) == 0 {
		r, err = client.Get(url)
	} else {
		request, err = http.NewRequest("GET", url, bytes.NewBuffer(injson))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
			r, err = client.Do(request)
		}
	}
	if err != nil {
		t.Errorf("Failed to GET %s, err: %v", url, err)
		return nil
	}
	if testfail(err, fmt.Sprintf("list bucket %s", bucket), r, nil, t) {
		return nil
	}
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	var reslist = &dfc.BucketList{}
	reslist.Entries = make([]*dfc.BucketEntry, 0, 1000)
	b, err := ioutil.ReadAll(r.Body)
	r.Body.Close()

	if err == nil {
		err = json.Unmarshal(b, reslist)
		if err != nil {
			t.Errorf("Failed to json-unmarshal, err: %v [%s]", err, string(b))
			return nil
		}
	} else {
		t.Errorf("Failed to read json, err: %v", err)
		return nil
	}
	return reslist
}

func put(fname string, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) {
	if wg != nil {
		defer wg.Done()
	}
	proxyurl := RestAPIProxyPut + "/" + bucket + "/" + keyname
	if !silent {
		tlogf("PUT: object %s fname %s\n", keyname, fname)
	}
	file, err := os.Open(fname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file %s, err: %v", fname, err)
		if errch != nil {
			errch <- fmt.Errorf("Failed to open file %s, err: %v", fname, err)
		}
		return
	}
	defer file.Close()
	req, err := http.NewRequest(http.MethodPut, proxyurl, file)
	if err != nil {
		if errch != nil {
			errch <- fmt.Errorf("Failed to create new http request, err: %v", err)
		}
		return
	}
	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return os.Open(fname)
	}

	md5 := md5.New()
	// FIXME: the client must compute xxhash not md5
	md5hash, errstr := dfc.ComputeFileMD5(file, nil, md5)
	if errstr != "" {
		if errch != nil {
			errch <- fmt.Errorf("Failed to compute md5 for file %s, err: %s", fname, errstr)
		}
		return
	}
	req.Header.Set("Content-HASH", md5hash)
	_, err = file.Seek(0, 0)
	if err != nil {
		if errch != nil {
			errch <- fmt.Errorf("Failed to seek file %s, err: %v", fname, err)
		}
		return
	}
	r, err := client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	err = discardResponse(r, err, "put")
	emitError(r, err, errch)
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

func discardResponse(r *http.Response, err error, src string) error {
	if err == nil {
		if r.StatusCode >= http.StatusBadRequest {
			return fmt.Errorf("Bad status code from %s: http status %d", src, r.StatusCode)
		}
		bufreader := bufio.NewReader(r.Body)
		if _, err = dfc.ReadToNull(bufreader); err != nil {
			return fmt.Errorf("Failed to read http response, err: %v", err)
		}
	} else {
		return fmt.Errorf("Failed to get response from %s, err %v", src, err)
	}
	return nil
}

func del(bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) {
	if wg != nil {
		defer wg.Done()
	}
	proxyurl := RestAPIProxyPut + "/" + bucket + "/" + keyname
	if !silent {
		tlogf("DEL: %s\n", keyname)
	}
	req, err := http.NewRequest(http.MethodDelete, proxyurl, nil)
	if err != nil {
		if errch != nil {
			errch <- fmt.Errorf("Failed to create new http request, err: %v", err)
		}
		return
	}
	r, err := client.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	err = discardResponse(r, err, "delete")
	emitError(r, err, errch)
}

func getfromfilelist(t *testing.T, bucket string, errch chan error, fileslist []string) {
	getsGroup := &sync.WaitGroup{}
	for i := 0; i < len(fileslist); i++ {
		if fileslist[i] != "" {
			getsGroup.Add(1)
			go get(bucket, fileslist[i], getsGroup, errch, false)
		}
	}
	getsGroup.Wait()
}

func evictobjects(t *testing.T, fileslist []string) {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
		bucket = clibucket
	)
	EvictMsg := dfc.ActionMsg{Action: dfc.ActEvict}
	for _, fname := range fileslist {
		EvictMsg.Name = bucket + "/" + fname
		injson, err = json.Marshal(EvictMsg)
		if err != nil {
			t.Fatalf("Failed to marshal EvictMsg: %v", err)
		}
		req, err = http.NewRequest("DELETE", RestAPIProxyDel+"/"+bucket+"/"+fname, bytes.NewBuffer(injson))
		if err != nil {
			t.Fatalf("Failed to create request: %v", err)
		}
		r, err = client.Do(req)
		if r != nil {
			r.Body.Close()
		}
		if testfail(err, EvictMsg.Name, r, nil, t) {
			return
		}
	}
}
