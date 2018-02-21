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
	"math/rand"
	"net/http"
	_ "net/http/pprof" // profile
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/dfc"
)

// commandline examples:
// # go test -v -run=down -args -numfiles=10
// # go test -v -run=down -args -bucket=mybucket
// # go test -v -run=down -args -bucket=mybucket -numworkers 5
// # go test -v -run=list
// # go test -v -run=xxx -bench . -count 10

const (
	LocalRootDir    = "/tmp/dfc/iocopy"        // client-side download destination
	SmokeDir        = "/tmp/dfc/smoke"         // smoke test dir
	ProxyURL        = "http://localhost:8080"  // assuming local proxy is listening on 8080
	TargetURL       = "http://localhost:8081"  // assuming local target is listening on 8081
	RestAPIGet      = ProxyURL + "/v1/files"   // version = 1, resource = files
	RestAPIProxyPut = ProxyURL + "/v1/files"   // version = 1, resource = files
	RestAPITgtPut   = TargetURL + "/v1/files"  // version = 1, resource = files
	RestAPICluster  = ProxyURL + "/v1/cluster" // version = 1, resource= cluster
	TestFile        = "/tmp/dfc/xattrfile"     // Test file for setting and getting xattr.
)

const (
	blocksize = 1048576
	baseseed  = 1062984096
)

// globals
var (
	clibucket  string
	numfiles   int
	numworkers int
	numops     int
	match      string
	role       string
	prop       string
	value      string
	fnlen      int
	filesizes  = [3]int{128 * 1024, 1024 * 1024, 4 * 1024 * 1024} // 128 KiB, 1MiB, 4 MiB
	ratios     = [6]float32{0, 0.1, 0.25, 0.5, 0.75, 0.9}         // #gets / #puts
)

// worker's result
type workres struct {
	totfiles int
	totbytes int64
}

func init() {
	flag.StringVar(&clibucket, "bucket", "shri-new", "AWS or GCP bucket")
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
	flag.IntVar(&numworkers, "numworkers", 10, "Number of the workers")
	flag.IntVar(&numops, "numops", 4, "Number of PUT/GET per worker")
	flag.IntVar(&fnlen, "fnlen", 20, "Length of randomly generated filenames")
	flag.StringVar(&match, "match", ".*", "regex match for the keyname")
	flag.StringVar(&role, "role", "proxy", "proxy or target")
}

func Test_smoke(t *testing.T) {
	flag.Parse()
	if err := dfc.CreateDir(LocalRootDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", LocalRootDir, err)
	}
	if err := dfc.CreateDir(SmokeDir); err != nil {
		t.Fatalf("Failed to create dir %s, err: %v", SmokeDir, err)
	}
	fp := make(chan string, len(filesizes)*len(ratios)*numops*numworkers)
	bs := int64(baseseed)
	for _, fs := range filesizes {
		for _, r := range ratios {

			t.Run(fmt.Sprintf("Filesize:%dB,Ratio:%.3f%%", fs, r*100), func(t *testing.T) { oneSmoke(t, fs, r, bs, fp) })
			bs += int64(numworkers + 1)
		}
	}
	close(fp)
	//clean up all the files from the test
	wg := &sync.WaitGroup{}
	errch := make(chan error, 100)
	for file := range fp {
		err := os.Remove(SmokeDir + "/" + file)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go del(clibucket, "smoke/"+file, wg, errch)
	}
	wg.Wait()
	select {
	case err := <-errch:
		t.Error(err)
	default:
	}
}

func oneSmoke(t *testing.T, filesize int, ratio float32, bseed int64, filesput chan string) {
	// Start the worker pools
	errch := make(chan error, 100)
	var wg = &sync.WaitGroup{}
	// Decide the number of each type
	var (
		nGet = int(float32(numworkers) * ratio)
		nPut = numworkers - nGet
	)
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		if (i%2 == 0 && nPut > 0) || nGet == 0 {
			go func(i int) {
				putRandomFiles(i, bseed+int64(i), uint64(filesize), numops, clibucket, t, wg, errch, filesput, "smoke")
			}(i)
			nPut--
		} else {
			go func(i int) { getRandomFiles(i, bseed+int64(i), numops, clibucket, t, wg, errch) }(i)
			nGet--
		}
	}
	wg.Wait()
	select {
	case err := <-errch:
		t.Error(err)
	default:
	}
}

func Test_download(t *testing.T) {
	flag.Parse()

	// Declare one channel per worker to pass the keyname
	keyname_chans := make([]chan string, numworkers)
	result_chans := make([]chan workres, numworkers)

	for i := 0; i < numworkers; i++ {
		// Allow a bunch of messages at a time to be written asynchronously to a channel
		keyname_chans[i] = make(chan string, 100)

		// Initialize number of files downloaded
		result_chans[i] = make(chan workres, 100)
	}

	// Start the worker pools
	errch := make(chan error, 100)

	var wg = &sync.WaitGroup{}
	// Get the workers started
	for i := 0; i < numworkers; i++ {
		wg.Add(1)
		// false: read the response and drop it, true: write it to a file
		go getAndCopyTmp(i, keyname_chans[i], t, wg, true, errch, result_chans[i], clibucket)
	}

	// list the bucket
	var msg = &dfc.GetMsg{}
	jsbytes, err := json.Marshal(msg)
	if err != nil {
		t.Errorf("Unexpected json-marshal failure, err: %v", err)
		return
	}
	reslist := listbucket(t, clibucket, jsbytes)
	if reslist == nil {
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
	t.Logf("Expecting to get %d files", num)

	// Close the channels after the reading is done
	for i := 0; i < numworkers; i++ {
		close(keyname_chans[i])
	}

	wg.Wait()

	// Now find the total number of files and data downloaed
	var sumtotfiles int = 0
	var sumtotbytes int64 = 0
	for i := 0; i < numworkers; i++ {
		res := <-result_chans[i]
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
	jsbytes, err := json.Marshal(msg)
	if err != nil {
		t.Errorf("Unexpected json-marshal failure, err: %v", err)
		return
	}
	reslist := listbucket(t, clibucket, jsbytes)
	if reslist == nil {
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

// fastRandomFilename is taken from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func fastRandomFilename(src *rand.Rand) string {
	b := make([]byte, fnlen)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := fnlen-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

func getRandomFiles(id int, seed int64, numGets int, bucket string, t *testing.T, wg *sync.WaitGroup, errch chan error) {
	if wg != nil {
		defer wg.Done()
	}
	src := rand.NewSource(seed)
	random := rand.New(src)
	getsGroup := &sync.WaitGroup{}
	var msg = &dfc.GetMsg{}
	jsbytes, err := json.Marshal(msg)
	if err != nil {
		t.Errorf("Unexpected json-marshal failure, err: %v", err)
		return
	}
	for i := 0; i < numGets; i++ {
		items := listbucket(t, bucket, jsbytes)
		if items == nil {
			errch <- fmt.Errorf("Nil listbucket response")
			return
		}
		files := make([]string, 0)
		for _, it := range items.Entries {
			// Directories retrieved from listbucket show up as files with '/' endings -
			// this filters them out.
			if it.Name[len(it.Name)-1] != '/' {
				files = append(files, it.Name)
			}
		}
		if len(files) == 0 {
			errch <- fmt.Errorf("Cannot retrieve from an empty bucket")
			return
		}
		keyname := files[random.Intn(len(files)-1)]
		if testing.Verbose() {
			fmt.Println("GET: " + keyname)
		}
		getsGroup.Add(1)
		go get(keyname, getsGroup, errch, bucket)
	}
	getsGroup.Wait()
}

func writeRandomData(fname string, bytes []byte, filesize int, random *rand.Rand) (int, error) {
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666) //wr-wr-wr-
	if err != nil {
		return 0, err
	}
	nblocks := filesize / blocksize
	tot := 0
	var r int
	for i := 0; i <= nblocks; i++ {
		if blocksize < filesize-tot {
			r = blocksize
		} else {
			r = filesize - tot
		}
		random.Read(bytes[0:r])
		n, err := f.Write(bytes[0:r])
		if err != nil {
			return tot, err
		} else if n < r {
			return tot, io.ErrShortWrite
		}
		tot += n
	}
	return tot, f.Close()
}

func putRandomFiles(id int, seed int64, fileSize uint64, numPuts int, bucket string,
	t *testing.T, wg *sync.WaitGroup, errch chan error, filesput chan string, dir string) {
	if wg != nil {
		defer wg.Done()
	}
	src := rand.NewSource(seed)
	random := rand.New(src)
	buffer := make([]byte, blocksize)
	for i := 0; i < numPuts; i++ {
		fname := fastRandomFilename(random)
		size := fileSize
		if size == 0 {
			size = uint64(random.Intn(1024)+1) * 1024
		}
		if _, err := writeRandomData(SmokeDir+"/"+fname, buffer, int(size), random); err != nil {
			t.Error(err)
			fmt.Fprintf(os.Stderr, "Failed to generate random file %s, err: %v\n", err)
			if errch != nil {
				errch <- err
			}
			return
		}
		// We could PUT while creating files, but that makes it
		// begin all the puts immediately (because creating random files is fast
		// compared to the listbucket call that getRandomFiles does)
		put(SmokeDir+"/"+fname, bucket, dir+"/"+fname, nil, errch)
		filesput <- fname
	}
}

func getAndCopyTmp(id int, keynames <-chan string, t *testing.T, wg *sync.WaitGroup, copy bool,
	errch chan error, resch chan workres, bucket string) {
	var md5hash string
	res := workres{0, 0}
	defer wg.Done()

	for keyname := range keynames {
		url := RestAPIGet + "/" + bucket + "/" + keyname
		t.Logf("Worker %2d: GET %q", id, url)
		r, err := http.Get(url)
		hdhash := r.Header.Get("Content-HASH")
		if testfail(err, fmt.Sprintf("Worker %2d: get key %s from bucket %s", id, keyname, bucket), r, errch, t) {
			t.Errorf("Failing test")
			return
		}
		defer func() {
			if r != nil {
				r.Body.Close()
			}
		}()
		if !copy {
			bufreader := bufio.NewReader(r.Body)
			bytes, err := dfc.ReadToNull(bufreader)
			if err != nil {
				t.Errorf("Worker %2d: Failed to read http response, err: %v", id, err)
				return
			}
			t.Logf("Worker %2d: Downloaded %q (size %.2f MB)", id, url, float64(bytes)/1000/1000)
			return
		}
		// alternatively, create a local copy
		fname := LocalRootDir + "/" + keyname
		file, err := dfc.Createfile(fname)
		if err != nil {
			t.Errorf("Worker %2d: Failed to create file, err: %v", id, err)
			return
		}
		md5 := md5.New()
		written, errstr := dfc.ReceiveFile(file, r.Body, nil, md5)
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
		r.Body.Close()
		res.totfiles += 1
		res.totbytes += written
		file.Close()
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
		go del(bucket, keyname, dwg, errch)
	}
	dwg.Wait()
}

func testfail(err error, str string, r *http.Response, errch chan error, t *testing.T) bool {
	if err != nil {
		if match, _ := regexp.MatchString("connection refused", err.Error()); match {
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

func Benchmark_one(b *testing.B) {
	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		keyname := "dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
		go get(keyname, wg, errch, clibucket)
	}
	wg.Wait()
	select {
	case err := <-errch:
		b.Error(err)
	default:
	}
}

func get(keyname string, wg *sync.WaitGroup, errch chan error, bucket string) {
	if wg != nil {
		defer wg.Done()
	}
	url := RestAPIGet + "/" + bucket + "/" + keyname
	r, err := http.Get(url)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		s := fmt.Sprintf("Failed to get key %s from bucket %s, http status %d", keyname, bucket, r.StatusCode)
		if errch != nil {
			errch <- errors.New(s)
		}
		return
	}
	if err != nil {
		if errch != nil {
			errch <- err
		}
		return
	}
	bufreader := bufio.NewReader(r.Body)
	if _, err = dfc.ReadToNull(bufreader); err != nil {
		errch <- fmt.Errorf("Failed to read http response, err: %v", err)
	}
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
				fmt.Printf("%s %d %s %s\n", m.Name, m.Size, m.Ctime, m.Checksum[:8]+"...")
			} else {
				fmt.Printf("%s %d %s %s\n", m.Name, m.Size, m.Ctime, m.Checksum)
			}
		}
		return
	}
	// alternatively, write to a local filename = bucket
	fname := LocalRootDir + "/" + bucket
	if err := dfc.CreateDir(LocalRootDir); err != nil {
		t.Errorf("Failed to create dir %s, err: %v", LocalRootDir, err)
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

func listbucket(t *testing.T, bucket string, injson []byte) *dfc.BucketList {
	var (
		url     = RestAPIGet + "/" + bucket
		err     error
		request *http.Request
		r       *http.Response
	)
	if testing.Verbose() {
		fmt.Printf("LIST %q\n", url)
	}
	if injson == nil || len(injson) == 0 {
		r, err = http.Get(url)
	} else {
		request, err = http.NewRequest("GET", url, bytes.NewBuffer(injson))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
			r, err = http.DefaultClient.Do(request)
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

func Test_xattr(t *testing.T) {
	f := TestFile
	file, err := dfc.Createfile(f)
	if err != nil {
		t.Errorf("Failed to create file %s, err:%v", f, err)
		return
	}
	// Set objstate to valid
	errstr := dfc.Setxattr(f, dfc.ObjstateAttr, []byte(dfc.XAttrInvalid))
	if errstr != "" {
		t.Errorf("Unable to set xattr %s to file %s, err: %v",
			dfc.ObjstateAttr, f, errstr)
		_ = os.Remove(f)
		file.Close()
		return
	}
	// Check if xattr got set correctly.
	data, errstr := dfc.Getxattr(f, dfc.ObjstateAttr)
	if string(data) == dfc.XAttrInvalid && errstr == "" {
		t.Logf("Successfully got file %s attr %s value %v",
			f, dfc.ObjstateAttr, data)
	} else {
		t.Errorf("Failed to get file %s attr %s value %v, err %v",
			f, dfc.ObjstateAttr, data, errstr)
		_ = os.Remove(f)
		file.Close()
		return
	}
	t.Logf("Successfully set and retrieved xattr")
	_ = os.Remove(f)
	file.Close()
}
func Test_proxyput(t *testing.T) {
	flag.Parse()
	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for i := 0; i < numfiles; i++ {
		wg.Add(1)
		keyname := "dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
		fname := "/" + keyname
		go put(fname, clibucket, keyname, wg, errch)
	}
	wg.Wait()
	select {
	case <-errch:
		t.Fail()
	default:
	}
}
func Test_proxydel(t *testing.T) {
	flag.Parse()
	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for i := 0; i < numfiles; i++ {
		wg.Add(1)
		keyname := "dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
		go del(clibucket, keyname, wg, errch)
	}
	wg.Wait()
	select {
	case <-errch:
		t.Fail()
	default:
	}
}

func put(fname string, bucket string, keyname string, wg *sync.WaitGroup, errch chan error) {
	if wg != nil {
		defer wg.Done()
	}
	proxyurl := RestAPIProxyPut + "/" + bucket + "/" + keyname
	//
	// FIXME: one client per what exactly?
	//
	client := &http.Client{}
	if testing.Verbose() {
		fmt.Printf("PUT: %s fname %s\n", keyname, fname)
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
	if r != nil {
		if r.StatusCode >= http.StatusBadRequest {
			if errch != nil {
				errch <- fmt.Errorf("Bad status code: http status %d", r.StatusCode)
			}
			return
		}
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			if errch != nil {
				errch <- fmt.Errorf("Failed to read response content, err %v", err)
			}
			return
		}
	} else {
		if errch != nil {
			errch <- fmt.Errorf("Failed to get proxy put response, err %v", err)
		}
		return
	}
}
func del(bucket string, keyname string, wg *sync.WaitGroup, errch chan error) {
	if wg != nil {
		defer wg.Done()
	}
	proxyurl := RestAPIProxyPut + "/" + bucket + "/" + keyname
	//
	// FIXME: one client per what exactly?
	//
	client := &http.Client{}
	if testing.Verbose() {
		fmt.Printf("DEL: %s\n", keyname)
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
	if r != nil {
		if r.StatusCode >= http.StatusBadRequest {
			if errch != nil {
				errch <- fmt.Errorf("Bad status code: http status %d", r.StatusCode)
			}
			return
		}
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			if errch != nil {
				errch <- fmt.Errorf("Failed to read response content, err %v", err)
			}
			return
		}
	} else {
		if errch != nil {
			errch <- fmt.Errorf("Failed to get delete response, err %v", err)
		}
		return
	}
}
