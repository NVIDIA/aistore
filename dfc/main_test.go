// Package dfc provides distributed file-based cache with Amazon and Google Cloud backends._test
/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"bufio"
	"bytes"
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
	LocalRootDir    = "/tmp/iocopy"           // client-side download destination
	ProxyURL        = "http://localhost:8080" // assuming local proxy is listening on 8080
	TargetURL       = "http://localhost:8081" // assuming local target is listening on 8081
	RestAPIGet      = ProxyURL + "/v1/files"  // version = 1, resource = files
	RestAPIProxyPut = ProxyURL + "/v1/files"  // version = 1, resource = files
	RestAPITgtPut   = TargetURL + "/v1/files" // version = 1, resource = files
	TestFile        = "/tmp/xattrfile"        // Test file for setting and getting xattr.
	SmokeDir        = "/tmp/nvidia/smoke/"
)

const (
	DeleteOP  = "delete"
	PutOP     = "put"
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
	fnlen      int
	filesizes  = [3]int{131072, 1048576, 4194304}      // 128 KiB, 1MiB, 4 MiB
	ratios     = [5]float32{0.1, 0.25, 0.5, 0.75, 0.9} // #gets / #puts
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
	os.MkdirAll(SmokeDir, os.ModePerm) //rwxrwxrwx
	fp := make(chan string, len(filesizes)*len(ratios)*numops*numworkers)
	for _, fs := range filesizes {
		for _, r := range ratios {
			t.Run(fmt.Sprintf("Filesize:%dB,Ratio:%.3f%%", fs, r*100), func(t *testing.T) { oneSmoke(t, fs, r, fp) })
		}
	}
	close(fp)
	//clean up all the files from the test
	wg := &sync.WaitGroup{}
	errch := make(chan error, 100)
	for file := range fp {
		err := os.Remove(SmokeDir + file)
		if err != nil {
			t.Error(err)
		}
		wg.Add(1)
		go proxyop(SmokeDir+file, clibucket, "smoke/"+file, "delete", t, wg, errch)
	}
	wg.Wait()
	select {
	case err := <-errch:
		t.Error(err)
	default:
	}
}

func oneSmoke(t *testing.T, filesize int, ratio float32, filesput chan string) {
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
		// false: read the response and drop it, true: write it to a file
		if (i%2 == 0 && nPut > 0) || nGet == 0 {
			go putRandomFiles(i, baseseed+int64(i), filesize, numops, t, wg, errch, filesput)
			nPut--
		} else {
			go getRandomFiles(i, baseseed+int64(i), numops, t, wg, errch)
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

func getRandomFiles(id int, seed int64, numGets int, t *testing.T, wg *sync.WaitGroup, errch chan error) {
	defer wg.Done()
	src := rand.NewSource(seed)
	random := rand.New(src)
	getsGroup := &sync.WaitGroup{}
	for i := 0; i < numGets; i++ {
		var msg = &dfc.GetMsg{}
		jsbytes, err := json.Marshal(msg)
		if err != nil {
			t.Errorf("Unexpected json-marshal failure, err: %v", err)
			return
		}
		items := listbucket(t, clibucket, jsbytes)
		if items == nil {
			t.Fatal("Bucket has no items to get.")
		}
		files := make([]string, 0)
		for _, it := range items.Entries {
			// Directories retrieved from listbucket show up as files with '/' endings - this filters them out.
			if it.Name[len(it.Name)-1] != '/' {
				files = append(files, it.Name)
			}
		}
		keyname := files[random.Intn(len(files)-1)]
		if testing.Verbose() {
			fmt.Println("GET: " + keyname)
		}
		getsGroup.Add(1)
		go get(keyname, getsGroup, errch, clibucket)
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

func putRandomFiles(id int, seed int64, fileSize int, numPuts int, t *testing.T, wg *sync.WaitGroup, errch chan error, filesput chan string) {
	defer wg.Done()
	src := rand.NewSource(seed)
	random := rand.New(src)
	putsGroup := &sync.WaitGroup{}
	buffer := make([]byte, blocksize)
	for i := 0; i < numPuts; i++ {
		//ioutil.Tempfile would be used here but it uses a mutex, which is undesirable behavior.
		fname := fastRandomFilename(random)
		_, err := writeRandomData(SmokeDir+fname, buffer, fileSize, random)
		if err != nil {
			t.Error(err)
			errch <- err
		}
		putsGroup.Add(1)
		// We could do Put operation while creating files, but that makes it begin all the puts immediately (because creating random files is fast compared to the listbucket call that getRandomFiles does)
		proxyop(SmokeDir+fname, clibucket, "smoke/"+fname, "put", t, putsGroup, errch)
		filesput <- fname
	}
	putsGroup.Wait()
}

func getAndCopyTmp(id int, keynames <-chan string, t *testing.T, wg *sync.WaitGroup, copy bool,
	errch chan error, resch chan workres, bucket string) {
	var md5sum, errstr string
	res := workres{0, 0}
	defer wg.Done()

	for keyname := range keynames {
		url := RestAPIGet + "/" + bucket + "/" + keyname
		t.Logf("Worker %2d: GET %q", id, url)
		r, err := http.Get(url)
		hmd5 := r.Header.Get("Content-MD5")
		if testfail(err, fmt.Sprintf("Worker %2d: get key %s from bucket %s", id, keyname, bucket), r, errch, t) {
			t.Errorf("Failing test")
			return
		}
		defer func() {
			if r != nil {
				r.Body.Close()
			}
		}()
		teebuf, b := dfc.Maketeerw(r.ContentLength, r.Body)
		md5sum, errstr = dfc.CalculateMD5(teebuf)
		if errstr != "" {
			t.Errorf("Worker %2d: Failed to calculate MD5sum, err: %v", id, errstr)
			return
		}
		r.Body = ioutil.NopCloser(b)
		if hmd5 != md5sum {
			t.Errorf("Worker %2d: Header MD5sum %v does not match with file's MD5 %v", hmd5, md5sum)
			return
		}
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
		written, err := dfc.ReceiveFile(fname, r.Body, md5sum)
		r.Body.Close()
		if err != nil {
			t.Errorf("Worker %2d: Failed to write to file, err: %v", id, err)
			return
		} else {
			res.totfiles += 1
			res.totbytes += written
		}
	}
	// Send information back
	resch <- res
	close(resch)
}

func testfail(err error, str string, r *http.Response, errch chan error, t *testing.T) bool {
	if err != nil {
		if match, _ := regexp.MatchString("connection refused", err.Error()); match {
			t.Fatalf("http connection refused - terminating")
		}
		s := fmt.Sprintf("Failed %s, err: %v", str, err)
		t.Error(s)
		if errch != nil {
			errch <- errors.New(s)
		}
		return true
	}
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		s := fmt.Sprintf("Failed %s, http status %d", str, r.StatusCode)
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
	defer wg.Done()
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
		errch <- errors.New(fmt.Sprintf("Failed to read http response, err: %v", err))
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
			fmt.Fprintf(os.Stdout, "%s %d %s %s\n", m.Name, m.Size, m.Ctime, m.Checksum[:8]+"...")
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
	t.Logf("LIST %q", url)
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
	if err == nil {
		err = json.Unmarshal(b, reslist)
		if err != nil {
			t.Errorf("Failed to json-unmarshal, err: %v", err)
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
	errstr := dfc.Setxattr(f, dfc.Objstateattr, []byte(dfc.XAttrInvalid))
	if errstr != "" {
		t.Errorf("Unable to set xattr %s to file %s, err: %v",
			dfc.Objstateattr, f, errstr)
		_ = os.Remove(f)
		file.Close()
		return
	}
	// Check if xattr got set correctly.
	data, errstr := dfc.Getxattr(f, dfc.Objstateattr)
	if string(data) == dfc.XAttrInvalid && errstr == "" {
		t.Logf("Successfully got file %s attr %s value %v",
			f, dfc.Objstateattr, data)
	} else {
		t.Errorf("Failed to get file %s attr %s value %v, err %v",
			f, dfc.Objstateattr, data, errstr)
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
		go proxyop(fname, clibucket, keyname, PutOP, t, wg, errch)
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
		go proxyop("", clibucket, keyname, DeleteOP, t, wg, errch)
	}
	wg.Wait()
	select {
	case <-errch:
		t.Fail()
	default:
	}
}

//tgturl = RestAPITgtPut + "/" + bucket + "/" + keyname
func proxyop(fname string, bucket string, keyname string, op string, t *testing.T, wg *sync.WaitGroup, errch chan error) {
	var tgturl, errstr string
	defer wg.Done()
	proxyurl := RestAPIProxyPut + "/" + bucket + "/" + keyname
	if testing.Verbose() {
		fmt.Printf("Proxy %s: %q\n", op, proxyurl)
	}
	tgturl, errstr = gettargeturl(proxyurl)
	if errstr != "" {
		goto proxyoperr
	}
	switch op {
	case "put":
		errstr = puttotgt(fname, tgturl)
		if errstr != "" {
			goto proxyoperr
		}
	case "delete":
		errstr = deleteontgt(fname, tgturl)
		if errstr != "" {
			goto proxyoperr
		}
	}
	return
proxyoperr:
	t.Error(errstr)
	if errch != nil {
		errch <- errors.New(errstr)
	}
	return
}
func gettargeturl(rqurl string) (url string, errstr string) {
	client := &http.Client{}

	req, err := http.NewRequest(http.MethodDelete, rqurl, nil)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create new http request, err: %v", err)
		return "", errstr
	}
	r, err := client.Do(req)
	if r != nil {
		returl, err := ioutil.ReadAll(r.Body)
		if err != nil {
			errstr = fmt.Sprintf("Failed to read response content, err %v", err)
			url = ""
			r.Body.Close()
			return
		}
		r.Body.Close()
		url = string(returl)
	} else {
		errstr = fmt.Sprintf("Failed to get proxy response, err %v", err)
		url = ""
	}
	return url, errstr
}

func puttotgt(fname string, rqurl string) (errstr string) {
	errstr = ""
	file, err := os.Open(fname)
	if err != nil {
		errstr = fmt.Sprintf("Failed to open file %s, err: %v", fname, err)
		return errstr
	}
	defer file.Close()
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPut, rqurl, file)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create new http request, err: %v", err)
		return errstr
	}
	md5, errstr1 := dfc.CalculateMD5(file)
	if errstr1 != "" {
		errstr = fmt.Sprintf("Failed to calculate MD5 sum for file %s, err: %s", fname, errstr1)
		return errstr
	}
	req.Header.Set("Content-MD5", md5)
	_, err = file.Seek(0, 0)
	if err != nil {
		errstr = fmt.Sprintf("Failed to seek file %s, err: %v", fname, err)
		return errstr
	}
	r, err := client.Do(req)
	if r != nil {
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			errstr = fmt.Sprintf("Failed to read response content, err %v", err)
			r.Body.Close()
			return errstr
		}
		r.Body.Close()
	} else {
		errstr = fmt.Sprintf("Failed to get proxy put response, err %v", err)
	}
	return errstr
}
func deleteontgt(fname string, rqurl string) (errstr string) {
	errstr = ""
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodDelete, rqurl, nil)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create new http request, err: %v", err)
		return errstr
	}
	r, err := client.Do(req)
	if r != nil {
		_, err := ioutil.ReadAll(r.Body)
		if err != nil {
			errstr = fmt.Sprintf("Failed to read response content, err %v", err)
			r.Body.Close()
			return errstr
		}
		r.Body.Close()
	} else {
		errstr = fmt.Sprintf("Failed to get delete response, err %v", err)
	}
	return errstr
}
