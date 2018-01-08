/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // profile
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/NVIDIA/dfcpub/dfc"
)

// commandline examples:
// # go test -v -run=down -args -numfiles=10
// # go test -v -run=down -args -bucket=mybucket
// # go test -v -run=list
// # go test -v -run=xxx -bench . -count 10

const (
	LocalRootDir = "/tmp/iocopy"           // client-side download destination
	ProxyURL     = "http://localhost:8080" // assuming local proxy is listening on 8080
	RestAPIGet   = ProxyURL + "/v1/files"  // version = 1, resource = files
)

var (
	bucket   string
	numfiles int
)

func init() {
	flag.StringVar(&bucket, "bucket", "shri-new", "AWS or GCP bucket")
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
}

func Test_download(t *testing.T) {
	flag.Parse()

	url := RestAPIGet + "/" + bucket
	t.Logf("LIST %q", url)
	r, err := http.Get(url)
	if testfail(err, fmt.Sprintf("list bucket %s", bucket), r, nil, t) {
		return
	}
	reader := bufio.NewReader(r.Body)
	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for i := 0; i < numfiles; i++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}
		wg.Add(1)
		keyname := strings.TrimSuffix(string(line), "\n")
		// false: read the response and drop it, true: write it to a file
		go getAndCopyTmp(keyname, t, wg, true, errch)
	}
	wg.Wait()
	select {
	case <-errch:
		t.Fail()
	default:
	}
}

// NOTE: this test assumes the files in the AWS or GCP bucket are named
//       in a certain special way - see keyname below
func Test_shri(t *testing.T) {
	flag.Parse()

	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for i := 0; i < numfiles; i++ {
		wg.Add(1)
		keyname := "dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
		// false: read the response and drop it, true: write it to a file
		go getAndCopyTmp(keyname, t, wg, true, errch)
	}
	wg.Wait()
	select {
	case <-errch:
		t.Fail()
	default:
	}
}

func getAndCopyTmp(keyname string, t *testing.T, wg *sync.WaitGroup, copy bool, errch chan error) {
	defer wg.Done()
	url := RestAPIGet + "/" + bucket + "/" + keyname
	t.Logf("GET %q", url)
	r, err := http.Get(url)
	if testfail(err, fmt.Sprintf("get key %s from bucket %s", keyname, bucket), r, errch, t) {
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
			t.Errorf("Failed to read http response, err: %v", err)
			return
		}
		t.Logf("Downloaded %q (size %.2f MB)", url, float64(bytes)/1000/1000)
		return
	}

	// alternatively, create a local copy
	fname := LocalRootDir + "/" + keyname
	written, err := dfc.ReceiveFile(fname, r)
	if err != nil {
		t.Errorf("Failed to write to file, err: %v", err)
		return
	}
	t.Logf("Downloaded and copied %q (size %.2f MB)", fname, float64(written/1000/1000))
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
		go get(keyname, b, wg, errch)
	}
	wg.Wait()
	select {
	case <-errch:
		b.Fail()
	default:
	}
}

func get(keyname string, b *testing.B, wg *sync.WaitGroup, errch chan error) {
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
		b.Error(s)
		if errch != nil {
			errch <- errors.New(s)
		}
		return
	}
	if err != nil {
		b.Error(err.Error())
		if errch != nil {
			errch <- err
		}
		return
	}
	bufreader := bufio.NewReader(r.Body)
	if _, err = dfc.ReadToNull(bufreader); err != nil {
		b.Errorf("Failed to read http response, err: %v", err)
	}
}

func Test_list(t *testing.T) {
	flag.Parse()
	listAndCopyTmp(t, false) // false: read the response and drop it; true: write to file
}

func listAndCopyTmp(t *testing.T, copy bool) {
	url := RestAPIGet + "/" + bucket
	t.Logf("LIST %q", url)
	r, err := http.Get(url)
	if testfail(err, fmt.Sprintf("list bucket %s", bucket), r, nil, t) {
		return
	}
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()

	if !copy {
		reader := bufio.NewReader(r.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				break
			}
			fmt.Fprintf(os.Stdout, string(line))
		}
		return
	}

	// alternatively, create a local copy
	fname := LocalRootDir + "/" + bucket
	written, err := dfc.ReceiveFile(fname, r)
	if err != nil {
		t.Errorf("Failed to write file, err: %v", err)
		return
	}
	t.Logf("Got bucket list and copied %q (size %d B)", fname, written)
}
