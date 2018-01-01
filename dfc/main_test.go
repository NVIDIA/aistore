/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof" // profile
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
)

// commandline examples:
// # go test -v -run=down -args -numfiles=10
// # go test -v -run=down -args -bucket=mybucket
// # go test -v -run=list
// # go test -v -run=xxx -bench . -count 10

const (
	locroot = "/iocopy"
)

var (
	bucket   string
	numfiles int
)

func init() {
	flag.StringVar(&bucket, "bucket", "/shri-new", "AWS or GCP bucket")
	flag.IntVar(&numfiles, "numfiles", 100, "Number of the files to download")
}

func Test_download(t *testing.T) {
	flag.Parse()

	var wg = &sync.WaitGroup{}
	errch := make(chan error, 100)
	for i := 0; i < numfiles; i++ {
		wg.Add(1)
		keyname := "/dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
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

	url := "http://localhost:" + "8080" + "/v1/files/" + bucket + keyname
	fname := "/tmp" + locroot + keyname
	dirname := filepath.Dir(fname)
	_, err := os.Stat(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755)
			if err != nil {
				t.Errorf("Failed to create bucket dir %q, err: %v", dirname, err)
				return
			}
		} else {
			t.Errorf("Failed to fstat, dir %q, err: %v", dirname, err)
			return
		}
	}
	file, err := os.Create(fname)
	if err != nil {
		t.Errorf("Unable to create file %q, err: %v", fname, err)
		return
	}
	t.Logf("GET %q", url)
	resp, err := http.Get(url)
	if testfail(err, fmt.Sprintf("get key %s from bucket %s", keyname, bucket), resp.StatusCode, errch, t) {
		return
	}
	//
	defer resp.Body.Close()
	if copy {
		numBytesWritten, err := io.Copy(file, resp.Body)
		if err != nil {
			t.Errorf("Failed to write to file, err: %v", err)
			return
		}
		t.Logf("Downloaded and copied %q size %d", fname, numBytesWritten)
	} else {
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read http response, err: %v", err)
			return
		}
		t.Logf("Downloaded %q", fname)
	}
}

func testfail(err error, str string, httpstatus int, errch chan error, t *testing.T) bool {
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
	if httpstatus >= http.StatusBadRequest {
		s := fmt.Sprintf("Failed %s, http status %d", str, httpstatus)
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
		keyname := "/dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
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
	url := "http://localhost:" + "8080" + "/v1/files/" + bucket + keyname
	resp, err := http.Get(url)
	if resp.StatusCode >= http.StatusBadRequest || err != nil {
		s := fmt.Sprintf("Failed to get key %s from bucket %s, http status %d", keyname, bucket, resp.StatusCode)
		b.Error(s)
		if errch != nil {
			errch <- errors.New(s)
		}
		return
	}

	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
}

func Test_list(t *testing.T) {
	flag.Parse()
	listAndCopyTmp(t, true) // false: read the response and drop it; true: write to file
}

func listAndCopyTmp(t *testing.T, copy bool) {
	url := "http://localhost:" + "8080" + "/v1/files/" + bucket
	fname := "/tmp" + locroot + bucket
	dirname := filepath.Dir(fname)
	_, err := os.Stat(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755)
			if err != nil {
				t.Errorf("Failed to create dir %q, err: %v", dirname, err)
				return
			}
		} else {
			t.Errorf("Failed to fstat, dir %q, err: %v", dirname, err)
			return
		}
	}
	file, err := os.Create(fname)
	if err != nil {
		t.Errorf("Unable to create file %q, err: %v", fname, err)
		return
	}
	t.Logf("GET %q", url)
	resp, err := http.Get(url)
	if testfail(err, fmt.Sprintf("list bucket %s", bucket), resp.StatusCode, nil, t) {
		return
	}

	defer resp.Body.Close()
	if copy {
		numBytesWritten, err := io.Copy(file, resp.Body)
		if err != nil {
			t.Errorf("Failed to write file, err: %v", err)
			return
		}
		t.Logf("Got bucket list and copied %q size %d", fname, numBytesWritten)
	} else {
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read http response, err: %v", err)
			return
		}
		t.Logf("Got bucket list %q", fname)
	}
}
