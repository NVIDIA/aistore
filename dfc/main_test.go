package dfc_test

import (
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

const (
	remroot = "/shri-new"
	locroot = "/iocopy"
)

func Test_thirty(t *testing.T) {
	var wg = &sync.WaitGroup{}
	for i := 0; i < 30; i++ {
		wg.Add(1)
		keyname := "/dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
		// false: read the response and drop it, true: write it to a file
		go getAndCopyTmp(keyname, t, wg, true)
	}
	wg.Wait()
}

func getAndCopyTmp(keyname string, t *testing.T, wg *sync.WaitGroup, copy bool) {
	defer wg.Done()
	url := "http://localhost:" + "8080" + remroot + keyname
	fname := "/tmp" + locroot + keyname
	dirname := filepath.Dir(fname)
	_, err := os.Stat(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dirname, 0755)
			if err != nil {
				t.Logf("Failed to create bucket dir %q err: %v", dirname, err)
				return
			}
		} else {
			t.Logf("Failed to fstat, dir = %s err = %q \n", dirname, err)
			return
		}
	}
	file, err := os.Create(fname)
	if err != nil {
		t.Logf("Unable to create file = %s err = %v", fname, err)
		return
	}
	t.Logf("URL = %s \n", url)
	resp, err := http.Get(url)
	if err != nil {
		if match, _ := regexp.MatchString("connection refused", err.Error()); match {
			t.Fatalf("http connection refused - terminating")
		}
		t.Logf("Failed to get key %s err: %v", keyname, err)
	}
	if resp == nil {
		return
	}

	defer resp.Body.Close()
	if copy {
		numBytesWritten, err := io.Copy(file, resp.Body)
		if err != nil {
			t.Errorf("Failed to write to file err: %v", err)
			return
		}
		t.Logf("Downloaded and copied %q size %d", fname, numBytesWritten)
	} else {
		_, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("Failed to read http response %v", err)
			return
		}
		t.Logf("Downloaded %q", fname)
	}
}

func Benchmark_one(b *testing.B) {
	var wg = &sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		keyname := "/dir" + strconv.Itoa(i%3+1) + "/a" + strconv.Itoa(i)
		go get(keyname, b, wg)
	}
	wg.Wait()
}

func get(keyname string, b *testing.B, wg *sync.WaitGroup) {
	defer wg.Done()
	url := "http://localhost:" + "8080" + remroot + keyname
	resp, err := http.Get(url)
	if err != nil {
		if match, _ := regexp.MatchString("connection refused", err.Error()); match {
			fmt.Println("http connection refused - terminating")
			os.Exit(1)
		}
		fmt.Printf("Failed to get key %s err: %v", keyname, err)
	}
	if resp == nil {
		return
	}
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
}
