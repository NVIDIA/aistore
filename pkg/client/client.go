// Package client provides common operations for files in cloud storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package client

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/OneOfOne/xxhash"
)

var (
	httpclient = &http.Client{}
)

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

func Tcping(url string) (err error) {
	addr := strings.TrimPrefix(url, "http://")
	if addr == url {
		addr = strings.TrimPrefix(url, "https://")
	}
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
	}
	return
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
		return fmt.Errorf("%s failed, err: %v", src, err)
	}
	return nil
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

func Get(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool, validate bool) error {
	var (
		hash, hdhash, hdhashtype string
		errstr                   string
	)
	if wg != nil {
		defer wg.Done()
	}
	url := proxyurl + "/v1/files/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("GET: object %s\n", keyname)
	}
	r, err := http.Get(url)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	if validate && err == nil {
		hdhash = r.Header.Get(dfc.HeaderDfcChecksumVal)
		hdhashtype = r.Header.Get(dfc.HeaderDfcChecksumType)
		if hdhashtype == dfc.ChecksumXXHash {
			xx := xxhash.New64()
			if hash, errstr = dfc.ComputeXXHash(r.Body, nil, xx); errstr != "" {
				errch <- errors.New(errstr)
			}
			if hdhash != hash {
				s := fmt.Sprintf("Header's hash %s doesn't match the file's %s \n", hdhash, hash)
				if errch != nil {
					errch <- errors.New(s)
				}
			} else {
				if !silent {
					fmt.Printf("Header's hash %s matches the file's %s \n", hdhash, hash)
				}
			}
		}
	}
	err = discardResponse(r, err, fmt.Sprintf("GET (object %s from bucket %s)", keyname, bucket))
	emitError(r, err, errch)
	return err
}
func Put(proxyurl, fname, bucket, keyname, htype string, wg *sync.WaitGroup, errch chan error, silent bool) (err error) {
	var (
		hash   string
		errstr string
	)
	if wg != nil {
		defer wg.Done()
	}
	puturl := proxyurl + "/v1/files/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("PUT: object %s fname %s\n", keyname, fname)
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

	req, err := http.NewRequest(http.MethodPut, puturl, file)
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
	if htype == dfc.ChecksumXXHash {
		xx := xxhash.New64()
		hash, errstr = dfc.ComputeXXHash(file, nil, xx)
		if errstr != "" {
			errch <- fmt.Errorf("Failed to compute xxhash file %s, err: %v", fname, errstr)
			return
		}
		req.Header.Set(dfc.HeaderDfcChecksumType, htype)
		req.Header.Set(dfc.HeaderDfcChecksumVal, hash)
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		if errch != nil {
			errch <- fmt.Errorf("Failed to seek file %s, err: %v", fname, err)
		}
		return
	}
	r, err := httpclient.Do(req)
	defer func() {
		if r != nil {
			r.Body.Close()
		}
	}()
	err = discardResponse(r, err, "PUT")
	emitError(r, err, errch)
	return err
}

func Del(proxyurl, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) (err error) {
	if wg != nil {
		defer wg.Done()
	}

	delurl := proxyurl + "/v1/files/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("DEL: %s\n", keyname)
	}
	req, httperr := http.NewRequest(http.MethodDelete, delurl, nil)
	if httperr != nil {
		err = fmt.Errorf("Failed to create new http request, err: %v", err)
		emitError(nil, err, errch)
		return err
	}

	r, httperr := httpclient.Do(req)
	if httperr != nil {
		err = fmt.Errorf("Failed to delete file, err: %v", err)
		emitError(nil, err, errch)
		return err
	}

	defer func() {
		r.Body.Close()
	}()
	err = discardResponse(r, err, "DELETE")
	emitError(r, err, errch)
	return err
}

func ListBucket(proxyurl, bucket string, injson []byte) (*dfc.BucketList, error) {
	var (
		url     = proxyurl + "/v1/files/" + bucket
		err     error
		request *http.Request
		r       *http.Response
	)
	if len(injson) == 0 {
		r, err = httpclient.Get(url)
	} else {
		request, err = http.NewRequest("GET", url, bytes.NewBuffer(injson))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
			r, err = httpclient.Do(request)
		}
	}
	if err != nil {
		return nil, err
	}
	if r != nil && r.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("List bucket %s failed, HTTP status %d", bucket, r.StatusCode)
	}

	defer func() {
		r.Body.Close()
	}()
	var reslist = &dfc.BucketList{}
	reslist.Entries = make([]*dfc.BucketEntry, 0, 1000)
	b, err := ioutil.ReadAll(r.Body)

	if err == nil {
		err = json.Unmarshal(b, reslist)
		if err != nil {
			return nil, fmt.Errorf("Failed to json-unmarshal, err: %v [%s]", err, string(b))
		}
	} else {
		return nil, fmt.Errorf("Failed to read json, err: %v", err)
	}

	return reslist, nil
}

func EvictObjects(proxyurl, bucket string, fileslist []string) error {
	var (
		req    *http.Request
		r      *http.Response
		injson []byte
		err    error
	)
	EvictMsg := dfc.ActionMsg{Action: dfc.ActEvict}
	for _, fname := range fileslist {
		EvictMsg.Name = bucket + "/" + fname
		injson, err = json.Marshal(EvictMsg)
		if err != nil {
			return fmt.Errorf("Failed to marshal EvictMsg: %v", err)
		}

		req, err = http.NewRequest("DELETE", proxyurl+"/v1/files/"+bucket+"/"+fname, bytes.NewBuffer(injson))
		if err != nil {
			return fmt.Errorf("Failed to create request: %v", err)
		}

		r, err = httpclient.Do(req)
		if r != nil {
			r.Body.Close()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// fastRandomFilename is taken from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func FastRandomFilename(src *rand.Rand, fnlen int) string {
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

func WriteRandomData(fname string, bytes []byte, filesize int, blocksize int, random *rand.Rand) (int, error) {
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
