// Package client provides common operations for files in cloud storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package client

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sync"

	"github.com/NVIDIA/dfcpub/dfc"
)

var (
	httpclient = &http.Client{}

	ProxyProto      = "http"
	ProxyIP         = "localhost"
	ProxyPort       = 8080
	RestAPIVersion  = "v1"
	RestAPIResource = "files"
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

func ProxyURL() string {
	if ProxyPort == 0 {
		return fmt.Sprintf("%s://%s/%s/%s", ProxyProto, ProxyIP, RestAPIVersion, RestAPIResource)
	}
	return fmt.Sprintf("%s://%s:%d/%s/%s", ProxyProto, ProxyIP, ProxyPort, RestAPIVersion, RestAPIResource)
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

func Get(bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) error {
	if wg != nil {
		defer wg.Done()
	}

	url := ProxyURL() + "/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("GET: object %s\n", keyname)
	}

	r, err := http.Get(url)
	if err != nil {
		return err
	}

	defer func() {
		r.Body.Close()
	}()
	err = discardResponse(r, err, fmt.Sprintf("object %s from bucket %s", keyname, bucket))
	emitError(r, err, errch)

	return err
}

func Put(fname string, bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) (err error) {
	if wg != nil {
		defer wg.Done()
	}

	proxyurl := ProxyURL() + "/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("PUT: object %s fname %s\n", keyname, fname)
	}

	file, oserr := os.Open(fname)
	if oserr != nil {
		err = fmt.Errorf("Failed to open file %s, err: %v", fname, oserr)
		fmt.Fprintf(os.Stderr, "%v", err)
		emitError(nil, err, errch)
		return err
	}

	defer func() {
		if e := file.Close(); e != nil {
			err = fmt.Errorf("Failed to close file: %s", e)
		}
	}()

	req, httperr := http.NewRequest(http.MethodPut, proxyurl, file)
	if httperr != nil {
		err = fmt.Errorf("Failed to create new http request, err: %v", err)
		emitError(nil, err, errch)
		return err
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
		err = fmt.Errorf("Failed to compute md5 for file %s, err: %s", fname, errstr)
		emitError(nil, err, errch)
		return err
	}

	req.Header.Set("Content-HASH", md5hash)
	_, oserr = file.Seek(0, 0)
	if oserr != nil {
		err = fmt.Errorf("Failed to seek file %s, err: %v", fname, err)
		emitError(nil, err, errch)
		return err
	}

	r, httperr := httpclient.Do(req)
	if httperr != nil {
		err = fmt.Errorf("Failed to put file, err: %v", err)
		emitError(nil, err, errch)
		return err
	}

	defer func() {
		r.Body.Close()
	}()
	err = discardResponse(r, err, "put")
	emitError(r, err, errch)
	return err
}

func Del(bucket string, keyname string, wg *sync.WaitGroup, errch chan error, silent bool) (err error) {
	if wg != nil {
		defer wg.Done()
	}

	proxyurl := ProxyURL() + "/" + bucket + "/" + keyname
	if !silent {
		fmt.Printf("DEL: %s\n", keyname)
	}
	req, httperr := http.NewRequest(http.MethodDelete, proxyurl, nil)
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
	err = discardResponse(r, err, "delete")
	emitError(r, err, errch)
	return err
}

func ListBucket(bucket string, injson []byte) (*dfc.BucketList, error) {
	var (
		url     = fmt.Sprintf("%s/%s", ProxyURL(), bucket)
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

func EvictObjects(bucket string, fileslist []string) error {
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

		req, err = http.NewRequest("DELETE", ProxyURL()+"/"+bucket+"/"+fname, bytes.NewBuffer(injson))
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
