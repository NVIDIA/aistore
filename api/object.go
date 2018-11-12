/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/dfcpub/cmn"
)

// GetObjectInput is used to hold optional parameters for GetObject and GetObjectWithValidation
type GetObjectInput struct {
	// If not specified otherwise, the Writer field defaults to ioutil.Discard
	Writer io.Writer
	// Map of strings as keys and string slices as values used for url formulation
	Query url.Values
}

// HeadObject API operation for DFC
//
// Returns the size and version of the object specified by bucket/object
func HeadObject(httpClient *http.Client, proxyURL, bucket, object string) (*cmn.ObjectProps, error) {
	r, err := httpClient.Head(proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	if r != nil && r.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read response, err: %v", err)
		}
		return nil, fmt.Errorf("HEAD bucket/object: %s/%s failed, HTTP status: %d, HTTP response: %s",
			bucket, object, r.StatusCode, string(b))
	}

	size, err := strconv.Atoi(r.Header.Get(cmn.HeaderSize))
	if err != nil {
		return nil, err
	}

	return &cmn.ObjectProps{
		Size:    size,
		Version: r.Header.Get(cmn.HeaderVersion),
	}, nil
}

// DeleteObject API operation for DFC
//
// Deletes an object specified by bucket/object
func DeleteObject(httpClient *http.Client, proxyURL, bucket, object string) error {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	_, err := doHTTPRequest(httpClient, http.MethodDelete, url, nil)
	return err
}

// GetObject API operation for DFC
//
// Returns the length of the object. Does not validate checksum of the object in the response.
//
// Writes the response body to a writer if one is specified in the optional GetObjectInput.Writer.
// Otherwise, it discards the response body read.
//
// If a memory manager/slab allocator is specified, a reusable buffer allocated from the slab will be used
// for copying over the contents from the io.Reader to the io.Writer values.
// Otherwise, a temporary buffer is allocated in io.CopyBuffer.
func GetObject(httpClient *http.Client, proxyURL, bucket, object string, options ...GetObjectInput) (n int64, err error) {
	var (
		w = ioutil.Discard
		q url.Values
	)
	if len(options) != 0 {
		w, q = getObjectOptParams(options[0])
	}
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	resp, err := doHTTPRequestGetResp(httpClient, http.MethodGet, url, nil, q)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	buf, slab := Mem2.AllocFromSlab2(cmn.DefaultBufSize)
	n, err = io.CopyBuffer(w, resp.Body, buf)
	slab.Free(buf)

	if err != nil {
		return 0, fmt.Errorf("Failed to Copy HTTP response body, err: %v", err)
	}
	return n, nil
}

// GetObjectWithValidation API operation for DFC
//
// Same behaviour as GetObject, but performs checksum validation of the object
// by comparing the checksum in the response header with the calculated checksum
// value derived from the returned object.
//
// Similar to GetObject, if a memory manager/slab allocator is not specified, a temporary buffer
// is allocated when reading from the response body to compute the object checksum.
//
// Returns InvalidCksumError when the expected and actual checksum values are different.
func GetObjectWithValidation(httpClient *http.Client, proxyURL, bucket, object string, options ...GetObjectInput) (int64, error) {
	var (
		n    int64
		hash string
		w    = ioutil.Discard
		q    url.Values
	)
	if len(options) != 0 {
		w, q = getObjectOptParams(options[0])
	}
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	resp, err := doHTTPRequestGetResp(httpClient, http.MethodGet, url, nil, q)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	hdrHash := resp.Header.Get(cmn.HeaderDFCChecksumVal)
	hdrHashType := resp.Header.Get(cmn.HeaderDFCChecksumType)

	if hdrHashType == cmn.ChecksumXXHash {
		buf, slab := Mem2.AllocFromSlab2(cmn.DefaultBufSize)
		n, hash, err = cmn.ReadWriteWithHash(resp.Body, w, buf)
		slab.Free(buf)

		if err != nil {
			return 0, fmt.Errorf("Failed to calculate xxHash from HTTP response body, err: %v", err)
		}
		if hash != hdrHash {
			return 0, cmn.NewInvalidCksumError(hdrHash, hash)
		}
	} else {
		return 0, fmt.Errorf("Can't validate hash types other than %s, object's hash type: %s", cmn.ChecksumXXHash, hdrHashType)
	}
	return n, nil
}

// PutObject API operation for DFC
//
// Creates an object from the body of the io.Reader parameter and puts it in the 'bucket' bucket
// The object name is specified by the 'object' argument.
// If the object hash passed in is not empty, the value is set
// in the request header with the default checksum type "xxhash"
func PutObject(httpClient *http.Client, proxyURL, bucket, object, hash string, reader cmn.ReadOpenCloser) error {
	url := proxyURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	handle, err := reader.Open()
	if err != nil {
		return fmt.Errorf("Failed to open reader, err: %v", err)
	}
	defer handle.Close()

	req, err := http.NewRequest(http.MethodPut, url, handle)
	if err != nil {
		return fmt.Errorf("Failed to create new http request, err: %v", err)
	}

	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return reader.Open()
	}
	if hash != "" {
		req.Header.Set(cmn.HeaderDFCChecksumType, cmn.ChecksumXXHash)
		req.Header.Set(cmn.HeaderDFCChecksumVal, hash)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to PUT, err: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Failed to read response, err: %v", err)
		}
		return fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}
	return nil
}
