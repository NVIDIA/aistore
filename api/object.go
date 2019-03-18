// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

const (
	httpMaxRetries = 5                     // maximum number of retries for an HTTP request
	httpRetrySleep = 30 * time.Millisecond // a sleep between HTTP request retries
)

// GetObjectInput is used to hold optional parameters for GetObject and GetObjectWithValidation
type GetObjectInput struct {
	// If not specified otherwise, the Writer field defaults to ioutil.Discard
	Writer io.Writer
	// Map of strings as keys and string slices as values used for url formulation
	Query url.Values
}

// ReplicateObjectInput is used to hold optional parameters for PutObject when it is used for replication
type ReplicateObjectInput struct {
	// Used to set the request header to determine whether PUT object request is for replication in AIStore
	SourceURL string
}

type PutObjectArgs struct {
	BaseParams     *BaseParams
	Bucket         string
	BucketProvider string
	Object         string
	Hash           string
	Reader         cmn.ReadOpenCloser
}

// HeadObject API
//
// Returns the size and version of the object specified by bucket/object
func HeadObject(baseParams *BaseParams, bucket, bckProvider, object string) (*cmn.ObjectProps, error) {
	baseParams.Method = http.MethodHead
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	params := OptionalParams{Query: query}

	r, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	size, err := strconv.Atoi(r.Header.Get(cmn.HeaderObjSize))
	if err != nil {
		return nil, err
	}

	return &cmn.ObjectProps{
		Size:    size,
		Version: r.Header.Get(cmn.HeaderObjVersion),
	}, nil
}

// DeleteObject API
//
// Deletes an object specified by bucket/object
func DeleteObject(baseParams *BaseParams, bucket, object, bckProvider string) error {
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	query := url.Values{cmn.URLParamBckProvider: []string{bckProvider}}
	params := OptionalParams{Query: query}

	_, err := DoHTTPRequest(baseParams, path, nil, params)
	return err
}

// EvictObject API
//
// Evicts an object specified by bucket/object
func EvictObject(baseParams *BaseParams, bucket, object string) error {
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActEvictObjects, Name: cmn.URLPath(bucket, object)})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// GetObject API
//
// Returns the length of the object. Does not validate checksum of the object in the response.
//
// Writes the response body to a writer if one is specified in the optional GetObjectInput.Writer.
// Otherwise, it discards the response body read.
//
func GetObject(baseParams *BaseParams, bucket, object string, options ...GetObjectInput) (n int64, err error) {
	var (
		w         = ioutil.Discard
		q         url.Values
		optParams OptionalParams
	)
	if len(options) != 0 {
		w, q = getObjectOptParams(options[0])
		if len(q) != 0 {
			optParams.Query = q
		}
	}
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	resp, err := doHTTPRequestGetResp(baseParams, path, nil, optParams)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	buf, slab := Mem2.AllocFromSlab2(cmn.DefaultBufSize)
	n, err = io.CopyBuffer(w, resp.Body, buf)
	slab.Free(buf)

	if err != nil {
		return 0, fmt.Errorf("failed to Copy HTTP response body, err: %v", err)
	}
	return n, nil
}

// GetObjectWithValidation API
//
// Same behaviour as GetObject, but performs checksum validation of the object
// by comparing the checksum in the response header with the calculated checksum
// value derived from the returned object.
//
// Similar to GetObject, if a memory manager/slab allocator is not specified, a temporary buffer
// is allocated when reading from the response body to compute the object checksum.
//
// Returns InvalidCksumError when the expected and actual checksum values are different.
func GetObjectWithValidation(baseParams *BaseParams, bucket, object string, options ...GetObjectInput) (int64, error) {
	var (
		n         int64
		cksumVal  string
		w         = ioutil.Discard
		q         url.Values
		optParams OptionalParams
	)
	if len(options) != 0 {
		w, q = getObjectOptParams(options[0])
		if len(q) != 0 {
			optParams.Query = q
		}
	}
	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	resp, err := doHTTPRequestGetResp(baseParams, path, nil, optParams)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	hdrHash := resp.Header.Get(cmn.HeaderObjCksumVal)
	hdrHashType := resp.Header.Get(cmn.HeaderObjCksumType)

	if hdrHashType == cmn.ChecksumXXHash {
		buf, slab := Mem2.AllocFromSlab2(cmn.DefaultBufSize)
		n, cksumVal, err = cmn.WriteWithHash(w, resp.Body, buf)
		slab.Free(buf)

		if err != nil {
			return 0, fmt.Errorf("failed to calculate xxHash from HTTP response body, err: %v", err)
		}
		if cksumVal != hdrHash {
			return 0, cmn.NewInvalidCksumError(hdrHash, cksumVal)
		}
	} else {
		return 0, fmt.Errorf("can't validate hash types other than %s, object's hash type: %s", cmn.ChecksumXXHash, hdrHashType)
	}
	return n, nil
}

// PutObject API
//
// Creates an object from the body of the io.Reader parameter and puts it in the 'bucket' bucket
// If there is a local bucket and cloud bucket with the same name, specify with bckProvider ("local", "cloud")
// The object name is specified by the 'object' argument.
// If the object hash passed in is not empty, the value is set
// in the request header with the default checksum type "xxhash"
func PutObject(args PutObjectArgs, replicateOpts ...ReplicateObjectInput) error {
	handle, err := args.Reader.Open()
	if err != nil {
		return fmt.Errorf("failed to open reader, err: %v", err)
	}
	defer handle.Close()

	path := cmn.URLPath(cmn.Version, cmn.Objects, args.Bucket, args.Object)
	var query = url.Values{}
	query.Add(cmn.URLParamBckProvider, args.BucketProvider)
	reqURL := args.BaseParams.URL + path + "?" + query.Encode()

	req, err := http.NewRequest(http.MethodPut, reqURL, handle)
	if err != nil {
		return fmt.Errorf("failed to create new HTTP request, err: %v", err)
	}

	// The HTTP package doesn't automatically set this for files, so it has to be done manually
	// If it wasn't set, we would need to deal with the redirect manually.
	req.GetBody = func() (io.ReadCloser, error) {
		return args.Reader.Open()
	}
	if args.Hash != "" {
		req.Header.Set(cmn.HeaderObjCksumType, cmn.ChecksumXXHash)
		req.Header.Set(cmn.HeaderObjCksumVal, args.Hash)
	}
	if len(replicateOpts) > 0 {
		req.Header.Set(cmn.HeaderObjReplicSrc, replicateOpts[0].SourceURL)
	}

	resp, err := args.BaseParams.Client.Do(req)
	if err != nil {
		sleep := httpRetrySleep
		if cmn.IsErrBrokenPipe(err) || cmn.IsErrConnectionRefused(err) {
			for i := 0; i < httpMaxRetries && err != nil; i++ {
				time.Sleep(sleep)
				resp, err = args.BaseParams.Client.Do(req)
				sleep += sleep / 2
			}
		}
	}

	if err != nil {
		return fmt.Errorf("failed to %s, err: %v", http.MethodPut, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response, err: %v", err)
		}
		return fmt.Errorf("HTTP error = %d, message = %s", resp.StatusCode, string(b))
	}
	return nil
}

// RenameObject API
//
// Creates a cmn.ActionMsg with the new name of the object
// and sends a POST HTTP Request to /v1/objects/bucket-name/object-name
func RenameObject(baseParams *BaseParams, bucket, oldName, newName string) error {
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActRename, Name: newName})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, oldName)
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// ReplicateObject API
//
// ReplicateObject replicates given object in bucket using targetrunner's replicate endpoint.
func ReplicateObject(baseParams *BaseParams, bucket, object string) error {
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActReplicate})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// Downloader API

func DownloadSingle(baseParams *BaseParams, bucket, objname, link string) (string, error) {
	dlBody := cmn.DlSingle{
		DlObj: cmn.DlObj{
			Objname: objname,
			Link:    link,
		},
	}
	dlBody.Bucket = bucket
	return DownloadSingleWithParam(baseParams, dlBody)
}

func DownloadSingleWithParam(baseParams *BaseParams, dlBody cmn.DlSingle) (string, error) {
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	id, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return string(id), err
}

func DownloadRange(baseParams *BaseParams, bucket, base, template string) (string, error) {
	dlBody := cmn.DlRangeBody{
		Base:     base,
		Template: template,
	}
	dlBody.Bucket = bucket
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	id, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return string(id), err
}

func DownloadMulti(baseParams *BaseParams, bucket string, m interface{}) (string, error) {
	dlBody := cmn.DlMultiBody{}
	dlBody.Bucket = bucket
	query := dlBody.AsQuery()

	msg, err := jsoniter.Marshal(m)
	if err != nil {
		return "", err
	}

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	id, err := DoHTTPRequest(baseParams, path, msg, optParams)
	return string(id), err
}

func DownloadBucket(baseParams *BaseParams, bucket, prefix, suffix string) error {
	dlBody := cmn.DlBucketBody{
		Prefix: prefix,
		Suffix: suffix,
	}
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download, cmn.DownloadBucket, bucket)
	optParams := OptionalParams{
		Query: query,
	}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}

func DownloadStatus(baseParams *BaseParams, id string) (resp []byte, err error) {
	dlBody := cmn.DlAdminBody{
		ID: id,
	}
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	resp, err = DoHTTPRequest(baseParams, path, nil, optParams)
	return resp, err
}

func DownloadCancel(baseParams *BaseParams, id string) error {
	dlBody := cmn.DlAdminBody{
		ID: id,
	}
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}
