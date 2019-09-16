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
	Size           uint64 // optional
}

// HeadObject API
//
// Returns the size and version of the object specified by bucket/object
func HeadObject(baseParams *BaseParams, bucket, bckProvider, object string, checkCached ...bool) (*cmn.ObjectProps, error) {
	checkIsCached := false
	if len(checkCached) > 0 {
		checkIsCached = checkCached[0]
	}
	baseParams.Method = http.MethodHead
	path := cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)
	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)
	query.Add(cmn.URLParamCheckCached, strconv.FormatBool(checkIsCached))
	params := OptionalParams{Query: query}

	r, err := doHTTPRequestGetResp(baseParams, path, nil, params)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if checkIsCached {
		io.Copy(ioutil.Discard, r.Body)
		return nil, err
	}
	var (
		size           int64
		atime          time.Time
		numCopies      int
		present, isais bool
	)
	size, err = strconv.ParseInt(r.Header.Get(cmn.HeaderObjSize), 10, 64)
	if err != nil {
		return nil, err
	}
	numCopiesStr := r.Header.Get(cmn.HeaderObjNumCopies)
	if numCopiesStr != "" {
		numCopies, err = strconv.Atoi(numCopiesStr)
		if err != nil {
			return nil, err
		}
	}
	atimeStr := r.Header.Get(cmn.HeaderObjAtime)
	if atimeStr != "" {
		atime, err = time.Parse(time.RFC822, atimeStr)
		if err != nil {
			return nil, err
		}
	}
	present, err = cmn.ParseBool(r.Header.Get(cmn.HeaderObjPresent))
	if err != nil {
		return nil, err
	}
	isais, err = cmn.ParseBool(r.Header.Get(cmn.HeaderObjBckIsAIS))
	if err != nil {
		return nil, err
	}
	return &cmn.ObjectProps{
		Size:      size,
		Version:   r.Header.Get(cmn.HeaderObjVersion),
		Atime:     atime,
		NumCopies: numCopies,
		Checksum:  r.Header.Get(cmn.HeaderObjCksumVal),
		Present:   present,
		BckIsAIS:  isais,
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

	buf, slab := Mem2.AllocForSize(cmn.DefaultBufSize)
	n, err = io.CopyBuffer(w, resp.Body, buf)
	slab.Free(buf)

	if err != nil {
		return 0, fmt.Errorf("failed to Copy HTTP response body, err: %v", err)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return 0, fmt.Errorf("returned with status code: %d", resp.StatusCode)
	}
	return n, nil
}

// GetObjectWithValidation API
//
// Same behavior as GetObject, but performs checksum validation of the object
// by comparing the checksum in the response header with the calculated checksum
// value derived from the returned object.
//
// Similar to GetObject, if a memory manager/slab allocator is not specified, a temporary buffer
// is allocated when reading from the response body to compute the object checksum.
//
// Returns InvalidCksumError when the expected and actual checksum values are different.
func GetObjectWithValidation(baseParams *BaseParams, bucket, object string, options ...GetObjectInput) (n int64, err error) {
	var (
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
		buf, slab := Mem2.AllocForSize(cmn.DefaultBufSize)
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
// If there is an ais bucket and cloud bucket with the same name, specify with bckProvider ("local", "cloud")
// The object name is specified by the 'object' argument.
// If the object hash passed in is not empty, the value is set
// in the request header with the default checksum type "xxhash"
func PutObject(args PutObjectArgs, replicateOpts ...ReplicateObjectInput) error {
	handle, err := args.Reader.Open()
	if err != nil {
		return fmt.Errorf("failed to open reader, err: %v", err)
	}
	defer handle.Close()

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, args.BucketProvider)
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   args.BaseParams.URL,
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, args.Bucket, args.Object),
		Query:  query,
		BodyR:  handle,
	}
	req, err := reqArgs.Req()
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
	if args.Size != 0 {
		req.ContentLength = int64(args.Size) // as per https://tools.ietf.org/html/rfc7230#section-3.3.2
	}

	setAuthToken(req, args.BaseParams)
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

	_, err = checkBadStatus(req, resp)
	return err
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

func DownloadSingle(baseParams *BaseParams, description, bucket, objname, link string) (string, error) {
	dlBody := cmn.DlSingleBody{
		DlObj: cmn.DlObj{
			Objname: objname,
			Link:    link,
		},
	}
	dlBody.Bucket = bucket
	dlBody.Description = description
	return DownloadSingleWithParam(baseParams, dlBody)
}

func DownloadSingleWithParam(baseParams *BaseParams, dlBody cmn.DlSingleBody) (string, error) {
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	return doDlDownloadRequest(baseParams, path, nil, optParams)
}

func DownloadRange(baseParams *BaseParams, description string, bucket, template string) (string, error) {
	dlBody := cmn.DlRangeBody{
		Template: template,
	}
	dlBody.Bucket = bucket
	dlBody.Description = description
	return DownloadRangeWithParam(baseParams, dlBody)
}

func DownloadRangeWithParam(baseParams *BaseParams, dlBody cmn.DlRangeBody) (string, error) {
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	return doDlDownloadRequest(baseParams, path, nil, optParams)
}

func DownloadMulti(baseParams *BaseParams, description string, bucket string, m interface{}) (string, error) {
	dlBody := cmn.DlMultiBody{}
	dlBody.Bucket = bucket
	dlBody.Description = description
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
	return doDlDownloadRequest(baseParams, path, msg, optParams)
}

func DownloadCloud(baseParams *BaseParams, description string, bucket, prefix, suffix string) (string, error) {
	dlBody := cmn.DlCloudBody{
		Prefix: prefix,
		Suffix: suffix,
	}
	dlBody.Bucket = bucket
	dlBody.Description = description
	return DownloadCloudWithParam(baseParams, dlBody)
}

func DownloadCloudWithParam(baseParams *BaseParams, dlBody cmn.DlCloudBody) (string, error) {
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	return doDlDownloadRequest(baseParams, path, nil, optParams)
}

func DownloadStatus(baseParams *BaseParams, id string) (cmn.DlStatusResp, error) {
	dlBody := cmn.DlAdminBody{
		ID: id,
	}
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	return doDlStatusRequest(baseParams, path, optParams)
}

func DownloadGetList(baseParams *BaseParams, regex string) (map[string]cmn.DlJobInfo, error) {
	dlBody := cmn.DlAdminBody{
		Regex: regex,
	}
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	resp, err := DoHTTPRequest(baseParams, path, nil, optParams)
	if err != nil {
		return nil, err
	}
	var parsedResp map[string]cmn.DlJobInfo
	err = jsoniter.Unmarshal(resp, &parsedResp)
	if err != nil {
		return nil, err
	}
	return parsedResp, nil
}

func DownloadAbort(baseParams *BaseParams, id string) error {
	dlBody := cmn.DlAdminBody{
		ID: id,
	}
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Download, cmn.Abort)
	optParams := OptionalParams{
		Query: query,
	}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}

func DownloadRemove(baseParams *BaseParams, id string) error {
	dlBody := cmn.DlAdminBody{
		ID: id,
	}
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodDelete
	path := cmn.URLPath(cmn.Version, cmn.Download, cmn.Remove)
	optParams := OptionalParams{
		Query: query,
	}
	_, err := DoHTTPRequest(baseParams, path, nil, optParams)
	return err
}

func doDlDownloadRequest(baseParams *BaseParams, path string, msg []byte, optParams OptionalParams) (string, error) {
	respBytes, err := DoHTTPRequest(baseParams, path, msg, optParams)
	if err != nil {
		return "", err
	}

	var resp cmn.DlPostResp
	err = jsoniter.Unmarshal(respBytes, &resp)
	if err != nil {
		return "", err
	}

	return resp.ID, nil
}

func doDlStatusRequest(baseParams *BaseParams, path string, optParams OptionalParams) (resp cmn.DlStatusResp, err error) {
	respBytes, err := DoHTTPRequest(baseParams, path, nil, optParams)
	if err != nil {
		return resp, err
	}

	err = jsoniter.Unmarshal(respBytes, &resp)
	return resp, err
}
