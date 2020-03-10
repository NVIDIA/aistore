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
	"github.com/NVIDIA/aistore/downloader"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/memsys"
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
	BaseParams BaseParams
	Bck        cmn.Bck
	Object     string
	Hash       string
	Reader     cmn.ReadOpenCloser
	Size       uint64 // optional
}

type PromoteArgs struct {
	BaseParams BaseParams
	Bck        cmn.Bck
	Object     string
	Target     string
	FQN        string
	Recurs     bool
	Overwrite  bool
	Verbose    bool
}

type AppendArgs struct {
	BaseParams BaseParams
	Bck        cmn.Bck
	Object     string
	Handle     string
	Reader     cmn.ReadOpenCloser
	Size       int64
}

// HeadObject API
//
// Returns the size and version of the object specified by bucket/object
func HeadObject(baseParams BaseParams, bck cmn.Bck, object string, checkExists ...bool) (*cmn.ObjectProps, error) {
	checkIsCached := false
	if len(checkExists) > 0 {
		checkIsCached = checkExists[0]
	}
	baseParams.Method = http.MethodHead
	path := cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object)
	query := make(url.Values)
	query.Add(cmn.URLParamCheckExists, strconv.FormatBool(checkIsCached))
	query = cmn.AddBckToQuery(query, bck)
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
		size      int64
		atime     time.Time
		numCopies int
		present   bool
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

	objProps := &cmn.ObjectProps{
		Size:      size,
		Version:   r.Header.Get(cmn.HeaderObjVersion),
		Atime:     atime,
		Provider:  r.Header.Get(cmn.HeaderObjProvider),
		NumCopies: numCopies,
		Checksum:  r.Header.Get(cmn.HeaderObjCksumVal),
		Present:   present,
	}

	if ecStr := r.Header.Get(cmn.HeaderObjECMeta); ecStr != "" {
		if md, err := ec.StringToMeta(ecStr); err == nil {
			objProps.DataSlices = md.Data
			objProps.ParitySlices = md.Parity
			objProps.IsECCopy = md.IsCopy
		}
	}
	return objProps, nil
}

// DeleteObject API
//
// Deletes an object specified by bucket/object
func DeleteObject(baseParams BaseParams, bck cmn.Bck, object string) error {
	var (
		path   = cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object)
		query  = cmn.AddBckToQuery(nil, bck)
		params = OptionalParams{Query: query}
	)
	baseParams.Method = http.MethodDelete
	_, err := DoHTTPRequest(baseParams, path, nil, params)
	return err
}

// EvictObject API
//
// Evicts an object specified by bucket/object
func EvictObject(baseParams BaseParams, bck cmn.Bck, object string) error {
	var (
		msg, err = jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActEvictObjects, Name: cmn.URLPath(bck.Name, object)})
		path     = cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object)
	)
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodDelete
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
func GetObject(baseParams BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (n int64, err error) {
	var (
		w         = ioutil.Discard
		q         url.Values
		optParams OptionalParams
	)
	if len(options) != 0 {
		w, q = getObjectOptParams(options[0])
	}
	q = cmn.AddBckToQuery(q, bck)
	optParams.Query = q

	baseParams.Method = http.MethodGet
	path := cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object)
	resp, err := doHTTPRequestGetResp(baseParams, path, nil, optParams)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if MMSA == nil { // on demand
		MMSA = memsys.DefaultPageMM()
	}

	buf, slab := MMSA.Alloc()
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
func GetObjectWithValidation(baseParams BaseParams, bck cmn.Bck, object string, options ...GetObjectInput) (n int64, err error) {
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
	path := cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object)
	resp, err := doHTTPRequestGetResp(baseParams, path, nil, optParams)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	hdrHash := resp.Header.Get(cmn.HeaderObjCksumVal)
	hdrHashType := resp.Header.Get(cmn.HeaderObjCksumType)

	if hdrHashType == cmn.ChecksumXXHash {
		if MMSA == nil { // on demand
			MMSA = memsys.DefaultPageMM()
		}
		buf, slab := MMSA.Alloc()
		n, cksumVal, err = cmn.WriteWithHash(w, resp.Body, buf)
		slab.Free(buf)

		if err != nil {
			return 0, fmt.Errorf("failed to calculate xxHash from HTTP response body, err: %v", err)
		}
		if cksumVal != hdrHash {
			return 0, cmn.NewInvalidCksumError(hdrHash, cksumVal)
		}
	} else {
		return 0, fmt.Errorf("can't validate hash types other than %s, object's hash type: %s",
			cmn.ChecksumXXHash, hdrHashType)
	}
	return n, nil
}

// PutObject API
//
// Creates an object from the body of the io.Reader parameter and puts it in the 'bucket' bucket
// If there is an ais bucket and cloud bucket with the same name, specify with provider ("ais", "cloud")
// The object name is specified by the 'object' argument.
// If the object hash passed in is not empty, the value is set
// in the request header with the default checksum type "xxhash"
// Assumes that args.Reader is already opened and ready for usage
func PutObject(args PutObjectArgs, replicateOpts ...ReplicateObjectInput) (err error) {
	var (
		resp *http.Response
		req  *http.Request
	)
	query := cmn.AddBckToQuery(nil, args.Bck)
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   args.BaseParams.URL,
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, args.Bck.Name, args.Object),
		Query:  query,
		BodyR:  args.Reader,
	}

	newRequest := func(reqArgs cmn.ReqArgs) (*http.Request, error) {
		req, err := reqArgs.Req()
		if err != nil {
			return nil, cmn.NewFailedToCreateHTTPRequest(err)
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
		return req, nil
	}

	resp, req, err = doReqWithRetry(args.BaseParams.Client, newRequest, reqArgs)

	if err != nil {
		return fmt.Errorf("failed to %s, err: %v", http.MethodPut, err)
	}
	_, err = checkBadStatus(req, resp) // nolint:bodyclose // it's closed later
	if errC := resp.Body.Close(); err == nil {
		return errC
	}
	return err
}

// AppendObject API
//
// Append builds the object which should be finished with `FlushObject` request.
// It returns handle which works as id for subsequent append requests so the
// correct object can be identified.
//
// NOTE: Until `FlushObject` is called one cannot access the object yet as
// it is yet not fully operational.
func AppendObject(args AppendArgs) (handle string, err error) {
	query := make(url.Values)
	query.Add(cmn.URLParamAppendType, cmn.AppendOp)
	query.Add(cmn.URLParamAppendHandle, args.Handle)
	query = cmn.AddBckToQuery(query, args.Bck)

	var header http.Header
	if args.Size > 0 {
		header = make(http.Header)
		header.Add("Content-Length", strconv.FormatInt(args.Size, 10))
	}

	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   args.BaseParams.URL,
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, args.Bck.Name, args.Object),
		Header: header,
		Query:  query,
		BodyR:  args.Reader,
	}

	newRequest := func(reqArgs cmn.ReqArgs) (*http.Request, error) {
		req, err := reqArgs.Req()
		if err != nil {
			return nil, cmn.NewFailedToCreateHTTPRequest(err)
		}

		// The HTTP package doesn't automatically set this for files, so it has to be done manually
		// If it wasn't set, we would need to deal with the redirect manually.
		req.GetBody = func() (io.ReadCloser, error) {
			return args.Reader.Open()
		}
		setAuthToken(req, args.BaseParams)
		return req, nil
	}

	resp, req, err := doReqWithRetry(args.BaseParams.Client, newRequest, reqArgs)
	if err != nil {
		return "", fmt.Errorf("failed to %s, err: %v", http.MethodPut, err)
	}

	defer resp.Body.Close()
	_, err = checkBadStatus(req, resp) // nolint:bodyclose // it's closed in defer
	return resp.Header.Get(cmn.HeaderAppendHandle), err
}

// Makes Client.Do request and retries it when got Broken Pipe or Connection Refused error
// Should be used for PUT requests as it puts reader into a request
func doReqWithRetry(client *http.Client, newRequest func(_ cmn.ReqArgs) (*http.Request, error), reqArgs cmn.ReqArgs) (resp *http.Response, req *http.Request, err error) {
	var r io.ReadCloser
	reader := reqArgs.BodyR.(cmn.ReadOpenCloser)
	if req, err = newRequest(reqArgs); err != nil {
		return
	}
	if resp, err = client.Do(req); !shouldRetryHTTP(err) { // nolint:bodyclose // it's closed by a caller
		return
	}

	sleep := httpRetrySleep
	for i := 0; i < httpMaxRetries; i++ {
		time.Sleep(sleep)
		sleep += sleep / 2

		if r, err = reader.Open(); err != nil {
			return
		}
		reqArgs.BodyR = r

		if req, err = newRequest(reqArgs); err != nil {
			r.Close()
			return
		}
		if resp, err = client.Do(req); !shouldRetryHTTP(err) { // nolint:bodyclose // closed by a caller
			return
		}
	}
	return
}

func shouldRetryHTTP(err error) bool {
	return err != nil && (cmn.IsErrBrokenPipe(err) || cmn.IsErrConnectionRefused(err))
}

// FlushObject API
//
// Flushing should occur once all appends have finished successfully.
// This call will create a fully operational object and requires handle to be set.
func FlushObject(args AppendArgs) (err error) {
	query := make(url.Values)
	query.Add(cmn.URLParamAppendType, cmn.FlushOp)
	query.Add(cmn.URLParamAppendHandle, args.Handle)
	query = cmn.AddBckToQuery(query, args.Bck)
	params := OptionalParams{Query: query}

	args.BaseParams.Method = http.MethodPut
	path := cmn.URLPath(cmn.Version, cmn.Objects, args.Bck.Name, args.Object)
	_, err = DoHTTPRequest(args.BaseParams, path, nil, params)
	return err
}

// RenameObject API
//
// Creates a cmn.ActionMsg with the new name of the object
// and sends a POST HTTP Request to /v1/objects/bucket-name/object-name
//
// FIXME: handle cloud provider - here and elsewhere
func RenameObject(baseParams BaseParams, bck cmn.Bck, oldName, newName string) error {
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActRename, Name: newName})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, oldName)
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// PromoteFileOrDir API
//
// promote AIS-colocated files and directories to objects (NOTE: advanced usage only)
func PromoteFileOrDir(args *PromoteArgs) error {
	msg := cmn.ActionMsg{Action: cmn.ActPromote, Name: args.FQN}
	msg.Value = &cmn.ActValPromote{
		Target:    args.Target,
		ObjName:   args.Object,
		Recurs:    args.Recurs,
		Overwrite: args.Overwrite,
		Verbose:   args.Verbose,
	}
	msgbody, err := jsoniter.Marshal(&msg)
	if err != nil {
		return err
	}
	query := cmn.AddBckToQuery(nil, args.Bck)
	params := OptionalParams{Query: query}

	args.BaseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Objects, args.Bck.Name)
	_, err = DoHTTPRequest(args.BaseParams, path, msgbody, params)
	return err
}

// ReplicateObject API
//
// ReplicateObject replicates given object in bucket using targetrunner's replicate endpoint.
func ReplicateObject(baseParams BaseParams, bck cmn.Bck, object string) error {
	msg, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActReplicate})
	if err != nil {
		return err
	}
	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Objects, bck.Name, object)
	_, err = DoHTTPRequest(baseParams, path, msg)
	return err
}

// Downloader API

func DownloadSingle(baseParams BaseParams, description string, bck cmn.Bck, objName, link string) (string, error) {
	dlBody := downloader.DlSingleBody{
		DlObj: downloader.DlObj{
			Objname: objName,
			Link:    link,
		},
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadSingleWithParam(baseParams, dlBody)
}

func DownloadSingleWithParam(baseParams BaseParams, dlBody downloader.DlSingleBody) (string, error) {
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	return doDlDownloadRequest(baseParams, path, nil, optParams)
}

func DownloadRange(baseParams BaseParams, description string, bck cmn.Bck, template string) (string, error) {
	dlBody := downloader.DlRangeBody{
		Template: template,
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadRangeWithParam(baseParams, dlBody)
}

func DownloadRangeWithParam(baseParams BaseParams, dlBody downloader.DlRangeBody) (string, error) {
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	return doDlDownloadRequest(baseParams, path, nil, optParams)
}

func DownloadMulti(baseParams BaseParams, description string, bck cmn.Bck, m interface{}) (string, error) {
	dlBody := downloader.DlMultiBody{}
	dlBody.Bck = bck
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

func DownloadCloud(baseParams BaseParams, description string, bck cmn.Bck, prefix, suffix string) (string, error) {
	dlBody := downloader.DlCloudBody{
		Prefix: prefix,
		Suffix: suffix,
	}
	dlBody.Bck = bck
	dlBody.Description = description
	return DownloadCloudWithParam(baseParams, dlBody)
}

func DownloadCloudWithParam(baseParams BaseParams, dlBody downloader.DlCloudBody) (string, error) {
	query := dlBody.AsQuery()

	baseParams.Method = http.MethodPost
	path := cmn.URLPath(cmn.Version, cmn.Download)
	optParams := OptionalParams{
		Query: query,
	}
	return doDlDownloadRequest(baseParams, path, nil, optParams)
}

func DownloadStatus(baseParams BaseParams, id string) (downloader.DlStatusResp, error) {
	dlBody := downloader.DlAdminBody{
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

func DownloadGetList(baseParams BaseParams, regex string) (map[string]downloader.DlJobInfo, error) {
	dlBody := downloader.DlAdminBody{
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
	var parsedResp map[string]downloader.DlJobInfo
	err = jsoniter.Unmarshal(resp, &parsedResp)
	if err != nil {
		return nil, err
	}
	return parsedResp, nil
}

func DownloadAbort(baseParams BaseParams, id string) error {
	dlBody := downloader.DlAdminBody{
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

func DownloadRemove(baseParams BaseParams, id string) error {
	dlBody := downloader.DlAdminBody{
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

func doDlDownloadRequest(baseParams BaseParams, path string, msg []byte, optParams OptionalParams) (string, error) {
	respBytes, err := DoHTTPRequest(baseParams, path, msg, optParams)
	if err != nil {
		return "", err
	}

	var resp downloader.DlPostResp
	err = jsoniter.Unmarshal(respBytes, &resp)
	if err != nil {
		return "", err
	}

	return resp.ID, nil
}

func doDlStatusRequest(baseParams BaseParams, path string, optParams OptionalParams) (resp downloader.DlStatusResp, err error) {
	respBytes, err := DoHTTPRequest(baseParams, path, nil, optParams)
	if err != nil {
		return resp, err
	}

	err = jsoniter.Unmarshal(respBytes, &resp)
	return resp, err
}
