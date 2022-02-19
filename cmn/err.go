// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"errors"
	"fmt"
	iofs "io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

// This source contains common AIS node inter-module errors -
// the errors that some AIS packages (within a given running AIS node) return
// and other AIS packages handle.

const (
	stackTracePrefix = "stack: ["

	fmtErrBckName = "bucket name %q is invalid: may only contain letters, numbers, dashes (-), underscores (_), and dots (.)"

	FmtErrIntegrity      = "[%s%d, for troubleshooting see %s/blob/master/docs/troubleshooting.md]"
	FmtErrUnmarshal      = "%s: failed to unmarshal %s (%s), err: %w"
	FmtErrMorphUnmarshal = "%s: failed to unmarshal %s (%T), err: %w"
	FmtErrUnsupported    = "%s: %s is not supported"
	FmtErrUnknown        = "%s: unknown %s %q"

	FmtErrLogFailed  = "%s: failed to %s %s, err: %v" // node: action, obj, err (glog)
	FmtErrWrapFailed = "%s: failed to %s %s, err: %w" // ditto (wrapped error)

	EmptyProtoSchemeForURL = "empty protocol scheme for URL path"
)

type (
	ErrBucketAlreadyExists struct{ bck Bck }
	ErrRemoteBckNotFound   struct{ bck Bck }
	ErrRemoteBucketOffline struct{ bck Bck }
	ErrBckNotFound         struct{ bck Bck }
	ErrBucketIsBusy        struct{ bck Bck }

	ErrInvalidBucketProvider struct {
		bck Bck
	}
	ErrCapacityExceeded struct {
		totalBytes     uint64
		totalBytesUsed uint64
		highWM         int64
		usedPct        int32
		oos            bool
	}
	ErrBucketAccessDenied struct{ errAccessDenied }
	ErrObjectAccessDenied struct{ errAccessDenied }
	errAccessDenied       struct {
		entity      string
		operation   string
		accessAttrs AccessAttrs
	}

	ErrInvalidCksum struct {
		expectedHash string
		actualHash   string
	}

	ErrMountpathNotFound struct {
		mpath    string
		fqn      string
		disabled bool
	}
	ErrInvalidMountpath struct {
		mpath string
		cause string
	}
	ErrInvalidFSPathsConf struct {
		err error
	}

	ErrNoNodes struct {
		role string
	}
	ErrXactionNotFound struct {
		cause string
	}
	ErrObjDefunct struct {
		name   string // object's name
		d1, d2 uint64 // lom.md.(bucket-ID) and lom.bck.(bucket-ID), respectively
	}
	ErrAborted struct {
		what string
		ctx  string
		err  error
		//
		timestamp time.Time
	}
	ErrNotFound struct {
		what string
	}
	ErrInitBackend struct {
		Provider string
	}
	ErrMissingBackend struct {
		Provider string
	}
	ErrETL struct {
		Reason string
		ETLErrorContext
	}
	ETLErrorContext struct {
		TID     string
		UUID    string
		ETLName string
		PodName string
		SvcName string
	}
	ErrSoft struct {
		what string
	}
	// Error structure for HTTP errors
	ErrHTTP struct {
		Status     int    `json:"status"`
		Message    string `json:"message"`
		Method     string `json:"method"`
		URLPath    string `json:"url_path"`
		RemoteAddr string `json:"remote_addr"`
		Caller     string `json:"caller"`
		Node       string `json:"node"`
		trace      []byte
	}

	ErrLmetaCorrupted struct {
		err error
	}
	ErrLmetaNotFound struct {
		err error
	}

	ErrLimitedCoexistence struct {
		node    string // this (local) node
		xaction string
		action  string
		detail  string
	}
	ErrUsePrevXaction struct { // equivalent to xreg.WprUse
		xaction string
	}
)

var (
	ErrSkip             = errors.New("skip")
	ErrStartupTimeout   = errors.New("startup timeout")
	ErrQuiesceTimeout   = errors.New("timed out waiting for quiescence")
	ErrNotEnoughTargets = errors.New("not enough target nodes")
	ErrETLMissingUUID   = errors.New("ETL UUID can't be empty")
	ErrNoMountpaths     = errors.New("no mountpaths")
	// aborts
	ErrXactRenewAbort   = errors.New("renewal abort")
	ErrXactUserAbort    = errors.New("user abort")              // via cmn.ActXactStop
	ErrXactICNotifAbort = errors.New("IC(notifications) abort") // ditto
	ErrXactNoErrAbort   = errors.New("no-error abort")
)

// ais ErrBucketAlreadyExists

func NewErrBckAlreadyExists(bck Bck) *ErrBucketAlreadyExists {
	return &ErrBucketAlreadyExists{bck: bck}
}

func (e *ErrBucketAlreadyExists) Error() string {
	return fmt.Sprintf("bucket %q already exists", e.bck)
}

func IsErrBucketAlreadyExists(err error) bool {
	_, ok := err.(*ErrBucketAlreadyExists)
	return ok
}

// remote ErrRemoteBckNotFound (compare with ErrBckNotFound)

func NewErrRemoteBckNotFound(bck Bck) *ErrRemoteBckNotFound {
	return &ErrRemoteBckNotFound{bck: bck}
}

func (e *ErrRemoteBckNotFound) Error() string {
	if e.bck.IsCloud() {
		return fmt.Sprintf("cloud bucket %q does not exist", e.bck)
	}
	return fmt.Sprintf("remote bucket %q does not exist", e.bck)
}

func IsErrRemoteBckNotFound(err error) bool {
	_, ok := err.(*ErrRemoteBckNotFound)
	return ok
}

// ErrBckNotFound - applies to ais buckets exclusively
// (compare with ErrRemoteBckNotFound)

func NewErrBckNotFound(bck Bck) *ErrBckNotFound {
	return &ErrBckNotFound{bck: bck}
}

func (e *ErrBckNotFound) Error() string {
	return fmt.Sprintf("bucket %q does not exist", e.bck)
}

func IsErrBckNotFound(err error) bool {
	_, ok := err.(*ErrBckNotFound)
	return ok
}

// ErrRemoteBucketOffline

func NewErrRemoteBckOffline(bck Bck) *ErrRemoteBucketOffline {
	return &ErrRemoteBucketOffline{bck: bck}
}

func (e *ErrRemoteBucketOffline) Error() string {
	return fmt.Sprintf("bucket %q is currently unreachable", e.bck)
}

func isErrRemoteBucketOffline(err error) bool {
	_, ok := err.(*ErrRemoteBucketOffline)
	return ok
}

// ErrInvalidBucketProvider

func NewErrorInvalidBucketProvider(bck Bck) *ErrInvalidBucketProvider {
	return &ErrInvalidBucketProvider{bck: bck}
}

func (e *ErrInvalidBucketProvider) Error() string {
	if e.bck.Name != "" {
		return fmt.Sprintf("invalid backend provider %q for bucket %s: must be one of [%s]",
			e.bck.Provider, e.bck, allProviders)
	}
	return fmt.Sprintf("invalid backend provider %q: must be one of [%s]", e.bck.Provider, allProviders)
}

func (*ErrInvalidBucketProvider) Is(target error) bool {
	_, ok := target.(*ErrInvalidBucketProvider)
	return ok
}

// ErrBucketIsBusy

func NewErrBckIsBusy(bck Bck) *ErrBucketIsBusy {
	return &ErrBucketIsBusy{bck: bck}
}

func (e *ErrBucketIsBusy) Error() string {
	return fmt.Sprintf("bucket %q is currently busy, please retry later", e.bck)
}

// errAccessDenied & ErrBucketAccessDenied

func (e *errAccessDenied) String() string {
	return fmt.Sprintf("%s: %s access denied (%#x)", e.entity, e.operation, e.accessAttrs)
}

func (e *ErrBucketAccessDenied) Error() string {
	return "bucket " + e.String()
}

func NewBucketAccessDenied(bucket, oper string, aattrs AccessAttrs) *ErrBucketAccessDenied {
	return &ErrBucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}

func (e *ErrObjectAccessDenied) Error() string {
	return "object " + e.String()
}

func NewObjectAccessDenied(object, oper string, aattrs AccessAttrs) *ErrObjectAccessDenied {
	return &ErrObjectAccessDenied{errAccessDenied{object, oper, aattrs}}
}

// ErrCapacityExceeded

func NewErrCapacityExceeded(highWM int64, totalBytesUsed, totalBytes uint64, usedPct int32, oos bool) *ErrCapacityExceeded {
	return &ErrCapacityExceeded{
		highWM:         highWM,
		usedPct:        usedPct,
		totalBytes:     totalBytes, // avail + used
		totalBytesUsed: totalBytesUsed,
		oos:            oos,
	}
}

func (e *ErrCapacityExceeded) Error() string {
	suffix := fmt.Sprintf("total used %s out of %s", cos.B2S(int64(e.totalBytesUsed), 2),
		cos.B2S(int64(e.totalBytes), 2))
	if e.oos {
		return fmt.Sprintf("out of space: used %d%% of total capacity on at least one of the mountpaths (%s)",
			e.usedPct, suffix)
	}
	return fmt.Sprintf("low on free space: used capacity %d%% exceeded high watermark(%d%%) (%s)",
		e.usedPct, e.highWM, suffix)
}

func IsErrCapacityExceeded(err error) bool {
	_, ok := err.(*ErrCapacityExceeded)
	return ok
}

// ErrInvalidCksum

func (e *ErrInvalidCksum) Error() string {
	return fmt.Sprintf("checksum: expected [%s], actual [%s]", e.expectedHash, e.actualHash)
}

func NewErrInvalidCksum(eHash, aHash string) *ErrInvalidCksum {
	return &ErrInvalidCksum{actualHash: aHash, expectedHash: eHash}
}

func (e *ErrInvalidCksum) Expected() string { return e.expectedHash }

// ErrMountpathNotFound

func (e *ErrMountpathNotFound) Error() string {
	if e.mpath != "" {
		if e.disabled {
			return "mountpath " + e.mpath + " is disabled"
		}
		return "mountpath " + e.mpath + " does not exist"
	}
	debug.Assert(e.fqn != "")
	if e.disabled {
		return "mountpath for fqn " + e.fqn + " is disabled"
	}
	return "mountpath for fqn " + e.fqn + " does not exist"
}

func NewErrMountpathNotFound(mpath, fqn string, disabled bool) *ErrMountpathNotFound {
	return &ErrMountpathNotFound{mpath: mpath, fqn: fqn, disabled: disabled}
}

func IsErrMountpathNotFound(err error) bool {
	_, ok := err.(*ErrMountpathNotFound)
	return ok
}

// ErrInvalidMountpath

func (e *ErrInvalidMountpath) Error() string {
	return "invalid mountpath [" + e.mpath + "]; " + e.cause
}

func NewErrInvalidaMountpath(mpath, cause string) *ErrInvalidMountpath {
	return &ErrInvalidMountpath{mpath: mpath, cause: cause}
}

// ErrInvalidFSPathsConf

func NewErrInvalidFSPathsConf(err error) *ErrInvalidFSPathsConf {
	return &ErrInvalidFSPathsConf{err}
}

func (e *ErrInvalidFSPathsConf) Error() string {
	return fmt.Sprintf("invalid \"fspaths\" configuration: %v", e.err)
}

// ErrNoNodes

func NewErrNoNodes(role string) *ErrNoNodes {
	return &ErrNoNodes{role: role}
}

func (e *ErrNoNodes) Error() string {
	if e.role == Proxy {
		return "no available proxies"
	}
	debug.Assert(e.role == Target)
	return "no available targets"
}

// ErrXactionNotFound

func (e *ErrXactionNotFound) Error() string {
	return "xaction " + e.cause + " not found"
}

func NewErrXactNotFoundError(cause string) *ErrXactionNotFound {
	return &ErrXactionNotFound{cause: cause}
}

// ErrObjDefunct

func (e *ErrObjDefunct) Error() string {
	return fmt.Sprintf("%s is defunct (%d != %d)", e.name, e.d1, e.d2)
}

func NewErrObjDefunct(name string, d1, d2 uint64) *ErrObjDefunct {
	return &ErrObjDefunct{name, d1, d2}
}

func isErrObjDefunct(err error) bool {
	_, ok := err.(*ErrObjDefunct)
	return ok
}

// ErrAborted

func NewErrAborted(what, ctx string, err error) *ErrAborted {
	return &ErrAborted{what: what, ctx: ctx, err: err, timestamp: time.Now()}
}

func (e *ErrAborted) Error() (s string) {
	s = fmt.Sprintf("%s aborted at %s", e.what, cos.FormatTimestamp(e.timestamp))
	if e.err != nil {
		s = fmt.Sprintf("%s, err: %v", s, e.err)
	}
	if e.ctx != "" {
		s += " (" + e.ctx + ")"
	}
	return
}

func (e *ErrAborted) Unwrap() (err error) { return e.err }

func IsErrAborted(err error) bool { return AsErrAborted(err) != nil }

func AsErrAborted(err error) (errAborted *ErrAborted) {
	var ok bool
	if errAborted, ok = err.(*ErrAborted); ok {
		return
	}
	target := &ErrAborted{}
	if errors.As(err, &target) {
		errAborted = target
	}
	return
}

// ErrNotFound

func NewErrNotFound(format string, a ...interface{}) *ErrNotFound {
	return &ErrNotFound{fmt.Sprintf(format, a...)}
}

func (e *ErrNotFound) Error() string { return e.what + " does not exist" }

func IsErrNotFound(err error) bool {
	if _, ok := err.(*ErrNotFound); ok {
		return true
	}
	return errors.Is(err, &ErrNotFound{})
}

// ErrInitBackend & ErrMissingBackend

func (e *ErrInitBackend) Error() string {
	return fmt.Sprintf(
		"cannot initialize %q backend (present in the cluster configuration): missing %s-supporting libraries in the build",
		e.Provider, e.Provider,
	)
}

func (e *ErrMissingBackend) Error() string {
	return fmt.Sprintf(
		"%q backend is missing in the cluster configuration. Hint: consider redeploying with -override_backends command-line and the corresponding build tag.", e.Provider)
}

// ErrETL

func NewErrETL(ctx *ETLErrorContext, format string, a ...interface{}) *ErrETL {
	e := &ErrETL{
		Reason: fmt.Sprintf(format, a...),
	}
	return e.WithContext(ctx)
}

func (e *ErrETL) Error() string {
	s := make([]string, 0, 3)
	if e.TID != "" {
		s = append(s, fmt.Sprintf("t[%s]", e.TID))
	}
	if e.UUID != "" {
		s = append(s, fmt.Sprintf("uuid=%q", e.UUID))
	}
	if e.ETLName != "" {
		s = append(s, fmt.Sprintf("etl=%q", e.ETLName))
	}
	if e.PodName != "" {
		s = append(s, fmt.Sprintf("pod=%q", e.PodName))
	}
	if e.SvcName != "" {
		s = append(s, fmt.Sprintf("service=%q", e.SvcName))
	}
	return fmt.Sprintf("[%s] %s", strings.Join(s, ","), e.Reason)
}

func (e *ErrETL) withUUID(uuid string) *ErrETL {
	if uuid != "" {
		e.UUID = uuid
	}
	return e
}

func (e *ErrETL) withTarget(tid string) *ErrETL {
	if tid != "" {
		e.TID = tid
	}
	return e
}

func (e *ErrETL) withETLName(name string) *ErrETL {
	if name != "" {
		e.ETLName = name
	}
	return e
}

func (e *ErrETL) withSvcName(name string) *ErrETL {
	if name != "" {
		e.SvcName = name
	}
	return e
}

func (e *ErrETL) WithPodName(name string) *ErrETL {
	if name != "" {
		e.PodName = name
	}
	return e
}

func (e *ErrETL) WithContext(ctx *ETLErrorContext) *ErrETL {
	if ctx == nil {
		return e
	}
	return e.
		withTarget(ctx.TID).
		withUUID(ctx.UUID).
		WithPodName(ctx.PodName).
		withETLName(ctx.ETLName).
		withSvcName(ctx.SvcName)
}

// ErrSoft
// non-critical and can be ignored in certain cases (e.g, when `--force` is set)

func NewErrSoft(what string) *ErrSoft {
	return &ErrSoft{what}
}

func (e *ErrSoft) Error() string {
	return e.what
}

func IsErrSoft(err error) bool {
	if _, ok := err.(*ErrSoft); ok {
		return true
	}
	target := &ErrSoft{}
	return errors.As(err, &target)
}

///////////////////////
// ErrLmetaCorrupted & ErrLmetaNotFound
///////////////////////

func NewErrLmetaCorrupted(err error) *ErrLmetaCorrupted { return &ErrLmetaCorrupted{err} }
func (e *ErrLmetaCorrupted) Error() string              { return e.err.Error() }

func IsErrLmetaCorrupted(err error) bool {
	_, ok := err.(*ErrLmetaCorrupted)
	return ok
}

func NewErrLmetaNotFound(err error) *ErrLmetaNotFound { return &ErrLmetaNotFound{err} }
func (e *ErrLmetaNotFound) Error() string             { return e.err.Error() }

func IsErrLmetaNotFound(err error) bool {
	_, ok := err.(*ErrLmetaNotFound)
	return ok
}

///////////////////////////
// ErrLimitedCoexistence //
///////////////////////////
func NewErrLimitedCoexistence(node, xaction, action, detail string) *ErrLimitedCoexistence {
	return &ErrLimitedCoexistence{node, xaction, action, detail}
}

func (e *ErrLimitedCoexistence) Error() string {
	return fmt.Sprintf("%s: %s is currently running, cannot run %q(%s) concurrently",
		e.node, e.xaction, e.action, e.detail)
}

///////////////////////
// ErrUsePrevXaction //
///////////////////////
func NewErrUsePrevXaction(xaction string) *ErrUsePrevXaction {
	return &ErrUsePrevXaction{xaction}
}

func (e *ErrUsePrevXaction) Error() string {
	return fmt.Sprintf("%s is already running - not starting", e.xaction)
}

func IsErrUsePrevXaction(err error) bool {
	_, ok := err.(*ErrUsePrevXaction)
	return ok
}

////////////////////////////
// error grouping helpers //
////////////////////////////

// nought: not a thing
func IsErrBucketNought(err error) bool {
	return IsErrBckNotFound(err) || IsErrRemoteBckNotFound(err) || isErrRemoteBucketOffline(err)
}

func IsErrObjNought(err error) bool {
	return IsObjNotExist(err) || IsStatusNotFound(err) || isErrObjDefunct(err) || IsErrLmetaNotFound(err)
}

// usage: lom.Load() (compare w/ IsNotExist)
func IsObjNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	return errors.Is(err, iofs.ErrNotExist) // when wrapped
}

// usage: everywhere where applicable (directories, xactions, nodes, ...)
// excluding LOM (see above)
func IsNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	if errors.Is(err, iofs.ErrNotExist) {
		return true
	}
	return IsErrNotFound(err)
}

func IsErrBucketLevel(err error) bool { return IsErrBucketNought(err) }
func IsErrObjLevel(err error) bool    { return IsErrObjNought(err) }

/////////////
// ErrHTTP //
/////////////

func NewErrHTTP(r *http.Request, msg string, errCode ...int) (e *ErrHTTP) {
	status := http.StatusBadRequest
	if len(errCode) > 0 && errCode[0] > status {
		status = errCode[0]
	}
	e = allocHTTPErr()
	e.Status, e.Message = status, msg
	if r != nil {
		e.Method, e.URLPath = r.Method, r.URL.Path
		e.RemoteAddr = r.RemoteAddr
		e.Caller = r.Header.Get(HdrCallerName)
	}
	e.Node = thisNodeName
	return
}

func IsStatusServiceUnavailable(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusServiceUnavailable
}

func IsStatusNotFound(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusNotFound
}

func IsStatusBadGateway(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusBadGateway
}

func IsStatusGone(err error) (yes bool) {
	hErr, ok := err.(*ErrHTTP)
	if !ok {
		return false
	}
	return hErr.Status == http.StatusGone
}

func S2HTTPErr(r *http.Request, msg string, status int) *ErrHTTP {
	if msg != "" {
		var httpErr ErrHTTP
		if err := jsoniter.UnmarshalFromString(msg, &httpErr); err == nil {
			return &httpErr
		}
	}
	return NewErrHTTP(r, msg, status)
}

func Err2HTTPErr(err error) (httpErr *ErrHTTP) {
	var ok bool
	if httpErr, ok = err.(*ErrHTTP); ok {
		return
	}
	httpErr = &ErrHTTP{}
	if !errors.As(err, &httpErr) {
		httpErr = nil
	}
	return
}

// Example:
// Bad Request: Bucket abc does not appear to be local or does not exist:
// DELETE /v1/buckets/abc from (127.0.0.1:54064, vhsjxq8000) stack: (httpcommon.go:840 <- proxy.go:484 <- proxy.go:264)
func (e *ErrHTTP) String() (s string) {
	s = http.StatusText(e.Status) + ": " + e.Message
	if e.Method != "" || e.URLPath != "" {
		if !strings.HasSuffix(s, ".") {
			s += ":"
		}
		if e.Method != "" {
			s += " " + e.Method
		}
		if e.URLPath != "" {
			s += " " + e.URLPath
		}
	}
	if thisNodeName != "" && !strings.Contains(e.Message, thisNodeName) {
		s += " (failed at " + thisNodeName + ")"
	}
	if e.Caller != "" {
		s += " (called by " + e.Caller + ")"
	}
	return s + " (" + string(e.trace) + ")"
}

func (e *ErrHTTP) Error() string {
	// Stop from escaping `<`, `>` and `&`.
	buf := new(bytes.Buffer)
	enc := jsoniter.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(e); err != nil {
		return err.Error()
	}
	return buf.String()
}

func (e *ErrHTTP) write(w http.ResponseWriter, r *http.Request, silent bool) {
	msg := e.Error()
	if len(msg) > 0 {
		// Error strings should not be capitalized (lint).
		if c := msg[0]; c >= 'A' && c <= 'Z' {
			msg = string(c+'a'-'A') + msg[1:]
		}
	}
	if !strings.Contains(msg, stackTracePrefix) {
		e.populateStackTrace()
	}
	if !silent {
		s := e.String()
		if thisNodeName != "" && !strings.Contains(e.Message, thisNodeName) {
			// node name instead of generic stack:
			replaced1 := strings.Replace(s, stackTracePrefix, thisNodeName+": ", 1)
			if replaced1 != s {
				replaced2 := strings.Replace(replaced1, " (failed at "+thisNodeName+")", "", 1)
				if replaced2 != replaced1 {
					s = replaced2
				}
			}
		}
		glog.Errorln(s)
	}
	// Make sure that the caller is aware that we return JSON error.
	w.Header().Set(HdrContentType, ContentJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method == http.MethodHead {
		w.Header().Set(HdrError, e.Error())
		w.WriteHeader(e.Status)
	} else {
		w.WriteHeader(e.Status)
		fmt.Fprintln(w, e.Error())
	}
}

func (e *ErrHTTP) populateStackTrace() {
	debug.Assert(len(e.trace) == 0)

	buffer := bytes.NewBuffer(e.trace)
	fmt.Fprint(buffer, stackTracePrefix)
	for i := 1; i < 9; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if !strings.Contains(file, "aistore") {
			break
		}
		f := filepath.Base(file)
		if f == "err.go" {
			continue
		}
		if buffer.Len() > len(stackTracePrefix) {
			buffer.WriteString(" <- ")
		}
		fmt.Fprintf(buffer, "%s:%d", f, line)
	}
	fmt.Fprint(buffer, "]")
	e.trace = buffer.Bytes()
}

//////////////////////////////
// invalid message handlers //
//////////////////////////////

// Write error into HTTP response.
func WriteErr(w http.ResponseWriter, r *http.Request, err error, opts ...int) {
	if httpErr := Err2HTTPErr(err); httpErr != nil {
		status := http.StatusBadRequest
		if len(opts) > 0 && opts[0] > status {
			status = opts[0]
		}
		httpErr.Status = status
		httpErr.write(w, r, len(opts) > 1 /*silent*/)
	} else if IsErrNotFound(err) {
		if len(opts) > 0 {
			// Override the status code.
			opts[0] = http.StatusNotFound
		} else {
			// Add status code if not set.
			opts = append(opts, http.StatusNotFound)
		}
		WriteErrMsg(w, r, err.Error(), opts...)
	} else {
		WriteErrMsg(w, r, err.Error(), opts...)
	}
}

// Create ErrHTTP (based on `msg` and `opts`) and write it into HTTP response.
func WriteErrMsg(w http.ResponseWriter, r *http.Request, msg string, opts ...int) {
	httpErr := NewErrHTTP(r, msg, opts...)
	httpErr.write(w, r, len(opts) > 1 /*silent*/)
	FreeHTTPErr(httpErr)
}

// 405 Method Not Allowed, see: https://tools.ietf.org/html/rfc2616#section-10.4.6
func WriteErr405(w http.ResponseWriter, r *http.Request, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

/////////////
// errPool //
/////////////

var (
	errPool sync.Pool
	err0    ErrHTTP
)

func allocHTTPErr() (a *ErrHTTP) {
	if v := errPool.Get(); v != nil {
		a = v.(*ErrHTTP)
		return
	}
	return &ErrHTTP{}
}

func FreeHTTPErr(a *ErrHTTP) {
	trace := a.trace
	*a = err0
	if trace != nil {
		a.trace = trace[:0]
	}
	errPool.Put(a)
}
