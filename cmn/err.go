// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	jsoniter "github.com/json-iterator/go"
)

// This source contains common AIS node inter-module errors -
// the errors that some AIS packages (within a given running AIS node) return
// and other AIS packages handle.

const (
	stackTracePrefix = "stack: ["

	fmtErrBckName   = "bucket name %q is invalid: " + cos.OnlyPlus
	fmtErrNamespace = "bucket namespace (uuid: %q, name: %q) " + cos.OnlyNice

	FmtErrIntegrity      = "[%s%d, for troubleshooting see %s/blob/master/docs/troubleshooting.md]"
	FmtErrUnmarshal      = "%s: failed to unmarshal %s (%s), err: %w"
	FmtErrMorphUnmarshal = "%s: failed to unmarshal %s (%T), err: %w"
	FmtErrUnknown        = "%s: unknown %s %q"
	FmtErrBackwardCompat = "%v (backward compatibility is supported only one version back, e.g. 3.9 => 3.10)"

	// (see ErrFailedTo)
	fmtErrFailedTo = "%s: failed to %s %s, err: %w"

	BadSmapPrefix = "[bad cluster map]"
)

type (
	ErrBucketAlreadyExists struct{ bck Bck }
	ErrRemoteBckNotFound   struct{ bck Bck }
	ErrRemoteBucketOffline struct{ bck Bck }
	ErrBckNotFound         struct{ bck Bck }
	ErrBucketIsBusy        struct{ bck Bck }

	ErrFailedTo struct {
		actor  any    // most of the time it's this (target|proxy) node but may also be some other "actor"
		what   any    // not necessarily LOM
		err    error  // original error that can be Unwrap-ed
		action string // not necessarily msg.Action
		status int    // http status, if available
	}
	ErrUnsupp struct {
		action, what string
	}
	ErrNotImpl struct {
		action, what string
	}

	ErrInvalidBackendProvider struct {
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
		accessAttrs apc.AccessAttrs
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
		role    string
		mmcount int // maintenance mode
	}
	ErrXactNotFound struct {
		cause string
	}
	ErrObjDefunct struct {
		name   string // object's name
		d1, d2 uint64 // lom.md.(bucket-ID) and lom.bck.(bucket-ID), respectively
	}
	ErrAborted struct {
		err  error
		what string
		ctx  string
		//
		timestamp time.Time
	}
	ErrInitBackend struct {
		Provider string
	}
	ErrMissingBackend struct {
		Provider string
		Msg      string
	}
	ErrETL struct {
		Reason string
		ETLErrCtx
	}
	ETLErrCtx struct {
		TID     string
		ETLName string
		PodName string
		SvcName string
	}
	ErrSoft struct {
		what string
	}
	// Error structure for HTTP errors
	ErrHTTP struct {
		TypeCode   string `json:"tcode,omitempty"`
		Message    string `json:"message"`
		Method     string `json:"method"`
		URLPath    string `json:"url_path"`
		RemoteAddr string `json:"remote_addr"`
		Caller     string `json:"caller"`
		Node       string `json:"node"`
		trace      []byte
		Status     int `json:"status"`
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
	ErrXactUsePrev struct { // equivalent to xreg.WprUse
		xaction string
	}
	ErrXactTgtInMaint struct {
		xaction string
		tname   string
	}

	ErrStreamTerminated struct {
		err    error
		stream string
		reason string
		detail string
	}
)

var (
	thisNodeName string
	cleanPathErr func(error)
)

func InitErrs(a string, b func(error)) { thisNodeName, cleanPathErr = a, b }

var (
	ErrSkip             = errors.New("skip")
	ErrStartupTimeout   = errors.New("startup timeout")
	ErrQuiesceTimeout   = errors.New("timed out waiting for quiescence")
	ErrNotEnoughTargets = errors.New("not enough target nodes")
	ErrNoMountpaths     = errors.New("no mountpaths")

	// aborts
	ErrXactRenewAbort   = errors.New("renewal abort")
	ErrXactUserAbort    = errors.New("user abort")              // via apc.ActXactStop
	ErrXactICNotifAbort = errors.New("IC(notifications) abort") // ditto
)

// ErrFailedTo

func NewErrFailedTo(actor any, action string, what any, err error, errCode ...int) *ErrFailedTo {
	if e, ok := err.(*ErrFailedTo); ok {
		return e
	}

	_clean(err)

	e := &ErrFailedTo{actor: actor, action: action, what: what, err: err, status: 0}
	if actor == nil {
		e.actor = thisNodeName
	}
	if len(errCode) > 0 {
		e.status = errCode[0]
	}
	return e
}

func (e *ErrFailedTo) Error() string {
	err := fmt.Errorf(fmtErrFailedTo, e.actor, e.action, e.what, e.err)
	return err.Error()
}

func (e *ErrFailedTo) Unwrap() (err error) { return e.err }

// ErrStreamTerminated

func NewErrStreamTerminated(stream string, err error, reason, detail string) *ErrStreamTerminated {
	return &ErrStreamTerminated{stream: stream, err: err, reason: reason, detail: detail}
}

func (e *ErrStreamTerminated) Error() string {
	return fmt.Sprintf("%s terminated(%q, %v): %s", e.stream, e.reason, e.err, e.detail)
}

func (e *ErrStreamTerminated) Unwrap() (err error) { return e.err }

func IsErrStreamTerminated(err error) bool {
	_, ok := err.(*ErrStreamTerminated)
	return ok
}

// ErrUnsupp & ErrNotImpl

func NewErrUnsupp(action, what string) *ErrUnsupp { return &ErrUnsupp{action, what} }

func (e *ErrUnsupp) Error() string {
	return fmt.Sprintf("cannot %s %s - operation not supported", e.action, e.what)
}

func NewErrNotImpl(action, what string) *ErrNotImpl { return &ErrNotImpl{action, what} }

func (e *ErrNotImpl) Error() string {
	return fmt.Sprintf("cannot %s %s - not impemented yet", e.action, e.what)
}

// (ais) ErrBucketAlreadyExists

func NewErrBckAlreadyExists(bck *Bck) *ErrBucketAlreadyExists {
	return &ErrBucketAlreadyExists{bck: *bck}
}

func (e *ErrBucketAlreadyExists) Error() string {
	return fmt.Sprintf("bucket %q already exists", e.bck)
}

func IsErrBucketAlreadyExists(err error) bool {
	_, ok := err.(*ErrBucketAlreadyExists)
	return ok
}

// remote ErrRemoteBckNotFound (compare with ErrBckNotFound)

func NewErrRemoteBckNotFound(bck *Bck) *ErrRemoteBckNotFound {
	return &ErrRemoteBckNotFound{bck: *bck}
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

func NewErrBckNotFound(bck *Bck) *ErrBckNotFound {
	return &ErrBckNotFound{bck: *bck}
}

func (e *ErrBckNotFound) Error() string {
	return fmt.Sprintf("bucket %q does not exist", e.bck)
}

func IsErrBckNotFound(err error) bool {
	_, ok := err.(*ErrBckNotFound)
	return ok
}

// ErrRemoteBucketOffline

func NewErrRemoteBckOffline(bck *Bck) *ErrRemoteBucketOffline {
	return &ErrRemoteBucketOffline{bck: *bck}
}

func (e *ErrRemoteBucketOffline) Error() string {
	return fmt.Sprintf("bucket %q is currently unreachable", e.bck)
}

func isErrRemoteBucketOffline(err error) bool {
	_, ok := err.(*ErrRemoteBucketOffline)
	return ok
}

// ErrInvalidBackendProvider

func (e *ErrInvalidBackendProvider) Error() string {
	if e.bck.Name != "" {
		return fmt.Sprintf("invalid backend provider %q for bucket %s: must be one of [%s]",
			e.bck.Provider, e.bck, apc.AllProviders)
	}
	return fmt.Sprintf("invalid backend provider %q: must be one of [%s]", e.bck.Provider, apc.AllProviders)
}

func (*ErrInvalidBackendProvider) Is(target error) bool {
	_, ok := target.(*ErrInvalidBackendProvider)
	return ok
}

// ErrBucketIsBusy

func NewErrBckIsBusy(bck *Bck) *ErrBucketIsBusy {
	return &ErrBucketIsBusy{bck: *bck}
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

func NewBucketAccessDenied(bucket, oper string, aattrs apc.AccessAttrs) *ErrBucketAccessDenied {
	return &ErrBucketAccessDenied{errAccessDenied{bucket, oper, aattrs}}
}

func (e *ErrObjectAccessDenied) Error() string {
	return "object " + e.String()
}

func NewObjectAccessDenied(object, oper string, aattrs apc.AccessAttrs) *ErrObjectAccessDenied {
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
	suffix := fmt.Sprintf("total used %s out of %s", cos.ToSizeIEC(int64(e.totalBytesUsed), 2),
		cos.ToSizeIEC(int64(e.totalBytes), 2))
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

func (e *ErrInvalidFSPathsConf) Unwrap() (err error) { return e.err }

func (e *ErrInvalidFSPathsConf) Error() string {
	return fmt.Sprintf("invalid \"fspaths\" configuration: %v", e.err)
}

// ErrNoNodes

func NewErrNoNodes(role string, mmcount int) *ErrNoNodes {
	return &ErrNoNodes{role: role, mmcount: mmcount}
}

func (e *ErrNoNodes) Error() (s string) {
	var what string
	if e.role == apc.Proxy {
		what = "gateway"
		s = "no proxies (gateways) in the cluster"
	} else {
		debug.Assert(e.role == apc.Target)
		what = "target"
		s = "no storage targets in the cluster"
	}
	if e.mmcount > 0 {
		s += fmt.Sprintf(" (%d %s%s in maintenance mode or being decommissioned)",
			e.mmcount, what, cos.Plural(e.mmcount))
	}
	return
}

// ErrXactNotFound

func (e *ErrXactNotFound) Error() string {
	return "xaction " + e.cause + " not found"
}

func NewErrXactNotFoundError(cause string) *ErrXactNotFound {
	return &ErrXactNotFound{cause: cause}
}

func IsErrXactNotFound(err error) bool {
	_, ok := err.(*ErrXactNotFound)
	return ok
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
	if e, ok := err.(*ErrAborted); ok {
		return e
	}
	_clean(err)
	return &ErrAborted{what: what, ctx: ctx, err: err, timestamp: time.Now()}
}

func (e *ErrAborted) Error() (s string) {
	s = fmt.Sprintf("%s aborted at %s", e.what, cos.FormatTime(e.timestamp, cos.StampMicro))
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

// ErrInitBackend & ErrMissingBackend

func (e *ErrInitBackend) Error() string {
	return fmt.Sprintf(
		"cannot initialize %q backend (present in the cluster configuration): missing %s-supporting libraries in the build",
		e.Provider, e.Provider,
	)
}

func (e *ErrMissingBackend) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return fmt.Sprintf("%q backend is missing in the cluster configuration", e.Provider)
}

// ErrETL

func NewErrETL(ctx *ETLErrCtx, format string, a ...any) *ErrETL {
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

func (e *ErrETL) WithContext(ctx *ETLErrCtx) *ErrETL {
	if ctx == nil {
		return e
	}
	return e.
		withTarget(ctx.TID).
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
func (e *ErrLmetaCorrupted) Unwrap() (err error)        { return e.err }

func IsErrLmetaCorrupted(err error) bool {
	_, ok := err.(*ErrLmetaCorrupted)
	return ok
}

func NewErrLmetaNotFound(err error) *ErrLmetaNotFound { return &ErrLmetaNotFound{err} }
func (e *ErrLmetaNotFound) Error() string             { return e.err.Error() }
func (e *ErrLmetaNotFound) Unwrap() (err error)       { return e.err }

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

////////////////////
// ErrXactUsePrev //
////////////////////

func NewErrXactUsePrev(xaction string) *ErrXactUsePrev {
	return &ErrXactUsePrev{xaction}
}

func (e *ErrXactUsePrev) Error() string {
	return fmt.Sprintf("%s is already running - not starting", e.xaction)
}

func IsErrXactUsePrev(err error) bool {
	_, ok := err.(*ErrXactUsePrev)
	return ok
}

///////////////////////
// ErrXactTgtInMaint //
///////////////////////

func NewErrXactTgtInMaint(xaction, tname string) *ErrXactTgtInMaint {
	return &ErrXactTgtInMaint{xaction, tname}
}

func (e *ErrXactTgtInMaint) Error() string {
	return fmt.Sprintf("%s is in maintenance or being decommissioned - cannot run %s",
		e.tname, e.xaction)
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

// used internally to report http.StatusNotFound _iff_ status is not set (is zero)
func isErrNotFoundExtended(err error) bool {
	return cos.IsErrNotFound(err) ||
		IsErrBckNotFound(err) || IsErrRemoteBckNotFound(err) ||
		IsObjNotExist(err) ||
		IsErrMountpathNotFound(err) ||
		IsErrXactNotFound(err)
}

// usage: lom.Load() (compare w/ IsNotExist)
func IsObjNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	return errors.Is(err, fs.ErrNotExist) // when wrapped
}

// usage: everywhere where applicable (directories, xactions, nodes, ...)
// excluding _local_ LOM (where the above applies)
func IsNotExist(err error) bool {
	if os.IsNotExist(err) {
		return true
	}
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	return cos.IsErrNotFound(err)
}

func IsFileAlreadyClosed(err error) bool {
	return errors.Is(err, fs.ErrClosed)
}

func IsErrBucketLevel(err error) bool { return IsErrBucketNought(err) }
func IsErrObjLevel(err error) bool    { return IsErrObjNought(err) }

/////////////
// ErrHTTP //
/////////////

func NewErrHTTP(r *http.Request, err error, errCode int) (e *ErrHTTP) {
	e = &ErrHTTP{}
	e.init(r, err, errCode)
	return e
}

// uses `allocHterr` to allocate - caller must free via `FreeHterr`
func InitErrHTTP(r *http.Request, err error, errCode int) (e *ErrHTTP) {
	e = allocHterr()
	e.init(r, err, errCode)
	return e
}

func (e *ErrHTTP) init(r *http.Request, err error, errCode int) {
	e.Status = http.StatusBadRequest
	if errCode != 0 {
		e.Status = errCode
	}
	tcode := fmt.Sprintf("%T", err)
	if i := strings.Index(tcode, "."); i > 0 {
		if pkg := tcode[:i]; pkg != "*errors" && pkg != "errors" {
			e.TypeCode = tcode[i+1:]
		}
	}
	_clean(err)
	e.Message = err.Error()
	if r != nil {
		e.Method, e.URLPath = r.Method, r.URL.Path
		e.RemoteAddr = r.RemoteAddr
		e.Caller = r.Header.Get(apc.HdrCallerName)
	}
	e.Node = thisNodeName
}

func (e *ErrHTTP) Error() (s string) {
	if e.TypeCode != "" && e.TypeCode != "ErrFailedTo" {
		if !strings.Contains(e.Message, e.TypeCode+":") {
			return e.TypeCode + ": " + e.Message
		}
	}
	return e.Message
}

func _clean(err error) {
	if cleanPathErr != nil {
		cleanPathErr(err)
	}
}

// Example:
// ErrBckNotFound: bucket "ais://abc" does not exist: HEAD /v1/buckets/abc (p[kWQp8080]: htrun.go:1035 <- prxtrybck.go:180 <- ...
func (e *ErrHTTP) StringEx() (s string) {
	s = e.Error()
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
	if len(e.trace) == 0 {
		e._trace()
	}
	return s + " (" + string(e.trace) + ")"
}

func (e *ErrHTTP) _jsonError(buf *bytes.Buffer) {
	enc := jsoniter.NewEncoder(buf)
	enc.SetEscapeHTML(false) // stop from escaping `<`, `>` and `&`.
	if err := enc.Encode(e); err != nil {
		buf.Reset()
		buf.WriteString(err.Error())
	}
}

func (e *ErrHTTP) write(w http.ResponseWriter, r *http.Request, silent bool) {
	if !silent {
		s := e.StringEx()
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
		nlog.Errorln(s)
	}
	hdr := w.Header()
	hdr.Set(cos.HdrContentType, cos.ContentJSON)
	hdr.Set(cos.HdrContentTypeOptions, "nosniff")

	berr := NewBuffer()
	e._jsonError(berr)
	if r.Method == http.MethodHead {
		hdr.Set(apc.HdrError, berr.String())
		w.WriteHeader(e.Status)
	} else {
		w.WriteHeader(e.Status)
		w.Write(berr.Bytes()) // no newline
	}
	FreeBuffer(berr)
}

func (e *ErrHTTP) _trace() {
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

func Str2HTTPErr(msg string) *ErrHTTP {
	var herr ErrHTTP
	if err := jsoniter.UnmarshalFromString(msg, &herr); err == nil {
		return &herr
	}
	return nil
}

func Err2HTTPErr(err error) *ErrHTTP {
	e, ok := err.(*ErrHTTP)
	if !ok {
		e = &ErrHTTP{}
		if !errors.As(err, &e) {
			return nil
		}
	}
	return e
}

// NOTE: internal use w/ duplication/simplicity traded off
func err2HTTP(err error) (*ErrHTTP, bool) {
	if e, ok := err.(*ErrHTTP); ok {
		return e, false
	}
	e := allocHterr()
	if !errors.As(err, &e) {
		FreeHterr(e)
		return nil, false
	}
	return e, true
}

//////////////////////////////
// invalid message handlers //
//////////////////////////////

// sends HTTP response header with the provided status
// (alloc/free via mem-pool)
func WriteErr(w http.ResponseWriter, r *http.Request, err error, opts ...int /*[status[, silent]]*/) {
	if herr, allocated := err2HTTP(err); herr != nil {
		herr.Status = http.StatusBadRequest
		if len(opts) > 0 && opts[0] > http.StatusBadRequest {
			herr.Status = opts[0]
		}
		herr.write(w, r, len(opts) > 1 /*silent*/)
		if allocated {
			FreeHterr(herr)
		}
		return
	}
	var (
		herr   = allocHterr()
		l      = len(opts)
		status = http.StatusBadRequest
	)

	// assign status (in order of priority)
	if cos.IsErrNotFound(err) {
		status = http.StatusNotFound
	} else if l > 0 {
		status = opts[0]
	} else if errf, ok := err.(*ErrFailedTo); ok {
		status = errf.status
	} else if isErrNotFoundExtended(err) {
		status = http.StatusNotFound
	}

	herr.init(r, err, status)
	herr.write(w, r, l > 1)
	FreeHterr(herr)
}

// Create ErrHTTP (based on `msg` and `opts`) and write it into HTTP response.
func WriteErrMsg(w http.ResponseWriter, r *http.Request, msg string, opts ...int) {
	var errCode int
	if len(opts) > 0 {
		errCode = opts[0]
	}
	herr := InitErrHTTP(r, errors.New(msg), errCode)
	herr.write(w, r, len(opts) > 1 /*silent*/)
	FreeHterr(herr)
}

// 405 Method Not Allowed, see:
// * https://www.rfc-editor.org/rfc/rfc7231#section-6.5.5
func WriteErr405(w http.ResponseWriter, r *http.Request, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	}
}

//
// 1) ErrHTTP struct pool
// 2) bytes.Buffer pool
//

const maxBuffer = 4 * cos.KiB

var (
	errPool sync.Pool
	bufPool sync.Pool

	err0 ErrHTTP
)

func allocHterr() (a *ErrHTTP) {
	if v := errPool.Get(); v != nil {
		a = v.(*ErrHTTP)
		return
	}
	return &ErrHTTP{}
}

func FreeHterr(a *ErrHTTP) {
	trace := a.trace
	*a = err0
	if trace != nil {
		a.trace = trace[:0]
	}
	errPool.Put(a)
}

func NewBuffer() (buf *bytes.Buffer) {
	if v := bufPool.Get(); v != nil {
		buf = v.(*bytes.Buffer)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	return
}

func FreeBuffer(buf *bytes.Buffer) {
	if buf.Cap() > maxBuffer {
		return
	}
	buf.Reset()
	bufPool.Put(buf)
}
