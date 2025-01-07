//go:build oci

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

// Outstanding [TODO] items:
//   4) Multi-Segment-Upload utilization (for fast/large object PUTs)... if practical
//   5) Multi-Segment-Download utilization (for fast/large object GETs)... if practical
//   6) Add support for object versioning
//   7) Resolve test:long:oci CI Pipeline failure in TestMultiProxy
//   8) Support "bucket props" that also avoids ENV OCI_COMPARTMENT_OCID
//   9) Define our own "rcFile" equivalent if desired (preferably in something like JSON)
//   10) Add in support for OCI SDK's rcFile if desired (perhaps in lieu of 9)

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	ocicmn "github.com/oracle/oci-go-sdk/v65/common"
	ocios "github.com/oracle/oci-go-sdk/v65/objectstorage"
)

const (
	compartmentOCIDDefault   = ""            // This will trigger a failure on ListBuckets() calls
	configFilePathDefault    = ".oci/config" // relative to ${HOME}
	profileDefault           = "DEFAULT"
	maxPageSizeMin           = 1
	maxPageSizeMax           = 1000
	maxPageSizeDefault       = maxPageSizeMax
	mpdSegmentMaxSizeMin     = 4 * cos.KiB
	mpdSegmentMaxSizeMax     = 256 * cos.MiB
	mpdSegmentMaxSizeDefault = mpdSegmentMaxSizeMax
	mpdThresholdMin          = 8 * cos.KiB
	mpdThresholdMax          = 512 * cos.MiB
	mpdThresholdDefault      = mpdThresholdMax
	mpdMaxThreadsMin         = 4
	mpdMaxThreadsMax         = 64
	mpdMaxThreadsDefault     = 16
	mpuSegmentMaxSizeMin     = 4 * cos.KiB
	mpuSegmentMaxSizeMax     = 256 * cos.MiB
	mpuSegmentMaxSizeDefault = mpuSegmentMaxSizeMax
	mpuThresholdMin          = 8 * cos.KiB
	mpuThresholdMax          = 512 * cos.MiB
	mpuThresholdDefault      = mpuThresholdMax
	mpuMaxThreadsMin         = 4
	mpuMaxThreadsMax         = 64
	mpuMaxThreadsDefault     = 16
)

type ocibp struct {
	t                     core.TargetPut
	configurationProvider ocicmn.ConfigurationProvider
	compartmentOCID       string
	maxPageSize           int64
	mpdSegmentMaxSize     int64
	mpdThreshold          int64
	mpdMaxThreads         int64
	mpuSegmentMaxSize     int64
	mpuThreshold          int64
	mpuMaxThreads         int64
	client                ocios.ObjectStorageClient
	namespace             string
	base
}

// interface guard
var _ core.Backend = (*ocibp)(nil)

func NewOCI(t core.TargetPut, tstats stats.Tracker, startingUp bool) (core.Backend, error) {
	bp := &ocibp{
		t:    t,
		base: base{provider: apc.OCI},
	}

	if err := bp.fetchCliConfig(); err != nil {
		return nil, err
	}
	if err := bp.fetchTuningENVs(); err != nil {
		return nil, err
	}

	client, err := ocios.NewObjectStorageClientWithConfigurationProvider(bp.configurationProvider)
	if err != nil {
		return nil, err
	}
	bp.client = client
	resp, err := bp.client.GetNamespace(context.Background(), ocios.GetNamespaceRequest{})
	if err != nil {
		return nil, err
	}
	bp.namespace = *resp.Value

	bp.base.init(t.Snode(), tstats, startingUp)

	return bp, nil
}

// fetchCliConfig populates bp with configurationProvider and compartmentOCID fetched
// from ENVs while defaulting to their corresponding key values from the configFile
// (selected via the optional OCI_CLI_CONFIG_FILE ENV or default ~/.oci/config).
//
// OCI's CLI also supports configuration of a default Profile and a CompartmentOCID
// in an rcFile, but we will not support that here. The Profile may already be selected
// via the OCI_CLI_PROFILE ENV. The CompartmentOCID more properly should derive from
// specific Bucket Props.
//
// Should the need arise to support OCI's CLI rcFile (optionally selected via an
// OCI_CLI_RC_FILE ENV that defaults to ~/.oci/oci_cli_rc), it (like the configFile)
// is in so-called INI file format. A default for the Profile is found at
// [OCI_CLI_SETTINGS]default_profile. That Profile (defaults to "DEFAULT") would still
// be overidden by the presence of a (non-empty) OCI_CLI_PROFILE ENV. The selected
// or defaulted Profile would then define a section in the rcFile (as it does in the
// configFile) with a key named "compartment-id". That value of that key would be used
// if the OCI_COMPARTMENT_OCID ENV is not set or empty. Parsing the rcFile is apparently
// not currently supported by the OCI SDK. As such, a tool like gopkg.in/ini.v1 may be
// used.
func (bp *ocibp) fetchCliConfig() (err error) {
	var (
		configFileProvider ocicmn.ConfigurationProvider
		fingerprint        string
		privateKey         string
		profile            string
		region             string
		tenancyOCID        string
		userOCID           string
	)

	if profile = os.Getenv(env.OCIProfile); profile == "" {
		profile = profileDefault
	}

	bp.compartmentOCID = os.Getenv(env.OCICompartmentOCID)

	if configFilePath := os.Getenv(env.OCIConfigFilePath); configFilePath == "" {
		configFileProvider, err = ocicmn.ConfigurationProviderFromFileWithProfile(
			filepath.Join(os.Getenv("HOME"), configFilePathDefault), profile, "")
		if err != nil {
			return
		}
	} else {
		configFileProvider, err = ocicmn.ConfigurationProviderFromFileWithProfile(
			configFilePath, profile, "")
		if err != nil {
			return
		}
	}

	if tenancyOCID = os.Getenv(env.OCITenancyOCID); tenancyOCID == "" {
		valueFromConfigFile, err := configFileProvider.TenancyOCID()
		if err == nil {
			tenancyOCID = valueFromConfigFile
		}
	}
	if userOCID = os.Getenv(env.OCIUserOCID); userOCID == "" {
		valueFromConfigFile, err := configFileProvider.UserOCID()
		if err == nil {
			userOCID = valueFromConfigFile
		}
	}
	if region = os.Getenv(env.OCIRegion); region == "" {
		valueFromConfigFile, err := configFileProvider.Region()
		if err == nil {
			region = valueFromConfigFile
		}
	}
	if fingerprint = os.Getenv(env.OCIFingerprint); fingerprint == "" {
		valueFromConfigFile, err := configFileProvider.KeyFingerprint()
		if err == nil {
			fingerprint = valueFromConfigFile
		}
	}
	if privateKey = os.Getenv(env.OCIPrivateKey); privateKey == "" {
		valueFromConfigFile, err := configFileProvider.PrivateRSAKey()
		if err == nil {
			priKeyBytes := x509.MarshalPKCS1PrivateKey(valueFromConfigFile)
			priKeyPEM := pem.EncodeToMemory(
				&pem.Block{
					Type:  "RSA_PRIVATE_KEY",
					Bytes: priKeyBytes,
				})
			privateKey = string(priKeyPEM)
		}
	}

	bp.configurationProvider = ocicmn.NewRawConfigurationProvider(
		tenancyOCID,
		userOCID,
		region,
		fingerprint,
		privateKey,
		nil,
	)

	return nil
}

func (bp *ocibp) fetchTuningENVs() (err error) {
	if err = bp.set(env.OCIMaxPageSize, maxPageSizeMin, maxPageSizeMax, maxPageSizeDefault, &bp.maxPageSize); err != nil {
		return
	}
	if err = bp.set(env.OCIMaxDownloadSegmentSize, mpdSegmentMaxSizeMin, mpdSegmentMaxSizeMax,
		mpdSegmentMaxSizeDefault, &bp.mpdSegmentMaxSize); err != nil {
		return
	}
	if err = bp.set(env.OCIMultiPartDownloadThreshold, mpdThresholdMin, mpdThresholdMax,
		mpdThresholdDefault, &bp.mpdThreshold); err != nil {
		return
	}
	if err = bp.set(env.OCIMultiPartDownloadMaxThreads, mpdMaxThreadsMin, mpdMaxThreadsMax,
		mpdMaxThreadsDefault, &bp.mpdMaxThreads); err != nil {
		return
	}
	if err = bp.set(env.OCIMaxUploadSegmentSize, mpuSegmentMaxSizeMin, mpuSegmentMaxSizeMax,
		mpuSegmentMaxSizeDefault, &bp.mpuSegmentMaxSize); err != nil {
		return
	}
	if err = bp.set(env.OCIMultiPartUploadThreshold, mpuThresholdMin, mpuThresholdMax,
		mpuThresholdDefault, &bp.mpuThreshold); err != nil {
		return
	}
	if err = bp.set(env.OCIMultiPartUploadMaxThreads, mpuMaxThreadsMin, mpuMaxThreadsMax,
		mpuMaxThreadsDefault, &bp.mpuMaxThreads); err != nil {
		return
	}

	return nil
}

func (*ocibp) set(envName string, envMin, envMax, envDefault int64, out *int64) error {
	s := os.Getenv(envName)
	if s == "" {
		*out = envDefault
		return nil
	}
	val, err := cos.ParseSize(s, "")
	switch {
	case err != nil:
		return fmt.Errorf("env '%s=%s' not parse-able: %v", envName, s, err)
	case val < 0:
		return fmt.Errorf("env '%s=%s' cannot be negative", envName, s)
	case val < envMin:
		return fmt.Errorf("env '%s=%s' cannot be less than %d", envName, s, envMin)
	case val > envMax:
		return fmt.Errorf("env '%s=%s' cannot be greater than %d", envName, s, envMax)
	}
	*out = val
	return nil
}

func ociStatus(rawResponse *http.Response) (ecode int) {
	if rawResponse == nil {
		ecode = http.StatusInternalServerError
	} else {
		ecode = rawResponse.StatusCode
	}
	return
}

// as core.Backend --------------------------------------------------------------

func (bp *ocibp) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error) {
	var (
		cloudBck            = bck.RemoteBck()
		continuationToken   string
		customKVs           cos.StrKVs
		delimiter           = string("/")
		fields              string
		fieldsIncludesOther bool
		fieldsIncludesSize  bool
		h                   = cmn.BackendHelpers.OCI
		limitAsInt          int
		noDirsFlag          = msg.IsFlagSet(apc.LsNoDirs)
		req                 = ocios.ListObjectsRequest{
			NamespaceName: &bp.namespace,
			BucketName:    &cloudBck.Name,
			Limit:         &limitAsInt,
			Fields:        &fields,
		}
		resp ocios.ListObjectsResponse
	)

	if msg.Prefix != "" {
		req.Prefix = &msg.Prefix
	}
	if msg.IsFlagSet(apc.LsNoRecursion) {
		req.Delimiter = &delimiter
	}

	switch {
	case msg.IsFlagSet(apc.LsNameOnly):
		fields = "name"
	case msg.IsFlagSet(apc.LsNameSize):
		fields = "name,size"
		fieldsIncludesSize = true
	default:
		fields = "name,size,md5,etag,timeModified"
		fieldsIncludesSize = true
		fieldsIncludesOther = true
		customKVs = make(cos.StrKVs, 3)
	}

	// Set limitAsInt as counting down cap on number of lst.Entries we will attempt to fill
	if (msg.PageSize == 0) || (int(bp.maxPageSize) < int(msg.PageSize)) {
		limitAsInt = int(bp.maxPageSize)
	} else {
		if msg.PageSize < maxPageSizeMin {
			ecode = http.StatusInternalServerError
			err = fmt.Errorf("msg.PageSize (%d) must be at least maxPageSizeMin (%d)", msg.PageSize, maxPageSizeMin)
			return
		}
		limitAsInt = int(msg.PageSize)
	}

	// Initialize internal continuationToken to msg.ContinuationToken but adjusted during loop below
	continuationToken = msg.ContinuationToken

	lst.Entries = make(cmn.LsoEntries, 0, limitAsInt)

	// Loop until end of list and/or limitAsInt has decremented to zero (based on len(lst.Entries))

	for limitAsInt > 0 {
		if continuationToken == "" {
			req.Start = nil
		} else {
			req.Start = &continuationToken
		}

		resp, err = bp.client.ListObjects(context.Background(), req)
		if err != nil {
			ecode = ociStatus(resp.RawResponse)
			return
		}

		// Note that we are trusting OCI to return a list of objects and, if present
		// (in the non-recursing delimiter case), "virtual" directories so as to not
		// exceed the total number requested. We can't simply truncate the list(s)
		// returned lest we our next continuationToken will skip some entries.

		if noDirsFlag {
			if len(resp.Objects) > limitAsInt {
				nlog.Warningf("OCI ListBuckets() returned %d objects (greater than the %d requested)\n", len(resp.Objects), limitAsInt)
				limitAsInt = 0
			} else {
				limitAsInt -= len(resp.Objects)
			}
		} else {
			if (len(resp.Objects) + len(resp.Prefixes)) > limitAsInt {
				nlog.Warningf("OCI ListBuckets() returned %d objects and %d prefixes (greater than the %d requested)\n", len(resp.Objects), len(resp.Prefixes), limitAsInt)
				limitAsInt = 0
			} else {
				limitAsInt -= len(resp.Objects) + len(resp.Prefixes)
			}
		}

		for _, en := range resp.Objects {
			lsoEnt := &cmn.LsoEnt{}
			lsoEnt.Name = *en.Name
			if fieldsIncludesSize && (en.Size != nil) {
				lsoEnt.Size = *en.Size
			}
			if fieldsIncludesOther {
				if v, ok := h.EncodeETag(en.Etag); ok {
					// [TODO] Validate whether lsoEnt.Checksum should be .Etag (aws) or .Md5 (gcp)
					// lsoEnt.Checksum = v
					customKVs[cmn.ETag] = v
				}
				if v, ok := h.EncodeCksum(en.Md5); ok {
					// [TODO] Validate whether lsoEnt.Checksum should be .Etag (aws) or .Md5 (gcp)
					lsoEnt.Checksum = v
					customKVs[cmn.MD5ObjMD] = v
				}
				if en.TimeModified != nil {
					customKVs[cmn.LastModified] = en.TimeModified.Time.Format(time.RFC3339)
				}
				if len(customKVs) > 0 {
					lsoEnt.Custom = cmn.CustomMD2S(customKVs)
					clear(customKVs)
				}
			}
			lst.Entries = append(lst.Entries, lsoEnt)
		}

		if !noDirsFlag {
			for _, en := range resp.Prefixes {
				lsoEnt := &cmn.LsoEnt{}
				lsoEnt.Name = en
				lsoEnt.Flags = apc.EntryIsDir
				lst.Entries = append(lst.Entries, lsoEnt)
			}
		}

		if (resp.NextStartWith == nil) || (*resp.NextStartWith == "") {
			continuationToken = ""
			break
		}

		continuationToken = *resp.NextStartWith
	}

	lst.ContinuationToken = continuationToken

	return
}

func (bp *ocibp) ListBuckets(_ cmn.QueryBcks) (bcks cmn.Bcks, ecode int, _ error) {
	req := ocios.ListBucketsRequest{
		NamespaceName: &bp.namespace,
		CompartmentId: &bp.compartmentOCID,
	}
	resp, err := bp.client.ListBuckets(context.Background(), req)
	if err != nil {
		return bcks, ociStatus(resp.RawResponse), err
	}

	bcks = make(cmn.Bcks, len(resp.Items))
	for idx, item := range resp.Items {
		bcks[idx] = cmn.Bck{
			Name:     *item.Name,
			Provider: apc.OCI,
		}
	}
	return bcks, 0, nil
}

// [TODO] Need to implement multi-threaded PUT when "length" exceeds bp.mpuThreshold
func (bp *ocibp) PutObj(r io.ReadCloser, lom *core.LOM, _ *http.Request) (int, error) {
	h := cmn.BackendHelpers.OCI
	cloudBck := lom.Bck().RemoteBck()
	req := ocios.PutObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
		PutObjectBody: r,
	}
	resp, err := bp.client.PutObject(context.Background(), req)
	// Note: in case PutObject() failed to close r...
	_ = r.Close()
	if err != nil {
		return ociStatus(resp.RawResponse), err
	}

	lom.SetCustomKey(apc.HdrBackendProvider, apc.OCI)
	if v, ok := h.EncodeETag(resp.ETag); ok {
		lom.SetCustomKey(cmn.ETag, v)
	}
	if v, ok := h.EncodeCksum(resp.OpcContentMd5); ok {
		lom.SetCustomKey(cmn.MD5ObjMD, v)
	}

	return 0, nil
}

func (bp *ocibp) DeleteObj(lom *core.LOM) (ecode int, err error) {
	cloudBck := lom.Bck().RemoteBck()
	req := ocios.DeleteObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
	}

	resp, err := bp.client.DeleteObject(context.Background(), req)
	if err != nil {
		ecode = ociStatus(resp.RawResponse)
		return
	}

	return
}

func (bp *ocibp) HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error) {
	cloudBck := bck.RemoteBck()
	req := ocios.HeadBucketRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
	}

	resp, err := bp.client.HeadBucket(ctx, req)
	if err != nil {
		ecode = ociStatus(resp.RawResponse)
		return
	}

	bckProps = make(cos.StrKVs, 2)
	bckProps[apc.HdrBackendProvider] = apc.OCI
	bckProps[apc.HdrBucketVerEnabled] = "false" // [TODO] At some point, if needed, add support for bucket versioning

	return
}

func (bp *ocibp) HeadObj(ctx context.Context, lom *core.LOM, _ *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error) {
	h := cmn.BackendHelpers.OCI
	cloudBck := lom.Bck().RemoteBck()
	req := ocios.HeadObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
	}

	resp, err := bp.client.HeadObject(ctx, req)
	if err != nil {
		ecode = ociStatus(resp.RawResponse)
		return
	}

	objAttrs = &cmn.ObjAttrs{
		CustomMD: make(cos.StrKVs, 3),
		Size:     resp.RawResponse.ContentLength,
	}
	objAttrs.CustomMD[cmn.SourceObjMD] = apc.OCI
	if v, ok := h.EncodeETag(resp.ETag); ok {
		objAttrs.CustomMD[cmn.ETag] = v
	}
	if v, ok := h.EncodeCksum(resp.ContentMd5); ok {
		objAttrs.CustomMD[cmn.MD5ObjMD] = v
	}

	return
}

func (bp *ocibp) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, _ *http.Request) (int, error) {
	res := bp.GetObjReader(ctx, lom, 0, 0)
	if res.Err != nil {
		return res.ErrCode, res.Err
	}

	putParams := allocPutParams(res, owt)
	err := bp.t.PutObject(lom, putParams)
	core.FreePutParams(putParams)

	return 0, err
}

// [TODO]
//  1. Need to implement multi-threaded GET when "length" exceeds bp.mpdThreshold
//  2. Consider setting req.IfMatch to lom.GetCustomKey(cmn.ETag) if present
func (bp *ocibp) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	var (
		cloudBck    = lom.Bck().RemoteBck()
		h           = cmn.BackendHelpers.OCI
		rangeHeader string
	)

	req := ocios.GetObjectRequest{
		NamespaceName: &bp.namespace,
		BucketName:    &cloudBck.Name,
		ObjectName:    &lom.ObjName,
	}
	if length > 0 {
		rangeHeader = cmn.MakeRangeHdr(offset, length)
		req.Range = &rangeHeader
	}

	resp, err := bp.client.GetObject(ctx, req)
	if err != nil {
		res.Err = err
		res.ErrCode = ociStatus(resp.RawResponse)
		return
	}

	if length == 0 {
		lom.ObjAttrs().Size = *resp.ContentLength
		lom.SetCustomKey(cmn.SourceObjMD, apc.OCI)
		if v, ok := h.EncodeETag(resp.ETag); ok {
			lom.SetCustomKey(cmn.ETag, v)
		}
		if v, ok := h.EncodeCksum(resp.ContentMd5); ok {
			lom.SetCustomKey(cmn.MD5ObjMD, v)
		}
	}

	res.R = resp.Content
	res.Size = *resp.ContentLength
	return res
}
