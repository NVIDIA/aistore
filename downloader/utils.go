// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

var (
	errInvalidTarget = errors.New("invalid target")
)

// buildDlObjs returns list of objects that must be downloaded by target.
func buildDlObjs(t cluster.Target, bck *cluster.Bck, objects cmn.SimpleKVs) ([]dlObj, error) {
	var (
		smap = t.GetSowner().Get()
		sid  = t.Snode().ID()
	)

	objs := make([]dlObj, 0, len(objects))
	for name, link := range objects {
		job, err := jobForObject(smap, sid, bck, false /*fromCloud*/, name, link)
		if err != nil {
			if err == errInvalidTarget {
				continue
			}
			return nil, err
		}
		objs = append(objs, job)
	}
	return objs, nil
}

func jobForObject(smap *cluster.Smap, sid string, bck *cluster.Bck, fromCloud bool, objName, link string) (dlObj, error) {
	objName, err := normalizeObjName(objName)
	if err != nil {
		return dlObj{}, err
	}
	// Make sure that link contains protocol (absence of protocol can result in errors).
	link = cmn.PrependProtocol(link)

	si, err := cluster.HrwTarget(bck.MakeUname(objName), smap)
	if err != nil {
		return dlObj{}, err
	}
	if si.ID() != sid {
		return dlObj{}, errInvalidTarget
	}

	return dlObj{
		objName:   objName,
		link:      link,
		fromCloud: fromCloud,
	}, nil
}

// Removes everything that goes after '?', eg. "?query=key..." so it will not
// be part of final object name.
func normalizeObjName(objName string) (string, error) {
	u, err := url.Parse(objName)
	if err != nil {
		return "", nil
	}

	if u.Path == "" {
		return objName, nil
	}

	return url.PathUnescape(u.Path)
}

func ParseStartDownloadRequest(ctx context.Context, r *http.Request, id string, t cluster.Target) (DlJob, error) {
	const (
		dlTypeSingle = iota + 1
		dlTypeRange
		dlTypeMulti
		dlTypeCloud
	)

	var (
		// link -> objName
		objects cmn.SimpleKVs

		payload       = &DlBase{}
		singlePayload = &DlSingleBody{}
		rangePayload  = &DlRangeBody{}
		multiPayload  = &DlMultiBody{}
		cloudPayload  = &DlCloudBody{}

		pt          cmn.ParsedTemplate
		sourceBck   *cluster.Bck
		description string
		dlType      int
	)

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	_ = jsoniter.Unmarshal(b, payload)
	_ = jsoniter.Unmarshal(b, singlePayload)
	_ = jsoniter.Unmarshal(b, rangePayload)
	_ = jsoniter.Unmarshal(b, multiPayload)
	_ = jsoniter.Unmarshal(b, cloudPayload)

	bck := cluster.NewBckEmbed(payload.Bck)
	if err := bck.Init(t.GetBowner(), t.Snode()); err != nil {
		return nil, err
	}
	if err := bck.Allow(cmn.AccessSYNC); err != nil {
		// TODO: return nil, err, 403
		return nil, err
	}

	if err = singlePayload.Validate(); err == nil {
		if objects, err = singlePayload.ExtractPayload(); err != nil {
			return nil, err
		}
		description = singlePayload.Describe()
		dlType = dlTypeSingle
	} else if err = rangePayload.Validate(); err == nil {
		// NOTE: Size of objects to be downloaded by a target will be unknown.
		//  So proxy won't be able to sum sizes from all targets when calculating total size.
		//  This should be taken care of somehow, as total is easy to know from range template anyway.
		if pt, err = cmn.ParseBashTemplate(rangePayload.Template); err != nil {
			return nil, err
		}
		description = rangePayload.Describe()
		dlType = dlTypeRange
	} else if err = multiPayload.Validate(); err == nil {
		if objects, err = multiPayload.ExtractPayload(); err != nil {
			return nil, err
		}
		description = multiPayload.Describe()
		dlType = dlTypeMulti
	} else if err = cloudPayload.Validate(); err == nil {
		dlType = dlTypeCloud
		if cloudPayload.SourceBck.IsEmpty() {
			cloudPayload.SourceBck = cloudPayload.Bck
		}

		sourceBck = cluster.NewBckEmbed(cloudPayload.SourceBck)
		if err := sourceBck.Init(t.GetBowner(), t.Snode()); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("input does not match any of the supported formats (single, range, multi, cloud)")
	}

	if payload.Description == "" {
		payload.Description = description
	}

	// TODO: this might be inaccurate if we download 1 or 2 objects because then
	//  other targets will have limits but will not use them.
	if payload.Limits.BytesPerHour > 0 {
		payload.Limits.BytesPerHour /= t.GetSowner().Get().CountTargets()
	}

	baseJob := newBaseDlJob(id, bck, payload.Timeout, payload.Description, payload.Limits)
	switch dlType {
	case dlTypeCloud:
		if !sourceBck.IsCloud() {
			return nil, errors.New("bucket download requires a cloud bucket")
		}
		return newCloudBucketDlJob(ctx, t, baseJob, sourceBck, cloudPayload.Prefix, cloudPayload.Suffix)
	case dlTypeRange:
		if !bck.IsAIS() {
			return nil, errors.New("range download requires ais bucket")
		}
		return newRangeDlJob(t, baseJob, pt, rangePayload.Subdir)
	case dlTypeSingle, dlTypeMulti:
		if !bck.IsAIS() {
			return nil, errors.New("regular download requires ais bucket")
		}
		objs, err := buildDlObjs(t, bck, objects)
		if err != nil {
			return nil, err
		}
		return newSliceDlJob(baseJob, objs), nil
	default:
		cmn.Assert(false)
		return nil, nil
	}
}

//
// Checksum and version validation helpers
//

const (
	// https://cloud.google.com/storage/docs/xml-api/reference-headers
	gsCksumHeader   = "x-goog-hash"
	gsVersionHeader = "x-goog-generation"

	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
	s3CksumHeader            = "ETag"
	s3CksumHeaderIllegalChar = "-"
	s3VersionHeader          = "x-amz-version-id"

	// https://docs.microsoft.com/en-us/rest/api/storageservices/get-blob-properties#response-headers
	azCksumHeader   = "Content-MD5"
	azVersionHeader = "ETag"
)

type (
	remoteObjInfo struct {
		size int64
		md   cmn.SimpleKVs
	}
)

// Get file info if link points to GCP, S3 or Azure.
func getRemoteObjInfo(link string, resp *http.Response) (roi remoteObjInfo) {
	u, err := url.Parse(link)
	cmn.AssertNoErr(err)

	if cmn.IsGoogleStorageURL(u) || cmn.IsGoogleAPIURL(u) {
		roi.md = make(cmn.SimpleKVs, 3)
		roi.md[cluster.SourceObjMD] = cluster.SourceGoogleObjMD
		if hdr := resp.Header[http.CanonicalHeaderKey(gsCksumHeader)]; len(hdr) > 0 {
			for cksumType, cksumValue := range parseGoogleCksumHeader(hdr) {
				switch cksumType {
				case cmn.ChecksumMD5:
					roi.md[cluster.GoogleMD5ObjMD] = cksumValue
				case cmn.ChecksumCRC32C:
					roi.md[cluster.GoogleCRC32CObjMD] = cksumValue
				default:
					glog.Errorf("unimplemented cksum type for custom metadata: %s", cksumType)
				}
			}
		}
		if hdr := resp.Header.Get(gsVersionHeader); hdr != "" {
			roi.md[cluster.GoogleVersionObjMD] = hdr
		}
	} else if cmn.IsS3URL(link) {
		roi.md = make(cmn.SimpleKVs, 3)
		roi.md[cluster.SourceObjMD] = cluster.SourceAmazonObjMD
		if hdr := resp.Header.Get(s3CksumHeader); hdr != "" && !strings.Contains(hdr, s3CksumHeaderIllegalChar) {
			roi.md[cluster.AmazonMD5ObjMD] = hdr
		}
		if hdr := resp.Header.Get(s3VersionHeader); hdr != "" && hdr != "null" {
			roi.md[cluster.AmazonVersionObjMD] = hdr
		}
	} else if cmn.IsAzureURL(u) {
		roi.md = make(cmn.SimpleKVs, 1)
		roi.md[cluster.SourceObjMD] = cluster.SourceAzureObjMD
		if hdr := resp.Header.Get(azCksumHeader); hdr != "" {
			roi.md[cluster.AzureMD5ObjMD] = hdr
		}
		if hdr := resp.Header.Get(azVersionHeader); hdr != "" {
			roi.md[cluster.AzureVersionObjMD] = strings.Trim(hdr, "\"")
		}
	} else {
		roi.md = make(cmn.SimpleKVs, 1)
		roi.md[cluster.SourceObjMD] = cluster.SourceWebObjMD
	}
	roi.size = resp.ContentLength
	return
}

func parseGoogleCksumHeader(hdr []string) cmn.SimpleKVs {
	var (
		cksums = make(cmn.SimpleKVs, 2)
	)
	for _, v := range hdr {
		entry := strings.SplitN(v, "=", 2)
		cmn.Assert(len(entry) == 2)
		cmn.AssertNoErr(cmn.ValidateCksumType(entry[0]))
		decoded, err := base64.StdEncoding.DecodeString(entry[1])
		cmn.AssertNoErr(err)
		cksums[entry[0]] = hex.EncodeToString(decoded)
	}
	return cksums
}

func headLink(link string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), headReqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, link, nil)
	if err != nil {
		return nil, err
	}
	resp, err := clientForURL(link).Do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return resp, nil
}

func compareObjects(obj dlObj, lom *cluster.LOM) (equal bool, err error) {
	resp, err := headLink(obj.link)
	if err != nil {
		return false, err
	}
	if resp.ContentLength != 0 && resp.ContentLength != lom.Size() {
		return false, nil
	}
	roi := getRemoteObjInfo(obj.link, resp)

	_, localMDPresent := lom.GetCustomMD(cluster.SourceObjMD)
	remoteSource := roi.md[cluster.SourceObjMD]
	if !localMDPresent {
		// Source is present only on the remote object. But if it's the cloud
		// object it will have version set to cloud version. Therefore, we can
		// try to compare it.
		switch remoteSource {
		case cluster.SourceGoogleObjMD:
			if lom.Version() == roi.md[cluster.GoogleVersionObjMD] {
				return true, nil
			}
		case cluster.SourceAmazonObjMD:
			if lom.Version() == roi.md[cluster.AmazonVersionObjMD] {
				return true, nil
			}
		case cluster.SourceAzureObjMD:
			if lom.Version() == roi.md[cluster.AzureVersionObjMD] {
				return true, nil
			}
		case cluster.SourceWebObjMD:
			// In case it is web we just assume the objects are equal since the
			// name and size matches.
			return true, nil
		}
		return false, nil
	}
	for k, v := range roi.md {
		if objValue, ok := lom.GetCustomMD(k); !ok {
			// Just skip check if `lom` doesn't have some metadata.
			continue
		} else if v != objValue {
			// Metadata does not match - objects are different.
			return false, nil
		}
	}
	// Cannot prove that the objects are different so assume they are equal.
	return true, nil
}
