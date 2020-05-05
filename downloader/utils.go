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
		job, err := jobForObject(smap, sid, bck, name, link)
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

func jobForObject(smap *cluster.Smap, sid string, bck *cluster.Bck, objName, link string) (dlObj, error) {
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
		fromCloud: bck.IsCloud(cmn.AnyCloud),
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
	var (
		// link -> objName
		objects cmn.SimpleKVs
		query   = r.URL.Query()

		payload        = &DlBase{}
		singlePayload  = &DlSingleBody{}
		rangePayload   = &DlRangeBody{}
		multiPayload   = &DlMultiBody{}
		cloudPayload   = &DlCloudBody{}
		objectsPayload interface{}

		description string
	)

	payload.InitWithQuery(query)

	singlePayload.InitWithQuery(query)
	rangePayload.InitWithQuery(query)
	multiPayload.InitWithQuery(query)
	cloudPayload.InitWithQuery(query)

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	bck := cluster.NewBckEmbed(payload.Bck)
	if err := bck.Init(t.GetBowner(), t.Snode()); err != nil {
		return nil, err
	}

	// TODO: this "switch" should be refactored - there is too much
	//  repetitions and inconsistencies.
	if err = singlePayload.Validate(); err == nil {
		if objects, err = singlePayload.ExtractPayload(); err != nil {
			return nil, err
		}
		description = singlePayload.Describe()
	} else if err = rangePayload.Validate(); err == nil {
		// NOTE: Size of objects to be downloaded by a target will be unknown.
		//  So proxy won't be able to sum sizes from all targets when calculating total size.
		//  This should be taken care of somehow, as total is easy to know from range template anyway.
		var pt cmn.ParsedTemplate
		pt, err = cmn.ParseBashTemplate(rangePayload.Template)
		if err != nil {
			return nil, err
		}
		description = rangePayload.Describe()
		if !bck.IsAIS() {
			return nil, errors.New("regular download requires ais bucket")
		}
		baseJob := newBaseDlJob(id, bck, payload.Timeout, description, payload.Limits)
		return newRangeDlJob(t, baseJob, pt, rangePayload.Subdir)
	} else if err = multiPayload.Validate(b); err == nil {
		if err := jsoniter.Unmarshal(b, &objectsPayload); err != nil {
			return nil, err
		}

		if objects, err = multiPayload.ExtractPayload(objectsPayload); err != nil {
			return nil, err
		}
		description = multiPayload.Describe()
	} else if err = cloudPayload.Validate(); err == nil {
		if !bck.IsCloud() {
			return nil, errors.New("bucket download requires a cloud bucket")
		}

		baseJob := newBaseDlJob(id, bck, payload.Timeout, payload.Description, payload.Limits)
		return newCloudBucketDlJob(ctx, t, baseJob, cloudPayload.Prefix, cloudPayload.Suffix)
	} else {
		return nil, errors.New("input does not match any of the supported formats (single, range, multi, cloud)")
	}

	if payload.Description == "" {
		payload.Description = description
	}

	if !bck.IsAIS() {
		return nil, errors.New("regular download requires ais bucket")
	}
	objs, err := buildDlObjs(t, bck, objects)
	if err != nil {
		return nil, err
	}

	baseJob := newBaseDlJob(id, bck, payload.Timeout, payload.Description, payload.Limits)
	return newSliceDlJob(baseJob, objs), nil
}

//
// Checksum and version validation helpers
//

const (
	// https://cloud.google.com/storage/docs/xml-api/reference-headers
	gsCksumHeader               = "x-goog-hash"
	gsCksumHeaderValuePrefix    = "crc32c="
	gsCksumHeaderValueSeparator = ","
	gsVersionHeader             = "x-goog-generation"

	// https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
	s3CksumHeader            = "ETag"
	s3CksumHeaderIllegalChar = "-"
	s3VersionHeader          = "x-amz-version-id"
)

type (
	remoteObjInfo struct {
		size    int64
		cksum   *cmn.Cksum
		version string
	}
)

// Get file info if link points to Google storage or s3
func getRemoteObjInfo(link string, resp *http.Response) (roi remoteObjInfo) {
	u, err := url.Parse(link)
	cmn.AssertNoErr(err)

	if cmn.IsGoogleStorageURL(u) || cmn.IsGoogleAPIURL(u) {
		if hdr := resp.Header.Get(gsCksumHeader); hdr != "" {
			hs := strings.Split(hdr, gsCksumHeaderValueSeparator)
			cksumValue := getChecksumFromGoogleFormat(hs)
			if cksumValue != "" {
				roi.cksum = cmn.NewCksum(cmn.ChecksumCRC32C, cksumValue)
			}
		}
		if hdr := resp.Header.Get(gsVersionHeader); hdr != "" {
			roi.version = hdr
		}
	} else if cmn.IsS3URL(link) {
		if hdr := resp.Header.Get(s3CksumHeader); hdr != "" && !strings.Contains(hdr, s3CksumHeaderIllegalChar) {
			roi.cksum = cmn.NewCksum(cmn.ChecksumMD5, hdr)
		}
		if hdr := resp.Header.Get(s3VersionHeader); hdr != "" && hdr != "null" {
			roi.version = hdr
		}
	}
	roi.size = resp.ContentLength
	return
}

func getChecksumFromGoogleFormat(hs []string) string {
	for _, h := range hs {
		if strings.HasPrefix(h, gsCksumHeaderValuePrefix) {
			encoded := strings.TrimPrefix(h, gsCksumHeaderValuePrefix)
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return ""
			}
			return hex.EncodeToString(decoded)
		}
	}
	return ""
}

func headLink(link string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), headReqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, link, nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
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
	if roi.version != "" {
		if roi.version != lom.Version() {
			return false, nil
		}
	}
	if roi.cksum != nil {
		computedCksum, err := lom.ComputeCksum(roi.cksum.Type())
		if err != nil || !cmn.EqCksum(roi.cksum, computedCksum) {
			return false, err
		}
	}
	// Cannot prove that the objects are different so assume they are equal.
	return true, nil
}
