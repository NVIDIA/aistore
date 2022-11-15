// Package downloader implements functionality to download resources into AIS cluster from external source.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package downloader

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	jsoniter "github.com/json-iterator/go"
)

const headReqTimeout = 5 * time.Second

var errInvalidTarget = errors.New("downloader: invalid target")

func clientForURL(u string) *http.Client {
	if cos.IsHTTPS(u) {
		return httpsClient
	}
	return httpClient
}

func countObjects(t cluster.Target, pt cos.ParsedTemplate, dir string, bck *cluster.Bck) (cnt int, err error) {
	var (
		smap = t.Sowner().Get()
		sid  = t.SID()
		si   *cluster.Snode
	)
	pt.InitIter()
	for link, ok := pt.Next(); ok; link, ok = pt.Next() {
		name := path.Join(dir, path.Base(link))
		name, err = NormalizeObjName(name)
		if err != nil {
			return
		}
		si, err = cluster.HrwTarget(bck.MakeUname(name), smap)
		if err != nil {
			return
		}
		if si.ID() == sid {
			cnt++
		}
	}
	return cnt, nil
}

// buildDlObjs returns list of objects that must be downloaded by target.
func buildDlObjs(t cluster.Target, bck *cluster.Bck, objects cos.StrKVs) ([]dlObj, error) {
	var (
		smap = t.Sowner().Get()
		sid  = t.SID()
	)

	objs := make([]dlObj, 0, len(objects))
	for name, link := range objects {
		obj, err := makeDlObj(smap, sid, bck, name, link)
		if err != nil {
			if err == errInvalidTarget {
				continue
			}
			return nil, err
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

func makeDlObj(smap *cluster.Smap, sid string, bck *cluster.Bck, objName, link string) (dlObj, error) {
	objName, err := NormalizeObjName(objName)
	if err != nil {
		return dlObj{}, err
	}

	si, err := cluster.HrwTarget(bck.MakeUname(objName), smap)
	if err != nil {
		return dlObj{}, err
	}
	if si.ID() != sid {
		return dlObj{}, errInvalidTarget
	}

	return dlObj{
		objName: objName,
		// Make sure that link contains protocol (absence of protocol can result in errors).
		link:       cmn.PrependProtocol(link),
		fromRemote: link == "",
	}, nil
}

// Removes everything that goes after '?', eg. "?query=key..." so it will not
// be part of final object name.
func NormalizeObjName(objName string) (string, error) {
	u, err := url.Parse(objName)
	if err != nil {
		return "", nil
	}

	if u.Path == "" {
		return objName, nil
	}

	return url.PathUnescape(u.Path)
}

func ParseStartDownloadRequest(t cluster.Target, bck *cluster.Bck, id string, dlb DlBody, dlXact *Xact) (DlJob, error) {
	switch dlb.Type {
	case DlTypeBackend:
		dp := &DlBackendBody{}
		err := jsoniter.Unmarshal(dlb.RawMessage, dp)
		if err != nil {
			return nil, err
		}
		if err := dp.Validate(); err != nil {
			return nil, err
		}
		return newBackendDlJob(t, id, bck, dp, dlXact)
	case DlTypeMulti:
		dp := &DlMultiBody{}
		err := jsoniter.Unmarshal(dlb.RawMessage, dp)
		if err != nil {
			return nil, err
		}
		if err := dp.Validate(); err != nil {
			return nil, err
		}
		return newMultiDlJob(t, id, bck, dp, dlXact)
	case DlTypeRange:
		dp := &DlRangeBody{}
		err := jsoniter.Unmarshal(dlb.RawMessage, dp)
		if err != nil {
			return nil, err
		}
		if err := dp.Validate(); err != nil {
			return nil, err
		}
		return newRangeDlJob(t, id, bck, dp, dlXact)
	case DlTypeSingle:
		dp := &DlSingleBody{}
		err := jsoniter.Unmarshal(dlb.RawMessage, dp)
		if err != nil {
			return nil, err
		}
		if err := dp.Validate(); err != nil {
			return nil, err
		}
		return newSingleDlJob(t, id, bck, dp, dlXact)
	default:
		return nil, errors.New("input does not match any of the supported formats (single, range, multi, backend)")
	}
}

// Given URL (link) and response header parse object attrs for GCP, S3 and Azure.
func attrsFromLink(link string, resp *http.Response, oah cmn.ObjAttrsHolder) (size int64) {
	u, err := url.Parse(link)
	debug.AssertNoErr(err)
	switch {
	case cos.IsGoogleStorageURL(u) || cos.IsGoogleAPIURL(u):
		h := cmn.BackendHelpers.Google
		oah.SetCustomKey(cmn.SourceObjMD, apc.GCP)
		if v, ok := h.EncodeVersion(resp.Header.Get(cos.GsVersionHeader)); ok {
			oah.SetCustomKey(cmn.VersionObjMD, v)
		}
		if hdr := resp.Header[http.CanonicalHeaderKey(cos.GsCksumHeader)]; len(hdr) > 0 {
			for cksumType, cksumValue := range parseGoogleCksumHeader(hdr) {
				switch cksumType {
				case cos.ChecksumMD5:
					oah.SetCustomKey(cmn.MD5ObjMD, cksumValue)
				case cos.ChecksumCRC32C:
					oah.SetCustomKey(cmn.CRC32CObjMD, cksumValue)
				default:
					glog.Errorf("unimplemented cksum type for custom metadata: %s", cksumType)
				}
			}
		}
	case cos.IsS3URL(link):
		h := cmn.BackendHelpers.Amazon
		oah.SetCustomKey(cmn.SourceObjMD, apc.AWS)
		if v, ok := h.EncodeVersion(resp.Header.Get(cos.S3VersionHeader)); ok {
			oah.SetCustomKey(cmn.VersionObjMD, v)
		}
		if v, ok := h.EncodeCksum(resp.Header.Get(cos.S3CksumHeader)); ok {
			oah.SetCustomKey(cmn.MD5ObjMD, v)
		}
	case cos.IsAzureURL(u):
		h := cmn.BackendHelpers.Azure
		oah.SetCustomKey(cmn.SourceObjMD, apc.Azure)
		if v, ok := h.EncodeVersion(resp.Header.Get(cos.AzVersionHeader)); ok {
			oah.SetCustomKey(cmn.VersionObjMD, v)
		}
		if v, ok := h.EncodeCksum(resp.Header.Get(cos.AzCksumHeader)); ok {
			oah.SetCustomKey(cmn.MD5ObjMD, v)
		}
	default:
		oah.SetCustomKey(cmn.SourceObjMD, cmn.WebObjMD)
	}
	return resp.ContentLength
}

func parseGoogleCksumHeader(hdr []string) cos.StrKVs {
	var (
		h      = cmn.BackendHelpers.Google
		cksums = make(cos.StrKVs, 2)
	)
	for _, v := range hdr {
		entry := strings.SplitN(v, "=", 2)
		debug.Assert(len(entry) == 2)
		if v, ok := h.EncodeCksum(entry[1]); ok {
			cksums[entry[0]] = v
		}
	}
	return cksums
}

func headLink(link string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), headReqTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, link, http.NoBody)
	if err != nil {
		return nil, err
	}
	resp, err := clientForURL(link).Do(req)
	if err != nil {
		return nil, err
	}
	cos.Close(resp.Body)
	return resp, nil
}

// Use all available metadata including {size, version, ETag, MD5, CRC}
// to compare local object with its remote counterpart.
func CompareObjects(lom *cluster.LOM, dst *DstElement) (equal bool, err error) {
	var oa *cmn.ObjAttrs
	if dst.Link != "" {
		resp, errHead := headLink(dst.Link)
		if errHead != nil {
			return false, errHead
		}
		oa = &cmn.ObjAttrs{}
		oa.Size = attrsFromLink(dst.Link, resp, oa)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), headReqTimeout)
		defer cancel()
		oa, _, err = cluster.T.Backend(lom.Bck()).HeadObj(ctx, lom)
		if err != nil {
			return false, err
		}
	}
	equal = lom.Equal(oa)
	return
}
