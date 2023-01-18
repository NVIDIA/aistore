//go:build hdfs

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/colinmarc/hdfs/v2"
)

type (
	hdfsProvider struct {
		t cluster.TargetPut
		c *hdfs.Client
	}
)

// interface guard
var _ cluster.BackendProvider = (*hdfsProvider)(nil)

func NewHDFS(t cluster.TargetPut) (cluster.BackendProvider, error) {
	var (
		config   = cmn.GCO.Get()
		anyConf  = config.Backend.Get(apc.HDFS)
		hdfsConf = anyConf.(cmn.BackendConfHDFS)
	)
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses:           hdfsConf.Addresses,
		User:                hdfsConf.User,
		UseDatanodeHostname: hdfsConf.UseDatanodeHostname,
	})
	if err != nil {
		return nil, err
	}
	if _, err := client.StatFs(); err != nil {
		return nil, fmt.Errorf("failed to stat filesystem (try to check connectivity with the HDFS cluster) err: %v", err)
	}
	return &hdfsProvider{t: t, c: client}, nil
}

func hdfsErrorToAISError(err error) (int, error) {
	if os.IsNotExist(err) {
		return http.StatusNotFound, err
	}
	if os.IsExist(err) {
		return http.StatusConflict, err
	}
	if os.IsPermission(err) {
		return http.StatusForbidden, err
	}
	return http.StatusBadRequest, err
}

func (*hdfsProvider) Provider() string  { return apc.HDFS }
func (*hdfsProvider) MaxPageSize() uint { return 10000 }

///////////////////
// CREATE BUCKET //
///////////////////

func (hp *hdfsProvider) CreateBucket(bck *cluster.Bck) (errCode int, err error) {
	return hp.checkDirectoryExists(bck)
}

func (hp *hdfsProvider) checkDirectoryExists(bck *cluster.Bck) (errCode int, err error) {
	debug.Assert(bck.Props != nil)
	refDirectory := bck.Props.Extra.HDFS.RefDirectory
	debug.Assert(refDirectory != "")

	fi, err := hp.c.Stat(refDirectory)
	if err != nil {
		return http.StatusBadRequest, err
	}
	if !fi.IsDir() {
		return http.StatusBadRequest, fmt.Errorf("specified path %q does not point to directory", refDirectory)
	}
	return 0, nil
}

/////////////////
// HEAD BUCKET //
/////////////////

func (hp *hdfsProvider) HeadBucket(_ ctx, bck *cluster.Bck) (bckProps cos.StrKVs,
	errCode int, err error) {
	if errCode, err = hp.checkDirectoryExists(bck); err != nil {
		return
	}

	bckProps = make(cos.StrKVs)
	bckProps[apc.HdrBackendProvider] = apc.HDFS
	bckProps[apc.HdrBucketVerEnabled] = "false"
	return
}

//////////////////
// LIST OBJECTS //
//////////////////

func (hp *hdfsProvider) ListObjects(bck *cluster.Bck, msg *apc.LsoMsg, lst *cmn.LsoResult) (int, error) {
	var (
		h   = cmn.BackendHelpers.HDFS
		idx int
	)
	msg.PageSize = calcPageSize(msg.PageSize, hp.MaxPageSize())

	err := hp.c.Walk(bck.Props.Extra.HDFS.RefDirectory, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			if cos.IsEOF(err) {
				return nil
			}
			return err
		}
		if uint(len(lst.Entries)) >= msg.PageSize {
			return skipDir(fi)
		}
		objName := strings.TrimPrefix(strings.TrimPrefix(path, bck.Props.Extra.HDFS.RefDirectory), string(filepath.Separator))
		if msg.Prefix != "" {
			if fi.IsDir() {
				if !cmn.DirNameContainsPrefix(objName, msg.Prefix) {
					return skipDir(fi)
				}
			} else if !cmn.ObjNameContainsPrefix(objName, msg.Prefix) {
				return skipDir(fi)
			}
		}
		if msg.ContinuationToken != "" && objName <= msg.ContinuationToken {
			return nil
		}
		if msg.StartAfter != "" && objName <= msg.StartAfter {
			return nil
		}
		if fi.IsDir() {
			return nil
		}

		var entry *cmn.LsoEntry
		if idx < len(lst.Entries) {
			entry = lst.Entries[idx]
		} else {
			entry = &cmn.LsoEntry{Name: objName}
			lst.Entries = append(lst.Entries, entry)
		}
		idx++
		entry.Size = fi.Size()
		if msg.WantProp(apc.GetPropsChecksum) {
			fr, err := hp.c.Open(path)
			if err != nil {
				return err
			}
			defer fr.Close()
			cksum, err := fr.Checksum()
			if err != nil {
				return err
			}
			if v, ok := h.EncodeCksum(cksum); ok {
				entry.Checksum = v
			}
		}
		return nil
	})
	if err != nil {
		return hdfsErrorToAISError(err)
	}
	lst.Entries = lst.Entries[:idx]
	// Set continuation token only if we reached the page size.
	if uint(len(lst.Entries)) >= msg.PageSize {
		lst.ContinuationToken = lst.Entries[len(lst.Entries)-1].Name
	}
	return 0, nil
}

// `hdfs.Walk` does not correctly handle `SkipDir` if the `fi` is non-directory.
func skipDir(fi os.FileInfo) error {
	if fi.IsDir() {
		return filepath.SkipDir
	}
	return nil
}

//////////////////
// LIST BUCKETS //
//////////////////

func (*hdfsProvider) ListBuckets(cmn.QueryBcks) (buckets cmn.Bcks, errCode int, err error) {
	debug.Assert(false)
	return
}

/////////////////
// HEAD OBJECT //
/////////////////

func (hp *hdfsProvider) HeadObj(_ ctx, lom *cluster.LOM) (oa *cmn.ObjAttrs, errCode int, err error) {
	var (
		fr       *hdfs.FileReader
		filePath = filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	)
	if fr, err = hp.c.Open(filePath); err != nil {
		errCode, err = hdfsErrorToAISError(err)
		return
	}
	oa = &cmn.ObjAttrs{}
	oa.SetCustomKey(cmn.SourceObjMD, apc.HDFS)
	oa.Size = fr.Stat().Size()
	if verbose {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (hp *hdfsProvider) GetObj(ctx context.Context, lom *cluster.LOM, owt cmn.OWT) (errCode int, err error) {
	reader, _, errCode, err := hp.GetObjReader(ctx, lom)
	if err != nil {
		return errCode, err
	}
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = reader
		params.OWT = owt
		params.Atime = time.Now()
	}
	if err = hp.t.PutObject(lom, params); err != nil {
		return
	}
	if verbose {
		glog.Infof("[get_object] %s", lom)
	}
	return
}

////////////////////
// GET OBJ READER //
////////////////////

func (hp *hdfsProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser,
	expectedCksm *cos.Cksum, errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	fr, err := hp.c.Open(filePath)
	if err != nil {
		errCode, err = hdfsErrorToAISError(err)
		return
	}
	lom.SetCustomKey(cmn.SourceObjMD, apc.HDFS)
	setSize(ctx, fr.Stat().Size())
	return wrapReader(ctx, fr), nil, 0, nil
}

////////////////
// PUT OBJECT //
////////////////

func (hp *hdfsProvider) PutObj(r io.ReadCloser, lom *cluster.LOM) (errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	fw, err := hp.c.Create(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			goto finish
		}

		// Create any missing directories.
		if err = hp.c.MkdirAll(filepath.Dir(filePath), cos.PermRWXRX|os.ModeDir); err != nil {
			goto finish
		}

		// Retry creating file. If it doesn't succeed we give up and report error.
		fw, err = hp.c.Create(filePath)
		if err != nil {
			goto finish
		}
	}

	if _, err = io.Copy(fw, r); err != nil {
		fw.Close()
		goto finish
	}

	err = fw.Close()

finish:
	cos.Close(r)

	// TODO: Cleanup if there was an error during `c.Create` or `io.Copy` (remove directories and file).
	if err != nil {
		errCode, err = hdfsErrorToAISError(err)
		return errCode, err
	}
	if verbose {
		glog.Infof("[put_object] %s", lom)
	}

	return 0, nil
}

///////////////////
// DELETE OBJECT //
///////////////////

func (hp *hdfsProvider) DeleteObj(lom *cluster.LOM) (errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	if err := hp.c.Remove(filePath); err != nil {
		errCode, err = hdfsErrorToAISError(err)
		return errCode, err
	}
	if verbose {
		glog.Infof("[delete_object] %s", lom)
	}
	return 0, nil
}
