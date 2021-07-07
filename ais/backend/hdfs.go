// +build hdfs

// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/colinmarc/hdfs/v2"
)

type (
	hdfsProvider struct {
		t cluster.Target
		c *hdfs.Client
	}
)

// interface guard
var _ cluster.BackendProvider = (*hdfsProvider)(nil)

func NewHDFS(t cluster.Target) (cluster.BackendProvider, error) {
	providerConf, ok := cmn.GCO.Get().Backend.ProviderConf(cmn.ProviderHDFS)
	debug.Assert(ok)
	hdfsConf := providerConf.(cmn.BackendConfHDFS)

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

func (*hdfsProvider) Provider() string  { return cmn.ProviderHDFS }
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

func (hp *hdfsProvider) HeadBucket(_ context.Context, bck *cluster.Bck) (bckProps cos.SimpleKVs,
	errCode int, err error) {
	if errCode, err = hp.checkDirectoryExists(bck); err != nil {
		return
	}

	bckProps = make(cos.SimpleKVs)
	bckProps[cmn.HdrBackendProvider] = cmn.ProviderHDFS
	bckProps[cmn.HdrBucketVerEnabled] = "false"
	return
}

//////////////////
// LIST OBJECTS //
//////////////////

func (hp *hdfsProvider) ListObjects(bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList,
	errCode int, err error) {
	msg.PageSize = calcPageSize(msg.PageSize, hp.MaxPageSize())

	h := cmn.BackendHelpers.HDFS
	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, msg.PageSize)}
	err = hp.c.Walk(bck.Props.Extra.HDFS.RefDirectory, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			if cos.IsEOF(err) {
				return nil
			}
			return err
		}
		if uint(len(bckList.Entries)) >= msg.PageSize {
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
		entry := &cmn.BucketEntry{
			Name:    objName,
			Version: "",
		}
		if msg.WantProp(cmn.GetPropsSize) {
			entry.Size = fi.Size()
		}
		if msg.WantProp(cmn.GetPropsChecksum) {
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
		bckList.Entries = append(bckList.Entries, entry)
		return nil
	})
	if err != nil {
		errCode, err = hdfsErrorToAISError(err)
		return nil, errCode, err
	}
	// Set continuation token only if we reached the page size.
	if uint(len(bckList.Entries)) >= msg.PageSize {
		bckList.ContinuationToken = bckList.Entries[len(bckList.Entries)-1].Name
	}
	return bckList, 0, nil
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

func (*hdfsProvider) ListBuckets(query cmn.QueryBcks) (buckets cmn.Bcks, errCode int, err error) {
	debug.Assert(false)
	return
}

/////////////////
// HEAD OBJECT //
/////////////////

func (hp *hdfsProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cos.SimpleKVs, errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	fr, err := hp.c.Open(filePath)
	if err != nil {
		errCode, err = hdfsErrorToAISError(err)
		return nil, errCode, err
	}

	objMeta = make(cos.SimpleKVs, 2)
	objMeta[cmn.HdrBackendProvider] = cmn.ProviderHDFS
	objMeta[cmn.HdrObjSize] = strconv.FormatInt(fr.Stat().Size(), 10)
	objMeta[cluster.VersionObjMD] = ""

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

////////////////
// GET OBJECT //
////////////////

func (hp *hdfsProvider) GetObj(ctx context.Context, lom *cluster.LOM) (errCode int, err error) {
	reader, _, errCode, err := hp.GetObjReader(ctx, lom)
	if err != nil {
		return errCode, err
	}
	params := cluster.PutObjectParams{
		Tag:      fs.WorkfileColdget,
		Reader:   reader,
		RecvType: cluster.ColdGet,
	}
	if err = hp.t.PutObject(lom, params); err != nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
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

	customMD := cos.SimpleKVs{
		cluster.SourceObjMD:  cluster.SourceHDFSObjMD,
		cluster.VersionObjMD: "",
	}

	lom.SetCustom(customMD)
	setSize(ctx, fr.Stat().Size())
	return wrapReader(ctx, fr), nil, 0, nil
}

////////////////
// PUT OBJECT //
////////////////

func (hp *hdfsProvider) PutObj(r io.ReadCloser, lom *cluster.LOM) (version string,
	errCode int, err error) {
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

	// TODO: Cleanup if there was an error during `c.Create` or `io.Copy`. We need
	//  to remove directories and file.
	if err != nil {
		errCode, err = hdfsErrorToAISError(err)
		return "", errCode, err
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[put_object] %s", lom)
	}

	return "", 0, nil
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
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return 0, nil
}
