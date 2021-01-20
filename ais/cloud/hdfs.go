// +build hdfs

// Package cloud contains implementation of various backend providers.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

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
var _ cluster.CloudProvider = (*hdfsProvider)(nil)

func NewHDFS(t cluster.Target) (cluster.CloudProvider, error) {
	providerConf, ok := cmn.GCO.Get().Cloud.ProviderConf(cmn.ProviderHDFS)
	cmn.Assert(ok)
	hdfsConf := providerConf.(cmn.CloudConfHDFS)

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses:           hdfsConf.Addresses,
		User:                hdfsConf.User,
		UseDatanodeHostname: false,
	})
	if err != nil {
		return nil, err
	}
	return &hdfsProvider{t: t, c: client}, nil
}

func (hp *hdfsProvider) hdfsErrorToAISError(err error) (int, error) {
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

func (hp *hdfsProvider) Provider() string  { return cmn.ProviderHDFS }
func (hp *hdfsProvider) MaxPageSize() uint { return 10000 }

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

func (hp *hdfsProvider) CreateBucket(ctx context.Context, bck *cluster.Bck) (errCode int, err error) {
	return hp.checkDirectoryExists(bck)
}

func (hp *hdfsProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, errCode int, err error) {
	if errCode, err = hp.checkDirectoryExists(bck); err != nil {
		return
	}

	bckProps = make(cmn.SimpleKVs)
	bckProps[cmn.HeaderCloudProvider] = cmn.ProviderHDFS
	bckProps[cmn.HeaderBucketVerEnabled] = "false"
	return
}

func (hp *hdfsProvider) ListObjects(ctx context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, errCode int, err error) {
	h := cmn.CloudHelpers.HDFS
	bckList = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, msg.PageSize)}
	err = hp.c.Walk(bck.Props.Extra.HDFS.RefDirectory, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			if cmn.IsEOF(err) {
				return filepath.SkipDir
			}
			return err
		}
		if uint(len(bckList.Entries)) >= msg.PageSize {
			return filepath.SkipDir
		}
		if fi.IsDir() {
			return nil
		}
		objName := strings.TrimPrefix(strings.TrimPrefix(path, bck.Props.Extra.HDFS.RefDirectory), string(filepath.Separator))
		if !strings.HasPrefix(objName, msg.Prefix) {
			return nil
		}
		if msg.StartAfter != "" && objName < msg.StartAfter {
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
		errCode, err = hp.hdfsErrorToAISError(err)
		return nil, errCode, err
	}
	return bckList, 0, nil
}

func (hp *hdfsProvider) ListBuckets(ctx context.Context, query cmn.QueryBcks) (buckets cmn.BucketNames, errCode int, err error) {
	cmn.Assert(false)
	return
}

func (hp *hdfsProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	fr, err := hp.c.Open(filePath)
	if err != nil {
		errCode, err = hp.hdfsErrorToAISError(err)
		return nil, errCode, err
	}

	objMeta = make(cmn.SimpleKVs, 2)
	objMeta[cmn.HeaderCloudProvider] = cmn.ProviderHDFS
	objMeta[cmn.HeaderObjSize] = strconv.FormatInt(fr.Stat().Size(), 10)
	objMeta[cluster.VersionObjMD] = ""

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[head_object] %s", lom)
	}
	return
}

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

func (hp *hdfsProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser, expectedCksm *cmn.Cksum, errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	fr, err := hp.c.Open(filePath)
	if err != nil {
		errCode, err = hp.hdfsErrorToAISError(err)
		return nil, nil, errCode, err
	}

	customMD := cmn.SimpleKVs{
		cluster.SourceObjMD:  cluster.SourceHDFSObjMD,
		cluster.VersionObjMD: "",
	}

	lom.SetCustomMD(customMD)
	setSize(ctx, fr.Stat().Size())
	return wrapReader(ctx, fr), nil, 0, nil
}

func (hp *hdfsProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	fw, err := hp.c.Create(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			goto finish
		}

		// Create any missing directories.
		if err = hp.c.MkdirAll(filepath.Dir(filePath), cmn.PermRWXRX|os.ModeDir); err != nil {
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
	// TODO: Cleanup if there was an error during `c.Create` or `io.Copy`. We need
	//  to remove directories and file.
	if err != nil {
		errCode, err = hp.hdfsErrorToAISError(err)
		return "", errCode, err
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[put_object] %s", lom)
	}

	return "", 0, nil
}

func (hp *hdfsProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (errCode int, err error) {
	filePath := filepath.Join(lom.Bck().Props.Extra.HDFS.RefDirectory, lom.ObjName)
	if err := hp.c.Remove(filePath); err != nil {
		errCode, err = hp.hdfsErrorToAISError(err)
		return errCode, err
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[delete_object] %s", lom)
	}
	return 0, nil
}
