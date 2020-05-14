// Package ta2tf provides core functionality for integrating with TensorFlow tools
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */

package tar2tf

import (
	"bytes"
	"io"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/go-tfdata/tfdata/pipeline"
)

var Cache = newTFRecordsCache()

type (
	tfrecordsRangesCache struct {
		M sync.Map
	}

	tfrecordCacheEntry struct {
		offset      int64
		totalSize   int64
		buf         []byte
		createdTime time.Time
		cksum       *cmn.Cksum
	}
)

const (
	cksmXattrName = "user.ais.cksm"
	prefetchRatio = 2 // previous length * prefetchRatio will be loaded in to the cache
)

func newTFRecordsCache() *tfrecordsRangesCache {
	c := &tfrecordsRangesCache{}
	go c.run()
	return c
}

func (c *tfrecordsRangesCache) run() {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()

	for range t.C {
		// infinite loop
		c.removeOld()
	}
}

func (c *tfrecordsRangesCache) removeOld() {
	c.M.Range(func(key, value interface{}) bool {
		entry := value.(*tfrecordCacheEntry)
		if time.Now().After(entry.createdTime.Add(10 * time.Minute)) {
			c.M.Delete(key)
		}
		return true
	})
}

func (c *tfrecordsRangesCache) GetSize(sourceTar *cluster.LOM) (int64, error) {
	var (
		stat           os.FileInfo
		err            error
		workFQN, uname string
	)
	uname = sourceTar.Uname()
	if val, ok := c.M.Load(uname); ok {
		entry := val.(*tfrecordCacheEntry)
		if sourceTar.Cksum().Equal(entry.cksum) {
			return entry.totalSize, nil
		}
		if glog.V(4) {
			glog.Infof("%q: tfrecordsRangesCache - checksums don't match", uname)
		}
	}

	workFQN = c.tfrecordWorkFQN(sourceTar)
	if err := c.generateTFRecordToDisk(sourceTar, workFQN); err != nil {
		return 0, err
	}

	if stat, err = os.Stat(workFQN); err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (c *tfrecordsRangesCache) Get(sourceTar *cluster.LOM, offset, length int64) (b []byte, err error) {
	var (
		tfrecordFile *os.File
		stat         os.FileInfo
		n            int64
	)
	if length == 0 {
		return []byte{}, nil
	}

	uname := sourceTar.Uname()
	if val, ok := c.M.Load(uname); ok {
		entry := val.(*tfrecordCacheEntry)
		if offset >= entry.totalSize {
			return []byte{}, nil
		}

		l := cmn.MinI64(length, entry.totalSize-offset)
		entryLength := int64(len(entry.buf))
		if offset >= entry.offset && length+offset <= entryLength+entry.offset {
			// FAST PATH: cache hit
			start := offset - entry.offset
			cachedBytes := entry.buf[start : start+l]
			go func() {
				err := c.prefetchNextChunk(sourceTar, offset+length, length*prefetchRatio)
				if err != nil {
					glog.Error(err) // cache won't be updated, but it should not influence correctness of next Get
				}
			}()

			return cachedBytes, nil
		}
		// SLOW PATH: cache miss
		if glog.V(4) {
			glog.Infof("[%q] SLOW PATH: cache entry doesn't fit. requested (%d, %d); had (%d, %d)", sourceTar.Uname(), offset, length, entry.offset, len(entry.buf))
		}
	}

	// SLOW PATH: read (or generate) TFRecord from disk
	workFQN := c.tfrecordWorkFQN(sourceTar)
	if err := c.generateTFRecordToDisk(sourceTar, workFQN); err != nil {
		return nil, err
	}

	tfrecordFile, err = os.Open(workFQN)
	if err != nil {
		return nil, err
	}
	defer tfrecordFile.Close()

	stat, err = tfrecordFile.Stat()
	if err != nil {
		return nil, err
	}
	if offset >= stat.Size() {
		return []byte{}, nil
	}

	buf := bytes.NewBuffer(make([]byte, 0, length))
	n, err = buf.ReadFrom(io.NewSectionReader(tfrecordFile, offset, length))
	if err != nil && err != io.EOF {
		return nil, err
	}

	go func() {
		err := c.prefetchNextChunk(sourceTar, offset+length, length*prefetchRatio)
		if err != nil {
			glog.Error(err) // cache won't be updated, but it should not influence correctness of next Get
		}
	}()

	// buffer might have been grown during ReadFrom, limit it to exactly number of read bytes
	return buf.Bytes()[:n], nil
}

func (*tfrecordsRangesCache) generateTFRecordToDisk(tar *cluster.LOM, destFQN string) error {
	var (
		tarFile, tfrecordFile *os.File
		err                   error
		b                     []byte
	)

	if _, err = os.Stat(destFQN); err == nil {
		b, err = fs.GetXattr(destFQN, cksmXattrName)
		if err != nil {
			// error getting xattribute - generate new TFRecord
			glog.Errorf("couldn't get xattr from %q; generating new TFRecord: %v", destFQN, err)
		} else {
			if tar.Cksum().Value() == string(b) {
				return nil
			}
			if glog.V(4) {
				glog.Infof("%q: TFRecordCache - checksum changed", tar.Uname())
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	tarFile, err = os.Open(tar.GetFQN())
	if err == nil {
		defer tarFile.Close()
		tfrecordFile, err = os.Create(destFQN)
	}
	if err != nil {
		return err
	}
	defer tfrecordFile.Close()

	p := pipeline.NewPipeline()
	p.FromTar(tarFile).SampleToTFExample().ToTFRecord(tfrecordFile).Do()

	// fixme: use LOM for destFQN TFRecord
	return fs.SetXattr(destFQN, cksmXattrName, []byte(tar.Cksum().Value()))
}

func (c *tfrecordsRangesCache) prefetchNextChunk(tar *cluster.LOM, offset, length int64) error {
	var (
		stat  os.FileInfo
		uname string
		n     int64
	)
	uname = tar.Uname()
	if val, ok := c.M.Load(uname); ok {
		entry := val.(*tfrecordCacheEntry)
		if offset > entry.totalSize {
			entry.offset = entry.totalSize
			entry.buf = []byte{}
			return nil
		}
		entryLength := int64(len(entry.buf))
		if offset >= entry.offset && length+offset <= entryLength+entry.offset {
			// correct entry already present
			return nil
		}
	}

	// assumes that the workfile is on a disk
	workFQN := c.tfrecordWorkFQN(tar)
	tfrecordFile, err := os.Open(workFQN)
	if err != nil {
		return err
	}
	defer tfrecordFile.Close()

	stat, err = tfrecordFile.Stat()
	if err != nil {
		return err
	}

	if offset > stat.Size() {
		// store entry with empty buf, it indicates end of TFRecord, it is useful to check total size of
		// TFRecord without doing any FS calls
		c.M.Store(tar.Uname(), &tfrecordCacheEntry{
			offset:      stat.Size(),
			totalSize:   stat.Size(),
			buf:         []byte{},
			cksum:       tar.Cksum(),
			createdTime: time.Now(),
		})
		return nil
	}

	// If we bother to do FS calls, prefetch at least 4KiB
	length = cmn.MaxI64(length, 4*cmn.KiB)
	buf := bytes.NewBuffer(make([]byte, 0, length))
	n, err = buf.ReadFrom(io.NewSectionReader(tfrecordFile, offset, length))
	if err != nil && err != io.EOF {
		return err
	}

	c.M.Store(tar.Uname(), &tfrecordCacheEntry{
		offset:      offset,
		totalSize:   stat.Size(),
		buf:         buf.Bytes()[:n],
		cksum:       tar.Cksum(),
		createdTime: time.Now(),
	})
	return nil
}

func (*tfrecordsRangesCache) tfrecordWorkFQN(sourceTar *cluster.LOM) string {
	fqn := sourceTar.GetParsedFQN()
	return fs.CSM.FQN(fqn.MpathInfo, fqn.Bck, fs.WorkfileType, fqn.ObjName+"."+cmn.TF)
}
