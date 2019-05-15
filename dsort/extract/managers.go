// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

var (
	_ RecordExtractor = &RecordManager{}

	mem *memsys.Mem2
)

type (
	// LoadContentFunc is type for the function which loads content from the
	// either remote or local target.
	LoadContentFunc func(w io.Writer, rec *Record, obj *RecordObj) (int64, error)

	// ExtractCreator is interface which describes set of functions which each
	// shard creator should implement.
	ExtractCreator interface {
		ExtractShard(fqn fs.ParsedFQN, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (int64, int, error)
		CreateShard(s *Shard, w io.Writer, loadContent LoadContentFunc) (int64, error)
		UsingCompression() bool
		SupportsOffset() bool
		MetadataSize() int64
	}

	RecordExtractor interface {
		ExtractRecordWithBuffer(t ExtractCreator, fqn fs.ParsedFQN, recordName string, r cmn.ReadSizer, metadata []byte, toDisk bool, offset int64, buf []byte) (int64, error)
	}

	RecordManager struct {
		Records *Records

		t                   cluster.Target
		daemonID            string
		bucket              string
		extension           string
		onDuplicatedRecords func(string) error

		keyExtractor    KeyExtractor
		contents        *sync.Map
		extractionPaths *sync.Map // Keys correspond to all paths to record contents on disk.

		enqueued struct {
			mu      sync.Mutex
			records []*Records // records received from other targets which are waiting to be merged
		}
	}

	ShardManager struct {
		Shards []*Shard
	}
)

func init() {
	mem = &memsys.Mem2{
		Name: "DSort.Extract.Mem2",
	}
	if err := mem.Init(false); err != nil {
		glog.Error(err)
		return
	}
}

func FreeMemory() {
	// Free memsys leftovers
	mem.Free(memsys.FreeSpec{
		Totally: true,
		ToOS:    true,
		MinSize: 1, // force toGC to free all (even small) memory to disk
	})
}

func NewRecordManager(t cluster.Target, daemonID, bucket, extension string, keyExtractor KeyExtractor, onDuplicatedRecords func(string) error) *RecordManager {
	return &RecordManager{
		Records: NewRecords(1000),

		t:                   t,
		daemonID:            daemonID,
		bucket:              bucket,
		extension:           extension,
		onDuplicatedRecords: onDuplicatedRecords,

		keyExtractor:    keyExtractor,
		contents:        &sync.Map{},
		extractionPaths: &sync.Map{},
	}
}

func (rm *RecordManager) ExtractRecordWithBuffer(t ExtractCreator, fqn fs.ParsedFQN, recordName string, r cmn.ReadSizer, metadata []byte, toDisk bool, offset int64, buf []byte) (size int64, err error) {
	var (
		storeType   string
		contentPath string
		mdSize      int64

		ext              = filepath.Ext(recordName)
		recordUniqueName = rm.genRecordUniqueName(fqn, recordName, ext)
	)

	// If the content already exists we should skip it but set error (caller
	// needs to handle it properly).
	if rm.Records.Exists(recordUniqueName, ext) {
		msg := fmt.Sprintf("record %q has been duplicated", recordName)
		rm.Records.DeleteDup(recordUniqueName, ext)

		// NOTE: there is no need to remove anything from `rm.extractionPaths`
		// or `rm.contents` since it will be removed anyway in cleanup.
		// Assumption is that there will be not much duplicates and we can live
		// with a little bit more files/memory.
		return 0, rm.onDuplicatedRecords(msg)
	}

	r, ske, needRead := rm.keyExtractor.PrepareExtractor(recordName, r, ext)
	// TODO: support SGLs even when offsets are supported.
	if t.SupportsOffset() {
		mdSize, size = t.MetadataSize(), r.Size()
		storeType = OffsetStoreType
		contentPath = rm.encodeRecordName(storeType, fqn, recordName)

		// If extractor was initialized we need to read the content, since it
		// may contain information about the sorting/shuffling key.
		if needRead {
			if _, err := io.CopyBuffer(ioutil.Discard, r, buf); err != nil {
				return 0, err
			}
		}
	} else if toDisk {
		mdSize = int64(len(metadata))
		storeType = DiskStoreType
		contentPath = rm.encodeRecordName(storeType, fqn, recordName)

		newF, err := cmn.CreateFile(contentPath)
		if err != nil {
			return size, err
		}
		if size, err = copyMetadataAndData(newF, r, metadata, buf); err != nil {
			newF.Close()
			return size, err
		}
		newF.Close()
		rm.extractionPaths.Store(contentPath, struct{}{})
	} else {
		mdSize = int64(len(metadata))
		storeType = SGLStoreType
		contentPath = rm.encodeRecordName(storeType, fqn, recordName)

		sgl := mem.NewSGL(r.Size() + int64(len(metadata)))
		if size, err = copyMetadataAndData(sgl, r, metadata, buf); err != nil {
			return size, err
		}
		rm.contents.Store(contentPath, sgl)
	}

	key, err := rm.keyExtractor.ExtractKey(ske)
	if err != nil {
		return size, err
	}

	cmn.AssertMsg(contentPath != "", fmt.Sprintf("shardName: %s; recordName: %s", fqn.Objname, recordName))
	cmn.Assert(storeType != "")
	rm.Records.Insert(&Record{
		Key:         key,
		Name:        recordUniqueName,
		DaemonID:    rm.daemonID,
		StoreType:   storeType,
		ContentPath: contentPath,
		Objects: []*RecordObj{&RecordObj{
			Offset:       offset,
			MetadataSize: mdSize,
			Size:         size,
			Extension:    ext,
		}},
	})
	return size, nil
}

func (rm *RecordManager) EnqueueRecords(records *Records) {
	rm.enqueued.mu.Lock()
	rm.enqueued.records = append(rm.enqueued.records, records)
	rm.enqueued.mu.Unlock()
}

func (rm *RecordManager) MergeEnqueuedRecords() {
	for {
		rm.enqueued.mu.Lock()
		lastIdx := len(rm.enqueued.records) - 1
		if lastIdx < 0 {
			rm.enqueued.mu.Unlock()
			break
		}
		records := rm.enqueued.records[lastIdx]
		rm.enqueued.records = rm.enqueued.records[:lastIdx]
		rm.enqueued.mu.Unlock()

		rm.Records.merge(records)
	}
}

func (rm *RecordManager) genRecordUniqueName(fqn fs.ParsedFQN, recordName, recordNameExt string) string {
	shardWithoutExt := strings.TrimSuffix(fqn.Objname, rm.extension)
	keyWithoutExt := strings.TrimSuffix(recordName, recordNameExt)
	return shardWithoutExt + "-" + keyWithoutExt
}

func (rm *RecordManager) encodeRecordName(storeType string, fqn fs.ParsedFQN, recordName string) string {
	switch storeType {
	case OffsetStoreType:
		return fqn.Objname // shard name
	case SGLStoreType:
		return fqn.Objname + recordName // unique key for record
	case DiskStoreType:
		shardWithoutExt := strings.TrimSuffix(fqn.Objname, rm.extension)
		recordPath := fs.CSM.GenContentFQN(shardWithoutExt+"-"+recordName, filetype.DSortFileType, "")
		return recordPath
	default:
		cmn.AssertMsg(false, storeType)
		return ""
	}
}

func (rm *RecordManager) FullContentPath(rec *Record) string {
	switch rec.StoreType {
	case OffsetStoreType:
		lom, errMsg := cluster.LOM{T: rm.t, Bucket: rm.bucket, Objname: rec.ContentPath}.Init()
		cmn.Assert(errMsg == "")
		return lom.FQN // full path to the shard
	case SGLStoreType:
		// Does not require any manipulation as it is just the key to the map
		return rec.ContentPath
	case DiskStoreType:
		return rec.ContentPath // disk has full path
	default:
		cmn.AssertMsg(false, rec.StoreType)
		return ""
	}
}

func (rm *RecordManager) RecordContents() *sync.Map {
	return rm.contents
}

func (rm *RecordManager) ExtractionPaths() *sync.Map {
	return rm.extractionPaths
}

func (rm *RecordManager) Cleanup() {
	rm.Records.Drain()
	rm.extractionPaths.Range(func(k, v interface{}) bool {
		if err := os.RemoveAll(k.(string)); err != nil {
			glog.Errorf("could not remove extraction path (%v) from previous run, err: %v", k, err)
		}
		rm.extractionPaths.Delete(k)
		return true
	})
	rm.extractionPaths = nil
	rm.contents.Range(func(k, v interface{}) bool {
		if sgl, ok := v.(*memsys.SGL); ok {
			sgl.Free()
		}
		rm.contents.Delete(k)
		return true
	})
	rm.contents = nil
}

func NewShardManager() *ShardManager {
	return &ShardManager{
		Shards: make([]*Shard, 0, 1000),
	}
}

func (sm *ShardManager) Cleanup() {
	sm.Shards = nil
}

func copyMetadataAndData(dst io.Writer, src io.Reader, metadata []byte, buf []byte) (int64, error) {
	// Save metadata to dst
	if _, err := io.CopyBuffer(dst, bytes.NewReader(metadata), buf); err != nil {
		return 0, err
	}
	// Save data to dst
	return io.CopyBuffer(dst, src, buf)
}
