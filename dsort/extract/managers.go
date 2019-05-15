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
		ExtractShard(shardName string, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (int64, int, error)
		CreateShard(s *Shard, w io.Writer, loadContent LoadContentFunc) (int64, error)
		UsingCompression() bool
		SupportsOffset() bool
		MetadataSize() int64
	}

	RecordExtractor interface {
		ExtractRecordWithBuffer(t ExtractCreator, shardName, recordName string, r cmn.ReadSizer, metadata []byte, toDisk bool, offset int64, buf []byte) (int64, error)
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

func (rm *RecordManager) ExtractRecordWithBuffer(t ExtractCreator, shardName, recordName string, r cmn.ReadSizer, metadata []byte, toDisk bool, offset int64, buf []byte) (size int64, err error) {
	var (
		storeType       string
		contentPath     string
		fullContentPath string
		mdSize          int64

		ext              = filepath.Ext(recordName)
		recordUniqueName = rm.genRecordUniqueName(shardName, recordName)
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
	if !toDisk {
		mdSize = int64(len(metadata))
		storeType = SGLStoreType
		contentPath, fullContentPath = rm.encodeRecordName(storeType, shardName, recordName)

		sgl := mem.NewSGL(r.Size() + int64(len(metadata)))
		if size, err = copyMetadataAndData(sgl, r, metadata, buf); err != nil {
			return size, err
		}
		rm.contents.Store(fullContentPath, sgl)
	} else if t.SupportsOffset() {
		mdSize, size = t.MetadataSize(), r.Size()
		storeType = OffsetStoreType
		contentPath, _ = rm.encodeRecordName(storeType, shardName, recordName)

		// If extractor was initialized we need to read the content, since it
		// may contain information about the sorting/shuffling key.
		if needRead {
			if _, err := io.CopyBuffer(ioutil.Discard, r, buf); err != nil {
				return 0, err
			}
		}
	} else {
		mdSize = int64(len(metadata))
		storeType = DiskStoreType
		contentPath, fullContentPath = rm.encodeRecordName(storeType, shardName, recordName)

		newF, err := cmn.CreateFile(fullContentPath)
		if err != nil {
			return size, err
		}
		if size, err = copyMetadataAndData(newF, r, metadata, buf); err != nil {
			newF.Close()
			return size, err
		}
		newF.Close()
		rm.extractionPaths.Store(fullContentPath, struct{}{})
	}

	key, err := rm.keyExtractor.ExtractKey(ske)
	if err != nil {
		return size, err
	}

	cmn.AssertMsg(contentPath != "", fmt.Sprintf("shardName: %s; recordName: %s", shardName, recordName))
	cmn.Assert(storeType != "")
	rm.Records.Insert(&Record{
		Key:      key,
		Name:     recordUniqueName,
		DaemonID: rm.daemonID,
		Objects: []*RecordObj{&RecordObj{
			ContentPath:  contentPath,
			StoreType:    storeType,
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

func (rm *RecordManager) genRecordUniqueName(shardName, recordName string) string {
	shardWithoutExt := strings.TrimSuffix(shardName, rm.extension)
	recordWithoutExt := strings.TrimSuffix(recordName, filepath.Ext(recordName))
	return shardWithoutExt + "|" + recordWithoutExt
}

func (rm *RecordManager) parseRecordUniqueName(recordUniqueName string) (shardName, recordName string) {
	splits := strings.SplitN(recordUniqueName, "|", 2)
	return splits[0] + rm.extension, splits[1]
}

func (rm *RecordManager) encodeRecordName(storeType string, shardName, recordName string) (contentPath, fullContentPath string) {
	switch storeType {
	case OffsetStoreType:
		// For offset:
		//  * contentPath = shard name (eg. shard_1.tar)
		//  * fullContentPath = not used
		return shardName, ""
	case SGLStoreType:
		// For sgl:
		//  * contentPath = recordUniqueName with extension (eg. shard_1-record_name.cls)
		//  * fullContentPath = recordUniqueName with extension (eg. shard_1-record_name.cls)
		recordExt := filepath.Ext(recordName)
		contentPath := rm.genRecordUniqueName(shardName, recordName) + recordExt
		return contentPath, contentPath // unique key for record
	case DiskStoreType:
		// For disk:
		//  * contentPath = recordUniqueName with extension  (eg. shard_1-record_name.cls)
		//  * fullContentPath = fqn to recordUniqueName with extension (eg. /tmp/ais/dsort/local/bucket/shard_1-record_name.cls)
		recordExt := filepath.Ext(recordName)
		contentPath := rm.genRecordUniqueName(shardName, recordName) + recordExt
		lom, errMsg := cluster.LOM{T: rm.t, Bucket: rm.bucket, Objname: contentPath}.Init()
		cmn.Assert(errMsg == "")
		return contentPath, fs.CSM.GenContentParsedFQN(lom.ParsedFQN, filetype.DSortFileType, "")
	default:
		cmn.AssertMsg(false, storeType)
		return "", ""
	}
}

func (rm *RecordManager) FullContentPath(obj *RecordObj) string {
	switch obj.StoreType {
	case OffsetStoreType:
		// To convert contentPath to fullContentPath we need to make shard name
		// full FQN.
		lom, errMsg := cluster.LOM{T: rm.t, Bucket: rm.bucket, Objname: obj.ContentPath}.Init()
		cmn.Assert(errMsg == "")
		return lom.FQN
	case SGLStoreType:
		// To convert contentPath to fullContentPath we need to add record
		// object extension.
		return obj.ContentPath
	case DiskStoreType:
		// To convert contentPath to fullContentPath we need to make record
		// unique name full FQN.
		contentPath := obj.ContentPath
		lom, errMsg := cluster.LOM{T: rm.t, Bucket: rm.bucket, Objname: contentPath}.Init()
		cmn.Assert(errMsg == "")
		return fs.CSM.GenContentParsedFQN(lom.ParsedFQN, filetype.DSortFileType, "")
	default:
		cmn.AssertMsg(false, obj.StoreType)
		return ""
	}
}

func (rm *RecordManager) ChangeStoreType(fullContentPath, newStoreType string, value interface{}, buf []byte) (n int64) {
	sgl := value.(*memsys.SGL)

	recordObjExt := filepath.Ext(fullContentPath)
	contentPath := strings.TrimSuffix(fullContentPath, recordObjExt)

	rm.Records.Lock()
	defer rm.Records.Unlock()

	// In SGLs `contentPath == recordUniqueName`
	record, exists := rm.Records.Find(contentPath)
	if !exists {
		// Generally should not happen but it is not proven that it cannot.
		// There is nothing wrong with just returning here though.
		return
	}

	idx := record.find(recordObjExt)
	if idx == -1 {
		// Duplicated records are removed so we cannot assert here.
		return
	}
	obj := record.Objects[idx]

	cmn.Assert(obj.StoreType == SGLStoreType) // only SGLs are supported

	switch newStoreType {
	case OffsetStoreType:
		shardName, _ := rm.parseRecordUniqueName(record.Name)
		obj.ContentPath = shardName
	case DiskStoreType:
		diskPath := rm.FullContentPath(obj)
		// No matter what the outcome we should store `path` in
		// `extractionPaths` to make sure that all files, even incomplete ones,
		// are deleted (if the file will not exist this is not much of a
		// problem).
		rm.extractionPaths.Store(diskPath, struct{}{})

		if _, err := cmn.SaveReader(diskPath, sgl, buf, false); err != nil {
			glog.Error(err)
			return
		}
	default:
		cmn.AssertMsg(false, newStoreType)
	}

	obj.StoreType = newStoreType
	rm.contents.Delete(fullContentPath)
	n = sgl.Size()
	sgl.Free()
	return
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
