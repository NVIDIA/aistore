// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dsort/filetype"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/pkg/errors"
)

const (
	// Extract methods
	ExtractToMem cos.Bits = 1 << iota
	ExtractToDisk
	ExtractToWriter
)

// interface guard
var _ RecordExtractor = (*RecordManager)(nil)

type (
	extractRecordArgs struct {
		shardName     string        // name of the original shard
		fileType      string        // fs content type where the shard is located
		recordName    string        // name of the current record
		r             cos.ReadSizer // body of the record
		w             io.Writer     // required when method is set to ExtractToWriter
		metadata      []byte        // metadata of the record
		extractMethod cos.Bits      // method which needs to be used to extract a record
		offset        int64         // offset of the body in the shard
		buf           []byte        // helper buffer for `CopyBuffer` methods
	}

	// LoadContentFunc is type for the function which loads content from the
	// either remote or local target.
	LoadContentFunc func(w io.Writer, rec *Record, obj *RecordObj) (int64, error)

	// Creator is interface which describes set of functions which each
	// shard creator should implement.
	Creator interface {
		ExtractShard(lom *cluster.LOM, r cos.ReadReaderAt, extractor RecordExtractor, toDisk bool) (int64, int, error)
		CreateShard(s *Shard, w io.Writer, loadContent LoadContentFunc) (int64, error)
		UsingCompression() bool
		SupportsOffset() bool
		MetadataSize() int64
	}

	RecordExtractor interface {
		ExtractRecordWithBuffer(args extractRecordArgs) (int64, error)
	}

	RecordManager struct {
		Records *Records

		t                   cluster.Target
		bck                 cmn.Bck
		extension           string
		onDuplicatedRecords func(string) error

		extractCreator  Creator
		keyExtractor    KeyExtractor
		contents        *sync.Map
		extractionPaths *sync.Map // Keys correspond to all paths to record contents on disk.

		enqueued struct {
			mu      sync.Mutex
			records []*Records // records received from other targets which are waiting to be merged
		}
	}
)

func NewRecordManager(t cluster.Target, bck cmn.Bck, extension string, extractCreator Creator,
	keyExtractor KeyExtractor, onDuplicatedRecords func(string) error) *RecordManager {
	return &RecordManager{
		Records: NewRecords(1000),

		t:                   t,
		bck:                 bck,
		extension:           extension,
		onDuplicatedRecords: onDuplicatedRecords,

		extractCreator:  extractCreator,
		keyExtractor:    keyExtractor,
		contents:        &sync.Map{},
		extractionPaths: &sync.Map{},
	}
}

func (rm *RecordManager) ExtractRecordWithBuffer(args extractRecordArgs) (size int64, err error) {
	var (
		storeType       string
		contentPath     string
		fullContentPath string
		mdSize          int64

		ext              = Ext(args.recordName)
		recordUniqueName = rm.genRecordUniqueName(args.shardName, args.recordName)
	)

	// If the content already exists we should skip it but set error (caller
	// needs to handle it properly).
	if rm.Records.Exists(recordUniqueName, ext) {
		msg := fmt.Sprintf("record %q has been duplicated", args.recordName)
		rm.Records.DeleteDup(recordUniqueName, ext)

		// NOTE: There is no need to remove anything from `rm.extractionPaths`
		//  or `rm.contents` since it will be removed anyway in cleanup.
		//  Assumption is that there will be not much duplicates and we can live
		//  with a little bit more files/memory.
		return 0, rm.onDuplicatedRecords(msg)
	}

	if args.extractMethod.Has(ExtractToWriter) {
		cos.Assert(args.w != nil)
	}

	r, ske, needRead := rm.keyExtractor.PrepareExtractor(args.recordName, args.r, ext)
	if args.extractMethod.Has(ExtractToMem) {
		mdSize = int64(len(args.metadata))
		storeType = SGLStoreType
		contentPath, fullContentPath = rm.encodeRecordName(storeType, args.shardName, args.recordName)

		sgl := rm.t.MMSA().NewSGL(r.Size() + int64(len(args.metadata)))
		// No need for `io.CopyBuffer` since SGL implements `io.ReaderFrom`.
		if _, err = io.Copy(sgl, bytes.NewReader(args.metadata)); err != nil {
			return 0, errors.WithStack(err)
		}

		var dst io.Writer = sgl
		if args.extractMethod.Has(ExtractToWriter) {
			dst = io.MultiWriter(sgl, args.w)
		}

		if size, err = io.CopyBuffer(dst, r, args.buf); err != nil {
			return size, errors.WithStack(err)
		}
		rm.contents.Store(fullContentPath, sgl)
	} else if args.extractMethod.Has(ExtractToDisk) && rm.extractCreator.SupportsOffset() {
		mdSize, size = rm.extractCreator.MetadataSize(), r.Size()
		storeType = OffsetStoreType
		contentPath, _ = rm.encodeRecordName(storeType, args.shardName, args.recordName)

		// If extractor was initialized we need to read the content, since it
		// may contain information about the sorting/shuffling key.
		if needRead || args.w != nil {
			dst := io.Discard
			if args.w != nil {
				dst = args.w
			}

			if _, err := io.CopyBuffer(dst, r, args.buf); err != nil {
				return 0, errors.WithStack(err)
			}
		}
	} else if args.extractMethod.Has(ExtractToDisk) {
		mdSize = int64(len(args.metadata))
		storeType = DiskStoreType
		contentPath, fullContentPath = rm.encodeRecordName(storeType, args.shardName, args.recordName)

		var f *os.File
		if f, err = cos.CreateFile(fullContentPath); err != nil {
			return size, errors.WithStack(err)
		}
		if size, err = copyMetadataAndData(f, r, args.metadata, args.buf); err != nil {
			cos.Close(f)
			return size, errors.WithStack(err)
		}
		cos.Close(f)
		rm.extractionPaths.Store(fullContentPath, struct{}{})
	} else {
		cos.Assertf(false, "%d %d", args.extractMethod, args.extractMethod&ExtractToDisk)
	}

	var key interface{}
	if key, err = rm.keyExtractor.ExtractKey(ske); err != nil {
		return size, errors.WithStack(err)
	}

	cos.Assertf(contentPath != "", "shardName: %s; recordName: %s", args.shardName, args.recordName)
	cos.Assert(storeType != "")

	rm.Records.Insert(&Record{
		Key:      key,
		Name:     recordUniqueName,
		DaemonID: rm.t.SID(),
		Objects: []*RecordObj{{
			ContentPath:    contentPath,
			ObjectFileType: args.fileType,
			StoreType:      storeType,
			Offset:         args.offset,
			MetadataSize:   mdSize,
			Size:           size,
			Extension:      ext,
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
		rm.enqueued.records[lastIdx] = nil
		rm.enqueued.records = rm.enqueued.records[:lastIdx]
		rm.enqueued.mu.Unlock()

		rm.Records.merge(records)
	}
	cos.FreeMemToOS()
}

func (rm *RecordManager) genRecordUniqueName(shardName, recordName string) string {
	shardWithoutExt := strings.TrimSuffix(shardName, rm.extension)
	recordWithoutExt := strings.TrimSuffix(recordName, Ext(recordName))
	return shardWithoutExt + "|" + recordWithoutExt
}

func (rm *RecordManager) parseRecordUniqueName(recordUniqueName string) (shardName, recordName string) {
	splits := strings.SplitN(recordUniqueName, "|", 2)
	return splits[0] + rm.extension, splits[1]
}

func (rm *RecordManager) encodeRecordName(storeType, shardName, recordName string) (contentPath, fullContentPath string) {
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
		recordExt := Ext(recordName)
		contentPath := rm.genRecordUniqueName(shardName, recordName) + recordExt
		return contentPath, contentPath // unique key for record
	case DiskStoreType:
		// For disk:
		//  * contentPath = recordUniqueName with extension  (eg. shard_1-record_name.cls)
		//  * fullContentPath = fqn to recordUniqueName with extension (eg. <bucket_fqn>/shard_1-record_name.cls)
		recordExt := Ext(recordName)
		contentPath := rm.genRecordUniqueName(shardName, recordName) + recordExt
		ct, err := cluster.NewCTFromBO(rm.bck, contentPath, nil)
		cos.Assert(err == nil)
		return contentPath, ct.Make(filetype.DSortFileType)
	default:
		cos.AssertMsg(false, storeType)
		return "", ""
	}
}

func (rm *RecordManager) FullContentPath(obj *RecordObj) string {
	switch obj.StoreType {
	case OffsetStoreType:
		// To convert contentPath to fullContentPath we need to make shard name
		// full FQN.
		ct, err := cluster.NewCTFromBO(rm.bck, obj.ContentPath, nil)
		cos.Assert(err == nil)
		return ct.Make(obj.ObjectFileType)
	case SGLStoreType:
		// To convert contentPath to fullContentPath we need to add record
		// object extension.
		return obj.ContentPath
	case DiskStoreType:
		// To convert contentPath to fullContentPath we need to make record
		// unique name full FQN.
		contentPath := obj.ContentPath
		ct, err := cluster.NewCTFromBO(rm.bck, contentPath, nil)
		cos.Assert(err == nil)
		return ct.Make(filetype.DSortFileType)
	default:
		cos.AssertMsg(false, obj.StoreType)
		return ""
	}
}

func (rm *RecordManager) ChangeStoreType(fullContentPath, newStoreType string, value interface{}, buf []byte) (n int64) {
	sgl := value.(*memsys.SGL)

	recordObjExt := Ext(fullContentPath)
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

	cos.Assert(obj.StoreType == SGLStoreType) // only SGLs are supported

	switch newStoreType {
	case OffsetStoreType:
		shardName, _ := rm.parseRecordUniqueName(record.Name)
		obj.ContentPath = shardName
		obj.MetadataSize = rm.extractCreator.MetadataSize()
	case DiskStoreType:
		diskPath := rm.FullContentPath(obj)
		// No matter what the outcome we should store `path` in
		// `extractionPaths` to make sure that all files, even incomplete ones,
		// are deleted (if the file will not exist this is not much of a
		// problem).
		rm.extractionPaths.Store(diskPath, struct{}{})

		if _, err := cos.SaveReader(diskPath, sgl, buf, cos.ChecksumNone, -1, ""); err != nil {
			glog.Error(err)
			return
		}
	default:
		cos.AssertMsg(false, newStoreType)
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

	// NOTE: forcefully free all MMSA memory to the OS
	// TODO: another reason to use a separate MMSA for extractions
	rm.t.MMSA().FreeSpec(memsys.FreeSpec{
		Totally: true,
		ToOS:    true,
		MinSize: 1, // force toGC to free all (even small) memory to system
	})
}

func copyMetadataAndData(dst io.Writer, src io.Reader, metadata, buf []byte) (int64, error) {
	// Save metadata to dst
	if _, err := io.CopyBuffer(dst, bytes.NewReader(metadata), buf); err != nil {
		return 0, err
	}
	// Save data to dst
	return io.CopyBuffer(dst, src, buf)
}
