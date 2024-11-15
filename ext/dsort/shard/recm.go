// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/ext/dsort/ct"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/pkg/errors"
)

const (
	// Extract methods
	ExtractToMem cos.Bits = 1 << iota
	ExtractToDisk
	ExtractToWriter
)

const recSepa = "|"

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

	// loads content from local or remote target
	ContentLoader interface {
		Load(w io.Writer, rec *Record, obj *RecordObj) (int64, error)
	}

	RecordExtractor interface {
		RecordWithBuffer(args *extractRecordArgs) (int64, error)
	}

	RecordManager struct {
		Records             *Records
		bck                 cmn.Bck
		onDuplicatedRecords func(string) error

		extractCreator  RW
		keyExtractor    KeyExtractor
		contents        *sync.Map
		extractionPaths *sync.Map // Keys correspond to all paths to record contents on disk.

		enqueued struct {
			mu      sync.Mutex
			records []*Records // records received from other targets which are waiting to be merged
		}
	}
)

///////////////////
// RecordManager //
///////////////////

func NewRecordManager(bck cmn.Bck, extractCreator RW, keyExtractor KeyExtractor, onDupRecs func(string) error) *RecordManager {
	return &RecordManager{
		Records:             NewRecords(1000),
		bck:                 bck,
		onDuplicatedRecords: onDupRecs,
		extractCreator:      extractCreator,
		keyExtractor:        keyExtractor,
		contents:            &sync.Map{},
		extractionPaths:     &sync.Map{},
	}
}

func (recm *RecordManager) RecordWithBuffer(args *extractRecordArgs) (size int64, err error) {
	var (
		storeType        string
		contentPath      string
		fullContentPath  string
		mdSize           int64
		ext              = cosExt(args.recordName)
		recordUniqueName = genRecordUname(args.shardName, args.recordName)
	)

	// handle record duplications (see m.react)
	if recm.Records.Exists(recordUniqueName, ext) {
		msg := fmt.Sprintf("record %q is duplicated", args.recordName)
		recm.Records.DeleteDup(recordUniqueName, ext)

		err = recm.onDuplicatedRecords(msg)
		if err != nil {
			return 0, err // react: abort
		}
		// react: ignore or warn
	}

	debug.Assert(!args.extractMethod.Has(ExtractToWriter) || args.w != nil)

	r, ske, needRead := recm.keyExtractor.PrepareExtractor(args.recordName, args.r, ext)
	switch {
	case args.extractMethod.Has(ExtractToMem):
		mdSize = int64(len(args.metadata))
		storeType = SGLStoreType
		contentPath, fullContentPath = recm.encodeRecordName(storeType, args.shardName, args.recordName)

		sgl := core.T.PageMM().NewSGL(r.Size() + int64(len(args.metadata)))
		// No need for `io.CopyBuffer` since SGL implements `io.ReaderFrom`.
		if _, err = io.Copy(sgl, bytes.NewReader(args.metadata)); err != nil {
			sgl.Free()
			return 0, errors.WithStack(err)
		}

		var dst io.Writer = sgl
		if args.extractMethod.Has(ExtractToWriter) {
			dst = io.MultiWriter(sgl, args.w)
		}
		if size, err = io.CopyBuffer(dst, r, args.buf); err != nil {
			sgl.Free()
			return size, errors.WithStack(err)
		}
		recm.contents.Store(fullContentPath, sgl)
	case args.extractMethod.Has(ExtractToDisk) && recm.extractCreator.SupportsOffset():
		mdSize, size = recm.extractCreator.MetadataSize(), r.Size()
		storeType = OffsetStoreType
		contentPath, _ = recm.encodeRecordName(storeType, args.shardName, args.recordName)

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
	case args.extractMethod.Has(ExtractToDisk):
		mdSize = int64(len(args.metadata))
		storeType = DiskStoreType
		contentPath, fullContentPath = recm.encodeRecordName(storeType, args.shardName, args.recordName)

		var f *os.File
		if f, err = cos.CreateFile(fullContentPath); err != nil {
			return size, errors.WithStack(err)
		}
		if size, err = copyMetadataAndData(f, r, args.metadata, args.buf); err != nil {
			cos.Close(f)
			return size, errors.WithStack(err)
		}
		cos.Close(f)
		recm.extractionPaths.Store(fullContentPath, struct{}{})
	default:
		debug.Assertf(false, "%d %d", args.extractMethod, args.extractMethod&ExtractToDisk)
	}

	var key any
	if key, err = recm.keyExtractor.ExtractKey(ske); err != nil {
		return size, errors.WithStack(err)
	}

	if contentPath == "" || storeType == "" {
		debug.Assertf(false, "shardName: %q, recordName: %q, storeType: %q", args.shardName, args.recordName, storeType)
	}
	recm.Records.Insert(&Record{
		Key:      key,
		Name:     recordUniqueName,
		DaemonID: core.T.SID(),
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

func (recm *RecordManager) EnqueueRecords(records *Records) {
	recm.enqueued.mu.Lock()
	recm.enqueued.records = append(recm.enqueued.records, records)
	recm.enqueued.mu.Unlock()
}

func (recm *RecordManager) MergeEnqueuedRecords() {
	for {
		recm.enqueued.mu.Lock()
		lastIdx := len(recm.enqueued.records) - 1
		if lastIdx < 0 {
			recm.enqueued.mu.Unlock()
			break
		}
		records := recm.enqueued.records[lastIdx]
		recm.enqueued.records[lastIdx] = nil
		recm.enqueued.records = recm.enqueued.records[:lastIdx]
		recm.enqueued.mu.Unlock()

		recm.Records.merge(records)
	}
	oom.FreeToOS(false /*force*/)
}

func (recm *RecordManager) encodeRecordName(storeType, shardName, recordName string) (contentPath, fullContentPath string) {
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
		recordExt := cosExt(recordName)
		contentPath := genRecordUname(shardName, recordName) + recordExt
		return contentPath, contentPath // unique key for record
	case DiskStoreType:
		// For disk:
		//  * contentPath = recordUniqueName with extension  (eg. shard_1-record_name.cls)
		//  * fullContentPath = fqn to recordUniqueName with extension (eg. <bucket_fqn>/shard_1-record_name.cls)
		recordExt := cosExt(recordName)
		contentPath := genRecordUname(shardName, recordName) + recordExt
		c, err := core.NewCTFromBO(&recm.bck, contentPath, nil)
		debug.AssertNoErr(err)
		return contentPath, c.Make(ct.DsortFileType)
	default:
		debug.Assert(false, storeType)
		return "", ""
	}
}

func (recm *RecordManager) FullContentPath(obj *RecordObj) string {
	switch obj.StoreType {
	case OffsetStoreType:
		// To convert contentPath to fullContentPath we need to make shard name
		// full FQN.
		ct, err := core.NewCTFromBO(&recm.bck, obj.ContentPath, nil)
		debug.AssertNoErr(err)
		return ct.Make(obj.ObjectFileType)
	case SGLStoreType:
		// To convert contentPath to fullContentPath we need to add record
		// object extension.
		return obj.ContentPath
	case DiskStoreType:
		// To convert contentPath to fullContentPath we need to make record
		// unique name full FQN.
		contentPath := obj.ContentPath
		c, err := core.NewCTFromBO(&recm.bck, contentPath, nil)
		debug.AssertNoErr(err)
		return c.Make(ct.DsortFileType)
	default:
		debug.Assert(false, obj.StoreType)
		return ""
	}
}

func (recm *RecordManager) FreeMem(fullContentPath, newStoreType string, value any, buf []byte) (n int64) {
	sgl, ok := value.(*memsys.SGL)
	debug.Assert(ok)

	recordObjExt := cosExt(fullContentPath)
	contentPath := strings.TrimSuffix(fullContentPath, recordObjExt)

	recm.Records.Lock()
	defer recm.Records.Unlock()

	// In SGLs `contentPath == recordUniqueName`
	record, exists := recm.Records.Find(contentPath)
	if !exists {
		// Generally should not happen but it is not proven that it cannot.
		// There is nothing wrong with just returning here though.
		nlog.Errorln("failed to find", fullContentPath, recordObjExt, contentPath) // TODO: FastV
		return
	}

	idx := record.find(recordObjExt)
	if idx == -1 {
		// Duplicated records are removed so we cannot assert here.
		return
	}
	obj := record.Objects[idx]

	debug.Assert(obj.StoreType == SGLStoreType, obj.StoreType+" vs "+SGLStoreType) // only SGLs are supported

	switch newStoreType {
	case OffsetStoreType:
		shardName, _ := parseRecordUname(record.Name)
		obj.ContentPath = shardName
		obj.MetadataSize = recm.extractCreator.MetadataSize()
	case DiskStoreType:
		diskPath := recm.FullContentPath(obj)
		// No matter what the outcome we should store `path` in
		// `extractionPaths` to make sure that all files, even incomplete ones,
		// are deleted (if the file will not exist this is not much of a
		// problem).
		recm.extractionPaths.Store(diskPath, struct{}{})

		if _, err := cos.SaveReader(diskPath, sgl, buf, cos.ChecksumNone, -1); err != nil {
			nlog.Errorln(err)
			return
		}
	default:
		debug.Assert(false, newStoreType)
	}

	obj.StoreType = newStoreType
	recm.contents.Delete(fullContentPath)
	n = sgl.Size()
	sgl.Free()
	return
}

func (recm *RecordManager) RecordContents() *sync.Map {
	return recm.contents
}

func (recm *RecordManager) ExtractionPaths() *sync.Map {
	return recm.extractionPaths
}

func (recm *RecordManager) Cleanup() {
	recm.Records.Drain()
	recm.extractionPaths.Range(func(k, _ any) bool {
		if err := fs.RemoveAll(k.(string)); err != nil {
			nlog.Errorf("could not remove extraction path (%v) from previous run, err: %v", k, err)
		}
		recm.extractionPaths.Delete(k)
		return true
	})
	recm.extractionPaths = nil
	recm.contents.Range(func(k, v any) bool {
		if sgl, ok := v.(*memsys.SGL); ok {
			sgl.Free()
		}
		recm.contents.Delete(k)
		return true
	})
	recm.contents = nil

	// NOTE: may call oom.FreeToOS
	core.T.PageMM().FreeSpec(memsys.FreeSpec{
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

// slightly altered cos.Ext to handle an additional "stop": record uname separator
// (that is, in addition to conventional path separator)
func cosExt(path string) (ext string) {
	for i := len(path) - 1; i >= 0 && !os.IsPathSeparator(path[i]) && path[i] != recSepa[0]; i-- {
		if path[i] == '.' {
			ext = path[i:]
		}
	}
	return
}

func genRecordUname(shardName, recordName string) string {
	recordWithoutExt := strings.TrimSuffix(recordName, cosExt(recordName))
	return shardName + recSepa + recordWithoutExt
}

func parseRecordUname(recordUniqueName string) (shardName, recordName string) {
	splits := strings.SplitN(recordUniqueName, recSepa, 2)
	return splits[0], splits[1]
}
