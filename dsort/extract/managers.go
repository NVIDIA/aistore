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
		ExtractShard(fqn string, r *io.SectionReader, extractor RecordExtractor, toDisk bool) (int64, int, error)
		CreateShard(s *Shard, w io.Writer, loadContent LoadContentFunc) (int64, error)
		UsingCompression() bool
		SupportsOffset() bool
		MetadataSize() int64
	}

	RecordExtractor interface {
		ExtractRecordWithBuffer(t ExtractCreator, fqn string, name string, r cmn.ReadSizer, metadata []byte, toDisk bool, offset int64, buf []byte) (int64, error)
	}

	RecordManager struct {
		Records *Records

		daemonID            string
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

func NewRecordManager(daemonID, extension string, keyExtractor KeyExtractor, onDuplicatedRecords func(string) error) *RecordManager {
	return &RecordManager{
		Records: NewRecords(1000),

		daemonID:            daemonID,
		extension:           extension,
		onDuplicatedRecords: onDuplicatedRecords,

		keyExtractor:    keyExtractor,
		contents:        &sync.Map{},
		extractionPaths: &sync.Map{},
	}
}

func (rm *RecordManager) ExtractRecordWithBuffer(t ExtractCreator, fqn, name string, r cmn.ReadSizer, metadata []byte, toDisk bool, offset int64, buf []byte) (size int64, err error) {
	var (
		contentPath string
		mdSize      int64

		ext                  = filepath.Ext(name)
		recordPath, fullPath = rm.paths(fqn, name, ext)
	)

	// If the content already exists we should skip it but set error (caller
	// needs to handle it properly).
	if rm.Records.Exists(recordPath, ext) {
		msg := fmt.Sprintf("record %q has been duplicated", recordPath)
		rm.Records.DeleteDup(recordPath, ext)

		// NOTE: there is no need to remove anything from `rm.extractionPaths`
		// or `rm.contents` since it will be removed anyway in cleanup.
		// Assumption is that there will be not much duplicates and we can live
		// with a little bit more files/memory.
		return 0, rm.onDuplicatedRecords(msg)
	}

	r, ske, needRead := rm.keyExtractor.PrepareExtractor(name, r, ext)
	// TODO: support SGLs even when offsets are supported.
	if t.SupportsOffset() {
		// If extractor was initialized we need to read the content, since it
		// may contain information about the sorting/shuffling key.
		if needRead {
			if _, err := io.CopyBuffer(ioutil.Discard, r, buf); err != nil {
				return 0, err
			}
		}

		mdSize = t.MetadataSize()
		size = r.Size()
		contentPath = fqn
	} else if toDisk {
		newF, err := cmn.CreateFile(fullPath)
		if err != nil {
			return size, err
		}
		if size, err = copyMetadataAndData(newF, r, metadata, buf); err != nil {
			newF.Close()
			return size, err
		}
		newF.Close()
		rm.extractionPaths.Store(fullPath, struct{}{})

		mdSize = int64(len(metadata))
		contentPath = recordPath
	} else {
		sgl := mem.NewSGL(r.Size() + int64(len(metadata)))
		if size, err = copyMetadataAndData(sgl, r, metadata, buf); err != nil {
			return size, err
		}
		rm.contents.Store(fullPath, sgl)

		mdSize = int64(len(metadata))
		contentPath = recordPath
	}

	key, err := rm.keyExtractor.ExtractKey(ske)
	if err != nil {
		return size, err
	}

	cmn.AssertMsg(contentPath != "", fmt.Sprintf("fqn: %s; name: %s", fqn, name))
	rm.Records.Insert(&Record{
		Key:         key,
		Name:        recordPath,
		DaemonID:    rm.daemonID,
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

func (rm *RecordManager) paths(fqn, name, ext string) (string, string) {
	fqnWithoutExt := strings.TrimSuffix(fqn, rm.extension)
	keyWithoutExt := strings.TrimSuffix(name, ext)
	recordPath := fs.CSM.GenContentFQN(fqnWithoutExt+"-"+keyWithoutExt, filetype.DSortFileType, "")
	fullPath := recordPath + ext
	return recordPath, fullPath
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
