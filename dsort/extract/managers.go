/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"bytes"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dsort/filetype"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
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
		MetadataSize() int64
	}

	RecordExtractor interface {
		ExtractRecord(fqn string, name string, r cmn.ReadSizer, metadata []byte, toDisk bool) (int64, error)
		ExtractRecordWithBuffer(fqn string, name string, r cmn.ReadSizer, metadata []byte, toDisk bool, buf []byte) (int64, error)
	}

	RecordManager struct {
		Records *Records

		daemonID  string
		extension string

		h               hash.Hash
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
	})
}

func NewRecordManager(daemonID, extension string, keyExtractor KeyExtractor) *RecordManager {
	return &RecordManager{
		Records: NewRecords(1000),

		daemonID:  daemonID,
		extension: extension,

		keyExtractor:    keyExtractor,
		contents:        &sync.Map{},
		extractionPaths: &sync.Map{},
	}
}

func (rm *RecordManager) ExtractRecord(fqn, name string, r cmn.ReadSizer, metadata []byte, toDisk bool) (int64, error) {
	return rm.ExtractRecordWithBuffer(fqn, name, r, metadata, toDisk, nil)
}

func (rm *RecordManager) ExtractRecordWithBuffer(fqn, name string, r cmn.ReadSizer, metadata []byte, toDisk bool, buf []byte) (size int64, err error) {
	ext := filepath.Ext(name)
	recordPath, fullPath := rm.paths(fqn, name, ext)

	r, ske := rm.keyExtractor.PrepareExtractor(name, r, ext)
	if toDisk {
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
	} else {
		sgl := mem.NewSGL(r.Size() + int64(len(metadata)))
		if size, err = copyMetadataAndData(sgl, r, metadata, buf); err != nil {
			return size, err
		}
		rm.contents.Store(fullPath, sgl)
	}

	key, err := rm.keyExtractor.ExtractKey(ske)
	if err != nil {
		return size, err
	}

	rm.Records.Insert(&Record{
		Key:         key,
		DaemonID:    rm.daemonID,
		ContentPath: recordPath,
		Objects: []*RecordObj{&RecordObj{
			MetadataSize: int64(len(metadata)),
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

func (rm *RecordManager) Cleanup() {
	rm.Records.Drain()
	rm.extractionPaths.Range(func(k, v interface{}) bool {
		if err := os.RemoveAll(k.(string)); err != nil {
			glog.Errorf("could not remove extraction path (%v) from previous run, err: %v", k, err)
		}
		rm.extractionPaths.Delete(k)
		return true
	})
	rm.contents.Range(func(k, v interface{}) bool {
		if sgl, ok := v.(*memsys.SGL); ok {
			sgl.Free()
		}
		rm.contents.Delete(k)
		return true
	})
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
