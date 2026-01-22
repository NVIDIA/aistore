//go:build dsort

// Package shard provides Extract(shard), Create(shard), and associated methods
// across all supported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"encoding/json"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/pkg/errors"
)

//
// NOTE: changes in this source MAY require re-running `msgp` code generation - see docs/msgp.md for details.
//
// NOTE: Since tags for unexported fields are not supported before generation
//       change `Records.arr` to `Records.Arr` and then rename it back.
//

// interface guard
var (
	_ json.Marshaler   = (*Records)(nil)
	_ json.Unmarshaler = (*Records)(nil)
)

const (
	// Values are small to save memory.
	OffsetStoreType = "o"
	SGLStoreType    = "s"
	DiskStoreType   = "d"
)

type (
	// RecordObj describes single object of record. Objects inside a single record
	// have different extensions.
	RecordObj struct {
		// Can represent, one of the following:
		//  * Shard name - in case offset is used.
		//  * Key for extractCreator's RecordContents - records stored in SGLs.
		//  * Location (full path) on disk where extracted record has been placed.
		//
		// To get path for given object you need to use `FullContentPath` method.
		ContentPath string `msg:"p"  json:"p"`

		// Filesystem file type where the shard is stored - used to determine
		// location for content path when asking filesystem.
		ObjectFileType string `msg:"ft" json:"ft"`

		// Determines where the record has been stored, can be either: OffsetStoreType,
		// SGLStoreType, DiskStoreType.
		StoreType string `msg:"st" json:"st"`

		// If set, determines the offset in shard file where the record begins.
		Offset       int64  `msg:"f,omitempty" json:"f,string,omitempty"`
		MetadataSize int64  `msg:"ms" json:"ms,string"`
		Size         int64  `msg:"s" json:"s,string"`
		Extension    string `msg:"e" json:"e"`
	}

	// Record represents the metadata corresponding to a single file from a shard.
	Record struct {
		Key      any    `msg:"k" json:"k"` // Used to determine the sorting order.
		Name     string `msg:"n" json:"n"` // Name that uniquely identifies record across all shards.
		DaemonID string `msg:"d" json:"d"` // ID of the target which maintains the contents for this record.
		// All objects associated with given record. Record can be composed of
		// multiple objects which have the same name but different extension.
		Objects []*RecordObj `msg:"o" json:"o"`
	}

	// Records abstract array of records. It safe to be used concurrently.
	Records struct {
		arr              []*Record           `msg:"a"`
		m                map[string]*Record  `msg:"-"`
		dups             map[string]struct{} `msg:"-"` // contains duplicate object names, if any
		totalObjectCount int                 `msg:"-"` // total number of objects in all records (dups are removed so not counted)
		sync.RWMutex     `msg:"-"`
	}
)

////////////
// Record //
////////////

// Merges two records into single one. It is required for records to have the
// same Name. Since records should only differ on objects this is the thing that
// is actually merged.
func (r *Record) mergeObjects(other *Record) {
	debug.Assert(r.Name == other.Name, r.Name+" vs "+other.Name)
	if r.Key == nil && other.Key != nil {
		r.Key = other.Key
	}
	r.Objects = append(r.Objects, other.Objects...)
}

func (r *Record) find(ext string) int {
	for idx, obj := range r.Objects {
		if obj.Extension == ext {
			return idx
		}
	}
	return -1
}

func (r *Record) exists(ext string) bool {
	return r.find(ext) >= 0
}

func (r *Record) delete(ext string) (deleted bool) {
	foundIdx := r.find(ext)
	if foundIdx >= 0 {
		// NOTE: We are required to preserve the order.
		r.Objects = append(r.Objects[:foundIdx], r.Objects[foundIdx+1:]...)
		return true
	}
	return false
}

func (r *Record) TotalSize() int64 {
	size := int64(0)
	for _, obj := range r.Objects {
		size += obj.Size
	}
	return size
}

func (r *Record) MakeUniqueName(obj *RecordObj) string {
	return r.Name + obj.Extension
}

/////////////
// Records //
/////////////

// NewRecords creates new instance of Records struct and allocates n places for
// the actual Record's
func NewRecords(n int) *Records {
	return &Records{
		arr:  make([]*Record, 0, n),
		m:    make(map[string]*Record, 100),
		dups: make(map[string]struct{}, 10),
	}
}

func (r *Records) Drain() {
	r.Lock()
	r.arr = nil
	r.m = nil
	r.dups = nil
	r.Unlock()
}

func (r *Records) Insert(records ...*Record) {
	r.Lock()
	for _, record := range records {
		// Checking if record is already registered. If that is the case we need
		// to merge extensions (files with same names but different extensions
		// should be in single record). Otherwise just add new record.
		if existingRecord, ok := r.m[record.Name]; ok {
			existingRecord.mergeObjects(record)
		} else {
			r.arr = append(r.arr, record)
			r.m[record.Name] = record
		}

		r.totalObjectCount += len(record.Objects)
	}
	r.Unlock()
}

func (r *Records) DeleteDup(name, ext string) {
	debug.Assert(r.Exists(name, ext), "record: "+name+", "+ext)
	r.Lock()
	if record, ok := r.m[name]; ok {
		if record.delete(ext) {
			r.totalObjectCount--
		}
	}
	r.dups[name+ext] = struct{}{}
	r.Unlock()
}

// NOTE: must be done under lock
func (r *Records) Find(name string) (record *Record, exists bool) {
	record, exists = r.m[name]
	return
}

func (r *Records) Exists(name, ext string) (exists bool) {
	r.RLock()
	if record, ok := r.m[name]; ok {
		exists = record.exists(ext)
		if !exists {
			_, exists = r.dups[name+ext]
		}
	}
	r.RUnlock()
	return
}

func (r *Records) merge(records *Records) {
	r.Insert(records.arr...)
}

func (r *Records) All() []*Record {
	return r.arr
}

func (r *Records) Slice(start, end int) *Records {
	return &Records{
		arr: r.arr[start:end],
	}
}

func (r *Records) Len() int {
	return len(r.arr)
}

func (r *Records) Swap(i, j int) { r.arr[i], r.arr[j] = r.arr[j], r.arr[i] }

func (r *Records) Less(i, j int, keyType string) (bool, error) {
	lhs, rhs := r.arr[i].Key, r.arr[j].Key
	if lhs == nil {
		return false, errors.Errorf("key is missing for %q", r.arr[i].Name)
	} else if rhs == nil {
		return false, errors.Errorf("key is missing for %q", r.arr[j].Name)
	}

	switch keyType {
	case ContentKeyInt:
		ilhs, lok := lhs.(int64)
		irhs, rok := rhs.(int64)
		if lok && rok {
			return ilhs < irhs, nil
		}
		// (motivation: javascript does not support int64 type)
		if !lok {
			ilhs = int64(lhs.(float64))
		} else {
			irhs = int64(rhs.(float64))
		}
		return ilhs < irhs, nil
	case ContentKeyFloat:
		flhs, lok := lhs.(float64)
		frhs, rok := rhs.(float64)
		debug.Assert(lok, lhs)
		debug.Assert(rok, rhs)
		return flhs < frhs, nil
	case ContentKeyString:
		slhs, lok := lhs.(string)
		srhs, rok := rhs.(string)
		debug.Assert(lok, lhs)
		debug.Assert(rok, rhs)
		return slhs < srhs, nil
	}

	debug.Assertf(false, "lhs: %v, rhs: %v, arr[i]: %v, arr[j]: %v", lhs, rhs, r.arr[i], r.arr[j])
	return false, nil
}

func (r *Records) TotalObjectCount() int {
	return r.totalObjectCount
}

func (r *Records) RecordMemorySize() (size uint64) {
	r.Lock()
	defer r.Unlock()

	var maxSize uint64
	for _, record := range r.arr {
		size = uint64(unsafe.Sizeof(*record))
		size += uint64(len(record.DaemonID))
		size += uint64(len(record.Name))
		size += uint64(unsafe.Sizeof(record.Key))
		maxSize = max(maxSize, size)

		// If there is record which has at least 1 record object we should get
		// the estimate of it and return the size. Some records might not have
		// at least 1 record object because there were duplicated and in
		// consequence were removed from the record.
		if len(record.Objects) > 0 {
			size += (uint64(unsafe.Sizeof(record.Objects)) +
				uint64(len(record.Objects[0].Extension)) +
				uint64(len(record.Objects[0].ContentPath)) +
				uint64(len(record.Objects[0].ObjectFileType)) +
				uint64(len(record.Objects[0].StoreType))) * uint64(len(record.Objects))
			return size
		}
	}
	return maxSize
}

func (*Records) MarshalJSON() ([]byte, error) { panic("should not be used") }
func (*Records) UnmarshalJSON([]byte) error   { panic("should not be used") }
