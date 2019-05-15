// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

var (
	_ json.Marshaler   = &Records{}
	_ json.Unmarshaler = &Records{}
)

const (
	// Values are small to save memory.
	OffsetStoreType = "o"
	SGLStoreType    = "s"
	DiskStoreType   = "d"
)

type (
	// RecordObj describes single object of record. Objects inside single record
	// differs by extension.
	RecordObj struct {
		// Can represent, one of the following:
		//  * Shard name - in case offset is used.
		//  * Key for extractCreator's RecordContents - records stored in SGLs.
		//  * Location (full path) on disk where extracted record has been placed.
		//
		// To get path for given object you need to use `FullContentPath` method.
		ContentPath string `json:"p"`

		// Determines where the record has been stored, can be either: OffsetStoreType,
		// SGLStoreType, DiskStoreType.
		StoreType string `json:"st"`

		// If set, determines the offset in shard file where the record begins.
		Offset       int64  `json:"f,omitempty"`
		MetadataSize int64  `json:"ms"`
		Size         int64  `json:"s"`
		Extension    string `json:"e"`
	}

	// Record represents the metadata corresponding to a single file from an archive file.
	Record struct {
		Key      interface{} `json:"k"` // Used to determine the sorting order.
		Name     string      `json:"n"` // Name which uniquely identifies record across all shards.
		DaemonID string      `json:"d"` // ID of the target which maintains the contents for this record.
		// All objects associated with given record. Record can be composed of
		// multiple objects which have the same name but different extension.
		Objects []*RecordObj `json:"o"`
	}

	// Records abstract array of records. It safe to be used concurrently.
	Records struct {
		sync.RWMutex
		arr              []*Record
		m                map[string]*Record
		dups             map[string]struct{} // contains duplicate object names, if any
		totalObjectCount int                 // total number of objects in all records (dups are removed so not counted)
	}
)

// Merges two records into single one. It is required for records to have the
// same Name. Since records should only differ on objects this is the thing that
// is actually merged.
func (r *Record) mergeObjects(other *Record) {
	cmn.Assert(r.Name == other.Name)
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

func (r *Record) delete(ext string) {
	foundIdx := r.find(ext)
	if foundIdx >= 0 {
		// NOTE: We are required to preserve the order.
		r.Objects = append(r.Objects[:foundIdx], r.Objects[foundIdx+1:]...)
	}
}

func (r *Record) TotalSize() int64 {
	size := int64(0)
	for _, obj := range r.Objects {
		size += obj.Size
	}
	return size
}

func (r *Record) MemorySize() uint64 {
	size := uint64(unsafe.Sizeof(*r))
	size += uint64(len(r.DaemonID))
	size += uint64(len(r.Name))
	size += (uint64(unsafe.Sizeof(r.Objects)) +
		uint64(len(r.Objects[0].Extension)) +
		uint64(len(r.Objects[0].ContentPath))) * uint64(len(r.Objects))
	return size
}

// NewRecords creates new instance of Records struct and allocates n places for
// the actual Record's
func NewRecords(n int) *Records {
	return &Records{
		arr:  make([]*Record, 0, n),
		m:    make(map[string]*Record, n),
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
	cmn.Assert(r.Exists(name, ext))
	r.Lock()
	if record, ok := r.m[name]; ok {
		record.delete(ext)
	}
	r.dups[name+ext] = struct{}{}
	r.totalObjectCount--
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

func (r *Records) Less(i, j int, formatType string) (bool, error) {
	lhs, rhs := r.arr[i].Key, r.arr[j].Key
	if lhs == nil {
		return false, fmt.Errorf("key is missing for %q", r.arr[i].Name)
	} else if rhs == nil {
		return false, fmt.Errorf("key is missing for %q", r.arr[j].Name)
	}

	switch formatType {
	case FormatTypeInt:
		ilhs, lok := lhs.(int64)
		irhs, rok := rhs.(int64)
		if lok && rok {
			return ilhs < irhs, nil
		}

		// One side was parsed as float64 - javascript does not support
		// int64 type and it fallback to float64
		if !lok {
			ilhs = int64(lhs.(float64))
		}
		if !rok {
			irhs = int64(rhs.(float64))
		}

		return ilhs < irhs, nil
	case FormatTypeFloat:
		return lhs.(float64) < rhs.(float64), nil
	case FormatTypeString:
		return lhs.(string) < rhs.(string), nil
	}

	cmn.AssertFmt(false, lhs, rhs, r.arr[i], r.arr[j])
	return false, nil
}

func (r *Records) objectCount() int {
	return r.totalObjectCount
}

func (r *Records) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(r.arr)
}

func (r *Records) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &r.arr)
}
