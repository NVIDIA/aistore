// Package kvdb provides a local key/value database server for AIS.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package kvdb

import (
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"

	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/buntdb"
)

// BuntDB:
// At this moment, BuntDB runs with the following settings:
// - calls filesystem sync every second (if we want to do it less frequently
//   may add `Flush` to `Driver` interface and call in Get/Set/Delete/List
//   when a certain number of seconds pass. There is not way to do it in easier
//   way with built-in stuff: either Never or every second)
// - after database size exceeds `autoShrinkSize`(1MB), BuntDB starts compacting
//   the database periodically(when it grows over a given limit)
// - compacting is executed when the database size is greater than the size
//   after previous compacting by `AutoShrinkPercentage`(50%)

const autoShrinkSize = cos.MiB

type BuntDriver struct {
	driver *buntdb.DB
}

// interface guard
var _ Driver = (*BuntDriver)(nil)

func NewBuntDB(path string) (*BuntDriver, error) {
	driver, err := buntdb.Open(path)
	if err != nil {
		return nil, err
	}
	driver.SetConfig(buntdb.Config{
		SyncPolicy:           buntdb.EverySecond, // periodical FS sync
		AutoShrinkMinSize:    autoShrinkSize,     // start autoShrink only of file exceeds the size
		AutoShrinkPercentage: 50,                 // run compacting when DB grows by half
	})
	return &BuntDriver{driver: driver}, nil
}

func buntToCommonErr(err error, collection, key string) (int, error) {
	if err == nil {
		return http.StatusOK, nil
	}
	what := collection
	if key != "" {
		what += " \"" + key + "\""
	}
	switch err { // TODO: more buntdb errors
	case buntdb.ErrNotFound:
		return http.StatusNotFound, cos.NewErrNotFound(nil, what)
	case buntdb.ErrIndexExists:
		return http.StatusConflict, cos.NewErrAlreadyExists(nil, what)
	default:
		return http.StatusInternalServerError, err
	}
}

// Create "unique" key from collection and key, so there was no trouble when
// there is an overlap. E.g, if key and collection uses the same separator
// for subkeys, two pairs ("abc", "def/ghi") and ("abc/def", "ghi") generate
// the same full path. The function should make them different.
func makePath(collection, key string) string {
	if strings.HasSuffix(collection, "##") {
		return collection + key
	}
	return collection + CollectionSepa + key
}

func (bd *BuntDriver) Close() error {
	return bd.driver.Close()
}

func (bd *BuntDriver) Set(collection, key string, object any) (int, error) {
	b := cos.MustMarshal(object)
	return bd.SetString(collection, key, string(b))
}

func (bd *BuntDriver) Get(collection, key string, object any) (int, error) {
	s, code, err := bd.GetString(collection, key)
	if err != nil {
		return code, err
	}
	if err := jsoniter.Unmarshal([]byte(s), object); err != nil {
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

func (bd *BuntDriver) SetString(collection, key, data string) (int, error) {
	name := makePath(collection, key)
	err := bd.driver.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(name, data, nil)
		return err
	})
	return buntToCommonErr(err, collection, key)
}

func (bd *BuntDriver) GetString(collection, key string) (string, int, error) {
	var value string
	name := makePath(collection, key)
	err := bd.driver.View(func(tx *buntdb.Tx) error {
		var err error
		value, err = tx.Get(name)
		return err
	})
	code, err := buntToCommonErr(err, collection, key)
	return value, code, err
}

func (bd *BuntDriver) Delete(collection, key string) (int, error) {
	name := makePath(collection, key)
	err := bd.driver.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(name)
		return err
	})
	return buntToCommonErr(err, collection, key)
}

func (bd *BuntDriver) List(collection, pattern string) ([]string, int, error) {
	var (
		keys   = make([]string, 0)
		filter string
	)
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		pattern += "*"
	}
	filter = makePath(collection, pattern)
	err := bd.driver.View(func(tx *buntdb.Tx) error {
		tx.AscendKeys(filter, func(path, _ string) bool {
			_, key := ParsePath(path)
			if key != "" {
				keys = append(keys, key)
			}
			return true
		})
		return nil
	})
	code, err := buntToCommonErr(err, collection, "")
	return keys, code, err
}

func (bd *BuntDriver) DeleteCollection(collection string) (int, error) {
	keys, code, err := bd.List(collection, "")
	if err != nil || len(keys) == 0 {
		return code, err
	}
	err = bd.driver.Update(func(tx *buntdb.Tx) error {
		for _, k := range keys {
			_, err := tx.Delete(k)
			if err != nil && err != buntdb.ErrNotFound {
				return err
			}
		}
		return nil
	})
	return buntToCommonErr(err, collection, "")
}

func (bd *BuntDriver) GetAll(collection, pattern string) (map[string]string, int, error) {
	var (
		values = make(map[string]string)
		filter string
	)
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		pattern += "*"
	}
	filter = makePath(collection, pattern)
	err := bd.driver.View(func(tx *buntdb.Tx) error {
		tx.AscendKeys(filter, func(path, val string) bool {
			_, key := ParsePath(path)
			if key != "" {
				values[key] = val
			}
			return true
		})
		return nil
	})
	code, err := buntToCommonErr(err, collection, "")
	return values, code, err
}
