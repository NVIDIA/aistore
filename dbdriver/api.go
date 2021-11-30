// Package dbdriver provides a local database server for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package dbdriver

import (
	"fmt"
	"strings"
)

// General info:
// ## Collection ##
//   For some databases(e.g., 'buntdb' or 'pudge') the collection is a pure
//   virtual stuff: it is just a prefix of a key in database. But it seems
//   useful to introduce this feature: other databases have 'bucket' concept
//   or other ways to separate data at database level similar to tables in
//   relational databases.
// ## List ##
//   If a pattern is empty, List should return the list of all keys in the
//   collection. A pattern may include '*' and '?'. If a pattern does not
//   include any of those characters, the pattern is considered a prefix and
//   trailing '*' is added automatically
// ## Errors ##
//   Different databases use different ways to returns erros. A driver must
//   convert original ones to `dbdriver` package errors for clients.

const CollectionSepa = "##"

type (
	Driver interface {
		// A driver should sync data with local drives on close
		Close() error
		// Write an object to database. Object is marshaled as JSON
		Set(collection, key string, object interface{}) error
		// Read an object from database.
		Get(collection, key string, object interface{}) error
		// Write an already marshaled object or simple string
		SetString(collection, key, data string) error
		// Read a string or an object as JSON from database
		GetString(collection, key string) (string, error)
		// Delete a single object
		Delete(collection, key string) error
		// Delete a collection. It iterates over all subkeys of key
		// `collection` and removes them one by one.
		DeleteCollection(collection string) error
		// Return subkeys of a collection(`key` is empty string) or a key.
		// Pattern is an arbitrary string and may contain '*' and '?' wildcards.
		// If a pattern does not include wildcards, the pattern is uses as a prefix.
		List(collection, pattern string) ([]string, error)
		// Return subkeys with their values: map[key]value
		GetAll(collection, pattern string) (map[string]string, error)
	}

	ErrNotFound struct {
		collection string
		key        string
	}
)

// Extract collection and key names from full key path
func ParsePath(path string) (string, string) {
	pos := strings.Index(path, CollectionSepa)
	if pos < 0 {
		return path, ""
	}
	return path[:pos], path[pos+len(CollectionSepa):]
}

func NewErrNotFound(collection, key string) *ErrNotFound {
	return &ErrNotFound{collection: collection, key: key}
}

func (e *ErrNotFound) Error() string {
	return fmt.Sprintf("%s %q not found", e.collection, e.key)
}

func IsErrNotFound(err error) bool {
	_, ok := err.(*ErrNotFound)
	return ok
}
