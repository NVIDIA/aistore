// Package extract provides provides functions for working with compressed files
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/pkg/errors"
)

const (
	FormatTypeInt    = "int"
	FormatTypeFloat  = "float"
	FormatTypeString = "string"
)

var (
	supportedFormatTypes = []string{FormatTypeInt, FormatTypeFloat, FormatTypeString}

	errInvalidAlgorithmFormatTypes = fmt.Errorf("invalid algorithm format type provided, shoule be one of: %+v", supportedFormatTypes)
)

type (
	SingleKeyExtractor struct {
		name string
		buf  *bytes.Buffer
	}

	KeyExtractor interface {
		PrepareExtractor(name string, r cos.ReadSizer, ext string) (cos.ReadSizer, *SingleKeyExtractor, bool)

		// ExtractKey extracts key from either name or reader (file/sgl)
		ExtractKey(ske *SingleKeyExtractor) (interface{}, error)
	}

	md5KeyExtractor struct {
		h hash.Hash
	}

	nameKeyExtractor    struct{}
	contentKeyExtractor struct {
		ty  string // type of key extracted, supported: supportedFormatTypes
		ext string // extension of object record whose content will be read
	}
)

func NewMD5KeyExtractor() (KeyExtractor, error) {
	return &md5KeyExtractor{h: md5.New()}, nil
}

func (ke *md5KeyExtractor) ExtractKey(ske *SingleKeyExtractor) (interface{}, error) {
	s := fmt.Sprintf("%x", ke.h.Sum([]byte(ske.name)))
	ke.h.Reset()
	return s, nil
}

func (*md5KeyExtractor) PrepareExtractor(name string, r cos.ReadSizer, ext string) (cos.ReadSizer, *SingleKeyExtractor, bool) {
	return r, &SingleKeyExtractor{name: name}, false
}

func NewNameKeyExtractor() (KeyExtractor, error) {
	return &nameKeyExtractor{}, nil
}

func (*nameKeyExtractor) PrepareExtractor(name string, r cos.ReadSizer, ext string) (cos.ReadSizer, *SingleKeyExtractor, bool) {
	return r, &SingleKeyExtractor{name: name}, false
}

func (*nameKeyExtractor) ExtractKey(ske *SingleKeyExtractor) (interface{}, error) {
	return ske.name, nil
}

func NewContentKeyExtractor(ty, ext string) (KeyExtractor, error) {
	if err := ValidateAlgorithmFormatType(ty); err != nil {
		return nil, err
	}

	return &contentKeyExtractor{ty: ty, ext: ext}, nil
}

func (ke *contentKeyExtractor) PrepareExtractor(name string, r cos.ReadSizer, ext string) (cos.ReadSizer, *SingleKeyExtractor, bool) {
	if ke.ext != ext {
		return r, nil, false
	}

	buf := &bytes.Buffer{}
	tee := cos.NewSizedReader(io.TeeReader(r, buf), r.Size())
	return tee, &SingleKeyExtractor{name: name, buf: buf}, true
}

func (ke *contentKeyExtractor) ExtractKey(ske *SingleKeyExtractor) (interface{}, error) {
	if ske == nil { // is not valid to be read
		return nil, nil
	}

	b, err := io.ReadAll(ske.buf)
	ske.buf = nil
	if err != nil {
		return nil, err
	}

	key := string(b)
	switch ke.ty {
	case FormatTypeInt:
		return strconv.ParseInt(key, 10, 64)
	case FormatTypeFloat:
		return strconv.ParseFloat(key, 64)
	case FormatTypeString:
		return key, nil
	default:
		return nil, errors.Errorf("not implemented extractor type: %s", ke.ty)
	}
}

func ValidateAlgorithmFormatType(ty string) error {
	if !cos.StringInSlice(ty, supportedFormatTypes) {
		return errInvalidAlgorithmFormatTypes
	}

	return nil
}
