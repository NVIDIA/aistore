/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package extract

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"strconv"

	"github.com/NVIDIA/dfcpub/cmn"
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
		PrepareExtractor(name string, r cmn.ReadSizer, ext string) (cmn.ReadSizer, *SingleKeyExtractor)

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

func (ke *md5KeyExtractor) PrepareExtractor(name string, r cmn.ReadSizer, ext string) (cmn.ReadSizer, *SingleKeyExtractor) {
	return r, &SingleKeyExtractor{name: name}
}

func NewNameKeyExtractor() (KeyExtractor, error) {
	return &nameKeyExtractor{}, nil
}

func (ke *nameKeyExtractor) PrepareExtractor(name string, r cmn.ReadSizer, ext string) (cmn.ReadSizer, *SingleKeyExtractor) {
	return r, &SingleKeyExtractor{name: name}
}

func (ke *nameKeyExtractor) ExtractKey(ske *SingleKeyExtractor) (interface{}, error) {
	return ske.name, nil
}

func NewContentKeyExtractor(ty, ext string) (KeyExtractor, error) {
	if err := ValidateAlgorithmFormatType(ty); err != nil {
		return nil, err
	}

	return &contentKeyExtractor{ty: ty, ext: ext}, nil
}

func (ke *contentKeyExtractor) PrepareExtractor(name string, r cmn.ReadSizer, ext string) (cmn.ReadSizer, *SingleKeyExtractor) {
	if ke.ext != ext {
		return r, nil
	}

	buf := &bytes.Buffer{}
	tee := cmn.NewSizedReader(io.TeeReader(r, buf), r.Size())
	return tee, &SingleKeyExtractor{name: name, buf: buf}
}

func (ke *contentKeyExtractor) ExtractKey(ske *SingleKeyExtractor) (interface{}, error) {
	if ske == nil { // is not valid to be read
		return nil, nil
	}

	b, err := ioutil.ReadAll(ske.buf)
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
		return nil, fmt.Errorf("not implemented extractor type: %s", ke.ty)
	}
}

func (ske *SingleKeyExtractor) extract() {
	return
}

func ValidateAlgorithmFormatType(ty string) error {
	if !cmn.StringInSlice(ty, supportedFormatTypes) {
		return errInvalidAlgorithmFormatTypes
	}

	return nil
}
