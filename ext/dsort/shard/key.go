// Package shard provides Extract(shard), Create(shard), and associated methods
// across all suppported archival formats (see cmn/archive/mime.go)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package shard

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strconv"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	ContentKeyInt    = "int"
	ContentKeyFloat  = "float"
	ContentKeyString = "string"

	fmtErrInvalidSortingKeyType = "invalid content sorting key %q (expecting one of: %+v)"
)

var (
	contentKeyTypes = []string{ContentKeyInt, ContentKeyFloat, ContentKeyString}
)

type (
	SingleKeyExtractor struct {
		name string
		buf  *bytes.Buffer
	}

	KeyExtractor interface {
		PrepareExtractor(name string, r cos.ReadSizer, ext string) (cos.ReadSizer, *SingleKeyExtractor, bool)

		// ExtractKey extracts key from either name or reader (file/sgl)
		ExtractKey(ske *SingleKeyExtractor) (any, error)
	}

	md5KeyExtractor struct {
		h hash.Hash
	}

	nameKeyExtractor    struct{}
	contentKeyExtractor struct {
		ty  string // one of contentKeyTypes: {"int", "string", ... } - see above
		ext string // file with this extension provides sorting key (of the type `ty`)
	}
)

func NewMD5KeyExtractor() (KeyExtractor, error) {
	return &md5KeyExtractor{h: md5.New()}, nil
}

func (ke *md5KeyExtractor) ExtractKey(ske *SingleKeyExtractor) (any, error) {
	s := hex.EncodeToString(ke.h.Sum([]byte(ske.name)))
	ke.h.Reset()
	return s, nil
}

func (*md5KeyExtractor) PrepareExtractor(name string, r cos.ReadSizer, _ string) (cos.ReadSizer, *SingleKeyExtractor, bool) {
	return r, &SingleKeyExtractor{name: name}, false
}

func NewNameKeyExtractor() (KeyExtractor, error) {
	return &nameKeyExtractor{}, nil
}

func (*nameKeyExtractor) PrepareExtractor(name string, r cos.ReadSizer, _ string) (cos.ReadSizer, *SingleKeyExtractor, bool) {
	return r, &SingleKeyExtractor{name: name}, false
}

func (*nameKeyExtractor) ExtractKey(ske *SingleKeyExtractor) (any, error) {
	return ske.name, nil
}

func NewContentKeyExtractor(ty, ext string) (KeyExtractor, error) {
	if err := ValidateContentKeyT(ty); err != nil {
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

func (ke *contentKeyExtractor) ExtractKey(ske *SingleKeyExtractor) (any, error) {
	if ske == nil {
		return nil, nil
	}
	b, err := io.ReadAll(ske.buf)
	ske.buf = nil
	if err != nil {
		return nil, err
	}
	key := string(b)
	switch ke.ty {
	case ContentKeyInt:
		return strconv.ParseInt(key, 10, 64)
	case ContentKeyFloat:
		return strconv.ParseFloat(key, 64)
	case ContentKeyString:
		return key, nil
	default:
		return nil, fmt.Errorf(fmtErrInvalidSortingKeyType, ke.ty, contentKeyTypes)
	}
}

func ValidateContentKeyT(ty string) error {
	if !cos.StringInSlice(ty, contentKeyTypes) {
		return fmt.Errorf(fmtErrInvalidSortingKeyType, ty, contentKeyTypes)
	}
	return nil
}
