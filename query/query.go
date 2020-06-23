// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	ObjectFilter func(*cluster.LOM) bool

	ObjectsSource struct {
		// regexp *regexp.Regexp // support in the future
		pt *cmn.ParsedTemplate
	}

	BucketSource struct {
		// regexp *regexp.Regexp // support in the future
		Bck *cmn.Bck
	}

	ObjectsQuery struct {
		ObjectsSource *ObjectsSource
		BckSource     *BucketSource
		filter        ObjectFilter
	}
)

func NewQuery(source *ObjectsSource, bckSource *BucketSource, filter ObjectFilter) *ObjectsQuery {
	return &ObjectsQuery{
		ObjectsSource: source,
		BckSource:     bckSource,
		filter:        filter,
	}
}

func (q *ObjectsQuery) Filter() ObjectFilter {
	if q.filter != nil {
		return q.filter
	}
	return func(*cluster.LOM) bool { return true }
}

func TemplateObjSource(pt *cmn.ParsedTemplate) *ObjectsSource {
	return &ObjectsSource{pt: pt}
}

func BckSource(bck cmn.Bck) *BucketSource {
	return &BucketSource{Bck: &bck}
}
