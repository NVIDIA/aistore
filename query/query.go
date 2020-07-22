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
	ObjectsSource struct {
		// regexp *regexp.Regexp // support in the future
		Pt *cmn.ParsedTemplate
	}

	BucketSource struct {
		// regexp *regexp.Regexp // support in the future
		Bck *cluster.Bck
	}

	ObjectsQuery struct {
		ObjectsSource *ObjectsSource
		BckSource     *BucketSource
		filter        cluster.ObjectFilter
	}
)

func NewQuery(source *ObjectsSource, bckSource *BucketSource, filter cluster.ObjectFilter) *ObjectsQuery {
	return &ObjectsQuery{
		ObjectsSource: source,
		BckSource:     bckSource,
		filter:        filter,
	}
}

func (q *ObjectsQuery) Filter() cluster.ObjectFilter {
	if q.filter != nil {
		return q.filter
	}
	return func(*cluster.LOM) bool { return true }
}

func TemplateObjSource(template string) (*ObjectsSource, error) {
	pt, err := cmn.ParseBashTemplate(template)
	if err != nil {
		return nil, err
	}
	return &ObjectsSource{Pt: &pt}, nil
}

func AllObjSource() *ObjectsSource {
	return &ObjectsSource{}
}

func BckSource(bck cmn.Bck, node cluster.Node) (*BucketSource, error) {
	b := cluster.NewBckEmbed(bck)
	if err := b.Init(node.GetBowner(), node.Snode()); err != nil {
		return nil, err
	}
	return &BucketSource{Bck: b}, nil
}

func NewQueryFromMsg(node cluster.Node, msg *DefMsg) (q *ObjectsQuery, err error) {
	q = &ObjectsQuery{}
	if msg.OuterSelect.Template != "" {
		if q.ObjectsSource, err = TemplateObjSource(msg.OuterSelect.Template); err != nil {
			return nil, err
		}
	} else {
		q.ObjectsSource = AllObjSource()
	}

	if q.BckSource, err = BckSource(msg.From.Bck, node); err != nil {
		return nil, err
	}
	if q.filter, err = ObjFilterFromMsg(msg.Where.Filter); err != nil {
		return nil, err
	}
	return q, nil
}
