// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package query

import "github.com/NVIDIA/aistore/cmn"

const (
	FUNCTION = "F"
	AND      = "AND"
	OR       = "OR"
)

type (
	InitMsg struct {
		QueryMsg   DefMsg `json:"query"`
		WorkersCnt uint   `json:"workers"`
	}

	NextMsg struct {
		Handle   string `json:"handle"`
		Size     uint   `json:"size"` // how many objects to fetch
		WorkerID uint   `json:"worker_id"`
	}

	// Definition of a query
	DefMsg struct {
		OuterSelect OuterSelectMsg `json:"outer_select"`
		InnerSelect InnerSelectMsg `json:"inner_select"`
		From        FromMsg        `json:"from"`
		Where       WhereMsg       `json:"where"`
		Fast        bool           `json:"fast"`
	}

	// OuterSelect -> Look only on objects' metadata.
	// In the future we might have InnerSelect, which looks into objects' contents
	OuterSelectMsg struct {
		Prefix   string `json:"prefix"`
		Template string `json:"objects_source"`
	}

	InnerSelectMsg struct {
		Props string `json:"props"`
	}

	FromMsg struct {
		Bck cmn.Bck `json:"bucket"`
	}

	WhereMsg struct {
		Filter *FilterMsg `json:"filter"`
	}

	FilterMsg struct {
		Type string `json:"type"` // one of: FUNCTION, AND, OR

		FName string   `json:"filter_name"`
		Args  []string `json:"args"`

		Filters []*FilterMsg `json:"inner_filters"`
	}
)
