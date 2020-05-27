// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
		QueryMsg DefMsg `json:"query"`
	}

	NextMsg struct {
		Handle string `json:"handle"`
		Size   uint   `json:"size"` // how many objects to fetch
	}

	// Definition of a query
	DefMsg struct {
		OuterSelect OuterSelectMsg `json:"outer_select"`
		From        FromMsg        `json:"from"`
		Where       WhereMsg       `json:"where"`
	}

	// OuterSelect -> Look only on objects' metadata.
	// In the future we might have InnerSelect, which looks into objects' contents
	OuterSelectMsg struct {
		Template string `json:"objects_source"`
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
