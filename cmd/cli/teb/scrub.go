// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"strconv"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	colBucket    = "BUCKET" // + [/PREFIX]
	colObjects   = "OBJECTS"
	colNotIn     = "NOT-CACHED" // "not in-cluster"
	colMisplaced = "MISPLACED"
	colMissingCp = "MISSING-COPIES"
	colSmallSz   = "SMALL"
	colLargeSz   = "LARGE"
	colVchanged  = "VERSION-CHANGED"
	colVremoved  = "VERSION-REMOVED"
)

const (
	ScrObjects = iota
	ScrNotIn
	ScrMisplaced
	ScrMissingCp
	ScrSmallSz
	ScrLargeSz
	ScrVchanged
	ScrVremoved

	ScrNumStats // NOTE: must be the last
)

var (
	ScrCols = [...]string{colObjects, colNotIn, colMisplaced, colMissingCp, colSmallSz, colLargeSz, colVchanged, colVremoved}
	ScrNums = [...]int64{0, 0, 0, 0, 0, 0, 0, 0}
)

type (
	CntSiz struct {
		Cnt int64
		Siz int64
	}
	ScrBp struct {
		Bck    cmn.Bck
		Prefix string
		Stats  [ScrNumStats]CntSiz
		// work
		Line  cos.Builder
		Cname string
	}
	ScrubHelper struct {
		All []*ScrBp
	}
)

func (h *ScrubHelper) colFirst() string {
	var num int
	for _, scr := range h.All {
		if scr.Prefix != "" {
			num++
		}
	}
	switch {
	case num == len(h.All):
		return colBucket + "/PREFIX"
	case num > 0:
		return colBucket + "[/PREFIX]"
	default:
		return colBucket
	}
}

func (h *ScrubHelper) MakeTab(units string, haveRemote bool) *Table {
	debug.Assert(len(ScrCols) == len(ScrNums))
	debug.Assert(len(ScrCols) == ScrNumStats)

	cols := make([]*header, 1, len(ScrCols)+1)
	cols[0] = &header{name: h.colFirst()}
	for _, col := range ScrCols {
		cols = append(cols, &header{name: col})
	}

	table := newTable(cols...)

	// hide assorted columns
	h.hideMissingCp(cols, colMissingCp)
	if !haveRemote {
		h._hideCol(cols, colNotIn)
		h._hideCol(cols, colVchanged)
		h._hideCol(cols, colVremoved)
	}

	// make tab
	for _, scr := range h.All {
		row := make([]string, 1, len(ScrCols)+1)
		row[0] = scr.Bck.Cname(scr.Prefix)

		for _, v := range scr.Stats {
			row = append(row, scr.fmtVal(v, units))
		}
		table.addRow(row)
	}

	return table
}

// missing-copies: hide when all-zeros
func (h *ScrubHelper) hideMissingCp(cols []*header, col string) {
	for _, scr := range h.All {
		if scr.Stats[ScrMissingCp].Cnt != 0 {
			return
		}
	}
	h._hideCol(cols, col)
}

func (*ScrubHelper) _hideCol(cols []*header, name string) {
	for _, col := range cols {
		if col.name == name {
			col.hide = true
		}
	}
}

// format values
const zeroCnt = "-"

func (*ScrBp) fmtVal(v CntSiz, units string) string {
	if v.Cnt == 0 {
		return zeroCnt
	}
	return strconv.FormatInt(v.Cnt, 10) + " (" + FmtSize(v.Siz, units, 1) + ")"
}
