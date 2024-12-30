// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"strconv"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	colBucket    = "BUCKET" // + [/PREFIX]
	colObjects   = "OBJECTS"
	colMisplaced = "MISPLACED"
	colMissing   = "MISSING-COPIES"
	colSmall     = "SMALL"
	colLarge     = "LARGE"
	colVchanged  = "VERSION-CHANGED"
	colVremoved  = "VERSION-REMOVED"
)

type (
	CntSiz struct {
		Cnt int64
		Siz int64
	}
	ScrubOne struct {
		Bck    cmn.Bck
		Prefix string
		Listed CntSiz
		Stats  struct {
			Misplaced CntSiz
			MissingCp CntSiz
			SmallSz   CntSiz
			LargeSz   CntSiz
			Vchanged  CntSiz
			Vremoved  CntSiz
		}
	}
	ScrubHelper struct {
		All []*ScrubOne
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
	var (
		cols = []*header{
			{name: h.colFirst()},
			{name: colObjects},
			{name: colMisplaced},
			{name: colMissing},
			{name: colSmall},
			{name: colLarge},
			{name: colVchanged},
			{name: colVremoved},
		}
		table = newTable(cols...)
	)

	// hide assorted columns
	h.hideMissingCp(cols, colMissing)
	if !haveRemote {
		h._hideCol(cols, colVchanged)
		h._hideCol(cols, colVremoved)
	}

	// make tab
	for _, scr := range h.All {
		row := []string{
			scr.Bck.Cname(scr.Prefix),
			scr.fmtListed(units),
			scr.fmtMisplaced(units),
			scr.fmtMissingCp(),
			scr.fmtSmallSz(units),
			scr.fmtLargeSz(units),
			scr.fmtVchanged(units),
			scr.fmtVremoved(units),
		}
		table.addRow(row)
	}

	return table
}

func (h *ScrubHelper) hideMissingCp(cols []*header, col string) {
	for _, scr := range h.All {
		if scr.Stats.MissingCp.Cnt != 0 {
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

//
// TODO -- FIXME: reduce code
//

const zeroCnt = "-"

func (v *CntSiz) fmt(units string) string {
	return strconv.FormatInt(v.Cnt, 10) + " (" + FmtSize(v.Siz, units, 1) + ")"
}

func (scr *ScrubOne) fmtListed(units string) string {
	return scr.Listed.fmt(units)
}

func (scr *ScrubOne) fmtMisplaced(units string) string {
	if scr.Stats.Misplaced.Cnt == 0 {
		return zeroCnt
	}
	return scr.Stats.Misplaced.fmt(units)
}

func (scr *ScrubOne) fmtMissingCp() string {
	if scr.Stats.MissingCp.Cnt == 0 {
		return zeroCnt
	}
	return strconv.FormatInt(scr.Stats.MissingCp.Cnt, 10)
}

func (scr *ScrubOne) fmtSmallSz(units string) string {
	if scr.Stats.SmallSz.Cnt == 0 {
		return zeroCnt
	}
	return scr.Stats.SmallSz.fmt(units)
}

func (scr *ScrubOne) fmtLargeSz(units string) string {
	if scr.Stats.LargeSz.Cnt == 0 {
		return zeroCnt
	}
	return scr.Stats.LargeSz.fmt(units)
}

func (scr *ScrubOne) fmtVchanged(units string) string {
	if scr.Stats.Vchanged.Cnt == 0 {
		return zeroCnt
	}
	return scr.Stats.Vchanged.fmt(units)
}

func (scr *ScrubOne) fmtVremoved(units string) string {
	if scr.Stats.Vremoved.Cnt == 0 {
		return zeroCnt
	}
	return scr.Stats.Vremoved.fmt(units)
}
