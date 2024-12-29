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
	colMissing   = "MISSING COPIES"
	colSmall     = "SMALL"
	colLarge     = "LARGE"
	colVchanged  = "VERSION-CHANGED"
	colVremoved  = "VERSION-REMOVED"
)

type (
	ScrubOne struct {
		Bck    cmn.Bck
		Prefix string
		Listed uint64
		Stats  struct {
			Misplaced uint64
			MissingCp uint64
			SmallSz   uint64
			LargeSz   uint64
			Vchanged  uint64
			Vremoved  uint64
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

func (h *ScrubHelper) MakeTab(units string) *Table {
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

	_ = units // TODO -- FIXME: add total size; use units

	h.hideMisplaced(cols, colMisplaced)
	h.hideMissing(cols, colMissing)
	h.hideVchanged(cols, colVchanged)
	h.hideVremoved(cols, colVremoved)

	for _, scr := range h.All {
		row := []string{
			scr.Bck.Cname(scr.Prefix),
			strconv.FormatUint(scr.Listed, 10),
			strconv.FormatUint(scr.Stats.Misplaced, 10),
			strconv.FormatUint(scr.Stats.MissingCp, 10),
			strconv.FormatUint(scr.Stats.SmallSz, 10),
			strconv.FormatUint(scr.Stats.LargeSz, 10),
			strconv.FormatUint(scr.Stats.Vchanged, 10),
			strconv.FormatUint(scr.Stats.Vremoved, 10),
		}
		table.addRow(row)
	}

	return table
}

//
// remove/hide a few named all-zero columns // TODO -- FIXME: copy-paste
//

func (h *ScrubHelper) hideMisplaced(cols []*header, col string) {
	for _, scr := range h.All {
		if scr.Stats.Misplaced != 0 {
			return
		}
	}
	h._hideCol(cols, col)
}

func (h *ScrubHelper) hideMissing(cols []*header, col string) {
	for _, scr := range h.All {
		if scr.Stats.MissingCp != 0 {
			return
		}
	}
	h._hideCol(cols, col)
}

func (h *ScrubHelper) hideVchanged(cols []*header, col string) {
	for _, scr := range h.All {
		if scr.Stats.Vchanged != 0 {
			return
		}
	}
	h._hideCol(cols, col)
}

func (h *ScrubHelper) hideVremoved(cols []*header, col string) {
	for _, scr := range h.All {
		if scr.Stats.Vremoved != 0 {
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
