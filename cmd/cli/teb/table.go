// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const rowSepa = "\t "

type Table struct {
	headers []*header
	rows    []row
}

type (
	header struct {
		name string
		hide bool
	}
	row []string
)

func newTable(headers ...*header) *Table { return &Table{headers: headers} }

func (t *Table) addRow(row row) {
	if len(row) != len(t.headers) {
		err := fmt.Errorf("invalid table row: expected %d values, got %d", len(t.headers), len(row))
		debug.AssertNoErr(err)
		return
	}
	t.rows = append(t.rows, row)
}

func (t *Table) Template(hideHeader bool) string {
	var sb strings.Builder
	if !hideHeader {
		headers := make([]string, 0, len(t.headers))
		for _, header := range t.headers {
			if !header.hide {
				headers = append(headers, header.name)
			}
		}
		sb.WriteString(strings.Join(headers, rowSepa))
		sb.WriteRune('\n')
	}
	for _, row := range t.rows {
		rowStrings := make([]string, 0, len(row))
		for i, value := range row {
			if !t.headers[i].hide {
				rowStrings = append(rowStrings, value)
			}
		}
		sb.WriteString(strings.Join(rowStrings, rowSepa))
		sb.WriteRune('\n')
	}
	return sb.String()
}
