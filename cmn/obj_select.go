// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const SelectObjectFormatCSV = "csv"

type (
	SelectObjectMsg struct {
		Query        string `json:"query"`
		InputFormat  string `json:"input_format"`
		OutputFormat string `json:"output_format"`
	}

	selectObjectQuery struct {
		columns   []string
		predicate *selectObjectPredicate
	}

	selectObjectPredicate struct {
		column string
		op     string
		value  string
	}
)

func (msg *SelectObjectMsg) Normalize() {
	if msg.InputFormat == "" {
		msg.InputFormat = SelectObjectFormatCSV
	}
	if msg.OutputFormat == "" {
		msg.OutputFormat = SelectObjectFormatCSV
	}
	msg.InputFormat = strings.ToLower(msg.InputFormat)
	msg.OutputFormat = strings.ToLower(msg.OutputFormat)
}

func (msg *SelectObjectMsg) Validate() error {
	msg.Normalize()
	if msg.Query == "" {
		return fmt.Errorf("select query is required")
	}
	if msg.InputFormat != SelectObjectFormatCSV {
		return fmt.Errorf("unsupported input format %q: currently supported format is %q", msg.InputFormat, SelectObjectFormatCSV)
	}
	if msg.OutputFormat != SelectObjectFormatCSV {
		return fmt.Errorf("unsupported output format %q: currently supported format is %q", msg.OutputFormat, SelectObjectFormatCSV)
	}
	_, err := parseSelectObjectQuery(msg.Query)
	return err
}

func SelectObject(in io.Reader, out io.Writer, msg *SelectObjectMsg) error {
	if err := msg.Validate(); err != nil {
		return err
	}
	return selectCSVObject(in, out, msg.Query)
}

func parseSelectObjectQuery(query string) (*selectObjectQuery, error) {
	const selectKeyword = "select "
	trimmed := strings.TrimSpace(query)
	if len(trimmed) < len(selectKeyword) || !strings.EqualFold(trimmed[:len(selectKeyword)], selectKeyword) {
		return nil, fmt.Errorf("query must start with SELECT")
	}

	body := strings.TrimSpace(trimmed[len(selectKeyword):])
	if body == "" {
		return nil, fmt.Errorf("missing SELECT projection")
	}

	whereIdx := indexSelectKeyword(body, "where")
	projection := body
	var where string
	if whereIdx >= 0 {
		projection = strings.TrimSpace(body[:whereIdx])
		where = strings.TrimSpace(body[whereIdx+len("where"):])
	}
	if fromIdx := indexSelectKeyword(projection, "from"); fromIdx >= 0 {
		projection = strings.TrimSpace(projection[:fromIdx])
	}
	if projection == "" {
		return nil, fmt.Errorf("missing SELECT projection")
	}

	parsed := &selectObjectQuery{}
	if projection == "*" {
		parsed.columns = []string{"*"}
	} else {
		cols := strings.Split(projection, ",")
		for _, col := range cols {
			col = strings.TrimSpace(col)
			if col == "" {
				return nil, fmt.Errorf("empty projection column in %q", projection)
			}
			parsed.columns = append(parsed.columns, col)
		}
	}

	if where != "" {
		predicate, err := parseSelectObjectPredicate(where)
		if err != nil {
			return nil, err
		}
		parsed.predicate = predicate
	}
	return parsed, nil
}

func parseSelectObjectPredicate(where string) (*selectObjectPredicate, error) {
	for _, op := range []string{">=", "<=", "!=", "=", ">", "<"} {
		if idx := strings.Index(where, op); idx >= 0 {
			column := strings.TrimSpace(where[:idx])
			value := strings.TrimSpace(where[idx+len(op):])
			if column == "" || value == "" {
				return nil, fmt.Errorf("invalid WHERE predicate %q", where)
			}
			value = strings.Trim(value, `"'`)
			return &selectObjectPredicate{column: column, op: op, value: value}, nil
		}
	}
	return nil, fmt.Errorf("unsupported WHERE predicate %q", where)
}

func indexSelectKeyword(s, keyword string) int {
	fields := strings.Fields(s)
	offset := 0
	for _, field := range fields {
		idx := strings.Index(s[offset:], field)
		if idx < 0 {
			continue
		}
		offset += idx
		if strings.EqualFold(field, keyword) {
			return offset
		}
		offset += len(field)
	}
	return -1
}

func selectCSVObject(in io.Reader, out io.Writer, query string) error {
	parsed, err := parseSelectObjectQuery(query)
	if err != nil {
		return err
	}

	reader := csv.NewReader(in)
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		return err
	}
	index := make(map[string]int, len(header))
	for i, col := range header {
		index[col] = i
	}

	projection, err := selectProjectionIndexes(header, index, parsed.columns)
	if err != nil {
		return err
	}
	predicateIndex := -1
	if parsed.predicate != nil {
		var ok bool
		predicateIndex, ok = index[parsed.predicate.column]
		if !ok {
			return fmt.Errorf("unknown WHERE column %q", parsed.predicate.column)
		}
	}

	writer := csv.NewWriter(out)
	if err := writer.Write(selectProjectRecord(header, projection)); err != nil {
		return err
	}
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if parsed.predicate != nil {
			if predicateIndex >= len(record) || !matchSelectPredicate(record[predicateIndex], parsed.predicate) {
				continue
			}
		}
		if err := writer.Write(selectProjectRecord(record, projection)); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func selectProjectionIndexes(header []string, index map[string]int, columns []string) ([]int, error) {
	if len(columns) == 1 && columns[0] == "*" {
		out := make([]int, len(header))
		for i := range header {
			out[i] = i
		}
		return out, nil
	}

	out := make([]int, 0, len(columns))
	for _, col := range columns {
		idx, ok := index[col]
		if !ok {
			return nil, fmt.Errorf("unknown SELECT column %q", col)
		}
		out = append(out, idx)
	}
	return out, nil
}

func selectProjectRecord(record []string, projection []int) []string {
	out := make([]string, len(projection))
	for i, idx := range projection {
		if idx < len(record) {
			out[i] = record[idx]
		}
	}
	return out
}

func matchSelectPredicate(got string, predicate *selectObjectPredicate) bool {
	switch predicate.op {
	case "=":
		return got == predicate.value
	case "!=":
		return got != predicate.value
	}

	left, lerr := strconv.ParseFloat(got, 64)
	right, rerr := strconv.ParseFloat(predicate.value, 64)
	if lerr != nil || rerr != nil {
		return false
	}
	switch predicate.op {
	case ">":
		return left > right
	case "<":
		return left < right
	case ">=":
		return left >= right
	case "<=":
		return left <= right
	default:
		return false
	}
}
