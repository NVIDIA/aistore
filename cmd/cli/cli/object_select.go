// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api"

	"github.com/urfave/cli"
)

const (
	objectSelectFormatCSV = "csv"
)

var (
	objectSelectQueryFlag = cli.StringFlag{
		Name:  "query,q",
		Usage: "SQL-like projection and filter expression, e.g. \"SELECT col1,col2 WHERE col3 > 10\"",
	}
	objectSelectInputFormatFlag = cli.StringFlag{
		Name:  "input-format",
		Value: objectSelectFormatCSV,
		Usage: "Input object format; currently supported: csv",
	}
	objectSelectOutputFormatFlag = cli.StringFlag{
		Name:  "output-format",
		Value: objectSelectFormatCSV,
		Usage: "Output format; currently supported: csv",
	}
)

type (
	objectSelectQuery struct {
		columns   []string
		predicate *objectSelectPredicate
	}

	objectSelectPredicate struct {
		column string
		op     string
		value  string
	}
)

func objectSelectHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	query := parseStrFlag(c, objectSelectQueryFlag)
	if query == "" {
		return missingArgumentsError(c, flprn(objectSelectQueryFlag))
	}
	inputFormat := strings.ToLower(parseStrFlag(c, objectSelectInputFormatFlag))
	if inputFormat != objectSelectFormatCSV {
		return fmt.Errorf("unsupported %s=%q: currently supported format is %q",
			flprn(objectSelectInputFormatFlag), inputFormat, objectSelectFormatCSV)
	}
	outputFormat := strings.ToLower(parseStrFlag(c, objectSelectOutputFormatFlag))
	if outputFormat != objectSelectFormatCSV {
		return fmt.Errorf("unsupported %s=%q: currently supported format is %q",
			flprn(objectSelectOutputFormatFlag), outputFormat, objectSelectFormatCSV)
	}

	bck, objName, err := parseBckObjURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}
	if objName == "" {
		return missingArgumentsError(c, "object name in the form "+objectArgument)
	}

	var warned bool
	encObjName := warnEscapeObjName(c, objName, &warned)
	reader, _, err := api.GetObjectReader(apiBP, bck, encObjName, nil)
	if err != nil {
		return err
	}
	defer reader.Close()

	return selectCSVObject(reader, c.App.Writer, query)
}

func parseObjectSelectQuery(query string) (*objectSelectQuery, error) {
	const selectKeyword = "select "
	trimmed := strings.TrimSpace(query)
	if len(trimmed) < len(selectKeyword) || !strings.EqualFold(trimmed[:len(selectKeyword)], selectKeyword) {
		return nil, fmt.Errorf("query must start with SELECT")
	}

	body := strings.TrimSpace(trimmed[len(selectKeyword):])
	if body == "" {
		return nil, fmt.Errorf("missing SELECT projection")
	}

	whereIdx := indexKeyword(body, "where")
	projection := body
	var where string
	if whereIdx >= 0 {
		projection = strings.TrimSpace(body[:whereIdx])
		where = strings.TrimSpace(body[whereIdx+len("where"):])
	}
	if fromIdx := indexKeyword(projection, "from"); fromIdx >= 0 {
		projection = strings.TrimSpace(projection[:fromIdx])
	}
	if projection == "" {
		return nil, fmt.Errorf("missing SELECT projection")
	}

	parsed := &objectSelectQuery{}
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
		predicate, err := parseObjectSelectPredicate(where)
		if err != nil {
			return nil, err
		}
		parsed.predicate = predicate
	}
	return parsed, nil
}

func parseObjectSelectPredicate(where string) (*objectSelectPredicate, error) {
	for _, op := range []string{">=", "<=", "!=", "=", ">", "<"} {
		if idx := strings.Index(where, op); idx >= 0 {
			column := strings.TrimSpace(where[:idx])
			value := strings.TrimSpace(where[idx+len(op):])
			if column == "" || value == "" {
				return nil, fmt.Errorf("invalid WHERE predicate %q", where)
			}
			value = strings.Trim(value, `"'`)
			return &objectSelectPredicate{column: column, op: op, value: value}, nil
		}
	}
	return nil, fmt.Errorf("unsupported WHERE predicate %q", where)
}

func indexKeyword(s, keyword string) int {
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
	parsed, err := parseObjectSelectQuery(query)
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

	projection, err := projectionIndexes(header, index, parsed.columns)
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
	if err := writer.Write(projectRecord(header, projection)); err != nil {
		return err
	}
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if predicateIndex >= len(record) {
			return fmt.Errorf("record has %d fields, WHERE column %q expects field %d",
				len(record), parsed.predicate.column, predicateIndex+1)
		}
		if parsed.predicate != nil && !matchObjectSelectPredicate(record[predicateIndex], parsed.predicate) {
			continue
		}
		if err := writer.Write(projectRecord(record, projection)); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func projectionIndexes(header []string, index map[string]int, columns []string) ([]int, error) {
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

func projectRecord(record []string, projection []int) []string {
	out := make([]string, 0, len(projection))
	for _, idx := range projection {
		if idx < len(record) {
			out = append(out, record[idx])
		} else {
			out = append(out, "")
		}
	}
	return out
}

func matchObjectSelectPredicate(got string, predicate *objectSelectPredicate) bool {
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
