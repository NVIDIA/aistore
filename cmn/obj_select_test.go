// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"io"
	"strings"
	"testing"
)

func TestSelectObjectCSVProjectionAndPredicate(t *testing.T) {
	input := strings.NewReader("name,kind,score\nalpha,a,7\nbeta,b,12\ngamma,b,3\n")
	var output strings.Builder

	err := SelectObject(input, &output, &SelectObjectMsg{Query: "SELECT name,score WHERE score > 10"})
	if err != nil {
		t.Fatal(err)
	}

	const expected = "name,score\nbeta,12\n"
	if got := output.String(); got != expected {
		t.Fatalf("unexpected output:\nexpected:\n%q\ngot:\n%q", expected, got)
	}
}

func TestSelectObjectCSVSelectAllWithStringPredicate(t *testing.T) {
	input := strings.NewReader("name,kind,score\nalpha,a,7\nbeta,b,12\ngamma,b,3\n")
	var output strings.Builder

	err := SelectObject(input, &output, &SelectObjectMsg{Query: `SELECT * WHERE kind = "b"`})
	if err != nil {
		t.Fatal(err)
	}

	const expected = "name,kind,score\nbeta,b,12\ngamma,b,3\n"
	if got := output.String(); got != expected {
		t.Fatalf("unexpected output:\nexpected:\n%q\ngot:\n%q", expected, got)
	}
}

func TestSelectObjectRejectsUnknownColumn(t *testing.T) {
	input := strings.NewReader("name,kind,score\nalpha,a,7\n")
	var output strings.Builder

	err := SelectObject(input, &output, &SelectObjectMsg{Query: "SELECT missing WHERE score > 10"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `unknown SELECT column "missing"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSelectObjectQueryAllowsFromClause(t *testing.T) {
	parsed, err := parseSelectObjectQuery("SELECT name, score FROM object WHERE score >= 10")
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.columns) != 2 || parsed.columns[0] != "name" || parsed.columns[1] != "score" {
		t.Fatalf("unexpected columns: %#v", parsed.columns)
	}
	if parsed.predicate == nil || parsed.predicate.column != "score" ||
		parsed.predicate.op != ">=" || parsed.predicate.value != "10" {
		t.Fatalf("unexpected predicate: %#v", parsed.predicate)
	}
	if !parsed.predicate.numeric || parsed.predicate.numValue != 10 {
		t.Fatalf("expected pre-parsed numeric predicate: %#v", parsed.predicate)
	}
}

func TestSelectObjectRejectsInvalidNumericPredicate(t *testing.T) {
	_, err := parseSelectObjectQuery("SELECT name WHERE score > high")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `invalid numeric WHERE value "high"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func BenchmarkSelectObjectCSVProjectionPredicate(b *testing.B) {
	var input strings.Builder
	input.WriteString("name,kind,score\n")
	for i := 0; i < 1000; i++ {
		input.WriteString("alpha,a,7\nbeta,b,12\ngamma,b,3\n")
	}
	data := input.String()
	msg := &SelectObjectMsg{Query: "SELECT name,score WHERE score > 10"}

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := SelectObject(strings.NewReader(data), io.Discard, msg); err != nil {
			b.Fatal(err)
		}
	}
}
