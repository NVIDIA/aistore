// Package stats keeps track of all the different statistics collected by the report
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

var (
	excelEpochOffsetDays = float64(25569)
	nanoPerDay           = float64(86400 * time.Second.Nanoseconds())
)

type Stat interface {
	getHeadingsText() map[string]string
	getHeadingsOrder() []string

	getContents() map[string]interface{}
}

type StatWriter struct {
	Path string
	file *os.File
}

func getTimestamp(t time.Time) float64 {
	_, tzOffset := t.Zone() // used to convert UTC timestamp to local timezone
	tzOffsetNano := int64(tzOffset) * time.Second.Nanoseconds()

	return float64(t.UnixNano()+tzOffsetNano)/nanoPerDay + excelEpochOffsetDays
}

// FIXME: replace usages with time.Milliseconds() when implemented
func getMilliseconds(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / float64(time.Millisecond)
}

func (w *StatWriter) writeHeadings(st Stat) {
	headingsText := st.getHeadingsText()
	headingsOrder := st.getHeadingsOrder()

	cos.Assert(len(headingsText) == len(headingsOrder))

	csvHeadings := make([]string, len(headingsOrder))
	for idx, x := range headingsOrder {
		var ok bool
		csvHeadings[idx], ok = headingsText[x]
		cos.Assert(ok)
	}

	w.file.WriteString(strings.Join(csvHeadings, ","))
	w.file.WriteString("\n")
}

func ignoreItem(item interface{}) bool {
	return item == nil || item == 0 || item == float64(0) || item == int64(0) || item == "" || item == false
}

func (w *StatWriter) writeContents(st Stat) {
	headingsOrder := st.getHeadingsOrder()
	contents := st.getContents()

	csvData := make([]string, len(headingsOrder))
	for idx, x := range headingsOrder {
		if item, ok := contents[x]; ok && !ignoreItem(item) {
			var err error
			csvData[idx], err = cos.ConvertToString(item)
			cos.AssertNoErr(err)
		}
	}

	w.file.WriteString(strings.Join(csvData, ","))
	w.file.WriteString("\n")
}

func (w *StatWriter) WriteStat(st Stat) {
	if w.file == nil {
		file, err := cos.CreateFile(w.Path)
		cos.AssertNoErr(err)
		w.file = file

		w.writeHeadings(st)
	}

	w.writeContents(st)
}

func (w *StatWriter) Flush() {
	w.file.Sync()
}

func (w *StatWriter) Close() {
	w.file.Close()
}
