// Package stats keeps track of all the different statistics collected by the report
package stats

import (
	"os"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	csvTimeFormat = time.RFC3339Nano
)

type Stat interface {
	writeHeadings(*os.File)
	writeStat(*os.File)
}

type StatWriter struct {
	Path string
	file *os.File
}

func (w *StatWriter) WriteStat(st Stat) {
	if w.file == nil {
		file, err := cmn.CreateFile(w.Path)
		cmn.AssertNoErr(err)
		w.file = file
		st.writeHeadings(w.file)
	}
	st.writeStat(w.file)
}

func (w *StatWriter) Flush() {
	w.file.Sync()
}

func (w *StatWriter) Close() {
	w.file.Close()
}
