// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"io"

	"github.com/fatih/color"
)

var Writer io.Writer

var (
	fred, fcyan func(format string, a ...any) string
)

func Init(w io.Writer, noColor bool) {
	Writer = w
	if noColor {
		fred, fcyan = fmt.Sprintf, fmt.Sprintf
	} else {
		fred = color.New(color.FgHiRed).Sprintf
		fcyan = color.New(color.FgHiCyan).Sprintf
	}
}
