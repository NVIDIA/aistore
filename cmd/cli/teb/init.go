// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"io"

	"github.com/fatih/color"
)

var Writer io.Writer

var (
	fred, fcyan, fgreen, fblue func(format string, a ...any) string
)

func Init(w io.Writer, noColor bool) {
	Writer = w
	if noColor {
		fred, fcyan, fgreen, fblue = fmt.Sprintf, fmt.Sprintf, fmt.Sprintf, fmt.Sprintf
	} else {
		fred = color.New(color.FgHiRed).Sprintf
		fcyan = color.New(color.FgHiCyan).Sprintf
		fgreen = color.New(color.FgHiGreen).Sprintf
		fblue = color.New(color.FgHiBlue).Sprintf
	}
}
