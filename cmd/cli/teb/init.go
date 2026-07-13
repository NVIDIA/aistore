// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"io"

	"github.com/fatih/color"
)

var Writer io.Writer

var (
	fred, fcyan, fgreen, fblue func(a ...any) string
)

func Init(w io.Writer, noColor bool) {
	Writer = w
	if noColor {
		fred, fcyan, fgreen, fblue = fmt.Sprint, fmt.Sprint, fmt.Sprint, fmt.Sprint
	} else {
		fred = color.New(color.FgHiRed).Sprint
		fcyan = color.New(color.FgHiCyan).Sprint
		fgreen = color.New(color.FgHiGreen).Sprint
		fblue = color.New(color.FgHiBlue).Sprint
	}
}
