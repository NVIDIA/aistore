// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import (
	"strings"
)

type (
	// py310 implements Runtime for "python3.10".
	py310 struct{}
)

func (py310) Type() string        { return Python310 }
func (py310) CodeEnvName() string { return "AISTORE_CODE" }
func (py310) DepsEnvName() string { return "AISTORE_DEPS" }
func (py310) PodSpec() string     { return strings.ReplaceAll(pyPodSpec, "<VERSION>", "3.10") }
