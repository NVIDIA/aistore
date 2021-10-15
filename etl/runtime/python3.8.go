// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import (
	"strings"
)

type (
	// py38 implements Runtime for "python3.8".
	py38 struct{}
)

func (py38) Type() string        { return Python38 }
func (py38) CodeEnvName() string { return "AISTORE_CODE" }
func (py38) DepsEnvName() string { return "AISTORE_DEPS" }
func (py38) PodSpec() string     { return strings.ReplaceAll(pyPodSpec, "<VERSION>", "3.8") }
