// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import _ "embed"

type (
	// py3 implements Runtime for "python3".
	py3 struct{}
)

//go:embed python3.yaml
var py3PodSpec string

func (py3) Type() string        { return Python3 }
func (py3) CodeEnvName() string { return "AISTORE_CODE" }
func (py3) DepsEnvName() string { return "AISTORE_DEPS" }
func (py3) PodSpec() string     { return py3PodSpec }
