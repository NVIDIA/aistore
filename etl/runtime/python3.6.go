// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import (
	"strings"
)

type (
	// py36 implements Runtime for "python3.6".
	py36 struct{}
)

func (py36) Type() string        { return Python36 }
func (py36) CodeEnvName() string { return "AISTORE_CODE" }
func (py36) DepsEnvName() string { return "AISTORE_DEPS" }
func (py36) PodSpec() string     { return strings.ReplaceAll(pyPodSpec, "<VERSION>", "3.6") }
