// Package runtime provides skeletons and static specifications for building ETL from scratch.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package runtime

import _ "embed"

//go:embed python_common.yaml
var pyPodSpec string
