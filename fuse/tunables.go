// Command-line mounting utility for aisfs.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package main

import "github.com/NVIDIA/aistore/cmn"

const (
	// Determines the size of chunks that we write with append. The only exception
	// when we write less is Flush (end-of-file).
	maxWriteBufSize = cmn.MiB
)
