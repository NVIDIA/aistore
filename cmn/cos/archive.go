// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cos

const (
	// ExtTar is tar files extension
	ExtTar = ".tar"
	// ExtTgz is short tar tgz files extension
	ExtTgz = ".tgz"
	// ExtTarTgz is tar tgz files extension
	ExtTarTgz = ".tar.gz"
	// ExtZip is zip files extension
	ExtZip = ".zip"
)

var ArchExtensions = []string{ExtTar, ExtTgz, ExtTarTgz, ExtZip}
