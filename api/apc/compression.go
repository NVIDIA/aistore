// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

// NOTE:
// LZ4 block and frame formats: http://fastcompression.blogspot.com/2013/04/lz4-streaming-format-final.html

// Compression enum
const (
	CompressAlways = "always"
	CompressNever  = "never"
)

// sent via req.Header.Set(apc.HdrCompress, LZ4Compression)
// (alternative to lz4 compressions upon popular request)
const LZ4Compression = "lz4"

var SupportedCompression = [...]string{CompressNever, CompressAlways}

func IsValidCompression(c string) bool {
	return c == "" || c == SupportedCompression[0] || c == SupportedCompression[1]
}
