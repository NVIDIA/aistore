// Package cmn provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

type (
	Jopts struct {
		// when non-zero, formatting version of the structure that's being (de)serialized
		// (not to confuse with the jsp encoding version - see above)
		Metaver uint32

		Compress  bool // lz4 when [version == 1 || version == 2]
		Checksum  bool // xxhash when [version == 1 || version == 2]
		Signature bool // when true, write 128bit prefix (of the layout shown above) at offset zero

		Indent bool // Determines if the JSON should be indented. Useful for CLI config.
		Local  bool // when true, use JSON local extension
	}
	GetJopts interface {
		GetJopts() Jopts
	}
)

func Plain() Jopts      { return Jopts{} }
func PlainLocal() Jopts { return Jopts{Local: true} }

func CCSignLocal(metaver uint32) Jopts {
	opts := CCSign(metaver)
	opts.Local = true
	return opts
}

func CCSign(metaver uint32) Jopts {
	return Jopts{Metaver: metaver, Compress: true, Checksum: true, Signature: true, Indent: false}
}

func CksumSign(metaver uint32) Jopts {
	return Jopts{Metaver: metaver, Checksum: true, Signature: true}
}
