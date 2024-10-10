// Package jsp (JSON persistence) provides utilities to store and load arbitrary
// JSON-encoded structures with optional checksumming and compression.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package jsp

type (
	Options struct {
		// when non-zero, formatting version of the structure that's being (de)serialized
		// (not to confuse with the jsp encoding version - see above)
		Metaver uint32
		// warn and keep loading
		OldMetaverOk uint32

		Compress  bool // lz4 when [version == 1 || version == 2]
		Checksum  bool // xxhash when [version == 1 || version == 2]
		Signature bool // when true, write 128bit prefix (of the layout shown above) at offset zero

		Indent bool // Determines if the JSON should be indented. Useful for CLI config.
	}
	Opts interface {
		JspOpts() Options
	}
)

func Plain() Options { return Options{} }

func CCSign(metaver uint32) Options {
	return Options{Metaver: metaver, Compress: true, Checksum: true, Signature: true, Indent: false}
}

func CksumSign(metaver uint32) Options {
	return Options{Metaver: metaver, Checksum: true, Signature: true}
}
