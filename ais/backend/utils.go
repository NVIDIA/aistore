// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"fmt"
	"io"
	"strings"
)

// byteSizeParse converts a human-friendly byte count/size specification into a unit64.
// A valid input, after removing spaces and commas, consists of an optional decimal
// number (defaults to 1), an optional multipler (power-of-10 {K|M|G|T|P} or power-of-2
// {Ki|Mi|Gi|Ti|Pi}), and an optional "byte" symbol "B" (i.e. it is presumed if absent).
func byteSizeParse(sizeStr string) (sizeU64 uint64, err error) {
	const (
		expectingDecimal int = iota
		expectingMultiplier
		expectingMultiplierI
		expectingTrailingB
		expectingEOF
	)
	var (
		decimalParsingStarted = bool(false)
		expectingMode         = expectingDecimal
		sizeStrByte           byte
		sizeStrByteSlice      = make([]byte, 1)
		sizeStrByteSliceLen   int
		sizeStrReader         *strings.Reader
		sizeStrMultipler      = string("") // if == "", don't apply a multiplier
	)

	sizeStrReader = strings.NewReader(sizeStr)

	for {
		sizeStrByteSliceLen, err = sizeStrReader.Read(sizeStrByteSlice)
		if err == io.EOF {
			break
		} else if err != nil {
			err = fmt.Errorf("sizeStrReader.Read(sizeStrByteSlice) failed: %v", err)
			return
		} else if sizeStrByteSliceLen != 1 {
			err = fmt.Errorf("sizeStrReader.Read(sizeStrByteSlice) returned unexpected n: %v", sizeStrByteSliceLen)
			return
		}

		sizeStrByte = sizeStrByteSlice[0]

	restartParse:

		switch expectingMode {
		case expectingDecimal:
			if (sizeStrByte == ' ') || (sizeStrByte == ',') || (sizeStrByte == '\t') {
				continue
			}
			if (sizeStrByte < '0') || (sizeStrByte > '9') {
				if !decimalParsingStarted {
					sizeU64 = 1
				}
				expectingMode = expectingMultiplier
				goto restartParse
			}
			if decimalParsingStarted {
				sizeU64 = (sizeU64 * 10) + uint64(sizeStrByte-'0')
			} else {
				decimalParsingStarted = true
				sizeU64 = uint64(sizeStrByte - '0')
			}
		case expectingMultiplier:
			if (sizeStrByte == 'K') || (sizeStrByte == 'M') || (sizeStrByte == 'G') || (sizeStrByte == 'T') || (sizeStrByte == 'P') {
				sizeStrMultipler = string(sizeStrByte)
				expectingMode = expectingMultiplierI
			} else {
				expectingMode = expectingTrailingB
				goto restartParse
			}
		case expectingMultiplierI:
			if sizeStrByte == 'i' {
				sizeStrMultipler += "i"
				expectingMode = expectingTrailingB
			} else {
				expectingMode = expectingTrailingB
				goto restartParse
			}
		case expectingTrailingB:
			if sizeStrByte == 'B' {
				expectingMode = expectingEOF
			} else {
				err = fmt.Errorf("while in expectingTrailingB, got unexpected sizeStrByte == \"%s\"", string(sizeStrByte))
				return
			}
		case expectingEOF:
			err = fmt.Errorf("while in expectingEOF, got unexpected sizeStrByte == \"%s\"", string(sizeStrByte))
			return
		default:
			err = fmt.Errorf("switch expectingMode == %v invalid", expectingMode)
			return
		}
	}

	switch sizeStrMultipler {
	case "":
		// Just return sizeU64 as is
	case "K":
		sizeU64 *= 1000
	case "M":
		sizeU64 *= 1000 * 1000
	case "G":
		sizeU64 *= 1000 * 1000 * 1000
	case "T":
		sizeU64 *= 1000 * 1000 * 1000 * 1000
	case "P":
		sizeU64 *= 1000 * 1000 * 1000 * 1000 * 1000
	case "Ki":
		sizeU64 *= 1024
	case "Mi":
		sizeU64 *= 1024 * 1024
	case "Gi":
		sizeU64 *= 1024 * 1024 * 1024
	case "Ti":
		sizeU64 *= 1024 * 1024 * 1024 * 1024
	case "Pi":
		sizeU64 *= 1024 * 1024 * 1024 * 1024 * 1024
	default:
		err = fmt.Errorf("switch sizeStrMultipler == \"%s\" invalid", sizeStrMultipler)
		return
	}

	err = nil
	return
}
