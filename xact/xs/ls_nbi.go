// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/tinylib/msgp/msgp"
)

// TODO -- FIXME:
// - double-check cleanup() called when aborted
// - see below

type nbiCtx struct {
	bck   *meta.Bck
	lom   *core.LOM   // .sys_inventory/BUCKET-UNAME/INV-NAME
	ufest *core.Ufest // completed and loaded

	// runtime state
	chunkNum int            // current chunk number (1-based)
	nidx     int            // next index within LsoEntries
	entries  cmn.LsoEntries // decoded from current chunk
	hdr      nbiChunkHdr    // current chunk header=[first, last, cnt]

	buf   []byte
	slab  *memsys.Slab
	cksum *cos.CksumHash
}

func (nbi *nbiCtx) init(invName string) error {
	lom := core.AllocLOM(nbiObjName(nbi.bck, invName))
	if err := lom.InitBck(meta.SysBckInv()); err != nil {
		core.FreeLOM(lom)
		return err
	}

	lom.Lock(false) // r-lock for the listing duration

	if err := nbi._load(lom); err != nil {
		nbi.cleanup()
		return err
	}

	nbi.cksum = cos.NewCksumHash(cos.ChecksumCRC32C)
	debug.Assert(nbi.cksum.H.Size() == cos.SizeofI32)

	nbi.buf, nbi.slab = core.T.PageMM().AllocSize(cmn.MsgpLsoBufSize)
	nbi.nidx = 0
	nbi.chunkNum = 1
	if err := nbi.readChunk(); err != nil {
		nbi.cleanup()
		return err
	}

	return nil
}

func (nbi *nbiCtx) _load(lom *core.LOM) error {
	if err := lom.Load(true, true); err != nil {
		return err
	}
	ufest, err := core.NewUfest("", lom, true /*mustExist*/)
	if err != nil {
		return nbi.emit(err)
	}
	if err := ufest.LoadCompleted(lom); err != nil {
		return nbi.emit(err)
	}

	nbi.lom = lom
	nbi.ufest = ufest
	return nil
}

func (nbi *nbiCtx) emit(err error) error {
	return fmt.Errorf("native bucket inventory: %w [%s]", err, nbi.bck.Cname(""))
}

func (nbi *nbiCtx) cleanup() {
	if nbi.lom != nil {
		nbi.lom.Unlock(false)
		core.FreeLOM(nbi.lom)
	}
	if nbi.buf != nil && nbi.slab != nil {
		nbi.slab.Free(nbi.buf)
	}
}

func (nbi *nbiCtx) readChunk() error {
	chunk, err := nbi.ufest.GetChunk(nbi.chunkNum)
	if err != nil {
		return nbi.emit(err) // TODO -- FIXME: add more details, here and elsewhere
	}

	fh, err := os.Open(chunk.Path())
	if err != nil {
		return nbi.emit(err)
	}
	defer cos.Close(fh)

	// 1. read chunk header length
	var frame [nbiFrameSize]byte
	if _, err := io.ReadFull(fh, frame[:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nbi.emit(err)
	}

	// 2. read chunk header
	hdrLen := binary.BigEndian.Uint32(frame[:])
	if hdrLen == 0 || hdrLen > nbiMaxHdrLen {
		return nbi.emit(fmt.Errorf("invalid chunk header length %d", hdrLen))
	}

	hdrBuf := nbi.buf[:hdrLen] // note: using the same buffer for hdr and msgp payload
	if _, err := io.ReadFull(fh, hdrBuf); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nbi.emit(err)
	}

	// 2.1. checksum
	debug.Assert(nbi.cksum != nil && nbi.cksum.Ty() == cos.ChecksumCRC32C, "must have CRC32c")
	nbi.cksum.H.Reset()
	nbi.cksum.H.Write(hdrBuf)
	crc := nbi.cksum.SumTo()
	exp := binary.BigEndian.Uint32(frame[cos.SizeofI32:])
	got := binary.BigEndian.Uint32(crc)
	if got != exp {
		return nbi.emit(fmt.Errorf("invalid chunk [%d, %q] header hash: expected %08x, got %08x",
			nbi.chunkNum, chunk.Path(), exp, got))
	}

	unpacker := cos.NewUnpacker(hdrBuf)
	if err := nbi.hdr.Unpack(unpacker); err != nil {
		return nbi.emit(err)
	}

	nbi.nidx = 0

	// 3. read chunk payload: msgp entries (note: hdrBuf consumed before msgp reuses the buffer)
	mr := msgp.NewReaderBuf(fh, nbi.buf)

	if cnt := int(nbi.hdr.entryCount); cap(nbi.entries) < cnt {
		nbi.entries = make(cmn.LsoEntries, 0, cnt)
	} else {
		nbi.entries = nbi.entries[:0]
	}
	return nbi.entries.DecodeMsg(mr)
}

func (nbi *nbiCtx) nextPage(msg *apc.LsoMsg, lst *cmn.LsoRes) error {
	lst.Entries = lst.Entries[:0]
	lst.ContinuationToken = ""

	pageSize := cos.NonZero(msg.PageSize, apc.MaxPageSizeAIS) // (not taking bucket's - native backend override)

	if msg.ContinuationToken != "" {
		saveNum := nbi.chunkNum
		if err := nbi.parseContToken(msg.ContinuationToken); err != nil {
			return err
		}
		if saveNum != nbi.chunkNum {
			if err := nbi.readChunk(); err != nil {
				return err
			}
		}
	}

	for len(lst.Entries) < int(pageSize) {
		// next chunk
		if nbi.nidx >= len(nbi.entries) {
			// TODO: rather than crossing chunk boundary, currently returning a partially filled page
			if len(lst.Entries) > 0 {
				break
			}

			nbi.chunkNum++
			if nbi.chunkNum > nbi.ufest.Count() {
				// no more chunks - we are done
				break
			}
			if err := nbi.readChunk(); err != nil {
				return err
			}
			// prefix - to skip this chunk
			if msg.Prefix != "" && nbi.hdr.last < msg.Prefix {
				continue
			}
		}

		out := nbi.entries[nbi.nidx]

		// prefix
		if msg.Prefix != "" {
			if out.Name < msg.Prefix {
				// skip until
				nbi.nidx++
				continue
			}
			if !strings.HasPrefix(out.Name, msg.Prefix) {
				// done
				return nil
			}
		}

		nbi.nidx++
		lst.Entries = append(lst.Entries, out)
	}

	if nbi.nidx < len(nbi.entries) || nbi.chunkNum < nbi.ufest.Count() {
		lst.ContinuationToken = nbi.makeContToken()
	}
	return nil
}

//
// helpers: (nbi.chunkNum, nbi.nidx) to/from continuation-token
//

const tokenBufLen = 1 + cos.SizeofI32 + cos.SizeofI32

func (nbi *nbiCtx) makeContToken() string {
	debug.Assert(nbi.chunkNum > 0 && nbi.chunkNum <= core.MaxChunkCount)
	debug.Assert(nbi.nidx >= 0 && nbi.nidx < apc.MaxInvEntriesPerChunk)

	var buf [tokenBufLen]byte
	buf[0] = nbiMetaVer
	off := 1
	binary.BigEndian.PutUint32(buf[off:], uint32(nbi.chunkNum))
	off += cos.SizeofI32
	binary.BigEndian.PutUint32(buf[off:], uint32(nbi.nidx))

	return base64.RawURLEncoding.EncodeToString(buf[:])
}

func (nbi *nbiCtx) parseContToken(tok string) error {
	debug.Assert(tok != "")
	decoded, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return nbi.emit(err)
	}
	if len(decoded) != tokenBufLen {
		err := fmt.Errorf("invalid continuation-token length: %d vs %d (expected)", len(decoded), tokenBufLen)
		return nbi.emit(err)
	}
	if decoded[0] != nbiMetaVer {
		err := fmt.Errorf("unsupported continuation-token meta-version: %d vs %d (expected)", decoded[0], nbiMetaVer)
		return nbi.emit(err)
	}
	nbi.chunkNum = int(binary.BigEndian.Uint32(decoded[1:]))
	nbi.nidx = int(binary.BigEndian.Uint32(decoded[1+4:]))

	debug.Assert(nbi.chunkNum > 0 && nbi.chunkNum <= core.MaxChunkCount)
	debug.Assert(nbi.nidx >= 0 && nbi.nidx < apc.MaxInvEntriesPerChunk)

	return nil
}
