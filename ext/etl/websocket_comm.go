// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

type (
	// statefulCommunicator manages long-lived connection channels to ETL pod (e.g., WebSocket)
	// and creates per-xaction sessions that use those channels to exchange data with ETL pods.
	statefulCommunicator interface {
		communicatorCommon
		createSession(xctn core.Xact) (Session, error)
	}

	// Session represents a per-xaction communication context created by the statefulCommunicator.
	Session interface {
		// Transform is an instance of `core.GetROC` function, which is driven by `TCB` and `TCO` to provide offline transformation
		Transform(lom *core.LOM, latestVer, sync bool) core.ReadResp
		// Finish cleans up the job's communication channel, and aborts the undergoing xaction (`TCB`/`TCO`) if errCause is provided
		Finish(errCause error) error
	}

	webSocketComm struct {
		baseComm
		sessions []Session
		m        sync.Mutex
	}

	wsConnCtx struct {
		name  string    // for logging
		txctn core.Xact // the undergoing tcb/tcobjs xaction
		conn  *websocket.Conn

		workCh   chan rwpair
		writerCh chan *io.PipeWriter

		// wait for read/write loop goroutines to exit when cleaning up
		eg     *errgroup.Group
		ctx    context.Context
		cancel context.CancelFunc

		finishCb func(Session) // to self-remove from `webSocketComm.jobs`
	}

	rwpair struct {
		r io.ReadCloser  // outbound messages the original objects to ETL pod
		w *io.PipeWriter // inbound messages of post-transformed objects from ETL pod
	}
)

var (
	_ statefulCommunicator = (*webSocketComm)(nil)
	_ Session              = (*wsConnCtx)(nil)
)

const (
	// TODO: make this limit configurable
	maxMsgSize = 16 * cos.GiB
	// TODO: compare performance over different channel sizes
	wockChSize = 128
)

// SetupConnection establishes a test connection to the ETL pod's websocket endpoint.
// Close immediately if the connection is successful (only for verifying connectivity).
func (ws *webSocketComm) SetupConnection() (err error) {
	if err := ws.boot.setupConnection("ws://"); err != nil {
		return err
	}
	ws.boot.uri += "/ws" // TODO: make this endpoint configurable
	conn, resp, err := websocket.DefaultDialer.Dial(ws.boot.uri, nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return conn.Close() // closing after verifying connectivity
}

func (ws *webSocketComm) createSession(xctn core.Xact) (Session, error) {
	if xctn == nil {
		return nil, cos.NewErrNotFound(core.T, "invalid xact parameter")
	}

	conn, resp, err := websocket.DefaultDialer.Dial(ws.boot.uri, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to dial %s: %w", xctn.Name(), ws.boot.uri, err)
	}
	resp.Body.Close()
	conn.SetReadLimit(maxMsgSize)

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)

	wcs := &wsConnCtx{
		name:     ws.ETLName(),
		txctn:    xctn,
		conn:     conn,
		workCh:   make(chan rwpair, wockChSize),
		writerCh: make(chan *io.PipeWriter, wockChSize),
		ctx:      ctx,
		cancel:   cancel,
		eg:       group,
		finishCb: func(session Session) {
			ws.m.Lock()
			for i, s := range ws.sessions {
				if session == s {
					ws.sessions = append(ws.sessions[:i], ws.sessions[i+1:]...)
					break
				}
			}
			ws.m.Unlock()
		},
	}

	group.Go(wcs.readLoop)
	group.Go(wcs.writeLoop)

	ws.m.Lock()
	ws.sessions = append(ws.sessions, wcs)
	ws.m.Unlock()
	return wcs, nil
}

func (ws *webSocketComm) Stop() {
	for _, session := range ws.sessions {
		session.Finish(cmn.ErrXactUserAbort)
	}
	debug.Assert(len(ws.sessions) == 0)
	ws.baseComm.Stop()
}

///////////////
// wsConnCtx //
///////////////

func (wctx *wsConnCtx) Transform(lom *core.LOM, latestVer, sync bool) core.ReadResp {
	srcResp := lom.GetROC(latestVer, sync)
	pr, pw := io.Pipe()

	wctx.workCh <- rwpair{srcResp.R, pw}

	return core.ReadResp{
		R:      cos.NopOpener(pr),
		OAH:    srcResp.OAH, // TODO: estimate the post-transformed Lsize for stats
		Err:    nil,
		Ecode:  srcResp.Ecode,
		Remote: srcResp.Remote,
	}
}

func (wctx *wsConnCtx) Finish(errCause error) error {
	if errCause != nil {
		wctx.txctn.Abort(errCause)
	}
	wctx.cancel()
	if err := wctx.eg.Wait(); err != nil {
		nlog.Errorf("error shutting down webSocketComm goroutines: %v", err)
	}
	if wctx.finishCb != nil {
		wctx.finishCb(wctx)
	}
	return wctx.conn.Close()
}

func (wctx *wsConnCtx) readLoop() error {
	wctx.conn.SetPingHandler(func(msg string) error {
		return wctx.conn.WriteMessage(websocket.PongMessage, []byte(msg))
	})

	for {
		select {
		case <-wctx.ctx.Done():
			return nil
		default:
			_, r, err := wctx.conn.NextReader()
			if err != nil {
				err = fmt.Errorf("error on reading message from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return err
			}
			writer := <-wctx.writerCh

			// TODOs:
			// - do not abort on soft error
			// - use slab from `memsys` as copy buffer
			if _, err := io.Copy(writer, r); err != nil {
				writer.Close()
				err = fmt.Errorf("error on copying message from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return err
			}
			writer.Close()
		}
	}
}

func (wctx *wsConnCtx) writeLoop() error {
	for {
		select {
		case <-wctx.ctx.Done():
			return nil
		case rw := <-wctx.workCh:
			writer, err := wctx.conn.NextWriter(websocket.BinaryMessage)
			if err != nil {
				err = fmt.Errorf("error on getting next writter from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return err
			}

			// TODOs:
			// - do not abort on soft error
			// - use slab from `memsys` as copy buffer
			if _, err := io.Copy(writer, rw.r); err != nil {
				rw.r.Close()
				writer.Close()
				err = fmt.Errorf("error on getting next writter from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return err
			}
			// Leverages the fact that WebSocket preserves message order and boundaries.
			// For each object sent to the ETL via `wctx.workCh`, we reserve a corresponding slot in `wctx.writerCh`
			// for its writer. This guarantees that the correct writer is matched with the response when it arrives.
			rw.r.Close()

			// TODO: implement chanFull feature
			wctx.writerCh <- rw.w
			writer.Close()
		}
	}
}
