// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync"

	"github.com/NVIDIA/aistore/cmn/atomic"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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
		Transform(lom *core.LOM, latestVer, sync bool, daddr string) core.ReadResp
		// Finish cleans up the job's communication channel, and aborts the undergoing xaction (`TCB`/`TCO`) if errCause is provided
		Finish(errCause error) error
	}

	// +--------------------------------------------------------------------------------------------+
	// |                                       Target Node                                          |
	// |            |                                                                               |
	// |            |       +-------------------+                 +---------------------------+     |
	// |            |       |   webSocketComm   | --- K8s API --> |   ETL Pod (Transformer)   |     |
	// |            |       +---|-----------|---+                 +---------------------------+     |
	// |            |           v           |                           ^ ^ ^ ^ ^   ^ ^ ^ ^ ^       |
	// |            |       +-------------------+                       | | | | |   | | | | |       |
	// |            |       |    wsSession (1) -|--> wsConnCtx (1)(1) <-+ | | | |   | | | | |       |
	// |            |       |                / -|--> wsConnCtx (1)(2) <---+ | | |   | | | | |       |
	// |  TCB (1) --------- | ---> workCh ---  -|--> wsConnCtx (1)(3) <-----+ | |   | | | | |       |
	// |            |       |                \ -|--> wsConnCtx (1)(4) <-------+ |   | | | | |       |
	// |            |       |                  -|--> wsConnCtx (1)(5) <---------+   | | | | |       |
	// |            |       +-------------------+                                   | | | | |       |
	// |            |                       |                                       | | | | |       |
	// |            |                       v                           (ws conns)  | | | | |       |
	// |            |       +-------------------+                                   | | | | |       |
	// |            |       |    wsSession (2) -|--> wsConnCtx (2)(1) <-------------+ | | | |       |
	// |            |       |                / -|--> wsConnCtx (2)(2) <---------------+ | | |       |
	// |  TCB (2) --------- | ---> workCh ---  -|--> wsConnCtx (2)(3) <-----------------+ | |       |
	// |            |       |                \ -| -> wsConnCtx (2)(4) <-------------------+-|       |
	// |            |       |                  -| -> wsConnCtx (2)(5) <---------------------+       |
	// |            |       +-------------------+                                                   |
	// +--------------------------------------------------------------------------------------------+

	webSocketComm struct {
		baseComm
		sessions map[string]Session
		m        sync.Mutex

		commCtx       context.Context
		commCtxCancel context.CancelFunc
	}

	wsSession struct {
		txctn       core.Xact
		connections []*wsConnCtx
		workCh      chan rwpair
		chanFull    cos.ChanFull

		sessionCtx       context.Context
		sessionCtxCancel context.CancelFunc
	}

	wsConnCtx struct {
		name    string    // for logging
		etlxctn core.Xact // the parent xaction of the underlying ETL pod (`xs.xactETL` type)
		txctn   core.Xact // the undergoing tcb/tcobjs xaction that uses this session to perform transformation
		conn    *websocket.Conn

		// outbound messages of the original objects to send to ETL pod
		workCh chan rwpair

		// inbound messages of post-transformed objects from ETL pod
		writerCh       chan *io.PipeWriter
		writerChanFull atomic.Int64

		// wait for read/write loop goroutines to exit when cleaning up
		eg  *errgroup.Group
		ctx context.Context
	}

	rwpair struct {
		r     io.ReadCloser
		w     *io.PipeWriter
		daddr string
	}
)

// interface guard
var (
	_ statefulCommunicator = (*webSocketComm)(nil)
	_ Session              = (*wsSession)(nil)
)

const (
	// TODO: make this limit configurable (depends on etl side configuration)
	maxMsgSize = 16 * cos.GiB
	// TODO: compare performance over different channel sizes
	wockChSize = 512
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

	connPerSession := ws.boot.config.TCB.SbundleMult // TODO: add specific ETL config on this
	wss := &wsSession{
		txctn:       xctn,
		workCh:      make(chan rwpair, wockChSize),
		connections: make([]*wsConnCtx, 0, connPerSession),
	}
	wss.sessionCtx, wss.sessionCtxCancel = context.WithCancel(ws.commCtx)

	for i := range connPerSession {
		conn, resp, err := websocket.DefaultDialer.Dial(ws.boot.uri, nil)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to dial %s: %w", xctn.Name(), ws.boot.uri, err)
		}
		resp.Body.Close()

		// connection is expected to remain active due to continuous data flow, don't need liveness checks via ping/pong
		conn.SetPongHandler(nil)
		conn.SetPingHandler(nil)
		conn.SetReadLimit(maxMsgSize)

		group, ctx := errgroup.WithContext(wss.sessionCtx)

		wcs := &wsConnCtx{
			name:     fmt.Sprintf("%s-%d", ws.ETLName(), i),
			etlxctn:  ws.Xact(), // for abort listening and runtime error report
			txctn:    xctn,      // for abort listening and runtime error report
			conn:     conn,
			workCh:   wss.workCh,
			writerCh: make(chan *io.PipeWriter, wockChSize),
			ctx:      ctx,
			eg:       group,
		}

		group.Go(wcs.readLoop)
		group.Go(wcs.writeLoop)

		wss.connections = append(wss.connections, wcs)
	}

	ws.m.Lock()
	ws.sessions[xctn.ID()] = wss
	ws.m.Unlock()
	return wss, nil
}

func (ws *webSocketComm) Stop() {
	for _, session := range ws.sessions {
		session.Finish(cmn.ErrXactUserAbort)
	}
	ws.sessions = nil
	ws.commCtxCancel()
	ws.baseComm.Stop()
}

///////////////
// wsSession //
///////////////

func (wss *wsSession) Transform(lom *core.LOM, latestVer, sync bool, daddr string) core.ReadResp {
	srcResp := lom.GetROC(latestVer, sync)
	if srcResp.Err != nil {
		return srcResp
	}
	pr, pw := io.Pipe() // TODO -- FIXME: revise and remove

	l, c := len(wss.workCh), cap(wss.workCh)
	wss.chanFull.Check(l, c)

	wss.workCh <- rwpair{srcResp.R, pw, daddr}

	return core.ReadResp{
		R:      cos.NopOpener(pr),
		OAH:    srcResp.OAH, // TODO: estimate the post-transformed Lsize for stats
		Err:    nil,
		Ecode:  http.StatusNoContent, // promise to deliver
		Remote: srcResp.Remote,
	}
}

func (wss *wsSession) Finish(errCause error) error {
	wss.sessionCtxCancel()
	for _, wsConn := range wss.connections {
		wsConn.finish(errCause)
	}
	return nil
}

///////////////
// wsConnCtx //
///////////////

func (wctx *wsConnCtx) finish(errCause error) error {
	if errCause != nil {
		wctx.txctn.Abort(errCause)
	}
	if err := wctx.eg.Wait(); err != nil {
		nlog.Errorf("error shutting down webSocketComm goroutines: %v", err)
	}
	return wctx.conn.Close()
}

func (wctx *wsConnCtx) readLoop() error {
	buf, slab := core.T.PageMM().Alloc()
	defer slab.Free(buf)

	for {
		select {
		case <-wctx.ctx.Done():
			return nil
		case <-wctx.txctn.ChanAbort():
			return nil
		case <-wctx.etlxctn.ChanAbort():
			return nil
		default:
			ty, r, err := wctx.conn.NextReader()
			if err != nil {
				err = fmt.Errorf("error on reading message from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
			}
			writer := <-wctx.writerCh

			// direct put success (TextMessage ack from ETL server)
			if ty == websocket.TextMessage {
				cos.Close(writer)
				continue
			}

			if _, err = cos.CopyBuffer(writer, r, buf); err != nil {
				cos.Close(writer)
				err = fmt.Errorf("error on copying message from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
			}
			cos.Close(writer)
		}
	}
}

func (wctx *wsConnCtx) writeLoop() error {
	buf, slab := core.T.PageMM().Alloc()
	defer slab.Free(buf)

	for {
		select {
		case <-wctx.ctx.Done():
			return nil
		case <-wctx.txctn.ChanAbort():
			return nil
		case <-wctx.etlxctn.ChanAbort():
			return nil
		case rw := <-wctx.workCh:
			// Leverages the fact that WebSocket preserves message order and boundaries.
			// Sends two consecutive WebSocket messages:
			//   1. A TextMessage containing the direct PUT address
			//   2. A BinaryMessage containing the object content
			//
			// The ETL server is expected to consume them in the same order and treat them as logically linked.

			// 1. send direct put address
			err := wctx.conn.WriteMessage(websocket.TextMessage, []byte(rw.daddr))
			if err != nil {
				err = fmt.Errorf("error on writing direct put address %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
			}

			// 2. send object content
			writer, err := wctx.conn.NextWriter(websocket.BinaryMessage)
			if err != nil {
				err = fmt.Errorf("error on getting next writter from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
			}
			if _, err := cos.CopyBuffer(writer, rw.r, buf); err != nil {
				cos.Close(rw.r)
				cos.Close(writer)
				err = fmt.Errorf("error on getting next writter from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
			}
			cos.Close(rw.r)

			// For each object sent to the ETL via `wctx.workCh`, we reserve a corresponding slot in `wctx.writerCh`
			// for its writer. This guarantees that the correct writer is matched with the response when it arrives.
			wctx.writerCh <- rw.w
			if l, c := len(wctx.writerCh), cap(wctx.writerCh); l > c/2 {
				runtime.Gosched() // poor man's throttle
				if l == c {
					cnt := wctx.writerChanFull.Inc()
					if (cnt >= 10 && cnt <= 20) || (cnt > 0 && cmn.Rom.FastV(5, cos.SmoduleETL)) {
						nlog.Errorln(cos.ErrWorkChanFull, "inbound messages channel of session", wctx.name, "cnt", cnt)
					}
				}
			}
			cos.Close(writer)
		}
	}
}
