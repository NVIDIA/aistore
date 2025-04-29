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
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
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
		Communicator
		createSession(xctn core.Xact) (Session, error)
	}

	// Session represents a per-xaction communication context created by the statefulCommunicator.
	Session interface {
		// OfflineTransform is an instance of `core.GetROC` function, which is driven by `TCB` and `TCO` to provide offline transformation
		OfflineTransform(lom *core.LOM, latestVer, sync bool, gargs *core.GetROCArgs) core.ReadResp
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
		commCtx       context.Context
		sessions      map[string]Session
		commCtxCancel context.CancelFunc
		m             sync.Mutex
	}

	wsSession struct {
		txctn            core.Xact
		sessionCtx       context.Context
		workCh           chan transformTask
		sessionCtxCancel context.CancelFunc
		argType          string
		connections      []*wsConnCtx
		chanFull         cos.ChanFull
		isDirectPut      bool
	}

	wsConnCtx struct {
		etlxctn        core.Xact // parent xaction of the underlying ETL pod (`xs.xactETL` type)
		txctn          core.Xact // tcb/tcobjs xaction that uses this session to perform transformatio
		ctx            context.Context
		conn           *websocket.Conn
		workCh         chan transformTask  // outbound messages of the original objects to send to ETL pod
		writerCh       chan *io.PipeWriter // inbound (post-transform) messages from ETL pod
		eg             *errgroup.Group
		name           string
		writerChanFull cos.ChanFull
	}

	transformTask struct {
		r     io.ReadCloser
		w     *io.PipeWriter
		daddr string
		fqn   string
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

func (ws *webSocketComm) InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, _ string) (int, error) {
	// establish WebSocket connection to ETL
	conn, resp, err := websocket.DefaultDialer.Dial(ws.boot.uri, nil)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	defer resp.Body.Close()
	defer conn.Close()

	// if the ETL expects direct put, send an initial empty TextMessage before streaming the object content
	if ws.boot.msg.IsDirectPut() {
		conn.WriteMessage(websocket.TextMessage, []byte(""))
	}

	// send object content with configued arg type
	writer, err := conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	buf, slab := core.T.PageMM().Alloc()
	defer slab.Free(buf)

	switch ws.boot.msg.ArgType() {
	case ArgTypeDefault, ArgTypeURL:
		latestVer := r.URL.Query().Get(apc.QparamLatestVer)
		srcResp := lom.GetROC(cos.IsParseBool(latestVer), false)
		if srcResp.Err != nil {
			return srcResp.Ecode, srcResp.Err
		}
		if _, err := cos.CopyBuffer(writer, srcResp.R, buf); err != nil {
			return http.StatusInternalServerError, err
		}
		srcResp.R.Close()
	case ArgTypeFQN:
		if ecode, err := lomLoad(lom, ws.Xact().Kind()); err != nil {
			return ecode, err
		}
		fqn := url.PathEscape(lom.FQN)
		if _, err := writer.Write(cos.UnsafeB(fqn)); err != nil {
			return http.StatusInternalServerError, err
		}
	}
	writer.Close()

	// receive transformed object back
	ty, reader, err := conn.NextReader()
	if err != nil {
		return http.StatusInternalServerError, err
	}
	debug.Assert(ty == websocket.BinaryMessage)
	if _, err := cos.CopyBuffer(w, reader, buf); err != nil {
		return http.StatusInternalServerError, err
	}

	return 0, nil
}

func (ws *webSocketComm) createSession(xctn core.Xact) (Session, error) {
	if xctn == nil {
		return nil, cos.NewErrNotFound(core.T, "invalid xact parameter")
	}

	connPerSession := ws.boot.config.TCB.SbundleMult * 4 // TODO: add specific ETL config on this
	wss := &wsSession{
		txctn:       xctn,
		isDirectPut: ws.boot.msg.IsDirectPut(),
		argType:     ws.boot.msg.ArgType(),
		workCh:      make(chan transformTask, wockChSize),
		connections: make([]*wsConnCtx, 0, connPerSession),
	}
	wss.sessionCtx, wss.sessionCtxCancel = context.WithCancel(ws.commCtx)

	for i := range connPerSession {
		conn, resp, err := websocket.DefaultDialer.Dial(ws.boot.uri, nil)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to dial %s: %w", xctn.Name(), ws.boot.uri, err)
		}
		resp.Body.Close()

		conn.SetReadLimit(maxMsgSize)

		group, ctx := errgroup.WithContext(wss.sessionCtx)

		wcs := &wsConnCtx{
			name:     ws.ETLName() + "-" + strconv.Itoa(i),
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

func (wss *wsSession) OfflineTransform(lom *core.LOM, latestVer, sync bool, gargs *core.GetROCArgs) core.ReadResp {
	var (
		daddr string
		fqn   string
		err   error
		r     io.ReadCloser
		oah   = &cos.SimpleOAH{Atime: time.Now().UnixNano()}
	)
	switch wss.argType {
	case ArgTypeDefault, ArgTypeURL:
		srcResp := lom.GetROC(latestVer, sync)
		if srcResp.Err != nil {
			return srcResp
		}
		r = srcResp.R
		oah.Size = srcResp.OAH.Lsize()
	case ArgTypeFQN:
		if ecode, err := lomLoad(lom, wss.txctn.Kind()); err != nil {
			return core.ReadResp{Err: err, Ecode: ecode}
		}
		fqn = url.PathEscape(lom.FQN)
	}

	pr, pw := io.Pipe() // TODO -- FIXME: revise and remove

	if wss.isDirectPut && gargs != nil && !gargs.Local {
		daddr = gargs.Daddr
		err = cmn.ErrSkip
	}

	l, c := len(wss.workCh), cap(wss.workCh)
	wss.chanFull.Check(l, c)

	wss.workCh <- transformTask{r, pw, daddr, fqn}

	return core.ReadResp{
		R:     cos.NopOpener(pr),
		OAH:   oah, // TODO: estimate the post-transformed Lsize for stats
		Err:   err,
		Ecode: http.StatusNoContent, // promise to deliver
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
				return err
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
				return err
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

			// 1. send direct put address (empty string for local object)
			err := wctx.conn.WriteMessage(websocket.TextMessage, cos.UnsafeB(rw.daddr))
			if err != nil {
				err = fmt.Errorf("error on writing direct put address %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return err
			}

			// 2. send object content or FQN path
			switch {
			case rw.fqn != "":
				err := wctx.conn.WriteMessage(websocket.TextMessage, cos.UnsafeB(rw.fqn))
				if err != nil {
					err = fmt.Errorf("error on writing direct put address %s: %w", wctx.name, err)
					wctx.txctn.AddErr(err)
					return err
				}
			case rw.r != nil:
				writer, err := wctx.conn.NextWriter(websocket.BinaryMessage)
				if err != nil {
					err = fmt.Errorf("error on getting next writter from %s: %w", wctx.name, err)
					wctx.txctn.AddErr(err)
					return err
				}
				if _, err := cos.CopyBuffer(writer, rw.r, buf); err != nil {
					cos.Close(rw.r)
					cos.Close(writer)
					err = fmt.Errorf("error on getting next writter from %s: %w", wctx.name, err)
					wctx.txctn.AddErr(err)
					return err
				}
				cos.Close(rw.r)
				cos.Close(writer)
			default:
				debug.Assert(false, "transform task should be either reader or fqn")
			}

			// For each object sent to the ETL via `wctx.workCh`, we reserve a corresponding slot in `wctx.writerCh`
			// for its writer. This guarantees that the correct writer is matched with the response when it arrives.
			l, c := len(wctx.writerCh), cap(wctx.writerCh)
			wctx.writerChanFull.Check(l, c)
			wctx.writerCh <- rw.w
		}
	}
}
