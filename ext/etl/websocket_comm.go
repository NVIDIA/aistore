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
		createSession(xctn core.Xact, multiplier int) (Session, error)
	}

	// Session represents a per-xaction communication context created by the statefulCommunicator.
	Session interface {
		// OfflineTransform is an instance of `core.GetROC` function, which is driven by `TCB` and `TCO` to provide offline transformation
		OfflineTransform(lom *core.LOM, latestVer, sync bool, gargs *core.GetROCArgs) core.ReadResp
		// Finish cleans up the job's communication channel, and aborts the undergoing xaction (`TCB`/`TCO`) if errCause is provided
		Finish(errCause error) error
	}

	//nolint:dupword // ASCII diagram contains repeated characters by design
	/*
	 * +--------------------------------------------------------------------------------------------+
	 * |                                       Target Node                                          |
	 * |            |                                                                               |
	 * |            |       +-------------------+                  (#inlineSessionMultiplier)       |
	 * |   inline   |       |    wsSession (0) -|--> wsConnCtx (0)(1) <--------------+              |
	 * |  transform  ------ | ---> workCh ---  -|--> wsConnCtx (0)(2) <------------+ |              |
	 * |  requests  |       |     (inline)     -|--> wsConnCtx (0)(3) <----------+ | |              |
	 * |            |       +-------------------+                                | | |              |
	 * |            |                  ^                                         v v v              |
	 * |            |       +----------|--------+                 +---------------------------+     |
	 * |            |       |   webSocketComm   | --- K8s API --> |   ETL Pod (Transformer)   |     |
	 * |            |       +---|-----------|---+                 +---------------------------+     |
	 * |            |           v           |                           ^ ^ ^ ^ ^   ^ ^ ^ ^ ^       |
	 * |            |       +-------------------+                       | | | | |   | | | | |       |
	 * |            |       |    wsSession (1) -|--> wsConnCtx (1)(1) <-+ | | | |   | | | | |       |
	 * |            |       |                / -|--> wsConnCtx (1)(2) <---+ | | |   | | | | |       |
	 * |  TCB (1) --------- | ---> workCh ---  -|--> wsConnCtx (1)(3) <-----+ | |   | | | | |       |
	 * |            |       |                \ -|--> wsConnCtx (1)(4) <-------+ |   | | | | |       |
	 * |            |       |    (offline)     -|--> wsConnCtx (1)(5) <---------+   | | | | |       |
	 * |            |       +-------------------+                                   | | | | |       |
	 * |            |                       |                                       | | | | |       |
	 * |            |                       v                           (ws conns)  | | | | |       |
	 * |            |       +-------------------+                                   | | | | |       |
	 * |            |       |    wsSession (2) -|--> wsConnCtx (2)(1) <-------------+ | | | |       |
	 * |            |       |                / -|--> wsConnCtx (2)(2) <---------------+ | | |       |
	 * |  TCB (2) --------- | ---> workCh ---  -|--> wsConnCtx (2)(3) <-----------------+ | |       |
	 * |            |       |                \ -| -> wsConnCtx (2)(4) <-------------------+-|       |
	 * |            |       |    (offline)     -| -> wsConnCtx (2)(5) <---------------------+       |
	 * |            |       +-------------------+                  (#offlineSessionMultiplier)      |
	 * +--------------------------------------------------------------------------------------------+
	 */

	webSocketComm struct {
		baseComm
		commCtx         context.Context
		inlineSession   Session
		offlineSessions map[string]Session
		commCtxCancel   context.CancelFunc
		m               sync.Mutex
	}

	wsSession struct {
		txctn            core.Xact
		sessionCtx       context.Context
		workCh           chan *transformTask
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
		workCh         chan *transformTask // outbound messages of the original objects to send to ETL pod
		writerCh       chan *io.PipeWriter // inbound (post-transform) messages from ETL pod
		eg             *errgroup.Group
		name           string
		writerChanFull cos.ChanFull
	}

	transformTask struct {
		Daddr  string `json:"dst_addr,omitempty"`
		Targs  string `json:"etl_args,omitempty"`
		FQN    string `json:"fqn,omitempty"`
		rwpair `json:"-"`
	}

	rwpair struct {
		r io.ReadCloser
		w *io.PipeWriter
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

	// TODO -- FIXME: make these specific ETL configs `ws-multiplier`
	inlineSessionMultiplier  = 1
	offlineSessionMultiplier = 4
)

// SetupConnection establishes a test connection to the ETL pod's websocket endpoint.
// Close immediately if the connection is successful (only for verifying connectivity).
func (ws *webSocketComm) SetupConnection() (err error) {
	if err := ws.boot.setupConnection("ws://"); err != nil {
		return err
	}
	ws.boot.uri += "/ws" // TODO: make this endpoint configurable

	ws.inlineSession, err = ws.createSession(ws.boot.xctn, inlineSessionMultiplier)
	return err
}

func (ws *webSocketComm) InlineTransform(w http.ResponseWriter, _ *http.Request, lom *core.LOM, latestVer bool, targs string) (int, error) {
	buf, slab := core.T.PageMM().Alloc()
	defer slab.Free(buf)

	resp := ws.inlineSession.OfflineTransform(lom, latestVer, false /*sync*/, &core.GetROCArgs{TransformArgs: targs})
	if _, err := cos.CopyBuffer(w, resp.R, buf); err != nil {
		return 0, err
	}
	if resp.Err != nil {
		return resp.Ecode, resp.Err
	}

	return 0, resp.R.Close()
}

func (ws *webSocketComm) createSession(xctn core.Xact, multiplier int) (Session, error) {
	if xctn == nil {
		return nil, cos.NewErrNotFound(core.T, "invalid xact parameter")
	}

	connPerSession := ws.boot.config.TCB.SbundleMult * multiplier // TODO: add specific ETL config on this
	wss := &wsSession{
		txctn:       xctn,
		isDirectPut: ws.boot.msg.IsDirectPut(),
		argType:     ws.boot.msg.ArgType(),
		workCh:      make(chan *transformTask, wockChSize),
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
	ws.offlineSessions[xctn.ID()] = wss
	ws.m.Unlock()
	return wss, nil
}

func (ws *webSocketComm) Stop() {
	for _, session := range ws.offlineSessions {
		session.Finish(cmn.ErrXactUserAbort)
	}
	ws.offlineSessions = nil
	ws.inlineSession.Finish(cmn.ErrXactUserAbort)
	ws.commCtxCancel()
	ws.baseComm.Stop()
}

///////////////
// wsSession //
///////////////

func (wss *wsSession) OfflineTransform(lom *core.LOM, latestVer, sync bool, gargs *core.GetROCArgs) core.ReadResp {
	var (
		task = &transformTask{}
		err  error
		r    io.ReadCloser
		oah  = &cos.SimpleOAH{Atime: time.Now().UnixNano()}
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
		task.FQN = url.PathEscape(lom.FQN)
	}

	pr, pw := io.Pipe() // TODO -- FIXME: revise and remove
	task.rwpair = rwpair{r, pw}

	if gargs != nil {
		task.Targs = gargs.TransformArgs
		if wss.isDirectPut && !gargs.Local && gargs.Daddr != "" {
			task.Daddr = gargs.Daddr
			err = cmn.ErrSkip
		}
	}

	// local object should contain empty direct put address; remote object should contain valid direct put address
	debug.Assert(gargs == nil || task.Daddr == "" && gargs.Local || task.Daddr != "" && !gargs.Local)
	debug.Assert(task.Targs == "" || task.Daddr == "") // Targs is for inline transform, while Daddr is for offline transform

	l, c := len(wss.workCh), cap(wss.workCh)
	wss.chanFull.Check(l, c)

	wss.workCh <- task

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
				err = fmt.Errorf("error reading message from %s: %w", wctx.name, err)
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
				err = fmt.Errorf("error copying message from %s: %w", wctx.name, err)
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
		case task := <-wctx.workCh:
			// Leverages the fact that WebSocket preserves message order and boundaries.
			// Sends two consecutive WebSocket messages:
			//   1. A TextMessage containing the direct PUT address
			//   2. A BinaryMessage containing the object content
			//
			// The ETL server is expected to consume them in the same order and treat them as logically linked.

			// 1. send control message
			err := wctx.conn.WriteMessage(websocket.BinaryMessage, cos.MustMarshal(*task))
			if err != nil {
				err = fmt.Errorf("error writing control message %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return err
			}

			// 2. send object content if any (not fqn case)
			if task.r != nil {
				writer, err := wctx.conn.NextWriter(websocket.BinaryMessage)
				if err != nil {
					err = fmt.Errorf("error getting next writter from %s: %w", wctx.name, err)
					wctx.txctn.AddErr(err)
					return err
				}
				if _, err := cos.CopyBuffer(writer, task.r, buf); err != nil {
					err = fmt.Errorf("error getting next writter from %s: %w", wctx.name, err)
					wctx.txctn.AddErr(err)
					return err
				}
				cos.Close(task.r)
				cos.Close(writer)
			}

			// For each object sent to the ETL via `wctx.workCh`, we reserve a corresponding slot in `wctx.writerCh`
			// for its writer. This guarantees that the correct writer is matched with the response when it arrives.
			l, c := len(wctx.writerCh), cap(wctx.writerCh)
			wctx.writerChanFull.Check(l, c)
			wctx.writerCh <- task.w
		}
	}
}
