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
		// Finish cleans up the job's communication channel, and aborts the undergoing xaction (`TCB`/`TCO`) if errCause is provided
		Finish(errCause error) error
		OfflineWrite(lom *core.LOM, latestVer, sync bool, writer io.WriteCloser, gargs *core.GetROCArgs) (written int64, ecode int, err error)

		transform(lom *core.LOM, latestVer, sync bool, writer io.WriteCloser, gargs *core.GetROCArgs) (written int64, ecode int, err error)
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
		msg              InitMsg
		txctn            core.Xact
		sessionCtx       context.Context
		workCh           chan *transformTask
		sessionCtxCancel context.CancelFunc
		connections      []*wsConnCtx
		chanFull         cos.ChanFull
	}

	wsConnCtx struct {
		etlxctn           *XactETL  // parent xaction of the underlying ETL pod (`xs.xactETL` type)
		txctn             core.Xact // tcb/tcobjs xaction that uses this session to perform transformatio
		ctx               context.Context
		conn              *websocket.Conn
		workCh            chan *transformTask // outbound messages of the original objects to send to ETL pod
		transformCh       chan *transformTask // inbound (post-transform) messages from ETL pod
		transformChanFull cos.ChanFull
		eg                *errgroup.Group
		name              string
	}

	transformTask struct {
		ctrlmsg WebsocketCtrlMsg
		wg      sync.WaitGroup // used to wait for the task to finish
		written int64
		err     error
		rwpair
	}

	rwpair struct {
		r io.ReadCloser
		w io.WriteCloser
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
	// use pre-established inline sessions to serve inline transform requests
	_, ecode, err := ws.inlineSession.transform(lom, latestVer, false /*sync*/, cos.NopWriteCloser(w), &core.GetROCArgs{
		TransformArgs: targs,
		Local:         true, // inline transform is always local
	})
	return ecode, err
}

func (ws *webSocketComm) createSession(xctn core.Xact, multiplier int) (Session, error) {
	if xctn == nil {
		return nil, cos.NewErrNotFound(core.T, "invalid xact parameter")
	}

	connPerSession := ws.boot.config.TCB.SbundleMult * multiplier // TODO: add specific ETL config on this
	wss := &wsSession{
		txctn:       xctn,
		msg:         ws.boot.msg,
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
			name:        ws.ETLName() + "-" + strconv.Itoa(i),
			etlxctn:     ws.Xact(), // for abort listening and runtime error report
			txctn:       xctn,      // for abort listening and runtime error report
			conn:        conn,
			workCh:      wss.workCh,
			transformCh: make(chan *transformTask, wockChSize),
			ctx:         ctx,
			eg:          group,
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
	if ws.inlineSession != nil {
		ws.inlineSession.Finish(cmn.ErrXactUserAbort)
		ws.inlineSession = nil
	}
	ws.commCtxCancel()
	ws.baseComm.Stop()
}

///////////////
// wsSession //
///////////////

func (wss *wsSession) transform(lom *core.LOM, latestVer, sync bool, woc io.WriteCloser, gargs *core.GetROCArgs) (written int64, ecode int, err error) {
	var (
		task = &transformTask{}
		r    io.ReadCloser
	)
	task.ctrlmsg.Path = lom.ObjName
	switch wss.msg.ArgType() {
	case ArgTypeDefault, ArgTypeURL:
		srcResp := lom.GetROC(latestVer, sync)
		if srcResp.Err != nil {
			return 0, 0, srcResp.Err
		}
		r = srcResp.R
	case ArgTypeFQN:
		if ecode, err := lomLoad(lom, wss.txctn.Kind()); err != nil {
			return 0, ecode, err
		}
		task.ctrlmsg.FQN = url.PathEscape(lom.FQN)
	}

	task.rwpair = rwpair{r, woc}

	if gargs != nil {
		task.ctrlmsg.Targs = gargs.TransformArgs
		if wss.msg.IsDirectPut() && !gargs.Local && gargs.Daddr != "" {
			task.ctrlmsg.Daddr = gargs.Daddr
		}
	}

	// local object should contain empty direct put address; remote object should contain valid direct put address
	debug.Assert(gargs == nil || task.ctrlmsg.Daddr == "" && gargs.Local || task.ctrlmsg.Daddr != "" && !gargs.Local)
	debug.Assert(task.ctrlmsg.Targs == "" || task.ctrlmsg.Daddr == "") // Targs is for inline transform, while Daddr is for offline transform

	l, c := len(wss.workCh), cap(wss.workCh)
	wss.chanFull.Check(l, c)

	// wait for the task to finish
	task.wg.Add(1)
	wss.workCh <- task
	task.wg.Wait()

	return task.written, 0, task.err
}

func (wss *wsSession) OfflineWrite(lom *core.LOM, latestVer, sync bool, woc io.WriteCloser, gargs *core.GetROCArgs) (written int64, ecode int, err error) {
	return wss.transform(lom, latestVer, sync, woc, gargs)
}

func (wss *wsSession) Finish(errCause error) error {
	wss.sessionCtxCancel()
	for _, wsConn := range wss.connections {
		wsConn.finish(errCause)
	}
	drainTaskCh(wss.workCh)
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
	drainTaskCh(wctx.transformCh)
	return wctx.conn.Close()
}

func (wctx *wsConnCtx) writeLoop() (err error) {
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
			//   1. A BinaryMessage as control message containing the direct PUT address, fqn, Path, and etl_args
			//   2. A BinaryMessage containing the object content (if not fqn)
			//
			// The ETL server is expected to consume them in the same order and treat them as logically linked.

			// 1. send control message
			err := wctx.conn.WriteMessage(websocket.BinaryMessage, cos.MustMarshal(task.ctrlmsg))
			if err != nil {
				err = fmt.Errorf("error writing control message %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return task.done(err)
			}

			// 2. send object content if any (not fqn case)
			if task.r != nil {
				connWriter, err := wctx.conn.NextWriter(websocket.BinaryMessage)
				if err != nil {
					err = fmt.Errorf("error getting connection writer from %s: %w", wctx.name, err)
					wctx.txctn.AddErr(err)
					return task.done(err)
				}
				if _, err := cos.CopyBuffer(connWriter, task.r, buf); err != nil {
					err = fmt.Errorf("error writing to %s: %w", wctx.name, err)
					wctx.txctn.AddErr(err)
					return task.done(err)
				}
				cos.Close(connWriter)
			}

			// serialize the task to the transform channel
			l, c := len(wctx.transformCh), cap(wctx.transformCh)
			wctx.transformChanFull.Check(l, c)
			wctx.transformCh <- task
		}
	}
}

func (wctx *wsConnCtx) readLoop() (err error) {
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
				// Handle benign errors that occur when the connection is closed by the ETL server
				// These errors indicate normal closure or server shutdown and should exit the read loop without reporting the error
				if cos.IsRetriableConnErr(err) || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return nil
				}
				// For other errors, log and propagate them as they indicate unexpected issues during message reading
				err = fmt.Errorf("error reading message from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return err
			}
			task := <-wctx.transformCh

			// direct put success (TextMessage ack from ETL server)
			if ty == websocket.TextMessage {
				// TODO: update task.written with the actual size of direct put (for stats)
				task.err = cmn.ErrSkip // indicates that the object was successfully handled by direct put
				task.done(nil)
				continue
			}

			written, err := cos.CopyBuffer(task.w, r, buf)
			if err != nil {
				err = fmt.Errorf("error copying message from %s: %w", wctx.name, err)
				wctx.txctn.AddErr(err)
				return task.done(err)
			}
			task.written = written
			task.done(nil)
		}
	}
}

func (task *transformTask) done(err error) error {
	task.wg.Done()
	if task.r != nil {
		cos.Close(task.r)
	}
	if task.w != nil {
		cos.Close(task.w)
	}
	task.err = err
	return err
}

// Non-blocking drain of a work channel (see also "func.*drain")
func drainTaskCh(workCh chan *transformTask) {
	for {
		select {
		case task, ok := <-workCh:
			if ok {
				task.done(nil)
			}
		default:
			return
		}
	}
}
