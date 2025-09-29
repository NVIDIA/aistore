// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package etl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
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
		OfflineWrite(lom *core.LOM, latestVer, sync bool, writer io.WriteCloser, args *core.ETLArgs) (written int64, ecode int, err error)

		transform(lom *core.LOM, latestVer, sync bool, writer io.WriteCloser, args *core.ETLArgs) (written int64, ecode int, err error)
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
		commCtx       context.Context
		sessions      map[string]Session // includes inlineSession
		inlineSession Session
		commCtxCancel context.CancelFunc
		m             sync.Mutex
	}

	wsSession struct {
		msg              InitMsg
		txctn            core.Xact // tcb/tcobjs xaction that uses this session to perform transformatio
		sessionCtx       context.Context
		workCh           chan *transformTask
		sessionCtxCancel context.CancelFunc
		connections      []*wsConnCtx
		chanFull         cos.ChanFull
		fincb            func() // callback to self-remove this session from the communicator' session list
		finished         atomic.Bool
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

		txctn core.Xact // reference to the TCB/TCO job that created this task
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

// setupConnection establishes a test connection to the ETL pod's websocket endpoint.
// Close immediately if the connection is successful (only for verifying connectivity).
func (ws *webSocketComm) setupConnection(_, podAddr string) (ecode int, err error) {
	if ecode, err := ws.baseComm.setupConnection("ws://", podAddr); err != nil {
		return ecode, err
	}
	ws.podURI += "/ws" // TODO: make this endpoint configurable

	ws.inlineSession, err = ws.createSession(ws.xctn, inlineSessionMultiplier)
	return 0, err
}

func (ws *webSocketComm) InlineTransform(w http.ResponseWriter, _ *http.Request, lom *core.LOM, args *InlineTransArgs) (int64, int, error) {
	// use pre-established inline sessions to serve inline transform requests
	return ws.inlineSession.transform(lom, args.LatestVer, false /*sync*/, cos.NopWriteCloser(w), &core.ETLArgs{
		Pipeline:      args.Pipeline,
		TransformArgs: args.TransformArgs,
	})
}

func (ws *webSocketComm) createSession(xctn core.Xact, multiplier int) (Session, error) {
	if xctn == nil {
		return nil, cos.NewErrNotFound(core.T, "invalid xact parameter")
	}

	connPerSession := ws.config.TCB.SbundleMult * multiplier // TODO: add specific ETL config on this
	wss := &wsSession{
		txctn:       xctn,
		msg:         ws.msg,
		workCh:      make(chan *transformTask, wockChSize),
		connections: make([]*wsConnCtx, 0, connPerSession),
		fincb: func() {
			ws.m.Lock()
			delete(ws.sessions, xctn.ID())
			ws.m.Unlock()
		},
	}
	wss.sessionCtx, wss.sessionCtxCancel = context.WithCancel(ws.commCtx)

	for i := range connPerSession {
		conn, resp, err := websocket.DefaultDialer.Dial(ws.podURI, nil)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to dial %s: %w", xctn.Name(), ws.podURI, err)
		}
		resp.Body.Close()
		debug.IncCounter(xctn.ID() + "-conn") // connection count for the session

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
	ws.sessions[xctn.ID()] = wss
	ws.m.Unlock()
	return wss, nil
}

func (ws *webSocketComm) stop() error {
	if err := ws.baseComm.stop(); err != nil {
		return err
	}
	// inlineSession is stored in ws.sessions
	for _, session := range ws.sessions {
		session.Finish(cmn.ErrXactUserAbort)
	}
	ws.sessions = nil
	ws.commCtxCancel()
	return nil
}

func (*webSocketComm) ProcessDownloadJob(_ *ETLObjDownloadCtx) (cos.ReadCloseSizer, int, error) {
	return nil, http.StatusNotImplemented, errors.New("ETL downloads not supported for websocket communication type")
}

///////////////
// wsSession //
///////////////

func (wss *wsSession) transform(lom *core.LOM, latestVer, sync bool, woc io.WriteCloser, args *core.ETLArgs) (written int64, ecode int, err error) {
	task, ecode, err := wss.createTask(lom, latestVer, sync, woc)
	if err != nil {
		return 0, ecode, err
	}

	if args != nil {
		task.ctrlmsg.Targs = args.TransformArgs
		if len(args.Pipeline) != 0 {
			task.ctrlmsg.Pipeline = args.Pipeline.Pack()
		}
	}

	if cmn.Rom.V(5, cos.ModETL) {
		nlog.Infoln(WebSocket, lom.Cname(), args.Pipeline.String(), err, ecode)
	}

	l, c := len(wss.workCh), cap(wss.workCh)
	wss.chanFull.Check(l, c)

	// Ensure `task.done()` is called exactly once after `wss.createTask()` succeeds to unblock `task.wg.Wait()`
	// Cases for calling `task.done()`:
	// 1. Task completes successfully (direct put or local copy) => call with `nil` error
	// 2. Task fails (e.g., network or I/O error) => call with the error
	// 3. Task is drained during session finish => call with the given `errCause` on abort
	task.wg.Add(1)
	wss.workCh <- task
	task.wg.Wait()

	return task.written, 0, task.err
}

func (wss *wsSession) createTask(lom *core.LOM, latestVer, sync bool, woc io.WriteCloser) (*transformTask, int, error) {
	task := &transformTask{txctn: wss.txctn}

	task.w = woc
	task.ctrlmsg.Path = lom.ObjName
	switch {
	case latestVer, sync, cmn.Rom.Features().IsSet(feat.DontAllowPassingFQNtoETL): // TODO -- FIXME: consider chunked case
		srcResp := lom.GetROC(latestVer, sync)
		if srcResp.Err != nil {
			if woc != nil {
				cos.Close(woc)
			}
			return nil, 0, srcResp.Err
		}
		task.r = srcResp.R
	default:
		// default to FQN
		if ecode, err := lomLoad(lom, wss.txctn.Kind()); err != nil {
			if woc != nil {
				cos.Close(woc)
			}
			return nil, ecode, err
		}
		task.ctrlmsg.FQN = url.PathEscape(lom.FQN)
	}

	debug.IncCounter(task.txctn.ID() + "-task") // count for tasks in this session

	return task, 0, nil
}

func (wss *wsSession) OfflineWrite(lom *core.LOM, latestVer, sync bool, woc io.WriteCloser, args *core.ETLArgs) (written int64, ecode int, err error) {
	return wss.transform(lom, latestVer, sync, woc, args)
}

func (wss *wsSession) Finish(errCause error) error {
	// Note: Finish can be called from communicator's `Stop()` or TCB/TCO's finish/abort
	if !wss.finished.CAS(false, true) {
		return nil
	}

	wss.fincb() // self-remove from the communicator's session list
	wss.sessionCtxCancel()
	for _, wsConn := range wss.connections {
		wsConn.finish(errCause)
	}
	drainTaskCh(wss.workCh, errCause)
	debug.AssertCounterEquals(wss.txctn.ID()+"-task", 0) // all tasks should be done
	debug.AssertCounterEquals(wss.txctn.ID()+"-conn", 0) // all connections should be closed
	return nil
}

func (wss *wsSession) String() string {
	return "[" + wss.msg.Name() + "]-" + wss.txctn.ID()
}

///////////////
// wsConnCtx //
///////////////

func (wctx *wsConnCtx) finish(errCause error) {
	if errCause != nil {
		wctx.txctn.Abort(errCause)
	}
	wctx.conn.Close()
	debug.DecCounter(wctx.txctn.ID() + "-conn")
	if err := wctx.eg.Wait(); err != nil {
		nlog.Errorf("error shutting down webSocketComm goroutines: %v", err)
	}
	drainTaskCh(wctx.transformCh, errCause)
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
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway, websocket.CloseServiceRestart) ||
					cos.IsRetriableConnErr(err) || strings.Contains(err.Error(), "use of closed network connection") { // common errors
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

			if task.w == nil {
				err = fmt.Errorf("task.w is nil, expected direct put but got result from pipeline: %s, task: %v", task.ctrlmsg.Pipeline, task)
				debug.AssertNoErr(err)
				task.done(err)
				cos.DrainReader(r)
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
	debug.DecCounter(task.txctn.ID() + "-task") // decrement task count for the session
	return err
}

// Non-blocking drain of work channel; see also transport/sendobj.go
func drainTaskCh(workCh chan *transformTask, err error) {
	for {
		select {
		case task, ok := <-workCh:
			if !ok {
				return
			}
			task.done(err)
		default:
			return
		}
	}
}
