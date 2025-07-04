// Package webserver provides a framework to impelemnt etl transformation webserver in golang.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package webserver

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/gorilla/websocket"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/etl"
)

///////////////////////
// public interfaces //
///////////////////////

type ETLServer interface {
	Transform(input io.ReadCloser, path, etlArgs string) (io.ReadCloser, error)
}

func Run(etlSvr ETLServer, ipAddress string, port int) error {
	if etlSvr == nil {
		return cos.NewErrNotFound(nil, "valid etl server implementation must be provided")
	}
	aisTargetURL, exists := os.LookupEnv("AIS_TARGET_URL")
	if !exists {
		return cos.NewErrNotFound(nil, "missing env variable AIS_TARGET_URL")
	}

	base := &etlServerBase{
		endpoint:     fmt.Sprintf("%s:%d", ipAddress, port),
		aisTargetURL: aisTargetURL,
		argType:      os.Getenv("ARG_TYPE"),
		client:       &http.Client{},
		ETLServer:    etlSvr,
	}
	http.HandleFunc("/", base.handler)
	http.HandleFunc("/health", base.healthHandler)
	http.HandleFunc("/ws", base.websocketHandler)

	log.Printf("Starting transformer at %s", base.endpoint)
	return http.ListenAndServe(base.endpoint, nil)
}

//////////////////////////////
// internal implementations //
//////////////////////////////

type etlServerBase struct {
	ETLServer
	aisTargetURL string
	argType      string
	endpoint     string
	client       *http.Client
}

func (*etlServerBase) healthHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Running"))
	default:
		cmn.WriteErr405(w, r, r.Method)
	}
}

func (base *etlServerBase) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		base.putHandler(w, r)
	case http.MethodGet:
		base.getHandler(w, r)
	default:
		cmn.WriteErr405(w, r, r.Method)
	}
}

// PUT /
func (base *etlServerBase) putHandler(w http.ResponseWriter, r *http.Request) {
	var (
		objReader io.ReadCloser
		err       error
	)

	switch base.argType {
	case etl.ArgTypeDefault, etl.ArgTypeURL:
		objReader = r.Body
	case etl.ArgTypeFQN:
		objReader, err = base.getFQNReader(r.URL.Path)
		if err != nil {
			cmn.WriteErr(w, r, fmt.Errorf("[%s] %w", "GET from FQN failed", err))
			return
		}
	default:
		cmn.WriteErrMsg(w, r, "invalid arg_type: "+base.argType)
	}

	transformedReader, err := base.Transform(objReader, r.URL.Path, r.URL.Query().Get(apc.QparamETLTransformArgs))
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("[%s] %w", "failed to transform", err))
		return
	}
	objReader.Close() // objReader is supposed to be fully consumed by the custom transform function - safe to close at this point

	if directPutURL := r.Header.Get(apc.HdrNodeURL); directPutURL != "" {
		err := base.handleDirectPut(directPutURL, transformedReader)
		if err != nil {
			// Note: r.Body (objReader) is consumed during direct put and cannot be restored afterward.
			// Therefore, if direct put fails, we cannot safely fall back to the normal response flow.
			// We enforce that direct put must succeed; otherwise, return HTTP 400.
			w.WriteHeader(http.StatusBadRequest)
		} else {
			setResponseHeaders(w.Header(), 0)
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	if _, err := io.Copy(w, transformedReader); err != nil {
		cmn.WriteErr(w, r, err)
	}
}

// GET /
func (base *etlServerBase) getHandler(w http.ResponseWriter, r *http.Request) {
	if base.aisTargetURL == "" {
		cmn.WriteErrMsg(w, r, "missing env variable AIS_TARGET_URL")
		return
	}

	p := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if p == "health" {
		return
	}

	var (
		objReader io.ReadCloser
	)
	switch base.argType {
	case etl.ArgTypeDefault, etl.ArgTypeURL:
		u := base.aisTargetURL + "/" + p
		req, e := http.NewRequestWithContext(context.Background(), http.MethodGet, u, http.NoBody)
		if e != nil {
			debug.AssertNoErr(e)
			cmn.WriteErr(w, r, e)
			return
		}
		resp, err := wrapHTTPError(base.client.Do(req)) //nolint:bodyclose // is closed from objReader
		if err != nil {
			cmn.WriteErr(w, r, fmt.Errorf("GET from AIStore failed: %v", err))
			return
		}
		objReader = resp.Body
	case etl.ArgTypeFQN:
		reader, err := base.getFQNReader(r.URL.Path)
		if err != nil {
			cmn.WriteErr(w, r, fmt.Errorf("GET(FQN) failed: %v", err))
			return
		}
		objReader = reader
	default:
		cmn.WriteErrMsg(w, r, "invalid arg_type: "+base.argType)
	}

	transformedReader, err := base.Transform(objReader, r.URL.Path, r.URL.Query().Get(apc.QparamETLTransformArgs))
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("[%s] %w", "failed to transform", err))
		return
	}
	objReader.Close() // objReader is supposed to be fully consumed by the custom transform function - safe to close at this point

	if directPutURL := r.Header.Get(apc.HdrNodeURL); directPutURL != "" {
		if err := base.handleDirectPut(directPutURL, transformedReader); err != nil {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			setResponseHeaders(w.Header(), 0)
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	if _, err := io.Copy(w, transformedReader); err != nil {
		cmn.WriteErr(w, r, err)
	}
}

func (base *etlServerBase) websocketHandler(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(_ *http.Request) bool { return true },
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		nlog.Errorln("failed to upgrade to websocket", err)
		return
	}
	defer conn.Close()

	for {
		var (
			ctrl   etl.WebsocketCtrlMsg
			reader io.Reader
		)
		if err := conn.ReadJSON(&ctrl); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				break
			}
			nlog.Errorln("error reading control message", err)
			break
		}

		if ctrl.FQN != "" {
			fqn, err := url.PathUnescape(ctrl.FQN)
			if err != nil {
				nlog.Errorln("failed to unescape", err)
				break
			}
			reader, err = base.getFQNReader(fqn)
			if err != nil {
				nlog.Errorln("failed to read FQN", err)
				break
			}
		} else {
			_, reader, err = conn.NextReader()
			if err != nil {
				nlog.Errorln("failed to read binary", err)
				break
			}
		}

		transformed, err := base.Transform(io.NopCloser(reader), ctrl.Path, ctrl.Targs)
		if err != nil {
			nlog.Errorln("transform error", err)
			break
		}

		if ctrl.Daddr != "" {
			err := base.handleDirectPut(ctrl.Daddr, transformed)
			if err == nil {
				conn.WriteMessage(websocket.TextMessage, cos.UnsafeB("direct put success"))
				continue
			}
			nlog.Errorln("direct put failed", err)
		}

		writer, err := conn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			nlog.Errorln("write error", err)
			break
		}
		if _, err := io.Copy(writer, transformed); err != nil {
			nlog.Errorln("copy error", err)
		}
		writer.Close()
	}
}

func (*etlServerBase) getFQNReader(urlPath string) (io.ReadCloser, error) {
	fqn := cos.JoinWords(urlPath)
	return os.Open(fqn)
}

func (base *etlServerBase) handleDirectPut(directPutURL string, r io.ReadCloser) error {
	parsedTarget, err := url.Parse(directPutURL)
	if err != nil {
		r.Close()
		return err
	}
	parsedHost, err := url.Parse(base.aisTargetURL)
	if err != nil {
		r.Close()
		return err
	}
	parsedHost.Host = parsedTarget.Host
	parsedHost.Path = path.Join(parsedHost.Path, parsedTarget.Path)
	parsedHost.RawQuery = parsedTarget.RawQuery

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, parsedHost.String(), r)
	if err != nil {
		r.Close()
		return err
	}
	resp, err := wrapHTTPError(base.client.Do(req)) //nolint:bodyclose // closed below
	if err != nil {
		return err
	}
	return resp.Body.Close()
}
