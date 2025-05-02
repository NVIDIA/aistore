// Package webserver provides a framework to impelemnt etl transformation webserver in golang.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package webserver

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/gorilla/websocket"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
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
		invalidMsgHandler(w, "invalid http method %s", r.Method)
	}
}

func (base *etlServerBase) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		base.putHandler(w, r)
	case http.MethodGet:
		base.getHandler(w, r)
	default:
		invalidMsgHandler(w, "invalid http method %s", r.Method)
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
			invalidMsgHandler(w, "GET from FQN failed; err %v", err)
			return
		}
	default:
		invalidMsgHandler(w, "invalid arg_type: %s", base.argType)
	}

	transformedReader, err := base.Transform(objReader, r.URL.Path, r.URL.Query().Get(apc.QparamETLTransformArgs))
	if err != nil {
		invalidMsgHandler(w, "failed to transform: %v", err)
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
		invalidMsgHandler(w, "%v", err)
	}
}

// GET /
func (base *etlServerBase) getHandler(w http.ResponseWriter, r *http.Request) {
	if base.aisTargetURL == "" {
		invalidMsgHandler(w, "missing env variable AIS_TARGET_URL")
		return
	}

	p := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if p == "health" {
		return
	}

	var (
		objReader io.ReadCloser
		err       error
	)
	switch base.argType {
	case etl.ArgTypeDefault, etl.ArgTypeURL:
		resp, err := wrapHTTPError(base.client.Get(fmt.Sprintf("%s/%s", base.aisTargetURL, p))) //nolint:bodyclose // is closed from objReader
		if err != nil {
			invalidMsgHandler(w, "GET from AIStore failed; err %v", err)
			return
		}
		objReader = resp.Body
	case etl.ArgTypeFQN:
		objReader, err = base.getFQNReader(r.URL.Path)
		if err != nil {
			invalidMsgHandler(w, "GET from FQN failed; err %v", err)
			return
		}
	default:
		invalidMsgHandler(w, "invalid arg_type: %s", base.argType)
	}

	transformedReader, err := base.Transform(objReader, r.URL.Path, r.URL.Query().Get(apc.QparamETLTransformArgs))
	if err != nil {
		invalidMsgHandler(w, "failed to transform: %v", err)
		return
	}
	objReader.Close() // objReader is supposed to be fully consumed by the custom transform function - safe to close at this point

	if directPutURL := r.Header.Get(apc.HdrNodeURL); directPutURL != "" {
		if err := base.handleDirectPut(directPutURL, transformedReader); err != nil {
			invalidMsgHandler(w, "%v", err)
		} else {
			setResponseHeaders(w.Header(), 0)
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	if _, err := io.Copy(w, transformedReader); err != nil {
		logErrorf("%v", err)
	}
}

func (base *etlServerBase) websocketHandler(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(_ *http.Request) bool { return true },
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logErrorf("failed to upgrade to websocket: %v", err)
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
			log.Printf("error reading control message: %v", err)
			break
		}

		if ctrl.FQN != "" {
			fqn, err := url.PathUnescape(ctrl.FQN)
			if err != nil {
				logErrorf("failed to unescape: %v", err)
				break
			}
			reader, err = base.getFQNReader(fqn)
			if err != nil {
				logErrorf("failed to read FQN: %v", err)
				break
			}
		} else {
			_, reader, err = conn.NextReader()
			if err != nil {
				logErrorf("failed to read binary: %v", err)
				break
			}
		}

		transformed, err := base.Transform(io.NopCloser(reader), "", ctrl.Targs)
		if err != nil {
			logErrorf("transform error: %v", err)
			break
		}

		if ctrl.Daddr != "" {
			err := base.handleDirectPut(ctrl.Daddr, transformed)
			if err == nil {
				conn.WriteMessage(websocket.TextMessage, cos.UnsafeB("direct put success"))
				continue
			}
			logErrorf("direct put failed: %v", err)
		}

		writer, err := conn.NextWriter(websocket.BinaryMessage)
		if err != nil {
			logErrorf("write error: %v", err)
			break
		}
		if _, err := io.Copy(writer, transformed); err != nil {
			logErrorf("copy error: %v", err)
		}
		writer.Close()
	}
}

func (*etlServerBase) getFQNReader(urlPath string) (io.ReadCloser, error) {
	fqn := filepath.Join("/", strings.TrimLeft(urlPath, "/"))
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

	req, err := http.NewRequest(http.MethodPut, parsedHost.String(), r)
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
