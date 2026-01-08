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
	"strconv"
	"strings"

	"github.com/gorilla/websocket"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/etl"
)

//
// public interfaces
//

type ETLServer interface {
	Transform(input io.ReadCloser, path, etlArgs string) (reader io.ReadCloser, size int64, err error)
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
		endpoint:     cmn.HostPort(ipAddress, strconv.Itoa(port)),
		aisTargetURL: aisTargetURL,
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        0,
				MaxIdleConnsPerHost: 32,
				WriteBufferSize:     4 * cos.KiB,
				ReadBufferSize:      4 * cos.KiB,
			},
		},
		ETLServer: etlSvr,
	}
	http.HandleFunc("/", base.handler)
	http.HandleFunc("/health", base.healthHandler)
	http.HandleFunc("/ws", base.websocketHandler)
	http.HandleFunc("/"+apc.ETLDownload, base.downloadHandler)

	log.Printf("Starting transformer at %s", base.endpoint)
	return http.ListenAndServe(base.endpoint, nil)
}

//
// internal
//

type (
	etlServerBase struct {
		ETLServer
		aisTargetURL string
		endpoint     string
		client       *http.Client
	}
	directPutResponse struct {
		StatusCode      int
		Size            int64
		DirectPutLength int64
		Body            io.ReadCloser
	}
)

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
		fqn       = r.URL.Query().Get(apc.QparamETLFQN)
		objReader io.ReadCloser
		err       error
	)

	if fqn != "" {
		objReader, err = base.getFQNReader(fqn)
		if err != nil {
			cmn.WriteErr(w, r, fmt.Errorf("[%s] %w", "GET from FQN failed", err))
			return
		}
	} else {
		objReader = r.Body
	}

	base.handleRequest(objReader, w, r)
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
		fqn       = r.URL.Query().Get(apc.QparamETLFQN)
		objReader io.ReadCloser
		err       error
	)

	if fqn != "" {
		objReader, err = base.getFQNReader(fqn)
		if err != nil {
			cmn.WriteErr(w, r, fmt.Errorf("[%s] %w", "GET from FQN failed", err))
			return
		}
	} else {
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
	}

	base.handleRequest(objReader, w, r)
}

func (base *etlServerBase) handleRequest(objReader io.ReadCloser, w http.ResponseWriter, r *http.Request) {
	transformedReader, size, err := base.Transform(objReader, r.URL.Path, r.URL.Query().Get(apc.QparamETLTransformArgs))
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("[%s] %w", "failed to transform", err))
		return
	}
	objReader.Close() // objReader is supposed to be fully consumed by the custom transform function - safe to close at this point

	if directPutURL := r.Header.Get(apc.HdrNodeURL); directPutURL != "" {
		base.handleDirectPut(w, transformedReader, size, r.URL.Path, directPutURL)
		return
	}

	// no next stage (empty directPutURL): return transformed content directly
	setResponseHeaders(w.Header(), size)
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, transformedReader)
	if err != nil {
		cmn.WriteErr(w, r, err)
	}
}

// isWebsocketCloseErr returns true if the error is a benign websocket close error
func isWebsocketCloseErr(err error) bool {
	return websocket.IsCloseError(err, websocket.CloseNormalClosure,
		websocket.CloseGoingAway, websocket.CloseServiceRestart)
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

	var ctrl etl.WebsocketCtrlMsg
	for {
		if err = conn.ReadJSON(&ctrl); err != nil {
			if isWebsocketCloseErr(err) {
				return // graceful exit
			}
			nlog.Errorln("error reading control message:", err)
			return
		}

		if err = base.handleWebsocketMessage(conn, ctrl); err != nil {
			if isWebsocketCloseErr(err) {
				return
			}
			nlog.Errorln("error handling websocket message:", err)
			return
		}
	}
}

func (base *etlServerBase) handleWebsocketMessage(conn *websocket.Conn, ctrl etl.WebsocketCtrlMsg) error {
	var (
		reader io.ReadCloser
		err    error
	)
	if ctrl.FQN != "" {
		reader, err = base.getFQNReader(ctrl.FQN)
		if err != nil {
			return err
		}
	} else {
		_, r, err := conn.NextReader()
		if err != nil {
			return err
		}
		reader = io.NopCloser(r)
	}

	transformed, size, err := base.Transform(reader, ctrl.Path, ctrl.Targs)
	if err != nil {
		return err
	}
	defer reader.Close()

	return base.handleWebsocketPipeline(conn, transformed, size, ctrl.Path, ctrl.Pipeline)
}

func (base *etlServerBase) handleWebsocketPipeline(conn *websocket.Conn, transformed io.ReadCloser, size int64, objPath, pipeline string) (err error) {
	// No pipeline, send transformed data
	if pipeline == "" {
		writer, _ := conn.NextWriter(websocket.BinaryMessage)
		_, err = io.Copy(writer, transformed)
		if err != nil {
			return err
		}
		return writer.Close()
	}

	firstURL, remainingPipeline := parsePipelineURL(pipeline)
	dresp, err := base.directPut(firstURL, transformed, size, objPath, remainingPipeline)
	if err != nil {
		writer, _ := conn.NextWriter(websocket.TextMessage)
		writer.Write([]byte(err.Error()))
		writer.Close()
		return err
	}

	if dresp.StatusCode == http.StatusOK {
		// from other ETL server, forward the content back
		writer, _ := conn.NextWriter(websocket.BinaryMessage)
		if dresp.Size > 0 && dresp.Body != nil {
			if _, err := io.Copy(writer, dresp.Body); err != nil {
				return err
			}
			dresp.Body.Close()
		}
		writer.Close()
	} else {
		// from target, no content
		writer, _ := conn.NextWriter(websocket.TextMessage)
		_, err = writer.Write([]byte("direct put success"))
		if err != nil {
			return err
		}
		writer.Close()
	}
	return nil
}

func (base *etlServerBase) handleDirectPut(w http.ResponseWriter, transformedReader io.ReadCloser, size int64, objPath, directPutURL string) {
	firstURL, remainingPipeline := parsePipelineURL(directPutURL)
	dresp, err := base.directPut(firstURL, transformedReader, size, objPath, remainingPipeline)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(dresp.StatusCode)
	if dresp.Size > 0 && dresp.Body != nil {
		setResponseHeaders(w.Header(), dresp.Size)
		_, err := io.Copy(w, dresp.Body)
		if err != nil {
			nlog.Errorln("copy error", err)
		}
		dresp.Body.Close()
	}
}

// handleETLObjectDownload processes individual object download requests from ETL communicator
func (base *etlServerBase) handleETLObjectDownload(w http.ResponseWriter, r *http.Request, etlArgs, objName, link string) {
	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infof("Processing ETL object download: %s from %s", objName, link)
	}

	// Download from external URL
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, link, http.NoBody)
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("failed to create download request: %w", err))
		return
	}

	resp, err := wrapHTTPError(base.client.Do(req)) //nolint:bodyclose // closed below
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("failed to download from %s: %w", link, err))
		return
	}
	defer resp.Body.Close()

	// Transform the downloaded data
	transformedReader, _, err := base.Transform(resp.Body, objName, etlArgs)
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("failed to transform downloaded data: %w", err))
		return
	}
	defer transformedReader.Close()

	// Stream transformed data back to target
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, transformedReader); err != nil {
		nlog.Errorf("Failed to stream transformed data: %v", err)
		return
	}

	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infof("Successfully streamed ETL transformed object: %s", objName)
	}
}

func (base *etlServerBase) downloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		cmn.WriteErr405(w, r, r.Method)
		return
	}

	etlArgs := r.URL.Query().Get(apc.QparamETLTransformArgs)
	objName := r.URL.Query().Get(apc.QparamObjTo)
	link := r.URL.Query().Get(apc.QparamOrigURL)

	if objName == "" || link == "" {
		cmn.WriteErrMsg(w, r, "object_name and original_url query parameters are required")
		return
	}

	if cmn.Rom.V(4, cos.ModAIS) {
		nlog.Infof("ETL pod processing object: %s from %s", objName, link)
	}

	base.handleETLObjectDownload(w, r, etlArgs, objName, link)
}

func (*etlServerBase) getFQNReader(urlPath string) (io.ReadCloser, error) {
	fqn, err := url.PathUnescape(urlPath)
	if err != nil {
		return nil, fmt.Errorf("[%s] %w", "failed to unescape FQN", err)
	}

	return os.Open(fqn)
}

func (base *etlServerBase) directPut(directPutURL string, r io.ReadCloser, size int64, objPath, remainingPipelineURL string) (*directPutResponse, error) {
	direct, err := url.Parse(directPutURL)
	if err != nil {
		r.Close()
		return nil, err
	}
	host, err := url.Parse(base.aisTargetURL)
	if err != nil {
		r.Close()
		return nil, err
	}

	finalURL := *host
	finalURL.Host = direct.Host
	finalURL.RawQuery = direct.RawQuery
	if direct.Path != "" {
		finalURL.Path = cos.JoinPath(host.Path, direct.Path)
	} else {
		finalURL.Path = objPath
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPut, finalURL.String(), r)
	if err != nil {
		r.Close()
		return nil, err
	}
	if size > cos.ContentLengthUnknown {
		req.ContentLength = size
	}
	if remainingPipelineURL != "" {
		req.Header.Set(apc.HdrNodeURL, remainingPipelineURL)
	}
	resp, err := wrapHTTPError(base.client.Do(req)) //nolint:bodyclose // closed below
	if err != nil {
		return nil, err
	}

	length := cos.ContentLengthUnknown
	directPutLength, err := strconv.Atoi(resp.Header.Get(apc.HdrDirectPutLength))
	if err == nil {
		length = directPutLength
	}

	// delivered to target, no content
	if resp.StatusCode == http.StatusNoContent {
		return &directPutResponse{
			StatusCode:      http.StatusNoContent,
			DirectPutLength: int64(length),
			Body:            nil,
		}, nil
	}

	if resp.StatusCode == http.StatusOK {
		// from other ETL server, forward the content back
		if resp.ContentLength > 0 {
			return &directPutResponse{
				StatusCode:      resp.StatusCode,
				Size:            resp.ContentLength,
				DirectPutLength: 0,
				Body:            resp.Body,
			}, nil
		}
		// from target, no content
		return &directPutResponse{
			StatusCode:      http.StatusNoContent,
			Size:            0,
			DirectPutLength: size,
			Body:            nil,
		}, nil
	}

	// error
	return &directPutResponse{
		StatusCode:      resp.StatusCode,
		Size:            resp.ContentLength,
		DirectPutLength: 0,
		Body:            resp.Body,
	}, nil
}
