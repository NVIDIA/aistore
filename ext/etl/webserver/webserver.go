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
		size      int64
		err       error
	)

	switch base.argType {
	case etl.ArgTypeDefault, etl.ArgTypeURL:
		objReader, err = base.Transform(r.Body, r.URL.Query().Get(apc.QparamETLTransformArgs), r.URL.Path)
		if err != nil {
			logErrorf("%v", err)
			return
		}
		size = r.ContentLength
	case etl.ArgTypeFQN:
		joined := filepath.Join("/", strings.TrimLeft(r.URL.Path, "/"))
		fqn := path.Clean(joined)
		objReader, size, err = base.getFQNReader(fqn)
		if err != nil {
			logErrorf("%v", err)
			return
		}
		objReader, err = base.Transform(objReader, r.URL.Query().Get(apc.QparamETLTransformArgs), r.URL.Path)
		if err != nil {
			logErrorf("%v", err)
			return
		}
	default:
		logErrorf("invalid arg_type: %s", base.argType)
	}

	if directPutURL := r.Header.Get(apc.HdrNodeURL); directPutURL != "" {
		err := base.handleDirectPut(directPutURL, objReader)
		if err != nil {
			// Note: r.Body (objReader) is consumed during direct put and cannot be restored afterward.
			// Therefore, if direct put fails, we cannot safely fall back to the normal response flow.
			// We enforce that direct put must succeed; otherwise, return HTTP 500.
			log.Printf("%v", err)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			setResponseHeaders(w.Header(), 0)
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	setResponseHeaders(w.Header(), size)
	if _, err := io.Copy(w, objReader); err != nil {
		logErrorf("%v", err)
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
		size      int64
		err       error
	)
	switch base.argType {
	case etl.ArgTypeDefault, etl.ArgTypeURL:
		resp, err := wrapHTTPError(base.client.Get(fmt.Sprintf("%s/%s", base.aisTargetURL, p))) //nolint:bodyclose // is closed from objReader
		if err != nil {
			invalidMsgHandler(w, "GET to AIStore failed; err %v", err)
			return
		}
		objReader = resp.Body
		size = resp.ContentLength
	case etl.ArgTypeFQN:
		joined := filepath.Join("/", strings.TrimLeft(r.URL.Path, "/"))
		fqn := path.Clean(joined)
		objReader, size, err = base.getFQNReader(fqn)
		if err != nil {
			logErrorf("%v", err)
		}
	default:
		logErrorf("invalid arg_type: %s", base.argType)
	}

	if directPutURL := r.Header.Get(apc.HdrNodeURL); directPutURL != "" {
		if err := base.handleDirectPut(directPutURL, objReader); err == nil {
			setResponseHeaders(w.Header(), 0)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		logErrorf("%v", err)
	}

	setResponseHeaders(w.Header(), size)
	if _, err := io.Copy(w, objReader); err != nil {
		logErrorf("%v", err)
	}
	objReader.Close()
}

func (*etlServerBase) getFQNReader(fqn string) (io.ReadCloser, int64, error) {
	fh, err := os.Open(fqn)
	if err != nil {
		return nil, 0, err
	}
	stat, err := fh.Stat()
	if err != nil {
		return nil, 0, err
	}
	return fh, stat.Size(), nil
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
