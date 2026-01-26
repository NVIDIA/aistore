// Package webserver provides a framework to impelemnt etl transformation webserver in golang.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package webserver

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

type EchoServer struct {
	ETLServer
}

func (*EchoServer) Transform(input io.ReadCloser, _, _ string) (io.ReadCloser, int64, error) {
	data, err := io.ReadAll(input)
	if err != nil {
		return nil, -1, err
	}
	input.Close()
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func TestInvalidETLServer(t *testing.T) {
	err := Run(nil, "0.0.0.0", 8080)
	if err == nil {
		t.Fatal("invalid ETL Server should return an error")
	}
}

func TestETLServerPutHandler(t *testing.T) {
	var (
		secretPrefix = "/v1/_object/some_secret"
		host         = "http://0.0.0.0"
		port         = "8080"

		svr = &etlServerBase{
			aisTargetURL: host + secretPrefix,
			endpoint:     host + ":" + port,
			client:       &http.Client{},
			ETLServer:    &EchoServer{},
		}
	)

	t.Run("directPut=none", func(t *testing.T) {
		t.Run("argType=default", func(t *testing.T) {
			var (
				body = []byte("test bytes")
				req  = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(body))
				w    = httptest.NewRecorder()
			)

			svr.putHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()

			tassert.Fatalf(t, http.StatusOK == resp.StatusCode, "expected status code 200, got %d", resp.StatusCode)
			tassert.Fatalf(t, tools.ReaderEqual(resp.Body, bytes.NewReader(body)), "expected body %s, got %s", body, resp.Body)
		})

		t.Run("argType=fqn", func(t *testing.T) {
			file, content := createFQNFile(t)
			defer os.Remove(file)

			var (
				path = "/" + url.PathEscape(file)
				req  = httptest.NewRequest(http.MethodPut, path, http.NoBody)
				w    = httptest.NewRecorder()
				q    = req.URL.Query()
			)
			q.Set(apc.QparamETLFQN, file)
			req.URL.RawQuery = q.Encode()
			svr.putHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()

			tassert.Fatalf(t, http.StatusOK == resp.StatusCode, "expected status code 200, got %d", resp.StatusCode)
			tassert.Fatalf(t, tools.ReaderEqual(resp.Body, bytes.NewReader(content)), "expected content %s, got %s", content, resp.Body)
		})
	})

	t.Run("directPut=success", func(t *testing.T) {
		var directPutPath = "ais@#test/obj"
		directPutTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tassert.Fatalf(t, r.Method == http.MethodPut, "expected PUT method, got %s", r.Method)
			tassert.Fatalf(t, cos.JoinWP(secretPrefix, directPutPath) == r.URL.Path, "expected path %s, got %s", cos.JoinWP(secretPrefix, directPutPath), r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer directPutTargetServer.Close()

		t.Run("argType=default", func(t *testing.T) {
			var (
				content = []byte("test bytes")
				req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
				w       = httptest.NewRecorder()
			)
			req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

			svr.putHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()
			result, _ := io.ReadAll(resp.Body)

			tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
			tassert.Fatalf(t, len(result) == 0, "expected no content")
		})

		t.Run("argType=fqn", func(t *testing.T) {
			file, _ := createFQNFile(t)
			defer os.Remove(file)

			var (
				path = "/" + url.PathEscape(file)
				req  = httptest.NewRequest(http.MethodPut, path, http.NoBody)
				w    = httptest.NewRecorder()
				q    = req.URL.Query()
			)
			q.Set(apc.QparamETLFQN, file)
			req.URL.RawQuery = q.Encode()
			req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

			svr.putHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()
			result, _ := io.ReadAll(resp.Body)

			tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
			tassert.Fatalf(t, len(result) == 0, "expected no content")
		})
	})
	t.Run("directPut=fail", func(t *testing.T) {
		var directPutPath = "ais@#test/obj"
		directPutTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tassert.Fatalf(t, r.Method == http.MethodPut, "expected PUT method, got %s", r.Method)
			tassert.Fatalf(t, cos.JoinWP(secretPrefix, directPutPath) == r.URL.Path, "expected path %s, got %s", cos.JoinWP(secretPrefix, directPutPath), r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer directPutTargetServer.Close()

		t.Run("argType=default", func(t *testing.T) {
			var (
				content = []byte("test bytes")
				req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
				w       = httptest.NewRecorder()
			)
			req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

			svr.putHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()

			tassert.Fatalf(t, http.StatusBadRequest == resp.StatusCode, "expected status code 500, got %d", resp.StatusCode)
		})

		t.Run("argType=fqn", func(t *testing.T) {
			file, _ := createFQNFile(t)
			defer os.Remove(file)

			var (
				path = "/" + url.PathEscape(file)
				req  = httptest.NewRequest(http.MethodPut, path, http.NoBody)
				w    = httptest.NewRecorder()
				q    = req.URL.Query()
			)
			q.Set(apc.QparamETLFQN, file)
			req.URL.RawQuery = q.Encode()
			req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

			svr.putHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()

			tassert.Fatalf(t, http.StatusBadRequest == resp.StatusCode, "expected status code 500, got %d", resp.StatusCode)
		})
	})
}

func TestEchoServerGetHandler(t *testing.T) {
	var (
		secretPrefix = "/v1/_object/some_secret"
		host         = "http://0.0.0.0"
		port         = "8080"

		objUname   = "ais@#test/obj"
		objContent = []byte("mocked object content")
	)
	localTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tassert.Fatalf(t, r.Method == http.MethodGet, "expected GET method, got %s", r.Method)
		tassert.Fatalf(t, cos.JoinWP(secretPrefix, objUname) == r.URL.Path, "expected path %s, got %s", cos.JoinWP(secretPrefix, objUname), r.URL.Path)

		w.WriteHeader(http.StatusOK)
		w.Write(objContent)
	}))
	defer localTargetServer.Close()

	svr := &etlServerBase{
		aisTargetURL: localTargetServer.URL + secretPrefix,
		endpoint:     host + ":" + port,
		client:       &http.Client{},
		ETLServer:    &EchoServer{},
	}

	t.Run("directPut=none", func(t *testing.T) {
		t.Run("argType=default", func(t *testing.T) {
			var (
				req = httptest.NewRequest(http.MethodGet, "/"+objUname, http.NoBody)
				w   = httptest.NewRecorder()
			)

			svr.getHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()

			tassert.Fatalf(t, http.StatusOK == resp.StatusCode, "expected status code 200, got %d", resp.StatusCode)
			tassert.Fatalf(t, tools.ReaderEqual(resp.Body, bytes.NewReader(objContent)), "expected content %s, got %s", objContent, resp.Body)
		})

		t.Run("argType=fqn", func(t *testing.T) {
			file, content := createFQNFile(t)
			defer os.Remove(file)

			var (
				path = "/" + url.PathEscape(file)
				req  = httptest.NewRequest(http.MethodGet, path, http.NoBody)
				w    = httptest.NewRecorder()
				q    = req.URL.Query()
			)
			q.Set(apc.QparamETLFQN, file)
			req.URL.RawQuery = q.Encode()
			svr.getHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()

			tassert.Fatalf(t, http.StatusOK == resp.StatusCode, "expected status code 200, got %d", resp.StatusCode)
			tassert.Fatalf(t, tools.ReaderEqual(resp.Body, bytes.NewReader(content)), "expected content %s, got %s", content, resp.Body)
		})
	})

	t.Run("directPut=success", func(t *testing.T) {
		var directPutPath = "ais@#test/obj"
		directPutTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tassert.Fatalf(t, r.Method == http.MethodPut, "expected PUT method, got %s", r.Method)
			tassert.Fatalf(t, cos.JoinWP(secretPrefix, directPutPath) == r.URL.Path, "expected path %s, got %s", cos.JoinWP(secretPrefix, directPutPath), r.URL.Path)

			w.WriteHeader(http.StatusNoContent)
		}))
		defer directPutTargetServer.Close()

		t.Run("argType=default", func(t *testing.T) {
			var (
				req = httptest.NewRequest(http.MethodGet, "/"+objUname, http.NoBody)
				w   = httptest.NewRecorder()
			)
			req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

			svr.getHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()
			result, _ := io.ReadAll(resp.Body)

			tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
			tassert.Fatalf(t, len(result) == 0, "expected no content")
		})

		t.Run("argType=fqn", func(t *testing.T) {
			file, _ := createFQNFile(t)
			defer os.Remove(file)

			var (
				path = "/" + url.PathEscape(file)
				req  = httptest.NewRequest(http.MethodGet, path, http.NoBody)
				w    = httptest.NewRecorder()
				q    = req.URL.Query()
			)
			q.Set(apc.QparamETLFQN, file)
			req.URL.RawQuery = q.Encode()
			req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

			svr.getHandler(w, req)

			resp := w.Result()
			defer resp.Body.Close()
			result, _ := io.ReadAll(resp.Body)

			tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
			tassert.Fatalf(t, len(result) == 0, "expected no content")
		})
	})
}

func TestWebSocketHandler(t *testing.T) {
	var (
		originalData = []byte("hello")
		directPutURL = "/ais/etl_dst/obj"

		secretPrefix = "/v1/_object/some_secret"
		host         = "http://0.0.0.0"
		port         = "8080"
	)

	// Direct PUT target
	directPutServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		tassert.Fatalf(t, bytes.Equal(data, originalData), "direct PUT got unexpected data")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer directPutServer.Close()

	// ETL server with websocket handler
	wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			base := &etlServerBase{
				aisTargetURL: host + secretPrefix,
				endpoint:     host + ":" + port,
				client:       &http.Client{},
				ETLServer:    &EchoServer{},
			}
			base.websocketHandler(w, r)
		}
	}))
	defer wsSrv.Close()

	// Connect to WebSocket endpoint
	u := "ws" + strings.TrimPrefix(wsSrv.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(u, nil) //nolint:bodyclose // closed below
	tassert.Fatalf(t, err == nil, "WebSocket connection failed: %v", err)

	// Test direct PUT
	err = conn.WriteJSON(etl.WebsocketCtrlMsg{Pipeline: directPutServer.URL + directPutURL})
	tassert.Fatalf(t, err == nil, "Write JSON failed")
	err = conn.WriteMessage(websocket.BinaryMessage, originalData)
	tassert.Fatalf(t, err == nil, "Write message failed")

	mt, msg, err := conn.ReadMessage()
	tassert.CheckError(t, err)
	err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	tassert.CheckError(t, err)
	tassert.CheckError(t, conn.Close())
	tassert.Fatalf(t, mt == websocket.TextMessage, "Expected TextMessage")
	tassert.Fatalf(t, string(msg) == "direct put success", "Unexpected ack: %s", msg)

	// Test fallback: no direct PUT
	conn2, _, err := websocket.DefaultDialer.Dial(u, nil) //nolint:bodyclose // closed below
	tassert.Fatalf(t, err == nil, "WebSocket connection 2 failed")
	err = conn2.WriteJSON(etl.WebsocketCtrlMsg{}) // no dst_addr
	tassert.Fatalf(t, err == nil, "Write empty JSON failed")
	err = conn2.WriteMessage(websocket.BinaryMessage, originalData)
	tassert.Fatalf(t, err == nil, "Write message failed")

	mt, msg, err = conn2.ReadMessage()
	tassert.CheckError(t, err)
	err = conn2.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	tassert.CheckError(t, err)
	tassert.CheckError(t, conn2.Close())
	tassert.Fatalf(t, mt == websocket.BinaryMessage, "Expected BinaryMessage")
	tassert.Fatalf(t, bytes.Equal(msg, originalData), "Unexpected content: %s", msg)

	// Test FQN
	conn3, _, err := websocket.DefaultDialer.Dial(u, nil) //nolint:bodyclose // closed below
	tassert.Fatalf(t, err == nil, "WebSocket connection failed: %v", err)
	file, content := createFQNFile(t)
	defer os.Remove(file)

	err = conn3.WriteJSON(etl.WebsocketCtrlMsg{FQN: file})
	tassert.Fatalf(t, err == nil, "Write JSON failed")

	mt, msg, err = conn3.ReadMessage()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, mt == websocket.BinaryMessage, "expected BinaryMessage")
	tassert.Fatalf(t, bytes.Equal(msg, content), "unexpected content: %s", msg)

	err = conn3.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, conn3.Close())
}

func createFQNFile(t *testing.T) (string, []byte) {
	var content = []byte("mocked file content")
	tmpfile, err := os.CreateTemp(t.TempDir(), "mockfile")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	if _, err := tmpfile.Write(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	tmpfile.Close()
	return tmpfile.Name(), content
}
