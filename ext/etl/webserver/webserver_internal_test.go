// Package webserver provides a framework to impelemnt etl transformation webserver in golang.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package webserver

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
	"time"

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

func TestETLServerDirectPutForwardsArgs(t *testing.T) {
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

		directPutPath = "ais@#test/obj"
		etlArgs       = `{"format":"jpeg"}`
	)

	t.Run("present", func(t *testing.T) {
		var gotArgs string
		directPutTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotArgs = r.URL.Query().Get(apc.QparamETLTransformArgs)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer directPutTargetServer.Close()

		var (
			content = []byte("test bytes")
			req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
			w       = httptest.NewRecorder()
			q       = req.URL.Query()
		)
		q.Set(apc.QparamETLTransformArgs, etlArgs)
		req.URL.RawQuery = q.Encode()
		req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

		svr.putHandler(w, req)

		resp := w.Result()
		defer resp.Body.Close()

		tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
		tassert.Fatalf(t, gotArgs == etlArgs, "expected forwarded etl_args %q, got %q", etlArgs, gotArgs)
	})

	t.Run("absent", func(t *testing.T) {
		var argsExists bool
		directPutTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, argsExists = r.URL.Query()[apc.QparamETLTransformArgs]
			w.WriteHeader(http.StatusNoContent)
		}))
		defer directPutTargetServer.Close()

		var (
			content = []byte("test bytes")
			req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
			w       = httptest.NewRecorder()
		)
		req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

		svr.putHandler(w, req)

		resp := w.Result()
		defer resp.Body.Close()

		tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
		tassert.Fatalf(t, !argsExists, "expected no etl_args query param when none provided")
	})
}

func TestETLServerDirectPutResponseForwarding(t *testing.T) {
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

		directPutPath = "ais@#test/obj"
	)

	t.Run("204 forwards Direct-Put-Length", func(t *testing.T) {
		const directPutLength = 1234
		directPutTargetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tassert.Fatalf(t, r.Method == http.MethodPut, "expected PUT method, got %s", r.Method)
			w.Header().Set(apc.HdrDirectPutLength, strconv.Itoa(directPutLength))
			w.WriteHeader(http.StatusNoContent)
		}))
		defer directPutTargetServer.Close()

		var (
			content = []byte("test bytes")
			req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
			w       = httptest.NewRecorder()
		)
		req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(directPutTargetServer.URL, url.PathEscape(directPutPath))}}

		svr.putHandler(w, req)

		resp := w.Result()
		defer resp.Body.Close()

		tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
		got := resp.Header.Get(apc.HdrDirectPutLength)
		tassert.Fatalf(t, got == strconv.Itoa(directPutLength), "expected Direct-Put-Length %d, got %q", directPutLength, got)
	})

	t.Run("200 forward preserves headers", func(t *testing.T) {
		nextStageContent := []byte("next stage content")
		nextStageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tassert.Fatalf(t, r.Method == http.MethodPut, "expected PUT method, got %s", r.Method)
			w.WriteHeader(http.StatusOK)
			w.Write(nextStageContent)
		}))
		defer nextStageServer.Close()

		var (
			content = []byte("test bytes")
			req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
			w       = httptest.NewRecorder()
		)
		req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(nextStageServer.URL, url.PathEscape(directPutPath))}}

		svr.putHandler(w, req)

		resp := w.Result()
		defer resp.Body.Close()
		result, _ := io.ReadAll(resp.Body)

		tassert.Fatalf(t, http.StatusOK == resp.StatusCode, "expected status code 200, got %d", resp.StatusCode)
		tassert.Fatalf(t, bytes.Equal(result, nextStageContent), "expected body %s, got %s", nextStageContent, result)
		tassert.Fatalf(t, resp.Header.Get(cos.HdrContentLength) == strconv.Itoa(len(nextStageContent)),
			"expected Content-Length %d, got %q", len(nextStageContent), resp.Header.Get(cos.HdrContentLength))
		tassert.Fatalf(t, resp.Header.Get(cos.HdrContentType) == GetContentType,
			"expected Content-Type %q, got %q", GetContentType, resp.Header.Get(cos.HdrContentType))
	})

	t.Run("200 chunked body not dropped", func(t *testing.T) {
		chunk1 := []byte("first chunk ")
		chunk2 := []byte("second chunk")
		nextStageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tassert.Fatalf(t, r.Method == http.MethodPut, "expected PUT method, got %s", r.Method)
			w.WriteHeader(http.StatusOK)
			w.Write(chunk1)
			w.(http.Flusher).Flush() // force chunked transfer so ContentLength == -1 on the client side
			w.Write(chunk2)
		}))
		defer nextStageServer.Close()

		var (
			content = []byte("test bytes")
			req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
			w       = httptest.NewRecorder()
		)
		req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(nextStageServer.URL, url.PathEscape(directPutPath))}}

		svr.putHandler(w, req)

		resp := w.Result()
		defer resp.Body.Close()
		result, _ := io.ReadAll(resp.Body)

		tassert.Fatalf(t, http.StatusOK == resp.StatusCode, "expected status code 200, got %d", resp.StatusCode)
		expected := append(append([]byte{}, chunk1...), chunk2...)
		tassert.Fatalf(t, bytes.Equal(result, expected), "expected body %s, got %s", expected, result)
	})

	t.Run("200 empty body maps to 204", func(t *testing.T) {
		nextStageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tassert.Fatalf(t, r.Method == http.MethodPut, "expected PUT method, got %s", r.Method)
			w.WriteHeader(http.StatusOK) // no body; client sees ContentLength == 0
		}))
		defer nextStageServer.Close()

		var (
			content = []byte("test bytes")
			req     = httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(content))
			w       = httptest.NewRecorder()
		)
		req.Header = http.Header{apc.HdrNodeURL: []string{cos.JoinPath(nextStageServer.URL, url.PathEscape(directPutPath))}}

		svr.putHandler(w, req)

		resp := w.Result()
		defer resp.Body.Close()

		tassert.Fatalf(t, http.StatusNoContent == resp.StatusCode, "expected status code 204, got %d", resp.StatusCode)
		got := resp.Header.Get(apc.HdrDirectPutLength)
		tassert.Fatalf(t, got == strconv.Itoa(len(content)), "expected Direct-Put-Length %d, got %q", len(content), got)
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

func TestWebSocketDirectPutChunkedForward(t *testing.T) {
	var (
		originalData = []byte("hello")
		directPutURL = "/ais/etl_dst/obj"

		secretPrefix = "/v1/_object/some_secret"
		host         = "http://0.0.0.0"
		port         = "8080"

		chunk1 = []byte("first chunk ")
		chunk2 = []byte("second chunk")
	)

	// Next pipeline stage: returns 200 with an unknown-length (chunked) body.
	nextStageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		tassert.Fatalf(t, bytes.Equal(data, originalData), "direct PUT got unexpected data")
		w.WriteHeader(http.StatusOK)
		w.Write(chunk1)
		w.(http.Flusher).Flush() // force chunked transfer so ContentLength == -1 on the client side
		w.Write(chunk2)
	}))
	defer nextStageServer.Close()

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

	u := "ws" + strings.TrimPrefix(wsSrv.URL, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(u, nil) //nolint:bodyclose // closed below
	tassert.Fatalf(t, err == nil, "WebSocket connection failed: %v", err)

	err = conn.WriteJSON(etl.WebsocketCtrlMsg{Pipeline: nextStageServer.URL + directPutURL})
	tassert.Fatalf(t, err == nil, "Write JSON failed")
	err = conn.WriteMessage(websocket.BinaryMessage, originalData)
	tassert.Fatalf(t, err == nil, "Write message failed")

	mt, msg, err := conn.ReadMessage()
	tassert.CheckError(t, err)
	err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "bye"))
	tassert.CheckError(t, err)
	tassert.CheckError(t, conn.Close())
	tassert.Fatalf(t, mt == websocket.BinaryMessage, "Expected BinaryMessage")
	expected := append(append([]byte{}, chunk1...), chunk2...)
	tassert.Fatalf(t, bytes.Equal(msg, expected), "expected content %s, got %s", expected, msg)
}

const midStreamTimeout = 5 * time.Second

var errMidStream = errors.New("mid-stream failure")

// net/http buffers the first 2 KiB of a response body: a failure within that buffer
// must fail the request itself, while a later one must leave the body visibly truncated
var midStreamFailures = []struct {
	name     string
	n        int  // bytes of output before the failure
	buffered bool // whether the failure happens before anything is sent
}{
	{"failAfter=100B", 100, true},
	{"failAfter=32KiB", 32 * cos.KiB, false},
}

// failingServer emits n bytes of transformed output and then fails
type failingServer struct {
	n    int
	size int64
}

func (s *failingServer) Transform(input io.ReadCloser, _, _ string) (io.ReadCloser, int64, error) {
	if _, err := io.Copy(io.Discard, input); err != nil {
		return nil, 0, err
	}
	r := io.MultiReader(bytes.NewReader(bytes.Repeat([]byte("x"), s.n)), iotest.ErrReader(errMidStream))
	return io.NopCloser(r), s.size, nil
}

// next pipeline stage whose 200 response is cut off after n bytes
func newAbortingStageServer(n int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		w.Write(bytes.Repeat([]byte("x"), n))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
}

// asserts that the client cannot mistake the failed response for a complete one;
// the handler must run behind a real server, since only net/http recovers http.ErrAbortHandler
func checkAborted(t *testing.T, req *http.Request, n int, buffered bool) {
	t.Helper()
	client := &http.Client{Timeout: midStreamTimeout}
	resp, err := client.Do(req)
	if buffered {
		if err == nil {
			resp.Body.Close()
		}
		tassert.Fatalf(t, errors.Is(err, io.EOF), "expected the request to fail with EOF, got %v", err)
		return
	}
	tassert.CheckFatal(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	tassert.Fatalf(t, http.StatusOK == resp.StatusCode, "expected status code 200, got %d", resp.StatusCode)
	tassert.Fatalf(t, errors.Is(err, io.ErrUnexpectedEOF), "expected a truncated body, got error %v", err)
	tassert.Fatalf(t, len(body) <= n, "expected at most %d bytes, got %d", n, len(body))
	tassert.Fatalf(t, !bytes.Contains(body, []byte(errMidStream.Error())), "expected no error in the response body")
}

func TestETLServerMidStreamTransformError(t *testing.T) {
	var (
		secretPrefix = "/v1/_object/some_secret"
		host         = "http://0.0.0.0"
		port         = "8080"
	)
	sizes := []struct {
		name string
		size int64
	}{
		{"size=unknown", cos.ContentLengthUnknown},
		{"size=known", 64 * cos.KiB}, // the short body alone already fails the client; checks nothing is appended
	}
	for _, s := range sizes {
		for _, f := range midStreamFailures {
			t.Run(s.name+"/"+f.name, func(t *testing.T) {
				svr := &etlServerBase{
					aisTargetURL: host + secretPrefix,
					endpoint:     host + ":" + port,
					client:       &http.Client{},
					ETLServer:    &failingServer{n: f.n, size: s.size},
				}
				etlServer := httptest.NewServer(http.HandlerFunc(svr.putHandler))
				defer etlServer.Close()

				req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, etlServer.URL, strings.NewReader("test bytes"))
				tassert.CheckFatal(t, err)
				checkAborted(t, req, f.n, f.buffered)
			})
		}
	}
}

func TestETLServerDirectPutMidStreamForwardError(t *testing.T) {
	var (
		secretPrefix = "/v1/_object/some_secret"
		host         = "http://0.0.0.0"
		port         = "8080"

		svr = &etlServerBase{
			aisTargetURL: host + secretPrefix,
			endpoint:     host + ":" + port,
			client:       &http.Client{Timeout: midStreamTimeout},
			ETLServer:    &EchoServer{},
		}
	)
	for _, f := range midStreamFailures {
		t.Run(f.name, func(t *testing.T) {
			nextStageServer := newAbortingStageServer(f.n)
			defer nextStageServer.Close()
			etlServer := httptest.NewServer(http.HandlerFunc(svr.putHandler))
			defer etlServer.Close()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, etlServer.URL, strings.NewReader("test bytes"))
			tassert.CheckFatal(t, err)
			req.Header.Set(apc.HdrNodeURL, nextStageServer.URL)
			checkAborted(t, req, f.n, f.buffered)
		})
	}
}

func TestETLServerDownloadMidStreamTransformError(t *testing.T) {
	var (
		secretPrefix = "/v1/_object/some_secret"
		host         = "http://0.0.0.0"
		port         = "8080"
	)
	remoteServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("remote object content"))
	}))
	defer remoteServer.Close()

	for _, f := range midStreamFailures {
		t.Run(f.name, func(t *testing.T) {
			svr := &etlServerBase{
				aisTargetURL: host + secretPrefix,
				endpoint:     host + ":" + port,
				client:       &http.Client{Timeout: midStreamTimeout},
				ETLServer:    &failingServer{n: f.n, size: cos.ContentLengthUnknown},
			}
			etlServer := httptest.NewServer(http.HandlerFunc(svr.downloadHandler))
			defer etlServer.Close()

			q := url.Values{apc.QparamObjTo: []string{"obj"}, apc.QparamOrigURL: []string{remoteServer.URL}}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, etlServer.URL+"?"+q.Encode(), http.NoBody)
			tassert.CheckFatal(t, err)
			checkAborted(t, req, f.n, f.buffered)
		})
	}
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
