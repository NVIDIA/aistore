// Package h2c implements HTTP/2 h2c.
//
// HTTP/2 h2c server implementation would not available in golang standard library
// because of the policy. This package provides h2c handy implementation.
package h2c

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"io"
	"log"
	"net"
	"net/http"
	"net/textproto"
	"strings"
	"unicode"
)

type Server struct {
	// Actual content handler
	http.Handler

	// Disable RFC 7540 3.4 behavior
	DisableDirect bool
}

// Converts byte slice of SETTINGS frame payload into http2.Setting slice.
func ParseSettings(b []byte) []http2.Setting {
	var r []http2.Setting
	for i := 0; i < len(b)/6; i++ {
		r = append(r, http2.Setting{
			ID:  http2.SettingID(binary.BigEndian.Uint16(b[i*6:])),
			Val: binary.BigEndian.Uint32(b[i*6+2:]),
		})
	}
	return r
}

var sensitiveHeaders []string = []string{
	"Cookie",
	"Set-Cookie",
	"Authorization",
}

// RFC7540 8.1.2.2
var connectionHeaders []string = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Connection",
	"Transfer-Encoding",
	"Upgrade",
	"HTTP2-Settings",
}

type InitRequest struct {
	Settings    []http2.Setting // HTTP2-Settings parameters
	HeaderBlock []byte          // http2 header block hpack-ed from HTTP/1.1 headers
}

// Prepares an http/2 request binaries from http.Request.
// Returns nil if upgrade failed.
func InitH2c(req *http.Request) *InitRequest {
	upgrade := false
	if vs, ok := req.Header[textproto.CanonicalMIMEHeaderKey("Upgrade")]; ok {
		sp := func(c rune) bool { return unicode.IsSpace(c) || c == ',' }
		// RFC 7230 3.2.2 comma-separated field value
		for _, v := range vs {
			for _, s := range strings.FieldsFunc(v, sp) {
				if s == "h2c" {
					upgrade = true
				}
			}
		}
	}
	if !upgrade {
		return nil
	}

	var settings []http2.Setting
	if vs, ok := req.Header[textproto.CanonicalMIMEHeaderKey("HTTP2-Settings")]; ok && len(vs) == 1 {
		if b, e := base64.RawURLEncoding.DecodeString(vs[0]); e != nil {
			return nil
		} else if len(b)%6 != 0 {
			return nil
		} else {
			settings = ParseSettings(b)
		}
	} else {
		return nil
	}

	buf := bytes.NewBuffer(nil)
	enc := hpack.NewEncoder(buf)
	for _, s := range settings {
		switch s.ID {
		case http2.SettingHeaderTableSize:
			enc.SetMaxDynamicTableSize(s.Val)
		}
	}

	wh := func(name, value string, sensitive bool) error {
		if len(value) == 0 {
			return nil
		}
		return enc.WriteField(hpack.HeaderField{
			Name:      strings.ToLower(name),
			Value:     value,
			Sensitive: sensitive,
		})
	}
	if err := wh(":method", req.Method, false); err != nil {
		return nil
	}
	host := req.URL.Host
	if len(host) == 0 {
		host = req.Host
	}
	if err := wh(":authority", host, false); err != nil {
		return nil
	}
	if req.Method != "CONNECT" {
		scheme := "http"
		if len(req.URL.Scheme) > 0 {
			scheme = req.URL.Scheme
		}
		if err := wh(":scheme", scheme, false); err != nil {
			return nil
		}
		if err := wh(":path", req.URL.Path, false); err != nil {
			return nil
		}
	}
	for k, vs := range req.Header {
		skip := false
		for _, h := range connectionHeaders {
			if k == textproto.CanonicalMIMEHeaderKey(h) {
				skip = true
			}
		}
		if skip {
			continue
		}

		sensitive := false
		for _, h := range sensitiveHeaders {
			if k == textproto.CanonicalMIMEHeaderKey(h) {
				sensitive = true
			}
		}
		for _, v := range vs {
			if err := wh(k, v, sensitive); err != nil {
				return nil
			}
		}
	}

	return &InitRequest{
		Settings:    settings,
		HeaderBlock: buf.Bytes(),
	}
}

type h2cInitReqBody struct {
	*http2.Framer
	*bytes.Buffer // Framer writes to this
	Body          io.Reader
	FrameSize     uint32
	buf           []byte
	streamEnd     bool
}

func (rd *h2cInitReqBody) Read(b []byte) (int, error) {
	if !rd.streamEnd && rd.Buffer.Len() < len(b) {
		if len(rd.buf) == 0 {
			rd.buf = make([]byte, rd.FrameSize)
		}
		n, err := rd.Body.Read(rd.buf)
		if n == 0 || err == io.EOF {
			rd.streamEnd = true
		}
		if err := rd.Framer.WriteData(1, rd.streamEnd, rd.buf[:n]); err != nil {
			return 0, err
		}
	}
	return rd.Buffer.Read(b)
}

type vacuumPreface struct {
	io.Reader
}

func (rd vacuumPreface) Read(b []byte) (int, error) {
	n, err := rd.Reader.Read([]byte(http2.ClientPreface))
	if n != len(http2.ClientPreface) {
		return n, io.ErrUnexpectedEOF
	}
	if err != nil && err != io.EOF {
		return n, err
	}
	return 0, io.EOF
}

type conn struct {
	net.Conn // embed for methods
	io.Reader
	*bufio.Writer
	vacuumAck bool
	buf       []byte
}

func (c conn) Read(b []byte) (int, error) {
	return c.Reader.Read(b)
}

func (c *conn) Write(b []byte) (int, error) {
	if c.vacuumAck {
		c.buf = append(c.buf, b...)
		for c.vacuumAck {
			if len(c.buf) < 9 {
				return len(b), nil // just buffered into c.buf
			}
			fh, err := http2.ReadFrameHeader(bytes.NewBuffer(c.buf))
			if err != nil {
				return 0, err // in case frame was broken
			} else if uint32(len(c.buf)) < 9+fh.Length {
				return len(b), nil // just buffered into c.buf
			}
			buf := c.buf[:9+fh.Length]
			c.buf = c.buf[9+fh.Length:]
			if http2.FrameSettings == fh.Type && fh.Flags.Has(http2.FlagSettingsAck) {
				c.vacuumAck = false
			} else if n, err := c.Writer.Write(buf); err != nil {
				return n, err
			}
		}
		n, err := c.Writer.Write(c.buf)
		c.Writer.Flush()
		return n, err
	}
	n, err := c.Writer.Write(b)
	c.Writer.Flush()
	return n, err
}

// ServeHTTP implements http.Handler interface for HTTP/2 h2c upgrade.
func (u Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if hijacker, ok := w.(http.Hijacker); ok {
		if !u.DisableDirect && r.Method == "PRI" && r.URL.Path == "*" && r.Proto == "HTTP/2.0" {
			body := "SM\r\n\r\n"
			con, rw, err := hijacker.Hijack()
			defer con.Close()
			if err != nil {
				log.Printf("Hijack failed %v", err)
			} else if n, err := io.MultiReader(r.Body, rw).Read([]byte(body)); n != len(body) {
				log.Printf("%d %v", n, err)
			} else {
				wrap := io.MultiReader(bytes.NewBuffer([]byte(http2.ClientPreface)), rw)
				nc := &conn{
					Conn:   con,
					Writer: rw.Writer,
					Reader: wrap,
				}
				h2c := &http2.Server{}
				h2c.ServeConn(nc, &http2.ServeConnOpts{Handler: u.Handler})
				return
			}
			http.Error(w, "Server could not handle the request.", http.StatusMethodNotAllowed)
			return
		}
		if initReq := InitH2c(r); initReq != nil {
			fsz := uint32(1 << 14) // RFC default
			for _, s := range initReq.Settings {
				if s.ID == http2.SettingMaxFrameSize && s.Val != 0 {
					fsz = s.Val
				}
			}
			onError := func(e error) {
				log.Print(e)
				http.Error(w, "Error in upgrading initial request", http.StatusInternalServerError)
			}

			h2req := bytes.NewBuffer([]byte(http2.ClientPreface))
			fr := http2.NewFramer(h2req, nil)

			if err := fr.WriteSettings(initReq.Settings...); err != nil {
				onError(err)
				return
			}

			hdr := initReq.HeaderBlock
			if uint32(len(hdr)) <= fsz {
				if err := fr.WriteHeaders(http2.HeadersFrameParam{
					StreamID:      1,
					BlockFragment: hdr,
					EndHeaders:    true,
				}); err != nil {
					onError(err)
					return
				}
			} else {
				if err := fr.WriteHeaders(http2.HeadersFrameParam{
					StreamID:      1,
					BlockFragment: hdr[:fsz],
				}); err != nil {
					onError(err)
					return
				}
				hdr = hdr[fsz:]
				for len(hdr) > 0 {
					if uint32(len(hdr)) > fsz {
						if err := fr.WriteContinuation(1, false, hdr[:fsz]); err != nil {
							onError(err)
							return
						}
						hdr = hdr[fsz:]
					} else {
						if err := fr.WriteContinuation(1, true, hdr); err != nil {
							onError(err)
							return
						}
						hdr = nil
					}
				}
			}

			con, rw, err := hijacker.Hijack()
			// Note: It seems rw is a wrapper for con.
			// r.Body.Read still looks working unless rw.Read call.
			defer con.Close()
			if err != nil {
				onError(err)
				rw.Flush()
				return
			}

			rw.Write([]byte(
				"HTTP/1.1 101 Switching Protocols\r\n" +
					"Connection: upgrade\r\n" +
					"Upgrade: h2c\r\n" +
					"\r\n"))

			h2req2 := &h2cInitReqBody{
				Framer:    fr,
				Buffer:    h2req,
				Body:      r.Body,
				FrameSize: fsz,
			}
			nc := &conn{
				Conn:      con,
				Writer:    rw.Writer,
				Reader:    io.MultiReader(h2req2, vacuumPreface{rw}, rw),
				vacuumAck: true, // because we sent HTTP2-Settings payload
			}
			h2c := &http2.Server{}
			for _, s := range initReq.Settings {
				switch s.ID {
				case http2.SettingMaxConcurrentStreams:
					h2c.MaxConcurrentStreams = s.Val
				case http2.SettingMaxFrameSize:
					h2c.MaxReadFrameSize = s.Val
				default:
					// just ignore
				}
			}
			h2c.ServeConn(nc, &http2.ServeConnOpts{Handler: u.Handler})
			return
		}
	}
	if u.Handler != nil {
		u.Handler.ServeHTTP(w, r)
	} else {
		http.DefaultServeMux.ServeHTTP(w, r)
	}
	return
}
