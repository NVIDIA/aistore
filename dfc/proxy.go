/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

var (
	httpClient *http.Client
)

const (
	maxidleconnection int = 20 // maximum idle connections
	requesttimeout    int = 5  // http timeout in seconds (FIXME)
)

const (
	IP   = "ip"   // local IP address of the DFC instance
	PORT = "port" // expecting an integer > 1000
	ID   = "id"   // node ID must be unique
)

// createHTTPClient for connection re-use
func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxidleconnection,
		},
		Timeout: time.Duration(requesttimeout) * time.Second,
	}

	return client
}

// Proxyhdlr function serves request coming to listening port of DFC's Proxy Client.
// It supports GET and POST method only and return 405 error for non supported Methods.
func proxyhdlr(w http.ResponseWriter, r *http.Request) {
	stats := getproxystats()
	if glog.V(3) {
		glog.Infof("Proxy request from %s: %s %q", r.RemoteAddr, r.Method, r.URL)
	}
	switch r.Method {
	case "GET":
		atomic.AddInt64(&stats.numget, 1)
		if len(ctx.smap) < 1 {
			// TODO FIXME: No storage server is registered yet
			glog.Errorf("Storage server count %d proxy request from %s: %s %q",
				len(ctx.smap), r.RemoteAddr, r.Method, r.URL)
			http.Error(w, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)
			atomic.AddInt64(&stats.numerr, 1)
		} else {

			sid := doHashfindServer(html.EscapeString(r.URL.Path))
			if !ctx.config.Proxy.Passthru {
				err := proxyclientRequest(sid, w, r)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					atomic.AddInt64(&stats.numerr, 1)
				} else {
					// TODO FIXME: implement
					fmt.Fprintf(w, "DFC-Daemon %q", html.EscapeString(r.URL.Path))
				}
			} else { // passthrough
				if glog.V(3) {
					glog.Infof("Redirecting request %q", html.EscapeString(r.URL.Path))
				}
				storageurlurl := "http://" +
					ctx.smap[sid].ip + ":" +
					ctx.smap[sid].port +
					html.EscapeString(r.URL.Path)
				http.Redirect(w, r, storageurlurl, http.StatusMovedPermanently)
			}
		}
	case "POST":
		atomic.AddInt64(&stats.numpost, 1)
		// proxy server will get POST for storage registration only
		err := r.ParseForm()
		if err != nil {
			glog.Errorf("Failed to parse POST request, err: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			atomic.AddInt64(&stats.numerr, 1)
		}
		// parse
		var sinfo serverinfo
		for str, val := range r.Form {
			if str == IP {
				if glog.V(3) {
					glog.Infof("val : %s", strings.Join(val, ""))
				}
				sinfo.ip = strings.Join(val, "")
			}
			if str == PORT {
				if glog.V(3) {
					glog.Infof("val : %s", strings.Join(val, ""))
				}
				sinfo.port = strings.Join(val, "")
			}
			if str == ID {
				if glog.V(3) {
					glog.Infof("val : %s", strings.Join(val, ""))
				}
				sinfo.id = strings.Join(val, "")
			}
		}

		_, ok := ctx.smap[sinfo.id]
		assert(!ok)
		ctx.smap[sinfo.id] = sinfo

		if glog.V(3) {
			glog.Infof("IP %s port %s ID %s maplen %d",
				sinfo.ip, sinfo.port, sinfo.id, len(ctx.smap))
		}
		fmt.Fprintf(w, "DFC-Daemon %q", html.EscapeString(r.URL.Path))

	case "PUT":
	case "DELETE":
	default:
		errstr := fmt.Sprintf("Invalid proxy request from %s: %s %q", r.RemoteAddr, r.Method, r.URL)
		glog.Error(errstr)
		err := errors.New(errstr)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		atomic.AddInt64(&stats.numerr, 1)
	}
}

// It registers DFC's storage Server Instance with DFC's Proxy Client.
// A storage server uses ID, IP address and Port for registration with Proxy Client.
func registerwithproxy() (rerr error) {
	httpClient = createHTTPClient()
	//  well-known proxy address
	proxyURL := ctx.config.Proxy.URL
	resource := "/"
	data := url.Values{}
	ipaddr, err := getipaddr()
	if err != nil {
		return err
	}

	// Posting IP address, Port ID and ID as part of storage server registration.
	data.Set(IP, ipaddr)
	data.Add(PORT, string(ctx.config.Listen.Port))
	data.Add(ID, ctx.config.ID)

	u, _ := url.ParseRequestURI(string(proxyURL))
	u.Path = resource
	urlStr := u.String()
	if glog.V(3) {
		glog.Infof("URL %q", urlStr)
	}
	req, err := http.NewRequest("POST", urlStr, bytes.NewBufferString(data.Encode()))
	if err != nil {
		glog.Errorf("Error Occured. %+v", err)
		return err
	}
	req.Header.Add("Authorization", "auth_token=\"XXXXXXX\"")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	// Use httpClient to send request
	response, err := httpClient.Do(req)
	if err != nil && response == nil {
		glog.Errorf("Failed sending request to proxy, err: %+v", err)
		return err
	}
	// Close the connection to reuse it
	defer func() {
		err = response.Body.Close()
		if err != nil {
			rerr = err
		}
	}()

	// Let's check if the work actually is done
	// Did we get 200 OK response?
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		glog.Errorf("Couldn't parse response body: %+v", err)
		return err
	}

	if glog.V(3) {
		glog.Infof("Response body: %s", string(body))
	}
	return nil
}

// ProxyclientRequest submit a new http request to one of DFC storage server.
// Storage server ID is provided as one of argument to this call.
func proxyclientRequest(sid string, w http.ResponseWriter, r *http.Request) (rerr error) {
	if glog.V(3) {
		glog.Infof("Request path %s sid %s port %s",
			html.EscapeString(r.URL.Path), sid, ctx.smap[sid].port)
	}

	url := "http://" + ctx.smap[sid].ip + ":" + ctx.smap[sid].port + html.EscapeString(r.URL.Path)
	if glog.V(3) {
		glog.Infof("URL %q", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		glog.Errorf("Failed to get URL %q, err: %v", url, err)
		return err
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			rerr = err
		}
	}()

	_, err = io.Copy(w, resp.Body)
	if err != nil {
		glog.Errorf("Failed to copy data to http response, URL %q, err: %v",
			html.EscapeString(r.URL.Path), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		stats := getproxystats()
		atomic.AddInt64(&stats.numerr, 1)
	} else {
		if glog.V(3) {
			glog.Infof("Copied data, URL %q", html.EscapeString(r.URL.Path))
		}
	}
	return nil
}

//===========================================================================
//
// http runner
//
//===========================================================================
type glogwriter struct {
}

func (r *glogwriter) Write(p []byte) (int, error) {
	n := len(p)
	s := string(p[:n])
	glog.Errorln(s)
	return n, nil
}

type httprunner struct {
	namedrunner
	mux     *http.ServeMux
	h       *http.Server
	glogger *log.Logger
}

func (r *httprunner) runhandler(handler func(http.ResponseWriter, *http.Request)) error {
	r.mux = http.NewServeMux()
	r.mux.HandleFunc("/", handler)
	portstring := ":" + ctx.config.Listen.Port

	// a wrapper to glog http.Server errors - otherwise
	// os.Stderr would be used, as per golang.org/pkg/net/http/#Server
	r.glogger = log.New(&glogwriter{}, "net/http err: ", 0)

	r.h = &http.Server{Addr: portstring, Handler: r.mux, ErrorLog: r.glogger}
	if err := r.h.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			glog.Errorf("Terminated %s with err: %v", r.name, err)
			return err
		}
	}
	return nil
}

// stop gracefully
func (r *httprunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)

	contextwith, _ := context.WithTimeout(context.Background(), ctx.config.HttpTimeout)

	err = r.h.Shutdown(contextwith)
	if err != nil {
		glog.Infof("Stopped %s, err: %v", r.name, err)
	}
}

//===========================================================================
//
// proxy runner
//
//===========================================================================
type proxyrunner struct {
	httprunner
}

// start storage runner
func (r *proxyrunner) run() error {
	return r.runhandler(proxyhdlr)
}
