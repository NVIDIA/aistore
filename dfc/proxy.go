// CopyRight Notice: All rights reserved
//
//

package dfc

import (
	"bytes"
	"errors"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
)

var (
	httpClient *http.Client
)

const (

	// Maximum Idle connections allowed
	maxidleconnection int = 20

	// Timeout in seconds for HTTP request
	requesttimeout int = 5
)

const (

	// IP refers to Local IP address of DFC instance.
	IP = "ip"

	// PORT is specified as positive integer, it
	PORT = "port"

	// ID uniquely identifies a Node in DFC cluster.
	// It is specified as type string.
	// User can specifiy ID through config file or DFC node can auto
	// generate based on MAC ID.
	ID = "id"
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

	if glog.V(3) {
		glog.Infof("Proxy Request from %s: %s %q \n", r.RemoteAddr, r.Method, r.URL)
	}
	switch r.Method {
	case "GET":
		// Serve the resource.
		// TODO Give proper error if no server is registered and client is requesting data
		// or may be directly get from S3??
		if len(ctx.smap) < 1 {
			// No storage server is registered yet
			glog.Errorf("Storage Server count = %d  Proxy Request from %s: %s %q \n",
				len(ctx.smap), r.RemoteAddr, r.Method, r.URL)
			http.Error(w, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)

		} else {

			sid := doHashfindServer(html.EscapeString(r.URL.Path))
			if !ctx.config.Proxy.Passthru {
				err := proxyclientRequest(sid, w, r)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				} else {
					// TODO HTTP redirect
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
		//Proxy server will get POST for  Storage server registration only
		err := r.ParseForm()
		if err != nil {
			glog.Errorf("Failed to Parse Post Value err = %v \n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		if glog.V(2) {
			glog.Infof("request content %s  \n", r.Form)
		}
		var sinfo serverinfo
		// Parse POST values
		for str, val := range r.Form {
			if str == IP {
				if glog.V(3) {
					glog.Infof("val : %s \n", strings.Join(val, ""))
				}
				sinfo.ip = strings.Join(val, "")
			}
			if str == PORT {
				if glog.V(3) {
					glog.Infof("val : %s \n", strings.Join(val, ""))
				}
				sinfo.port = strings.Join(val, "")
			}
			if str == ID {
				if glog.V(3) {
					glog.Infof("val : %s \n", strings.Join(val, ""))
				}
				sinfo.id = strings.Join(val, "")
			}

		}

		// Insert into Map based on ID and fail if duplicates.
		// TODO Fail if there already client registered with same ID
		ctx.smap[sinfo.id] = sinfo
		if glog.V(3) {
			glog.Infof(" IP = %s Port = %s  Id = %s Curlen of map = %d \n",
				sinfo.ip, sinfo.port, sinfo.id, len(ctx.smap))
		}
		fmt.Fprintf(w, "DFC-Daemon %q", html.EscapeString(r.URL.Path))

	case "PUT":
	case "DELETE":
	default:
		errstr := fmt.Sprintf("Invalid Proxy Request from %s: %s %q \n", r.RemoteAddr, r.Method, r.URL)
		glog.Error(errstr)
		err := errors.New(errstr)
		http.Error(w, err.Error(), http.StatusInternalServerError)

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
		glog.Infof("Proxy URL : %s \n ", urlStr)
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
		glog.Errorf("Error sending request to Proxy server %+v \n", err)
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
		glog.Errorf("Couldn't parse response body. %+v \n", err)
		return err
	}

	if glog.V(3) {
		glog.Infof("Response Body: %s \n", string(body))
	}
	return nil
}

// ProxyclientRequest submit a new http request to one of DFC storage server.
// Storage server ID is provided as one of argument to this call.
func proxyclientRequest(sid string, w http.ResponseWriter, r *http.Request) (rerr error) {
	if glog.V(3) {
		glog.Infof(" Request path = %s Sid = %s Port = %s \n",
			html.EscapeString(r.URL.Path), sid, ctx.smap[sid].port)
	}

	url := "http://" + ctx.smap[sid].ip + ":" + ctx.smap[sid].port + html.EscapeString(r.URL.Path)
	if glog.V(3) {
		glog.Infof(" URL = %s \n", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		glog.Errorf("Failed to get url = %s err = %q", url, err)
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
		glog.Errorf("Failed to Copy data to http response for URL rq %s, err: %v \n",
			html.EscapeString(r.URL.Path), err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		if glog.V(3) {
			glog.Infof("Succefully copied data from Body to Response for rq %s \n",
				html.EscapeString(r.URL.Path))
		}
	}
	return nil
}
