// Webdav server for DFC
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

// Limitations:
// 1. Support local bucket only (not hard to support non local buckets)
// 2. Can't support empty directory
// 3. ATime: format, how do i know the time format returned (RFC822 or format string)
// 4. Objects for read and write are downloaded locally first.
// 5. Not all O_Flags are supported, for example, O_APPEND.
// 6. Permissions may not be correct.
// 7. Performance improvement

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"golang.org/x/net/webdav"
)

const (
	logLevelNone = iota
	logLevelWebDAV
	logLevelDFC
)

var (
	logLevel int
)

func main() {
	var (
		port   int    // webdav port used by this server
		proxy  string // proxy in form of ip:port
		tmpDir string // stores files that are used by webdav for get or put
	)

	flag.IntVar(&port, "port", 8079, "this server's port")
	flag.StringVar(&proxy, "proxyurl", "127.0.0.1:8080", "dfc proxy's url (ip:port)")
	flag.StringVar(&tmpDir, "tmpdir", "/tmp/dfc", "temporary directory to store files used by webdav")
	flag.IntVar(&logLevel, "loglevel", logLevelNone, "verbose level(0 = none, 1 = webdav, 2 = dfc)")
	flag.Parse()
	url := url.URL{Scheme: "http", Host: proxy}

	h := &webdav.Handler{
		FileSystem: NewFS(url, tmpDir),
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			switch r.Method {
			case "COPY", "MOVE":
				dst := ""
				if u, err := url.Parse(r.Header.Get("Destination")); err == nil {
					dst = u.Path
				}

				o := r.Header.Get("Overwrite")
				webdavLog(logLevelWebDAV, "%-15s: %-70s %-30s o=%-2s err = %v", r.Method, r.URL.Path, dst, o, err)

			default:
				webdavLog(logLevelWebDAV, "%-15s: %-70s err = %v", r.Method, r.URL.Path, err)
			}
		},
	}

	http.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
	}))

	webdavLog(logLevelNone, "DFC WebDAV server started, listening on %d, DFC = %s\n", port, url.String())
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func webdavLog(level int, format string, v ...interface{}) {
	if level <= logLevel {
		log.Printf(format, v...)
	}
}
