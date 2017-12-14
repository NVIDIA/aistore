package dfc

import (
	"flag"
	"fmt"
	"os"

	"github.com/golang/glog"
)

const (
	rolesetrver = "server"
	roleproxy   = "proxy"
)

//====================
//
// global
//
//====================
var ctx = &daemon{}
var grp = &rungroup{}

//====================
//
// types
//
//====================
// DFC instance: proxy or storage server
type daemon struct {
	smap    map[string]serverinfo // registered storage servers (proxy only)
	config  dfconfig              // Configuration
	mntpath []MountPoint          // List of usable mountpoints on storage server
	isproxy bool                  // DFC can run as a proxy (true) or a Server (false)
	httprun *httprunner
}

// registration info
type serverinfo struct {
	port    string       // http listening port
	ip      string       // FIXME: always picking the first IP address as the server's IP
	id      string       // macaddr (as a unique ID)
	mntpath []MountPoint // mount points FIXME: lowercase
}

// (any) runner
type runner interface {
	run() error
	stop(error)
}

// rungroup
type rungroup struct {
	runners []runner
	errch   chan error
	idxch   chan int
	stpch   chan error
}

type gstopError struct {
}

//====================
//
// rungroup
//
//====================
func (g *rungroup) add(r runner) {
	g.runners = append(g.runners, r)
}

func (g *rungroup) run() error {
	if len(g.runners) == 0 {
		return nil
	}
	g.errch = make(chan error, len(g.runners))
	g.idxch = make(chan int, len(g.runners))
	g.stpch = make(chan error, 1)
	for i, r := range g.runners {
		go func(i int, r runner) {
			g.errch <- r.run()
			g.idxch <- i
		}(i, r)
	}
	// wait here
	err := <-g.errch
	idx := <-g.idxch

	for _, r := range g.runners {
		r.stop(err)
	}
	glog.Flush()
	for i := 0; i < cap(g.errch); i++ {
		if i == idx {
			continue
		}
		<-g.errch
		glog.Flush()
	}
	g.stpch <- nil
	return err
}

func (g *rungroup) stop() {
	g.errch <- &gstopError{}
	g.idxch <- -1
	<-g.stpch
}

func (ge *gstopError) Error() string {
	return "rungroup stop"
}

//====================
//
// daemon init & run
//
//====================
func dfcinit() {
	// CLI to override dfc JSON config
	var (
		role     string
		conffile string
		loglevel string
	)
	flag.StringVar(&role, "role", "", "role: proxy OR server")
	flag.StringVar(&conffile, "configfile", "", "config filename")
	flag.StringVar(&loglevel, "loglevel", "", "glog loglevel")

	flag.Parse()
	if conffile == "" {
		fmt.Fprintf(os.Stderr, "Usage: go run dfc role=<proxy|server> configfile=<somefile.json>\n")
		os.Exit(2)
	}
	if role != roleproxy && role != rolesetrver {
		fmt.Fprintf(os.Stderr, "Invalid role %q\n", role)
		fmt.Fprintf(os.Stderr, "Usage: go run dfc role=<proxy|server> configfile=<somefile.json>\n")
		os.Exit(2)
	}
	err := initconfigparam(conffile, loglevel, role)
	if err != nil {
		// exit and dump stacktrace
		glog.Fatalf("Failed to initialize, config %q, err: %v", conffile, err)
	}
	if role == roleproxy {
		ctx.isproxy = true
		ctx.smap = make(map[string]serverinfo)
	}
	// add runners
	grp.add(&httprunner{})
	grp.add(&sigrunner{})
}

// main
func Run() {
	dfcinit()
	var ok bool

	err := grp.run()
	if err == nil {
		goto m
	}
	_, ok = err.(*signalError)
	if ok {
		goto m
	}
	// dump stack trace and exit
	glog.Fatalf("============== Terminated with err: %v\n", err)
m:
	glog.Infoln("============== Terminated OK")
	glog.Flush()
}
