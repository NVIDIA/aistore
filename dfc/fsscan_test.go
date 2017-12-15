//
// how to run:
// # go test -v -run=fss -args -lwm 10 -hwm 20 -dir /tmp/eviction
// The specified directory must be first populated with nested dirs and stuff..
//
package dfc

import (
	"flag"
	"sync"
	"testing"

	"github.com/golang/glog"
)

var lwm, hwm int
var dir string

func init() {
	flag.IntVar(&lwm, "lwm", 90, "low watermark")
	flag.IntVar(&hwm, "hwm", 99, "high watermark")
	flag.StringVar(&dir, "dir", "/tmp/eviction", "directory to evict")

	flag.Lookup("log_dir").Value.Set("/tmp")
	flag.Lookup("v").Value.Set("4")

	flag.Parse()
}

// e.g.: go test -v -run=fss -args -lwm 10 -hwm 20 -dir /tmp/eviction
func Test_fsscan(t *testing.T) {
	glog.Infoln("Test_fsscan: ", "lwm", lwm, "hwm", hwm, "dir", dir)

	ctx.config.Cache.FSHighWaterMark = uint32(hwm)
	ctx.config.Cache.FSLowWaterMark = uint32(lwm)

	fschkwg := &sync.WaitGroup{}
	fschkwg.Add(1)
	fsscan(dir, fschkwg)
	glog.Flush()
}
