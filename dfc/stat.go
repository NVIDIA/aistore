package dfc

import (
	"fmt"
	"github.com/golang/glog"
)

// DFC stats
type dfcstats struct {
	numget       int64
	numnotcached int64
	bytesloaded  int64
	bytesevicted int64
	filesevicted int64
}

var stats = &dfcstats{}

func (this *dfcstats) log() {
	s := fmt.Sprintf("stats: %+v", this)
	glog.Infoln(s)
}
