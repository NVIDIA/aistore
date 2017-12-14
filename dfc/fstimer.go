package dfc

import (
	"time"

	"github.com/golang/glog"
)

func fsCheckTimer(quit chan bool) {
	wttime := ctx.config.Cache.FSCheckfreq
	freq := time.Duration(wttime * 60)
	glog.Info("Entering fsChecktimer")
	ticker := time.NewTicker(freq * time.Second)
	for {
		select {
		case <-ticker.C:
			checkfs()
		case <-quit:
			glog.Infof("Exiting fsChecktimer")
			ticker.Stop()
			return
		}
	}
}
