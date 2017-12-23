/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"os"
	"time"

	"github.com/golang/glog"
)

type storstats struct {
	numget       int64
	numnotcached int64
	bytesloaded  int64
	bytesevicted int64
	filesevicted int64
}

type statslogger interface {
	log()
}

type proxystats struct {
	numget int64
	// TODO
}

type statsrunner struct {
	namedrunner
	statslogger
	chsts chan os.Signal
}

type proxystatsrunner struct {
	statsrunner
	stats proxystats
}

type storstatsrunner struct {
	statsrunner
	stats storstats
}

func (r *statsrunner) runcommon(logger statslogger) error {
	r.chsts = make(chan os.Signal, 1)

	glog.Infof("Starting %s", r.name)
	// ticker := time.NewTicker(ctx.config.Cache.StatsIval) // TODO - config
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			logger.log()
		case <-r.chsts:
			ticker.Stop()
			return nil
		}
	}
}

func (r *statsrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	close(r.chsts)
}

func (r *statsrunner) log() {
	assert(false)
}

func (r *proxystatsrunner) run() error {
	return r.runcommon(r)
}

func (r *proxystatsrunner) log() {
	s := fmt.Sprintf("%s: %+v", r.name, r.stats)
	glog.Infoln(s)
}

func (r *storstatsrunner) run() error {
	return r.runcommon(r)
}

func (r *storstatsrunner) log() {
	s := fmt.Sprintf("%s: %+v", r.name, r.stats)
	glog.Infoln(s)
}
