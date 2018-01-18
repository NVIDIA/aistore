package dfc

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

type tioxInterface interface {
	getid() int64
	getkind() string
}

type tioxRebalance struct {
	id         int64
	stime      time.Time
	etime      time.Time
	kind       string
	curversion int64
}

func (tiox *tioxRebalance) getid() int64 {
	return tiox.id
}

func (tiox *tioxRebalance) getkind() string {
	return tiox.kind
}

type tioxInProgress struct {
	tioxes []tioxInterface
	lock   *sync.Mutex
}

func newtioxes() *tioxInProgress {
	q := make([]tioxInterface, 4)
	qq := &tioxInProgress{tioxes: q[0:0]}
	qq.lock = &sync.Mutex{}
	return qq
}

func (q *tioxInProgress) add(tiox tioxInterface) {
	q.lock.Lock()
	defer q.lock.Unlock()
	l := len(q.tioxes)
	q.tioxes = append(q.tioxes, nil)
	q.tioxes[l] = tiox
}

func (q *tioxInProgress) find(by interface{}) (idx int, tiox tioxInterface) {
	q.lock.Lock()
	defer q.lock.Unlock()
	var id int64
	var kind string
	switch by.(type) {
	case int64:
		id = by.(int64)
	case string:
		kind = by.(string)
	default:
		assert(false, fmt.Sprintf("unexpected find param: %#v", by))
	}
	for i, tiox := range q.tioxes {
		if id != 0 && tiox.getid() == id {
			return i, tiox
		}
		if kind != "" && tiox.getkind() == kind {
			return i, tiox
		}
	}
	return -1, nil
}

func (q *tioxInProgress) del(by interface{}) {
	q.lock.Lock()
	defer q.lock.Unlock()
	k, tiox := q.find(by)
	if tiox == nil {
		glog.Errorf("Failed to find tiox by %#v", by)
		return
	}
	l := len(q.tioxes)
	if k < l-1 {
		copy(q.tioxes[k:], q.tioxes[k+1:])
	}
	q.tioxes[l-1] = nil
	q.tioxes = q.tioxes[:l-1]
}
