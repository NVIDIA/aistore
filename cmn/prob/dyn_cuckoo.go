// Package prob implements fully features dynamic probabilistic filter.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package prob

import (
	"sync"

	cuckoo "github.com/seiflotfy/cuckoofilter"
)

//
// Dynamic Probabilistic Filter
//

const (
	// size of the first (unconditional) filter: 128K entries
	filterInitSize = 128 * 1024

	// extra filters, if any required, currently will have a constant
	// (filterInitSize * growFactor) size;
	// TODO consider geometric growth, as in: next-size = (1 + ratio) * previous-size
	growFactor = 4
)

type Filter struct {
	filters []*cuckoo.Filter
	size    uint
	mtx     sync.RWMutex
}

func newFilter(initSize uint) *Filter {
	return &Filter{
		filters: make([]*cuckoo.Filter, 0, 5),
		size:    initSize,
	}
}

func NewDefaultFilter() *Filter {
	return newFilter(filterInitSize)
}

func (f *Filter) Lookup(k []byte) bool {
	f.mtx.RLock()
	for idx := len(f.filters) - 1; idx >= 0; idx-- {
		if f.filters[idx].Lookup(k) {
			f.mtx.RUnlock()
			return true
		}
	}
	f.mtx.RUnlock()
	return false
}

func (f *Filter) Insert(k []byte) {
	f.mtx.Lock()

	var lastFilter *cuckoo.Filter
	if len(f.filters) == 0 {
		lastFilter = cuckoo.NewFilter(f.size)
		f.filters = append(f.filters, lastFilter)
	} else {
		lastFilter = f.filters[len(f.filters)-1]
	}

	if !lastFilter.Insert(k) {
		sf := cuckoo.NewFilter(f.size * growFactor)
		f.filters = append(f.filters, sf)
		sf.Insert(k)
	}
	f.mtx.Unlock()
}

func (f *Filter) Delete(k []byte) {
	f.mtx.Lock()
	needCleanup := false
	for _, filter := range f.filters {
		filter.Delete(k)
		needCleanup = needCleanup || filter.Count() == 0
	}
	if needCleanup {
		resultFilters := f.filters[:0]
		for idx, filter := range f.filters {
			// idx == 0 because initial filter should be always included
			if idx == 0 || filter.Count() > 0 {
				resultFilters = append(resultFilters, filter)
			}
		}
		f.filters = resultFilters
	}
	f.mtx.Unlock()
}

func (f *Filter) Reset() {
	f.mtx.Lock()
	for idx := range len(f.filters) {
		f.filters[idx].Reset()
	}
	clear(f.filters)
	f.filters = nil // gc
	f.mtx.Unlock()
}
