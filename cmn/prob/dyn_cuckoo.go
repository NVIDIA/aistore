// Package prob implements fully features dynamic probabilistic filter.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package prob

import (
	"sync"

	cuckoo "github.com/seiflotfy/cuckoofilter"
)

const (
	// filterInitSize determines the number of keys we can keep in first filter.
	// For now we allow `10M` keys -> memory allocation of `10MB`. If we exceed
	// that number of keys, additional filter is added dynamically of size:
	// `last_filter_size` * `grow_factor`.
	filterInitSize = 10 * 1000 * 1000
	growFactor     = 3 // how much size of next, new filter will grow comparing to previous filter
)

// Filter is dynamic probabilistic filter which grows if there is more space
// needed.
//
// NOTE: Underneath it uses Cuckoo filters - Bloom filters could be also used
// but in the future we might need `Delete` method which Bloom filters cannot
// implement.
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
	f.filters = f.filters[:0]
	f.mtx.Unlock()
}
