// Package filter implements fully features dynamic probabilistic filter.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package filter

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

type (
	singleFilter struct {
		f    *cuckoo.Filter
		size uint
	}
)

func newSingleFilter(size uint) *singleFilter {
	sf := &singleFilter{
		f:    cuckoo.NewFilter(size),
		size: size,
	}
	return sf
}

// Filter is dynamic probabilistic filter which grows if there is more space
// needed.
//
// NOTE: Underneath it uses Cuckoo filters - Bloom filters could be also used
// but in the future we might need `Delete` method which Bloom filters cannot
// implement.
type Filter struct {
	mtx     sync.RWMutex
	filters []*singleFilter
}

func NewFilter(initSize uint) *Filter {
	filters := make([]*singleFilter, 1, 10)
	filters[0] = newSingleFilter(initSize)
	return &Filter{
		filters: filters,
	}
}

func NewDefaultFilter() *Filter {
	return NewFilter(filterInitSize)
}

func (f *Filter) Lookup(k []byte) bool {
	f.mtx.RLock()
	defer f.mtx.RUnlock()

	for idx := len(f.filters) - 1; idx >= 0; idx-- {
		if f.filters[idx].f.Lookup(k) {
			return true
		}
	}

	return false
}

func (f *Filter) Insert(k []byte) {
	f.mtx.Lock()
	lastFilter := f.filters[len(f.filters)-1]
	if !lastFilter.f.Insert(k) {
		sf := newSingleFilter(lastFilter.size * growFactor)
		f.filters = append(f.filters, sf)
		sf.f.Insert(k)
	}
	f.mtx.Unlock()
}

func (f *Filter) Delete(k []byte) {
	f.mtx.Lock()
	needCleanup := false
	for _, filter := range f.filters {
		filter.f.Delete(k)
		needCleanup = needCleanup || filter.f.Count() == 0
	}
	if needCleanup {
		resultFilters := f.filters[:0]
		for idx, filter := range f.filters {
			// idx == 0 because initial filter should be always included
			if idx == 0 || filter.f.Count() > 0 {
				resultFilters = append(resultFilters, filter)
			}
		}
		f.filters = resultFilters
	}
	f.mtx.Unlock()
}

func (f *Filter) Reset() {
	f.mtx.Lock()
	for idx := 1; idx < len(f.filters); idx++ {
		f.filters[idx] = nil
	}
	f.filters = f.filters[:1]
	f.filters[0].f.Reset()
	f.mtx.Unlock()
}
