/*
 * ServeMux was forked from golang.org http package to add
 * missing functions written below (targeting unregistration of handler).
 *
 * Also see: https://stackoverflow.com/questions/11738029/how-do-i-unregister-a-handler-in-net-http/11743786#11743786
 */

package mux

import "sort"

// Unhandle unregisters the handler for the given pattern.
// Returns boolean indicating if the handler for the pattern existed.
func (mux *ServeMux) Unhandle(pattern string) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if pattern == "" {
		panic("http: invalid pattern")
	}
	delete(mux.m, pattern)
	if pattern[len(pattern)-1] == '/' {
		mux.es = removeSorted(mux.es, pattern)
	}
}

func removeSorted(es []muxEntry, pattern string) []muxEntry {
	n := len(es)
	if n == 0 {
		return es
	}
	i := sort.Search(n, func(i int) bool {
		return len(es[i].pattern) < len(pattern)
	})
	if i == n { // not found
		return es
	}
	copy(es[i:], es[i+1:])
	return es[:n-1]
}
