/*
 * ServeMux was forked from golang.org http package to add
 * missing functions written below (targeting unregistration of handler).
 *
 * Also see: https://stackoverflow.com/questions/11738029/how-do-i-unregister-a-handler-in-net-http/11743786#11743786
 */

package mux

// Unhandle unregisters the handler for the given pattern.
// Returns boolean indicating if the handler for the pattern existed.
func (mux *ServeMux) Unhandle(pattern string) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if pattern == "" {
		panic("http: invalid pattern")
	}
	delete(mux.m, pattern)
}

// UnhandleFunc unregisters the handler function for the given pattern.
func (mux *ServeMux) UnhandleFunc(pattern string) {
	mux.Unhandle(pattern)
}

// UnhandleFunc unregisters the handler function for the given pattern in the DefaultServeMux.
func UnhandleFunc(pattern string) {
	DefaultServeMux.UnhandleFunc(pattern)
}
