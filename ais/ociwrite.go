// Package ais provides AIStore's proxy and target nodes.
// no-copyright
package ais

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
)

// Only the small in-flight set is protected by mu. It never spans backend I/O.
// If commits overlap, their response order cannot establish the backend order:
// discard cached data rather than publish a potentially older remote version.
type (
	ociWriteSet struct {
		mu      sync.Mutex
		pending map[string]*ociWriteState
	}
	ociWriteState struct {
		name       string
		active     int
		overlapped bool
	}
)

func (set *ociWriteSet) begin(name string) *ociWriteState {
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.pending == nil {
		set.pending = make(map[string]*ociWriteState)
	}
	state := set.pending[name]
	if state == nil {
		state = &ociWriteState{name: name}
		set.pending[name] = state
	} else {
		state.overlapped = true
	}
	state.active++
	return state
}

func (set *ociWriteSet) end(name string, state *ociWriteState) {
	set.mu.Lock()
	state.active--
	if state.active == 0 {
		delete(set.pending, name)
	}
	set.mu.Unlock()
}

// finish the write cohort even on an error. Publication itself also invalidates
// under its object lock, so an older local version is never visible in between.
func (set *ociWriteSet) finish(lom *core.LOM, state *ociWriteState, locked bool) error {
	defer set.end(state.name, state)
	_, err := set.invalidate(lom, state, locked)
	return err
}

func (set *ociWriteSet) invalidate(lom *core.LOM, state *ociWriteState, locked bool) (bool, error) {
	set.mu.Lock()
	overlapped := state.overlapped
	set.mu.Unlock()
	if !overlapped {
		return false, nil
	}
	if !locked {
		lom.Lock(true)
		defer lom.Unlock(true)
	}
	fresh := lom.Clone()
	defer core.FreeLOM(fresh)
	fresh.Uncache()
	err := fresh.Load(false /*cache it*/, true /*locked*/)
	if cmn.IsErrObjNought(err) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return true, fresh.RemoveObj()
}
