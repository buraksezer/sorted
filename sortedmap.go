// Copyright (c) 2018 Burak Sezer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*Package gsorted provides SortedMap and SortedSet implementations and supports sorting by score.*/
package gsorted

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"time"
)

// ErrKeyNotFound means that given key could not be found in the data structure.
var ErrKeyNotFound = errors.New("key not found")

const DefaultMaxGarbageRatio = 0.40

type fnv64a struct{}

func (f fnv64a) Sum64(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

// SortedMap is a map that provides a total ordering of its elements (elements can be traversed in sorted order of keys).
type SortedMap struct {
	mu sync.RWMutex

	hash            fnv64a
	skiplists       []*skipList
	maxGarbageRatio float64
	compacting      bool
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewSortedMap creates and returns a new SortedMap with given parameters.
func NewSortedMap(maxGarbageRatio float64) *SortedMap {
	if maxGarbageRatio == 0 {
		maxGarbageRatio = DefaultMaxGarbageRatio
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &SortedMap{
		hash:            fnv64a{},
		maxGarbageRatio: maxGarbageRatio,
		ctx:             ctx,
		cancel:          cancel,
	}
	m.skiplists = append(m.skiplists, newSkipList())
	return m
}

func (m *SortedMap) Close() error {
	select {
	case <-m.ctx.Done():
		// It's already closed.
		return nil
	default:
	}

	m.cancel()

	// Await for table merging processes gets closed.
	m.wg.Wait()
	return nil
}

func (m *SortedMap) Set(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the last value, map only calls Put on the last created table.
	s := m.skiplists[len(m.skiplists)-1]
	return s.set(key, value)
}

func (m *SortedMap) get(key []byte) ([]byte, error) {
	// Scan available tables by starting the last added table.
	for i := len(m.skiplists) - 1; i >= 0; i-- {
		s := m.skiplists[i]
		val, err := s.get(key)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		return val, nil
	}
	// Nothing here.
	return nil, ErrKeyNotFound
}

// Get returns the value for given key, if any.
func (m *SortedMap) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.get(key)
}

func (m *SortedMap) del(key []byte) error {
	// Scan available tables by starting the last added table.
	for i := len(m.skiplists) - 1; i >= 0; i-- {
		s := m.skiplists[i]
		err := s.delete(key)
		if err == ErrKeyNotFound {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return err
		}
		break
	}

	s := m.skiplists[0]
	if float64(len(s.kvData))*m.maxGarbageRatio <= float64(s.garbage) {
		if m.compacting {
			return nil
		}
		ns := newSkipList()
		m.skiplists = append(m.skiplists, ns)
		m.compacting = true
		m.wg.Add(1)
		go m.compaction()
	}
	return nil
}

// Delete deletes the value for given key. It returns nil if the key doesn't exist.
func (m *SortedMap) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.del(key)
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration. Range may be O(N) with
// the number of elements in the map even if f returns false after a constant
// number of calls.
func (m *SortedMap) Range(fn func(key, value []byte) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Scan available tables by starting the last added table.
	for i := len(m.skiplists) - 1; i >= 0; i-- {
		// FIXME: Range should work with the first skiplist, only.
		s := m.skiplists[i]
		it := s.newIterator()
		for it.next() {
			if !fn(it.key(), it.value()) {
				break
			}
		}
	}
}

// Len returns the key count in this map.
func (m *SortedMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total int
	for _, s := range m.skiplists {
		total += s.len()
	}
	return total
}

// Check checks the given key in SortedMap without reading its value.
func (m *SortedMap) Check(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.skiplists {
		if s.check(key) {
			return true
		}
	}
	// Nothing there.
	return false
}

// SubMap returns a view of the portion of this map whose keys range from fromKey, inclusive, to toKey, exclusive.
func (m *SortedMap) SubMap(fromKey, toKey []byte, f func(key, value []byte) bool) {
	// TODO: Check keys here.
	submap := func() {
		s := m.skiplists[0]
		it := s.subMap(fromKey, toKey)
		for it.next() {
			if !f(it.key(), it.value()) {
				break
			}
		}
	}

	for {
		select {
		case <-time.After(10 * time.Millisecond):
			m.mu.RLock()
			// Wait for compaction.
			if m.compacting {
				m.mu.RUnlock()
				continue
			}
			// Run actual SubMap code here.
			submap()
			m.mu.RUnlock()
			return
		case <-m.ctx.Done():
			return
		}
	}
}

// HeadMap returns a view of the portion of this map whose keys are strictly less than toKey.
func (m *SortedMap) HeadMap(toKey []byte, f func(key, value []byte) bool) {
	m.SubMap(nil, toKey, f)
}

// TailMap returns a view of the portion of this map whose keys are greater than or equal to fromKey.
func (m *SortedMap) TailMap(fromKey []byte, f func(key, value []byte) bool) {
	m.SubMap(fromKey, nil, f)
}
