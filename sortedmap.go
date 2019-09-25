// Copyright (c) 2018-2019 Burak Sezer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*Package sorted provides SortedMap and SortedSet implementations and supports sorting by score.*/
package sorted

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrKeyNotFound means that given key could not be found in the data structure.
	ErrKeyNotFound = errors.New("key not found")

	// ErrInvalidRange means that given range is invalid to iterate: if fromKey is greater than toKey.
	ErrInvalidRange = errors.New("given range is invalid")
)

const DefaultMaxGarbageRatio = 0.40

// SortedMap is a map that provides a total ordering of its elements (elements can be traversed in sorted order of keys).
type SortedMap struct {
	mu sync.RWMutex

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
		maxGarbageRatio: maxGarbageRatio,
		ctx:             ctx,
		cancel:          cancel,
	}
	m.skiplists = append(m.skiplists, newSkipList())
	return m
}

// Close stops background tasks and quits.
func (m *SortedMap) Close() {
	select {
	case <-m.ctx.Done():
		// It's already closed.
		return
	default:
	}

	m.cancel()

	// Await for table merging processes gets closed.
	m.wg.Wait()
}

// Set sets a new key/value pair to the map.
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
		m.compacting = true
		ns := newSkipList()
		m.skiplists = append(m.skiplists, ns)
		m.wg.Add(1)
		go m.compaction()
	}
	return nil
}

// Delete deletes key/value pair from map. It returns nil if the key doesn't exist.
func (m *SortedMap) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.del(key)
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
func (m *SortedMap) Range(f func(key, value []byte) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Compaction may run at background. Wait until it's done.
	// if compaction is false, we have only one skiplist instance.
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			m.mu.RLock()
			// Wait for compaction.
			if m.compacting {
				m.mu.RUnlock()
				continue
			}
			s := m.skiplists[0]
			it := s.newIterator()
			for it.next() {
				if !f(it.key(), it.value()) {
					break
				}
			}
			m.mu.RUnlock()
			return
		case <-m.ctx.Done():
			return
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
func (m *SortedMap) SubMap(fromKey, toKey []byte, f func(key, value []byte) bool) error {
	if bytes.Compare(fromKey, toKey) > 0 {
		return ErrInvalidRange
	}
	submap := func() {
		s := m.skiplists[0]
		it := s.subMap(fromKey, toKey)
		for it.next() {
			if !f(it.key(), it.value()) {
				break
			}
		}
	}

	// Compaction may run at background. Wait until it's done.
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
			return nil
		case <-m.ctx.Done():
			return nil
		}
	}
}

// HeadMap returns a view of the portion of this map whose keys are strictly less than toKey.
func (m *SortedMap) HeadMap(toKey []byte, f func(key, value []byte) bool) error {
	return m.SubMap(nil, toKey, f)
}

// TailMap returns a view of the portion of this map whose keys are greater than or equal to fromKey.
func (m *SortedMap) TailMap(fromKey []byte, f func(key, value []byte) bool) error {
	return m.SubMap(fromKey, nil, f)
}
