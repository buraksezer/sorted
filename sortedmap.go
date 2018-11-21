/*Package gsorted provides a single node, in-memory and off-heap key/value store for Go. Suitable for caching.*/
package gsorted

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

var ErrKeyNotFound = errors.New("key not found")

const DefaultMaxGarbageRatio = 0.40

type fnv64a struct{}

func (f fnv64a) Sum64(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

type SortedMap struct {
	mu sync.RWMutex

	hash            fnv64a
	skiplists       []*skipList
	maxGarbageRatio float64
	compacting      int32
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

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

	m.skiplists = nil
	return nil
}

func (m *SortedMap) Set(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get the last value, storage only calls Put on the last created table.
	s := m.skiplists[len(m.skiplists)-1]
	return s.Set(key, value)
}

func (m *SortedMap) get(key []byte) ([]byte, error) {
	// Scan available tables by starting the last added table.
	for i := len(m.skiplists) - 1; i >= 0; i-- {
		s := m.skiplists[i]
		val, err := s.Get(key)
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

func (m *SortedMap) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.get(key)
}

func (m *SortedMap) delete(key []byte) error {
	// Scan available tables by starting the last added table.
	for i := len(m.skiplists) - 1; i >= 0; i-- {
		s := m.skiplists[i]
		err := s.Delete(key)
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
		if atomic.LoadInt32(&m.compacting) == 1 {
			return nil
		}
		ns := newSkipList()
		m.skiplists = append(m.skiplists, ns)
		m.wg.Add(1)
		atomic.StoreInt32(&m.compacting, 1)
		go m.compaction()
	}
	return nil
}

// Delete deletes the value for the given key. Delete will not returns error if key doesn't exist.
func (m *SortedMap) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.delete(key)
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
		s := m.skiplists[i]
		it := s.NewIterator()
		for it.Next() {
			if !fn(it.Key(), it.Value()) {
				break
			}
		}
	}
}

// Len returns the key cound in this storage.
func (m *SortedMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total int
	for _, s := range m.skiplists {
		total += s.Len()
	}
	return total
}

// Check checks the key existence.
func (m *SortedMap) Check(key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, s := range m.skiplists {
		if s.Check(key) {
			return true
		}
	}
	// Nothing there.
	return false
}
