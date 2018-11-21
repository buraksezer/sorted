package gsorted

import (
	"log"
	"sync/atomic"
	"time"
)

func (o *SortedMap) compaction() {
	defer o.wg.Done()
	defer atomic.StoreInt32(&o.compacting, 0)

	// Run immediately. The ticker will trigger that function
	// every 100 milliseconds to prevent blocking the storage instance.
	if done := o.compactSkipLists(); done {
		// Fragmented skiplists are compactd. Quit.
		return
	}
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if done := o.compactSkipLists(); done {
				// Fragmented skiplists are compactd. Quit.
				return
			}
		case <-o.ctx.Done():
			return
		}
	}
}

func (m *SortedMap) compactSkipLists() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.skiplists) == 1 {
		return true
	}

	var total int
	fresh := m.skiplists[len(m.skiplists)-1]
	for _, old := range m.skiplists[:len(m.skiplists)-1] {
		if old.Len() == 0 {
			// It will be removed by garbage collection.
			break
		}
		// Removing keys while iterating on map is totally safe in Go.
		it := old.NewIterator()
		for it.Next() {
			key := it.Key()
			err := fresh.Set(key, it.Value())
			if err != nil {
				log.Printf("[ERROR] Failed to compact skiplists: %v", err)
				return false
			}
			// Dont check the returned val, it's useless because
			// we are sure that the key is already there.
			old.Delete(key)
			total++
			if total > 100000 {
				// It's enough. Don't block the instance.
				return false
			}
		}
	}
	// Remove empty skiplists. Keep the last table.
	tmp := []*skipList{m.skiplists[len(m.skiplists)-1]}
	for _, t := range m.skiplists[:len(m.skiplists)-1] {
		if t.Len() == 0 {
			continue
		}
		tmp = append(tmp, t)
	}
	m.skiplists = tmp
	return true
}
