// Copyright (c) 2018-2019 Burak Sezer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sorted

import (
	"log"
	"time"
)

func (m *SortedMap) compaction() {
	defer m.wg.Done()

	// Run immediately. The ticker will trigger that function
	// every 10 milliseconds to prevent blocking the SortedMap instance.
	if done := m.compactSkipLists(); done {
		// Fragmented skiplists are compactd. Quit.
		return
	}
	for {
		select {
		case <-time.After(10 * time.Millisecond):
			if done := m.compactSkipLists(); done {
				// Fragmented skiplists are compactd. Quit.
				return
			}
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *SortedMap) compactSkipLists() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.skiplists) == 1 {
		m.compacting = false
		return true
	}

	var total int
	fresh := m.skiplists[len(m.skiplists)-1]
	for _, old := range m.skiplists[:len(m.skiplists)-1] {
		if old.len() == 0 {
			// It will be removed by the garbage collector.
			break
		}
		it := old.newIterator()
		for it.next() {
			key := it.key()
			err := fresh.set(key, it.value())
			if err != nil {
				log.Printf("[ERROR] Failed to compact skiplists: %v", err)
				return false
			}
			old.delete(key)
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
		if t.len() == 0 {
			continue
		}
		tmp = append(tmp, t)
	}
	m.skiplists = tmp
	m.compacting = false
	return true
}
