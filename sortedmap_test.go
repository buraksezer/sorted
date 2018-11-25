package gsorted

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func bkey(i int) []byte {
	return []byte(fmt.Sprintf("%09d", i))
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func Test_SortedMapSet(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	for i := 0; i < 100; i++ {
		err := j.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}
}

func Test_SortedMapGet(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	for i := 0; i < 100; i++ {
		err := j.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		value, err := j.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if !bytes.Equal(value, bval(i)) {
			t.Fatalf("Value is malformed for %d", i)
		}
	}
}

func Test_SortedMapRange(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	keys := []string{}
	for i := 0; i < 100; i++ {
		err := j.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		key := string(bkey(i))
		keys = append(keys, key)
	}

	idx := 0
	j.Range(func(key, value []byte) bool {
		if keys[idx] != string(key) {
			t.Fatalf("Expected key: %s for idx: %d. Got: %s", keys[idx], idx, string(key))
		}
		idx++
		return true
	})
}

func Test_SortedMapDelete(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	for i := 0; i < 100; i++ {
		err := j.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		if err := j.Delete(bkey(i)); err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		_, err := j.Get(bkey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func Test_SortedMapCompaction(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	// Current free space is 1MB. Trigger a merge operation.
	for i := 0; i < 2000; i++ {
		value := []byte(fmt.Sprintf("%01000d", i))
		err := j.Set(bkey(i), value)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 2000; i++ {
		if i == 1000 {
			// Dont remove this. Check it on the new skipList
			continue
		}
		err := j.Delete(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		// Trigger garbage collection
		err := j.Delete(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		j.mu.Lock()
		if len(j.skiplists) == 1 {
			// Only has 1 table with minimum size.
			if len(j.skiplists[0].kvData) == 1012 { // value + key + metadata
				j.mu.Unlock()
				return
			}
		}
		j.mu.Unlock()
		<-time.After(100 * time.Millisecond)
	}
	t.Fatal("SortedMap cannot be compacted.")
}

func Test_SortedMapLen(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	for i := 0; i < 100; i++ {
		err := j.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	if j.Len() != 100 {
		t.Fatalf("Expected length: 100. Got: %d", j.Len())
	}
}

func Test_SortedMapCheck(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	for i := 0; i < 100; i++ {
		err := j.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		if !j.Check(bkey(i)) {
			t.Fatalf("key could not be found: %s", string(bkey(i)))
		}
	}
}

func Test_SortedMapComparator(t *testing.T) {
	j := NewSortedMap(0)
	defer j.Close()

	check := make(map[int]struct{})
	keys := []string{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		n := r.Intn(1000)
		if _, ok := check[n]; ok {
			i--
			continue
		}
		check[n] = struct{}{}
		keys = append(keys, string(bkey(n)))
		err := j.Set(bkey(n), bval(n))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	tmp := []string{}
	j.Range(func(key, value []byte) bool {
		tmp = append(tmp, string(key))
		return true
	})

	sort.Strings(keys)
	for idx, key := range keys {
		if tmp[idx] != key {
			t.Fatalf("Expected key: %s at index: %d. Found: %s", key, idx, tmp[idx])
		}
	}
}

func Test_SortedMapSubMap(t *testing.T) {
	m := NewSortedMap(0)
	defer m.Close()

	for i := 0; i < 100; i++ {
		err := m.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	err := m.SubMap(bkey(20), bkey(10), func(key, value []byte) bool {
		return true
	})
	if err != ErrInvalidRange {
		t.Fatalf("Expected ErrInvalidRange. Got: %v", err)
	}

	idx := 10
	err = m.SubMap(bkey(10), bkey(20), func(key, value []byte) bool {
		if !bytes.Equal(key, bkey(idx)) {
			t.Fatalf("Expected key %s. Got: %s", bkey(idx), string(key))
		}
		if !bytes.Equal(value, bval(idx)) {
			t.Fatalf("Expected key %s. Got: %s", bval(idx), string(value))
		}
		idx++
		return true
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func Test_SortedMapHeadMap(t *testing.T) {
	m := NewSortedMap(0)
	defer m.Close()

	for i := 0; i < 100; i++ {
		err := m.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	idx := 0
	m.HeadMap(bkey(20), func(key, value []byte) bool {
		if !bytes.Equal(key, bkey(idx)) {
			t.Fatalf("Expected key %s. Got: %s", bkey(idx), string(key))
		}
		if !bytes.Equal(value, bval(idx)) {
			t.Fatalf("Expected key %s. Got: %s", bval(idx), string(value))
		}
		idx++
		return true
	})
}

func Test_SortedMapTailMap(t *testing.T) {
	m := NewSortedMap(0)
	defer m.Close()

	for i := 0; i < 100; i++ {
		err := m.Set(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	idx := 10
	m.TailMap(bkey(10), func(key, value []byte) bool {
		if !bytes.Equal(key, bkey(idx)) {
			t.Fatalf("Expected key %s. Got: %s", bkey(idx), string(key))
		}
		if !bytes.Equal(value, bval(idx)) {
			t.Fatalf("Expected key %s. Got: %s", bval(idx), string(value))
		}
		idx++
		return true
	})
}
