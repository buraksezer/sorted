// Copyright (c) 2018-2019 Burak Sezer. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sorted

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func Test_SortedMapSetWithScore(t *testing.T) {
	sm := NewSortedMapWithScore(0, nil)
	defer sm.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := []string{}
	check := make(map[int]struct{})
	for i := 0; i < 100; i++ {
		key := r.Int()
		if _, ok := check[key]; ok {
			// Find a new key.
			i--
			continue
		}
		err := sm.Set(bkey(key), bval(key), uint64(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		keys = append(keys, string(bkey(key)))
		check[key] = struct{}{}
	}

	idx := 0
	// Sorted by iteration order.
	sm.Range(func(key, value []byte) bool {
		if keys[idx] != string(key) {
			t.Fatalf("Expected %s. Got: %s for id: %d", keys[idx], string(key), idx)
		}
		idx++
		return true
	})
}

func Test_SortedMapWithScoreGet(t *testing.T) {
	sm := NewSortedMapWithScore(0, nil)
	defer sm.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		score := r.Uint64()
		err := sm.Set(bkey(i), bval(i), score)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		val, err := sm.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if !bytes.Equal(val, bval(i)) {
			t.Fatalf("Values are differend for %s", string(bval(i)))
		}
	}
}

func Test_SortedMapWithScoreDelete(t *testing.T) {
	sm := NewSortedMapWithScore(0, nil)
	defer sm.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		score := r.Uint64()
		err := sm.Set(bkey(i), bval(i), score)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		err := sm.Delete(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		ok := sm.Check(bkey(i))
		if ok {
			t.Fatalf("Expected false. Got true for %s", string(bkey(i)))
		}
	}
}

func Test_SortedMapWithScoreRange(t *testing.T) {
	sm := NewSortedMapWithScore(0, nil)
	defer sm.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		err := sm.Set(bkey(i), bval(i), r.Uint64())
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		key := string(bkey(i))
		keys[key] = struct{}{}
	}

	sm.Range(func(key, value []byte) bool {
		if _, ok := keys[string(key)]; !ok {
			t.Fatalf("Invalid hkey: %s", string(key))
		}
		return true
	})
}

func Test_SortedMapWithScoreSubMap(t *testing.T) {
	sm := NewSortedMapWithScore(0, nil)
	defer sm.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := []string{}
	check := make(map[int]struct{})
	for i := 0; i < 100; i++ {
		key := r.Int()
		if _, ok := check[key]; ok {
			// Find a new key.
			i--
			continue
		}
		err := sm.Set(bkey(key), bval(key), uint64(i))
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		keys = append(keys, string(bkey(key)))
		check[key] = struct{}{}
	}

	idx := 23
	// Sorted by iteration order.
	sm.SubMap(23, 45, func(key, value []byte) bool {
		if keys[idx] != string(key) {
			t.Fatalf("Expected %s. Got: %s for id: %d", keys[idx], string(key), idx)
		}
		idx++
		return true
	})
}
