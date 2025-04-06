package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// MemoryEngine implements a simple in-memory storage engine
type MemoryEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryEngine creates a new in-memory storage engine
func NewMemoryEngine() *MemoryEngine {
	return &MemoryEngine{
		data: make(map[string][]byte),
	}
}

// Put stores a key-value pair
func (e *MemoryEngine) Put(ctx context.Context, key Key, value Value, ts Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// In a real implementation, we would store the timestamp with the value
	e.data[string(key)] = value
	fmt.Printf("MemoryEngine.Put: stored key=%s, value=%s\n", key, value)
	return nil
}

// Get retrieves a value for a key
func (e *MemoryEngine) Get(ctx context.Context, key Key, ts Timestamp) (Value, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	value, exists := e.data[string(key)]
	if !exists {
		fmt.Printf("MemoryEngine.Get: key=%s not found\n", key)
		return nil, nil
	}

	fmt.Printf("MemoryEngine.Get: found key=%s, value=%s\n", key, value)
	return value, nil
}

// Delete removes a key-value pair
func (e *MemoryEngine) Delete(ctx context.Context, key Key, ts Timestamp) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.data, string(key))
	return nil
}

// Scan returns an iterator over key-value pairs
func (e *MemoryEngine) Scan(ctx context.Context, start, end Key, ts Timestamp) Iterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	prefix := string(start)
	fmt.Printf("MemoryEngine.Scan: scanning with prefix=%s\n", prefix)
	fmt.Printf("MemoryEngine.Scan: current data=%v\n", e.data)

	// Create a sorted list of keys
	keys := make([]string, 0, len(e.data))
	for k := range e.data {
		// For table scans, we want to match the prefix exactly
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
			fmt.Printf("MemoryEngine.Scan: matched key=%s\n", k)
		}
	}

	// Sort keys for consistent iteration
	sort.Strings(keys)

	fmt.Printf("MemoryEngine.Scan: found %d matching keys: %v\n", len(keys), keys)
	return &MemoryIterator{
		engine: e,
		keys:   keys,
		index:  -1,
	}
}

// Close closes the engine
func (e *MemoryEngine) Close() error {
	return nil
}

// MemoryIterator implements the Iterator interface for MemoryEngine
type MemoryIterator struct {
	engine *MemoryEngine
	keys   []string
	index  int
}

// Valid returns whether the iterator is positioned at a valid entry
func (it *MemoryIterator) Valid() bool {
	return it.index >= 0 && it.index < len(it.keys)
}

// Next advances the iterator to the next entry
func (it *MemoryIterator) Next() bool {
	if it.index < -1 {
		it.index = -1
	}
	if it.index < len(it.keys)-1 {
		it.index++
		return true
	}
	return false
}

// Key returns the current key
func (it *MemoryIterator) Key() Key {
	if !it.Valid() {
		return nil
	}
	return Key(it.keys[it.index])
}

// Value returns the current value
func (it *MemoryIterator) Value() Value {
	if !it.Valid() {
		return nil
	}
	value := it.engine.data[it.keys[it.index]]
	fmt.Printf("MemoryIterator.Value: returning value=%s for key=%s\n", value, it.keys[it.index])
	return value
}

// Close closes the iterator
func (it *MemoryIterator) Close() error {
	return nil
}
