package storage

import (
	"context"
	"sync"
)

// MemoryEngine implements a simple in-memory storage engine
type MemoryEngine struct {
	mu    sync.RWMutex
	data  map[string][]byte
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
	return nil
}

// Get retrieves a value for a key
func (e *MemoryEngine) Get(ctx context.Context, key Key, ts Timestamp) (Value, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	value, exists := e.data[string(key)]
	if !exists {
		return nil, nil
	}

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

	// Create a sorted list of keys
	keys := make([]string, 0, len(e.data))
	for k := range e.data {
		if k >= string(start) && (len(end) == 0 || k <= string(end)) {
			keys = append(keys, k)
		}
	}

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
	it.index++
	return it.Valid()
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
	return it.engine.data[it.keys[it.index]]
}

// Close closes the iterator
func (it *MemoryIterator) Close() error {
	return nil
} 