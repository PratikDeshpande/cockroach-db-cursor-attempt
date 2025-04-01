package storage

import (
	"context"
	"time"
)

// Key represents a database key
type Key []byte

// Value represents a database value
type Value []byte

// Timestamp represents a database timestamp
type Timestamp struct {
	WallTime int64
	Logical  int32
}

// Engine represents the storage engine interface
type Engine interface {
	// Put stores a key-value pair with the given timestamp
	Put(ctx context.Context, key Key, value Value, ts Timestamp) error
	
	// Get retrieves the value for a key at the given timestamp
	Get(ctx context.Context, key Key, ts Timestamp) (Value, error)
	
	// Delete removes a key-value pair at the given timestamp
	Delete(ctx context.Context, key Key, ts Timestamp) error
	
	// Scan returns an iterator over key-value pairs in the given range
	Scan(ctx context.Context, start, end Key, ts Timestamp) Iterator
	
	// Close closes the engine and releases resources
	Close() error
}

// Iterator represents an iterator over key-value pairs
type Iterator interface {
	// Valid returns whether the iterator is positioned at a valid entry
	Valid() bool
	
	// Next advances the iterator to the next entry
	Next() bool
	
	// Key returns the current key
	Key() Key
	
	// Value returns the current value
	Value() Value
	
	// Close closes the iterator
	Close() error
}

// NewTimestamp creates a new timestamp with the current wall time
func NewTimestamp() Timestamp {
	return Timestamp{
		WallTime: time.Now().UnixNano(),
		Logical:  0,
	}
}

// CompareTimestamps compares two timestamps
func CompareTimestamps(a, b Timestamp) int {
	if a.WallTime < b.WallTime {
		return -1
	}
	if a.WallTime > b.WallTime {
		return 1
	}
	if a.Logical < b.Logical {
		return -1
	}
	if a.Logical > b.Logical {
		return 1
	}
	return 0
} 