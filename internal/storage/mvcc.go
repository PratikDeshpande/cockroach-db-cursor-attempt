package storage

import (
	"context"
	"encoding/binary"
	"fmt"
)

// MVCCKey represents a versioned key in the database
type MVCCKey struct {
	Key       Key
	Timestamp Timestamp
}

// MVCCValue represents a versioned value in the database
type MVCCValue struct {
	Value     Value
	Timestamp Timestamp
}

// MVCCEngine implements the Engine interface with MVCC support
type MVCCEngine struct {
	engine Engine
}

// NewMVCCEngine creates a new MVCC storage engine
func NewMVCCEngine(engine Engine) *MVCCEngine {
	return &MVCCEngine{
		engine: engine,
	}
}

// encodeKey encodes an MVCC key into a byte slice
func encodeKey(key MVCCKey) Key {
	// Format: key + timestamp
	buf := make([]byte, len(key.Key)+8+4)
	copy(buf, key.Key)
	binary.BigEndian.PutUint64(buf[len(key.Key):], uint64(key.Timestamp.WallTime))
	binary.BigEndian.PutUint32(buf[len(key.Key)+8:], uint32(key.Timestamp.Logical))
	return buf
}

// decodeKey decodes a byte slice into an MVCC key
func decodeKey(data Key) (MVCCKey, error) {
	if len(data) < 12 { // minimum length for key + timestamp
		return MVCCKey{}, fmt.Errorf("invalid key data length")
	}
	
	keyLen := len(data) - 12
	key := make(Key, keyLen)
	copy(key, data[:keyLen])
	
	timestamp := Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(data[keyLen:])),
		Logical:  int32(binary.BigEndian.Uint32(data[keyLen+8:])),
	}
	
	return MVCCKey{
		Key:       key,
		Timestamp: timestamp,
	}, nil
}

// Put implements the Engine interface with MVCC support
func (e *MVCCEngine) Put(ctx context.Context, key Key, value Value, ts Timestamp) error {
	mvccKey := MVCCKey{
		Key:       key,
		Timestamp: ts,
	}
	
	encodedKey := encodeKey(mvccKey)
	return e.engine.Put(ctx, encodedKey, value, ts)
}

// Get implements the Engine interface with MVCC support
func (e *MVCCEngine) Get(ctx context.Context, key Key, ts Timestamp) (Value, error) {
	// Create a key range from the given key to the maximum possible timestamp
	startKey := encodeKey(MVCCKey{Key: key, Timestamp: ts})
	endKey := encodeKey(MVCCKey{Key: key, Timestamp: Timestamp{WallTime: 1<<63 - 1, Logical: 1<<31 - 1}})
	
	iter := e.engine.Scan(ctx, startKey, endKey, ts)
	defer iter.Close()
	
	if !iter.Valid() {
		return nil, nil // Key not found
	}
	
	// Get the first (most recent) version
	return iter.Value(), nil
}

// Delete implements the Engine interface with MVCC support
func (e *MVCCEngine) Delete(ctx context.Context, key Key, ts Timestamp) error {
	// In MVCC, a delete is implemented as a tombstone value
	return e.Put(ctx, key, nil, ts)
}

// Scan implements the Engine interface with MVCC support
func (e *MVCCEngine) Scan(ctx context.Context, start, end Key, ts Timestamp) Iterator {
	// Create MVCC key ranges
	startMVCCKey := encodeKey(MVCCKey{Key: start, Timestamp: ts})
	endMVCCKey := encodeKey(MVCCKey{Key: end, Timestamp: ts})
	
	return e.engine.Scan(ctx, startMVCCKey, endMVCCKey, ts)
}

// Close implements the Engine interface
func (e *MVCCEngine) Close() error {
	return e.engine.Close()
} 