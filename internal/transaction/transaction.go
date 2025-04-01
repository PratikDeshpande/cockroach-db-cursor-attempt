package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"cockroach-db-cursor-attempt/internal/storage"
)

// TransactionStatus represents the current state of a transaction
type TransactionStatus int

const (
	TransactionStatusActive TransactionStatus = iota
	TransactionStatusCommitted
	TransactionStatusAborted
)

// Transaction represents a distributed transaction
type Transaction struct {
	ID        uuid.UUID
	Status    TransactionStatus
	Timestamp storage.Timestamp
	mu        sync.RWMutex
}

// TransactionManager handles transaction lifecycle and coordination
type TransactionManager struct {
	storage  storage.Engine
	txns     map[uuid.UUID]*Transaction
	txnsMu   sync.RWMutex
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(engine storage.Engine) *TransactionManager {
	return &TransactionManager{
		storage: engine,
		txns:    make(map[uuid.UUID]*Transaction),
	}
}

// BeginTransaction starts a new transaction
func (tm *TransactionManager) BeginTransaction(ctx context.Context) (*Transaction, error) {
	txn := &Transaction{
		ID:        uuid.New(),
		Status:    TransactionStatusActive,
		Timestamp: storage.NewTimestamp(),
	}

	tm.txnsMu.Lock()
	tm.txns[txn.ID] = txn
	tm.txnsMu.Unlock()

	return txn, nil
}

// Get retrieves a value within a transaction
func (tm *TransactionManager) Get(ctx context.Context, txn *Transaction, key storage.Key) (storage.Value, error) {
	txn.mu.RLock()
	defer txn.mu.RUnlock()

	if txn.Status != TransactionStatusActive {
		return nil, fmt.Errorf("transaction %s is not active", txn.ID)
	}

	return tm.storage.Get(ctx, key, txn.Timestamp)
}

// Put stores a value within a transaction
func (tm *TransactionManager) Put(ctx context.Context, txn *Transaction, key storage.Key, value storage.Value) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TransactionStatusActive {
		return fmt.Errorf("transaction %s is not active", txn.ID)
	}

	return tm.storage.Put(ctx, key, value, txn.Timestamp)
}

// Commit commits a transaction using two-phase commit
func (tm *TransactionManager) Commit(ctx context.Context, txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TransactionStatusActive {
		return fmt.Errorf("transaction %s is not active", txn.ID)
	}

	// Phase 1: Prepare
	// In a real implementation, this would coordinate with other nodes
	// For now, we'll just mark the transaction as prepared

	// Phase 2: Commit
	txn.Status = TransactionStatusCommitted

	// Clean up
	tm.txnsMu.Lock()
	delete(tm.txns, txn.ID)
	tm.txnsMu.Unlock()

	return nil
}

// Abort aborts a transaction
func (tm *TransactionManager) Abort(ctx context.Context, txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.Status != TransactionStatusActive {
		return fmt.Errorf("transaction %s is not active", txn.ID)
	}

	txn.Status = TransactionStatusAborted

	// Clean up
	tm.txnsMu.Lock()
	delete(tm.txns, txn.ID)
	tm.txnsMu.Unlock()

	return nil
}

// GetTransaction retrieves a transaction by ID
func (tm *TransactionManager) GetTransaction(id uuid.UUID) (*Transaction, bool) {
	tm.txnsMu.RLock()
	defer tm.txnsMu.RUnlock()

	txn, exists := tm.txns[id]
	return txn, exists
} 