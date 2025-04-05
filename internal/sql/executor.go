package sql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"cockroach-db-cursor-attempt/internal/storage"
	"cockroach-db-cursor-attempt/internal/transaction"
)

// Executor represents the SQL execution engine
type Executor struct {
	storage   storage.Engine
	txns      *transaction.TransactionManager
	schemas   map[string]*TableSchema
	schemasMu sync.RWMutex
}

// TableSchema represents the schema of a table
type TableSchema struct {
	Name    string
	Columns []Column
}

// Column represents a column in a table
type Column struct {
	Name     string
	Type     string
	Nullable bool
}

// NewExecutor creates a new SQL executor
func NewExecutor(engine storage.Engine, txns *transaction.TransactionManager) *Executor {
	return &Executor{
		storage: engine,
		txns:    txns,
		schemas: make(map[string]*TableSchema),
	}
}

// Execute executes a SQL statement
func (e *Executor) Execute(ctx context.Context, query string) error {
	parser := NewParser(query)
	stmt, err := parser.Parse()
	if err != nil {
		return fmt.Errorf("failed to parse query: %v", err)
	}

	// Start a transaction
	txn, err := e.txns.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer e.txns.Abort(ctx, txn)

	// Execute the statement
	switch stmt.Type {
	case StatementTypeCreateTable:
		err = e.executeCreateTable(ctx, txn, stmt)
	case StatementTypeDropTable:
		err = e.executeDropTable(ctx, txn, stmt)
	case StatementTypeInsert:
		err = e.executeInsert(ctx, txn, stmt)
	case StatementTypeSelect:
		err = e.executeSelect(ctx, txn, stmt)
	case StatementTypeUpdate:
		err = e.executeUpdate(ctx, txn, stmt)
	case StatementTypeDelete:
		err = e.executeDelete(ctx, txn, stmt)
	default:
		err = fmt.Errorf("unsupported statement type: %v", stmt.Type)
	}

	if err != nil {
		return err
	}

	// Commit the transaction
	return e.txns.Commit(ctx, txn)
}

// executeCreateTable creates a new table
func (e *Executor) executeCreateTable(ctx context.Context, txn *transaction.Transaction, stmt *Statement) error {
	e.schemasMu.Lock()
	defer e.schemasMu.Unlock()

	if _, exists := e.schemas[stmt.Table]; exists {
		return fmt.Errorf("table %s already exists", stmt.Table)
	}

	schema := &TableSchema{
		Name:    stmt.Table,
		Columns: make([]Column, len(stmt.Columns)),
	}

	for i, col := range stmt.Columns {
		// Parse column definition
		parts := strings.Fields(col)
		if len(parts) < 2 {
			return fmt.Errorf("invalid column definition: %s", col)
		}

		// Extract column name and type
		name := parts[0]
		dataType := parts[1]

		// Check for additional constraints
		nullable := true
		for j := 2; j < len(parts); j++ {
			if strings.ToLower(parts[j]) == "not" && j+1 < len(parts) && strings.ToLower(parts[j+1]) == "null" {
				nullable = false
				break
			}
		}

		schema.Columns[i] = Column{
			Name:     name,
			Type:     dataType,
			Nullable: nullable,
		}
	}

	e.schemas[stmt.Table] = schema
	return nil
}

// executeDropTable drops a table
func (e *Executor) executeDropTable(ctx context.Context, txn *transaction.Transaction, stmt *Statement) error {
	e.schemasMu.Lock()
	defer e.schemasMu.Unlock()

	if _, exists := e.schemas[stmt.Table]; !exists {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}

	delete(e.schemas, stmt.Table)
	return nil
}

// executeInsert inserts data into a table
func (e *Executor) executeInsert(ctx context.Context, txn *transaction.Transaction, stmt *Statement) error {
	e.schemasMu.RLock()
	_, exists := e.schemas[stmt.Table]
	e.schemasMu.RUnlock()

	if !exists {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}

	if len(stmt.Columns) != len(stmt.Values) {
		return fmt.Errorf("number of columns (%d) does not match number of values (%d)",
			len(stmt.Columns), len(stmt.Values))
	}

	// Create a key for the row
	key := storage.Key(fmt.Sprintf("%s:%d", stmt.Table, time.Now().UnixNano()))
	value := storage.Value(fmt.Sprintf("%v", stmt.Values))

	return e.storage.Put(ctx, key, value, txn.Timestamp)
}

// executeSelect retrieves data from a table
func (e *Executor) executeSelect(ctx context.Context, txn *transaction.Transaction, stmt *Statement) error {
	e.schemasMu.RLock()
	_, exists := e.schemas[stmt.Table]
	e.schemasMu.RUnlock()

	if !exists {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}

	// Create a key prefix for the table
	prefix := storage.Key(stmt.Table + ":")
	iter := e.storage.Scan(ctx, prefix, nil, txn.Timestamp)
	defer iter.Close()

	for iter.Valid() {
		// Process the row
		// In a real implementation, you would parse the value and filter based on WHERE clause
		iter.Next()
	}

	return nil
}

// executeUpdate updates data in a table
func (e *Executor) executeUpdate(ctx context.Context, txn *transaction.Transaction, stmt *Statement) error {
	e.schemasMu.RLock()
	_, exists := e.schemas[stmt.Table]
	e.schemasMu.RUnlock()

	if !exists {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}

	// Create a key prefix for the table
	prefix := storage.Key(stmt.Table + ":")
	iter := e.storage.Scan(ctx, prefix, nil, txn.Timestamp)
	defer iter.Close()

	for iter.Valid() {
		// Process and update the row
		// In a real implementation, you would parse the value, apply updates, and filter based on WHERE clause
		iter.Next()
	}

	return nil
}

// executeDelete deletes data from a table
func (e *Executor) executeDelete(ctx context.Context, txn *transaction.Transaction, stmt *Statement) error {
	e.schemasMu.RLock()
	_, exists := e.schemas[stmt.Table]
	e.schemasMu.RUnlock()

	if !exists {
		return fmt.Errorf("table %s does not exist", stmt.Table)
	}

	// Create a key prefix for the table
	prefix := storage.Key(stmt.Table + ":")
	iter := e.storage.Scan(ctx, prefix, nil, txn.Timestamp)
	defer iter.Close()

	for iter.Valid() {
		// Delete the row
		// In a real implementation, you would filter based on WHERE clause
		key := iter.Key()
		if err := e.storage.Delete(ctx, key, txn.Timestamp); err != nil {
			return err
		}
		iter.Next()
	}

	return nil
}
