package sql

import (
	"fmt"
	"strings"
)

// StatementType represents the type of SQL statement
type StatementType int

const (
	StatementTypeSelect StatementType = iota
	StatementTypeInsert
	StatementTypeUpdate
	StatementTypeDelete
	StatementTypeCreateTable
	StatementTypeDropTable
)

// Statement represents a parsed SQL statement
type Statement struct {
	Type    StatementType
	Table   string
	Columns []string
	Values  []interface{}
	Where   *WhereClause
}

// WhereClause represents a WHERE condition in a SQL statement
type WhereClause struct {
	Column    string
	Operator  string
	Value     interface{}
	Next      *WhereClause
	Connector string // AND or OR
}

// Parser represents a SQL parser
type Parser struct {
	query string
}

// NewParser creates a new SQL parser
func NewParser(query string) *Parser {
	return &Parser{
		query: strings.TrimSpace(query),
	}
}

// Parse parses the SQL query and returns a Statement
func (p *Parser) Parse() (*Statement, error) {
	// Convert query to lowercase for case-insensitive parsing
	query := strings.ToLower(p.query)

	// Basic parsing logic - this is a simplified version
	// In a real implementation, you would use a proper SQL parser like vitess/sqlparser
	if strings.HasPrefix(query, "select") {
		return p.parseSelect()
	} else if strings.HasPrefix(query, "insert") {
		return p.parseInsert()
	} else if strings.HasPrefix(query, "update") {
		return p.parseUpdate()
	} else if strings.HasPrefix(query, "delete") {
		return p.parseDelete()
	} else if strings.HasPrefix(query, "create table") {
		return p.parseCreateTable()
	} else if strings.HasPrefix(query, "drop table") {
		return p.parseDropTable()
	}

	return nil, fmt.Errorf("unsupported SQL statement: %s", p.query)
}

// parseSelect parses a SELECT statement
func (p *Parser) parseSelect() (*Statement, error) {
	// This is a very basic implementation
	// In a real implementation, you would parse the full SELECT syntax
	parts := strings.Fields(p.query)
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid SELECT statement")
	}

	// Extract columns
	columns := strings.Split(strings.Trim(parts[1], "()"), ",")
	for i := range columns {
		columns[i] = strings.TrimSpace(columns[i])
	}

	// Extract table name
	table := ""
	for i, part := range parts {
		if part == "from" && i+1 < len(parts) {
			table = parts[i+1]
			break
		}
	}

	return &Statement{
		Type:    StatementTypeSelect,
		Table:   table,
		Columns: columns,
	}, nil
}

// parseInsert parses an INSERT statement
func (p *Parser) parseInsert() (*Statement, error) {
	// This is a very basic implementation
	parts := strings.Fields(p.query)
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid INSERT statement")
	}

	// Extract table name
	table := ""
	for i, part := range parts {
		if part == "into" && i+1 < len(parts) {
			table = parts[i+1]
			break
		}
	}

	// Extract columns and values
	columns := strings.Split(strings.Trim(parts[3], "()"), ",")
	values := strings.Split(strings.Trim(parts[5], "()"), ",")

	for i := range columns {
		columns[i] = strings.TrimSpace(columns[i])
		values[i] = strings.TrimSpace(values[i])
	}

	return &Statement{
		Type:    StatementTypeInsert,
		Table:   table,
		Columns: columns,
		Values:  convertValues(values),
	}, nil
}

// parseUpdate parses an UPDATE statement
func (p *Parser) parseUpdate() (*Statement, error) {
	// This is a very basic implementation
	parts := strings.Fields(p.query)
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid UPDATE statement")
	}

	table := parts[1]
	columns := []string{}
	values := []interface{}{}

	// Parse SET clause
	for i := 2; i < len(parts); i++ {
		if parts[i] == "where" {
			break
		}
		if parts[i] == "set" {
			continue
		}
		if strings.Contains(parts[i], "=") {
			pair := strings.Split(parts[i], "=")
			columns = append(columns, strings.TrimSpace(pair[0]))
			values = append(values, strings.TrimSpace(pair[1]))
		}
	}

	return &Statement{
		Type:    StatementTypeUpdate,
		Table:   table,
		Columns: columns,
		Values:  values,
	}, nil
}

// parseDelete parses a DELETE statement
func (p *Parser) parseDelete() (*Statement, error) {
	// This is a very basic implementation
	parts := strings.Fields(p.query)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid DELETE statement")
	}

	table := ""
	for i, part := range parts {
		if part == "from" && i+1 < len(parts) {
			table = parts[i+1]
			break
		}
	}

	return &Statement{
		Type:  StatementTypeDelete,
		Table: table,
	}, nil
}

// parseCreateTable parses a CREATE TABLE statement
func (p *Parser) parseCreateTable() (*Statement, error) {
	// This is a very basic implementation
	// In a real implementation, you would use a proper SQL parser
	query := p.query

	// Extract table name
	tableNameStart := strings.Index(strings.ToLower(query), "create table") + len("create table")
	if tableNameStart == -1 {
		return nil, fmt.Errorf("invalid CREATE TABLE statement: missing table name")
	}

	// Find the opening parenthesis
	parenStart := strings.Index(query[tableNameStart:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("invalid CREATE TABLE statement: missing column definitions")
	}

	// Extract table name
	tableName := strings.TrimSpace(query[tableNameStart : tableNameStart+parenStart])

	// Find the closing parenthesis
	parenEnd := strings.LastIndex(query, ")")
	if parenEnd == -1 {
		return nil, fmt.Errorf("invalid CREATE TABLE statement: missing closing parenthesis")
	}

	// Extract column definitions
	columnDefs := query[tableNameStart+parenStart+1 : parenEnd]

	// Split by comma, but be careful with commas inside parentheses
	columns := []string{}
	currentCol := ""
	parenCount := 0

	for _, char := range columnDefs {
		if char == '(' {
			parenCount++
		} else if char == ')' {
			parenCount--
		} else if char == ',' && parenCount == 0 {
			columns = append(columns, strings.TrimSpace(currentCol))
			currentCol = ""
			continue
		}
		currentCol += string(char)
	}

	// Add the last column
	if strings.TrimSpace(currentCol) != "" {
		columns = append(columns, strings.TrimSpace(currentCol))
	}

	return &Statement{
		Type:    StatementTypeCreateTable,
		Table:   tableName,
		Columns: columns,
	}, nil
}

// parseDropTable parses a DROP TABLE statement
func (p *Parser) parseDropTable() (*Statement, error) {
	// This is a very basic implementation
	parts := strings.Fields(p.query)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid DROP TABLE statement")
	}

	return &Statement{
		Type:  StatementTypeDropTable,
		Table: parts[2],
	}, nil
}

// convertValues converts string values to appropriate types
func convertValues(values []string) []interface{} {
	result := make([]interface{}, len(values))
	for i, v := range values {
		// Remove quotes if present
		v = strings.Trim(v, "'\"")
		result[i] = v
	}
	return result
}
