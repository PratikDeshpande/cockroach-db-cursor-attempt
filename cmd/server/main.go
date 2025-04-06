package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"cockroach-db-cursor-attempt/internal/distribution"
	"cockroach-db-cursor-attempt/internal/sql"
	"cockroach-db-cursor-attempt/internal/storage"
	"cockroach-db-cursor-attempt/internal/transaction"
)

type Server struct {
	node     *distribution.Node
	storage  storage.Engine
	txns     *transaction.TransactionManager
	executor *sql.Executor
	httpSrv  *http.Server
}

func NewServer() (*Server, error) {
	// Create a new node
	node := distribution.NewNode(distribution.NodeID("node1"))

	// Create storage engine (in a real implementation, this would be RocksDB)
	memEngine := storage.NewMemoryEngine()
	engine := storage.NewMVCCEngine(memEngine)

	// Create transaction manager
	txns := transaction.NewTransactionManager(engine)

	// Create SQL executor
	executor := sql.NewExecutor(engine, txns)

	return &Server{
		node:     node,
		storage:  engine,
		txns:     txns,
		executor: executor,
	}, nil
}

func (s *Server) Start() error {
	// Start the node
	ctx := context.Background()
	if err := s.node.Start(ctx); err != nil {
		return fmt.Errorf("failed to start node: %v", err)
	}

	// Set up HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/execute", s.handleExecute)
	mux.HandleFunc("/health", s.handleHealth)

	s.httpSrv = &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start HTTP server
	go func() {
		log.Printf("Starting HTTP server on %s", s.httpSrv.Addr)
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop() error {
	// Stop the node
	s.node.Stop()

	// Stop HTTP server
	if err := s.httpSrv.Shutdown(context.Background()); err != nil {
		return fmt.Errorf("failed to shutdown HTTP server: %v", err)
	}

	return nil
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Query string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Execute the query
	if err := s.executor.Execute(r.Context(), req.Query); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := struct {
		Status    string `json:"status"`
		NodeState string `json:"node_state"`
	}{
		Status:    "healthy",
		NodeState: s.node.GetState().String(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func main() {
	// Create and start server
	server, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	log.Println("Shutting down server...")
	if err := server.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
	log.Println("Server stopped")
}
