/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package coredns

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	// Pure Go SQLite driver - no CGO required
	_ "modernc.org/sqlite"
)

// SQLiteBackend implements Backend using SQLite for storage.
// This provides a simpler alternative to etcd for single-node deployments
// or when a distributed key-value store isn't needed.
type SQLiteBackend struct {
	db   *sql.DB
	mu   sync.RWMutex
	path string
}

// Compile-time check that SQLiteBackend implements Backend
var _ Backend = (*SQLiteBackend)(nil)

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS services (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_services_key_prefix ON services(key);
`

// NewSQLiteBackend creates a new SQLite-based backend.
// The database file will be created if it doesn't exist.
// Path can be ":memory:" for an in-memory database (useful for testing).
func NewSQLiteBackend(path string) (*SQLiteBackend, error) {
	// Ensure parent directory exists (unless in-memory)
	if path != ":memory:" {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	// Open with WAL mode for better concurrent read performance
	dsn := path
	if path != ":memory:" {
		dsn = path + "?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL"
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	// Limit connections for SQLite (it doesn't handle high concurrency well)
	db.SetMaxOpenConns(1)

	// Initialize schema
	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, err
	}

	log.Infof("SQLite backend initialized at %s", path)

	return &SQLiteBackend{
		db:   db,
		path: path,
	}, nil
}

// GetServices retrieves all services matching the given key prefix.
func (s *SQLiteBackend) GetServices(ctx context.Context, prefix string) ([]*Service, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Query for all keys that start with the prefix
	query := `SELECT key, value FROM services WHERE key LIKE ? || '%'`
	rows, err := s.db.QueryContext(ctx, query, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Deduplication map (same logic as etcd backend)
	seen := make(map[Service]bool)
	var services []*Service

	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}

		svc := new(Service)
		if err := json.Unmarshal([]byte(value), svc); err != nil {
			log.Warnf("Failed to unmarshal service at %s: %v", key, err)
			continue
		}
		svc.Key = key

		// Deduplicate based on content (same as etcd implementation)
		dedupKey := Service{
			Host:     svc.Host,
			Port:     svc.Port,
			Priority: svc.Priority,
			Weight:   svc.Weight,
			Text:     svc.Text,
			Key:      key,
		}
		if seen[dedupKey] {
			continue
		}
		seen[dedupKey] = true

		// Default priority if not set
		if svc.Priority == 0 {
			svc.Priority = priority
		}

		services = append(services, svc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return services, nil
}

// SaveService persists a service record to SQLite.
func (s *SQLiteBackend) SaveService(ctx context.Context, service *Service) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, err := json.Marshal(service)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO services (key, value, updated_at)
		VALUES (?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			updated_at = CURRENT_TIMESTAMP
	`

	_, err = s.db.ExecContext(ctx, query, service.Key, string(value))
	return err
}

// DeleteService removes all services matching the key prefix.
func (s *SQLiteBackend) DeleteService(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Delete exact match and all children (prefix-based delete like etcd)
	query := `DELETE FROM services WHERE key = ? OR key LIKE ? || '/%'`
	_, err := s.db.ExecContext(ctx, query, key, key)
	return err
}

// Close closes the database connection.
func (s *SQLiteBackend) Close() error {
	return s.db.Close()
}

// Path returns the database file path (useful for testing/debugging).
func (s *SQLiteBackend) Path() string {
	return s.path
}

// Count returns the number of services stored (useful for testing/debugging).
func (s *SQLiteBackend) Count(ctx context.Context) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM services").Scan(&count)
	return count, err
}

// Keys returns all stored keys (useful for debugging).
func (s *SQLiteBackend) Keys(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.QueryContext(ctx, "SELECT key FROM services ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

// keyMatchesPrefix checks if a key matches a prefix (for hierarchical keys).
func keyMatchesPrefix(key, prefix string) bool {
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	// Ensure we're matching complete path components
	rest := key[len(prefix):]
	return rest == "" || strings.HasPrefix(rest, "/")
}
