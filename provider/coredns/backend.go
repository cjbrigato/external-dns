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
	"errors"
	"os"
	"strings"
)

// BackendType represents the type of backend storage
type BackendType string

const (
	// BackendTypeEtcd uses etcd as the storage backend (default)
	BackendTypeEtcd BackendType = "etcd"
	// BackendTypeSQLite uses SQLite as the storage backend
	BackendTypeSQLite BackendType = "sqlite"
	// BackendTypeMemory uses in-memory storage (non-persistent)
	BackendTypeMemory BackendType = "memory"
)

var (
	// ErrUnknownBackend is returned when an unknown backend type is specified
	ErrUnknownBackend = errors.New("unknown backend type")
)

// Backend defines the interface for CoreDNS service storage.
// This is the core abstraction that allows different storage backends
// (etcd, SQLite, etc.) to be used interchangeably.
//
// Implementations must be safe for concurrent use.
type Backend interface {
	// GetServices retrieves all services under the given prefix.
	// The prefix follows the CoreDNS etcd key format: /skydns/com/example/...
	// Returns an empty slice if no services are found.
	GetServices(ctx context.Context, prefix string) ([]*Service, error)

	// SaveService persists a service record.
	// If a service with the same key exists, it will be overwritten.
	SaveService(ctx context.Context, service *Service) error

	// DeleteService removes a service and all services under the given key prefix.
	// This is a prefix-based delete to support hierarchical key structures.
	DeleteService(ctx context.Context, key string) error

	// Close releases any resources held by the backend.
	Close() error
}

// BackendConfig holds configuration for backend creation
type BackendConfig struct {
	// Type specifies which backend to use (etcd, sqlite)
	Type BackendType

	// SQLite-specific settings
	SQLitePath string

	// Additional options can be added here for other backends
}

// GetBackendType returns the configured backend type from environment
func GetBackendType() BackendType {
	backendStr := strings.ToLower(os.Getenv("COREDNS_BACKEND"))
	switch backendStr {
	case "sqlite", "sqlite3":
		return BackendTypeSQLite
	case "memory", "mem", "inmemory", "in-memory":
		return BackendTypeMemory
	case "etcd", "":
		return BackendTypeEtcd
	default:
		return BackendType(backendStr)
	}
}

// GetBackendConfig builds a BackendConfig from environment variables
func GetBackendConfig() BackendConfig {
	return BackendConfig{
		Type:       GetBackendType(),
		SQLitePath: os.Getenv("COREDNS_SQLITE_PATH"),
	}
}

// NewBackend creates a new backend based on the configuration.
// If cfg is nil, configuration is read from environment variables.
func NewBackend(cfg *BackendConfig) (Backend, error) {
	if cfg == nil {
		c := GetBackendConfig()
		cfg = &c
	}

	switch cfg.Type {
	case BackendTypeEtcd:
		return newETCDClient()
	case BackendTypeSQLite:
		path := cfg.SQLitePath
		if path == "" {
			path = "/var/lib/external-dns/coredns.db"
		}
		return NewSQLiteBackend(path)
	case BackendTypeMemory:
		return NewMemoryBackend(), nil
	default:
		return nil, ErrUnknownBackend
	}
}
