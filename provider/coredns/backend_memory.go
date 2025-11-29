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
	"sort"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

// MemoryBackend implements Backend using an in-memory map.
// This is ideal for:
//   - Testing: Fast, no external dependencies
//   - Development: Quick iteration without setup
//   - Ephemeral deployments: When persistence isn't needed
//   - CI/CD pipelines: Isolated, reproducible tests
//
// Note: Data is lost when the process exits.
type MemoryBackend struct {
	mu       sync.RWMutex
	services map[string]Service
}

// Compile-time check that MemoryBackend implements Backend
var _ Backend = (*MemoryBackend)(nil)

// NewMemoryBackend creates a new in-memory backend.
func NewMemoryBackend() *MemoryBackend {
	log.Info("Memory backend initialized (data will not persist)")
	return &MemoryBackend{
		services: make(map[string]Service),
	}
}

// GetServices retrieves all services matching the given key prefix.
func (m *MemoryBackend) GetServices(ctx context.Context, prefix string) ([]*Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Deduplication map (same logic as etcd/sqlite backends)
	seen := make(map[Service]bool)
	var services []*Service

	for key, svc := range m.services {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		// Create a copy with the key set
		svcCopy := svc
		svcCopy.Key = key

		// Deduplicate based on content
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
		if svcCopy.Priority == 0 {
			svcCopy.Priority = priority
		}

		services = append(services, &svcCopy)
	}

	return services, nil
}

// SaveService persists a service record to memory.
func (m *MemoryBackend) SaveService(ctx context.Context, service *Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Store a copy without the Key field (Key is metadata, not data)
	svcCopy := *service
	svcCopy.Key = ""
	m.services[service.Key] = svcCopy

	return nil
}

// DeleteService removes all services matching the key prefix.
func (m *MemoryBackend) DeleteService(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Delete exact match and all children (prefix-based delete like etcd)
	for k := range m.services {
		if k == key || strings.HasPrefix(k, key+"/") {
			delete(m.services, k)
		}
	}

	return nil
}

// Close is a no-op for memory backend but satisfies the Backend interface.
func (m *MemoryBackend) Close() error {
	return nil
}

// Count returns the number of services stored (useful for testing/debugging).
func (m *MemoryBackend) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.services)
}

// Keys returns all stored keys sorted (useful for testing/debugging).
func (m *MemoryBackend) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.services))
	for k := range m.services {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Clear removes all services (useful for testing).
func (m *MemoryBackend) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services = make(map[string]Service)
}

// Snapshot returns a copy of all services (useful for debugging).
func (m *MemoryBackend) Snapshot() map[string]Service {
	m.mu.RLock()
	defer m.mu.RUnlock()

	snapshot := make(map[string]Service, len(m.services))
	for k, v := range m.services {
		snapshot[k] = v
	}
	return snapshot
}
