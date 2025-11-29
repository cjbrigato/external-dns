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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/external-dns/endpoint"
)

func TestSQLiteBackend_NewAndClose(t *testing.T) {
	// Test in-memory database
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	require.NotNil(t, backend)
	assert.NoError(t, backend.Close())

	// Test file-based database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	backend, err = NewSQLiteBackend(dbPath)
	require.NoError(t, err)
	require.NotNil(t, backend)
	assert.Equal(t, dbPath, backend.Path())
	assert.NoError(t, backend.Close())

	// Verify file was created
	_, err = os.Stat(dbPath)
	assert.NoError(t, err)
}

func TestSQLiteBackend_SaveAndGetServices(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save a service
	svc := &Service{
		Host:     "1.2.3.4",
		TTL:      300,
		Priority: 10,
		Key:      "/skydns/com/example/www",
	}
	err = backend.SaveService(ctx, svc)
	require.NoError(t, err)

	// Retrieve the service
	services, err := backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	require.Len(t, services, 1)

	assert.Equal(t, "1.2.3.4", services[0].Host)
	assert.Equal(t, uint32(300), services[0].TTL)
	assert.Equal(t, 10, services[0].Priority)
	assert.Equal(t, "/skydns/com/example/www", services[0].Key)
}

func TestSQLiteBackend_GetServices_EmptyPrefix(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save multiple services under different prefixes
	services := []*Service{
		{Host: "1.1.1.1", Key: "/skydns/com/example/www"},
		{Host: "2.2.2.2", Key: "/skydns/org/test/api"},
		{Host: "3.3.3.3", Key: "/skydns/net/mysite/mail"},
	}
	for _, svc := range services {
		require.NoError(t, backend.SaveService(ctx, svc))
	}

	// Get all services with root prefix
	result, err := backend.GetServices(ctx, "/skydns/")
	require.NoError(t, err)
	assert.Len(t, result, 3)
}

func TestSQLiteBackend_GetServices_WithPrefix(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save multiple services
	services := []*Service{
		{Host: "1.1.1.1", Key: "/skydns/com/example/www"},
		{Host: "2.2.2.2", Key: "/skydns/com/example/api"},
		{Host: "3.3.3.3", Key: "/skydns/org/other/www"},
	}
	for _, svc := range services {
		require.NoError(t, backend.SaveService(ctx, svc))
	}

	// Get only services under /skydns/com/example
	result, err := backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	assert.Len(t, result, 2)

	hosts := make(map[string]bool)
	for _, svc := range result {
		hosts[svc.Host] = true
	}
	assert.True(t, hosts["1.1.1.1"])
	assert.True(t, hosts["2.2.2.2"])
}

func TestSQLiteBackend_UpdateService(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save initial service
	svc := &Service{
		Host: "1.2.3.4",
		TTL:  300,
		Key:  "/skydns/com/example/www",
	}
	require.NoError(t, backend.SaveService(ctx, svc))

	// Update the service
	svc.Host = "5.6.7.8"
	svc.TTL = 600
	require.NoError(t, backend.SaveService(ctx, svc))

	// Verify update
	services, err := backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	require.Len(t, services, 1)
	assert.Equal(t, "5.6.7.8", services[0].Host)
	assert.Equal(t, uint32(600), services[0].TTL)
}

func TestSQLiteBackend_DeleteService(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save a service
	svc := &Service{
		Host: "1.2.3.4",
		Key:  "/skydns/com/example/www",
	}
	require.NoError(t, backend.SaveService(ctx, svc))

	// Verify it exists
	services, err := backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	require.Len(t, services, 1)

	// Delete it
	require.NoError(t, backend.DeleteService(ctx, "/skydns/com/example/www"))

	// Verify it's gone
	services, err = backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	assert.Len(t, services, 0)
}

func TestSQLiteBackend_DeleteService_Prefix(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save multiple services under a prefix
	services := []*Service{
		{Host: "1.1.1.1", Key: "/skydns/com/example/www"},
		{Host: "2.2.2.2", Key: "/skydns/com/example/api"},
		{Host: "3.3.3.3", Key: "/skydns/com/other/www"},
	}
	for _, svc := range services {
		require.NoError(t, backend.SaveService(ctx, svc))
	}

	// Delete all under /skydns/com/example
	require.NoError(t, backend.DeleteService(ctx, "/skydns/com/example"))

	// Verify only /skydns/com/other remains
	result, err := backend.GetServices(ctx, "/skydns/com/")
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "3.3.3.3", result[0].Host)
}

func TestSQLiteBackend_DefaultPriority(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save a service without priority
	svc := &Service{
		Host: "1.2.3.4",
		Key:  "/skydns/com/example/www",
	}
	require.NoError(t, backend.SaveService(ctx, svc))

	// Retrieve and verify default priority is set
	services, err := backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	require.Len(t, services, 1)
	assert.Equal(t, priority, services[0].Priority) // priority = 10
}

func TestSQLiteBackend_TXTRecords(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save a TXT record
	svc := &Service{
		Text: "heritage=external-dns,external-dns/owner=default",
		Key:  "/skydns/com/example/www",
	}
	require.NoError(t, backend.SaveService(ctx, svc))

	// Retrieve and verify
	services, err := backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	require.Len(t, services, 1)
	assert.Equal(t, "heritage=external-dns,external-dns/owner=default", services[0].Text)
}

func TestSQLiteBackend_Count(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Initially empty
	count, err := backend.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Add services
	for i := 0; i < 5; i++ {
		svc := &Service{
			Host: "1.2.3.4",
			Key:  "/skydns/com/example/svc" + string(rune('a'+i)),
		}
		require.NoError(t, backend.SaveService(ctx, svc))
	}

	count, err = backend.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}

func TestSQLiteBackend_Keys(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Add services
	expectedKeys := []string{
		"/skydns/com/example/api",
		"/skydns/com/example/www",
		"/skydns/org/other/mail",
	}
	for _, key := range expectedKeys {
		svc := &Service{Host: "1.2.3.4", Key: key}
		require.NoError(t, backend.SaveService(ctx, svc))
	}

	keys, err := backend.Keys(ctx)
	require.NoError(t, err)
	assert.Equal(t, expectedKeys, keys) // Should be sorted
}

func TestSQLiteBackend_CompleteService(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Save a complete service with all fields
	svc := &Service{
		Host:        "1.2.3.4",
		Port:        8080,
		Priority:    5,
		Weight:      100,
		Text:        "heritage=external-dns",
		Mail:        false,
		TTL:         3600,
		TargetStrip: 1,
		Group:       "mygroup",
		Key:         "/skydns/com/example/www",
	}
	require.NoError(t, backend.SaveService(ctx, svc))

	// Retrieve and verify all fields
	services, err := backend.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	require.Len(t, services, 1)

	got := services[0]
	assert.Equal(t, svc.Host, got.Host)
	assert.Equal(t, svc.Port, got.Port)
	assert.Equal(t, svc.Priority, got.Priority) // Should preserve original, not default
	assert.Equal(t, svc.Weight, got.Weight)
	assert.Equal(t, svc.Text, got.Text)
	assert.Equal(t, svc.Mail, got.Mail)
	assert.Equal(t, svc.TTL, got.TTL)
	assert.Equal(t, svc.TargetStrip, got.TargetStrip)
	assert.Equal(t, svc.Group, got.Group)
	assert.Equal(t, svc.Key, got.Key)
}

func TestSQLiteBackend_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	ctx := context.Background()

	// Create backend and save data
	backend, err := NewSQLiteBackend(dbPath)
	require.NoError(t, err)

	svc := &Service{
		Host: "1.2.3.4",
		TTL:  300,
		Key:  "/skydns/com/example/www",
	}
	require.NoError(t, backend.SaveService(ctx, svc))
	require.NoError(t, backend.Close())

	// Reopen and verify data persisted
	backend2, err := NewSQLiteBackend(dbPath)
	require.NoError(t, err)
	defer backend2.Close()

	services, err := backend2.GetServices(ctx, "/skydns/com/example")
	require.NoError(t, err)
	require.Len(t, services, 1)
	assert.Equal(t, "1.2.3.4", services[0].Host)
}

func TestSQLiteBackend_IntegrationWithProvider(t *testing.T) {
	backend, err := NewSQLiteBackend(":memory:")
	require.NoError(t, err)
	defer backend.Close()

	ctx := context.Background()

	// Create provider with SQLite backend
	filter := &endpoint.DomainFilter{}
	provider := NewCoreDNSProviderWithBackend(filter, "/skydns/", false, backend)

	// Initially no records
	records, err := provider.Records(ctx)
	require.NoError(t, err)
	assert.Len(t, records, 0)

	// Add a service directly to backend
	// Key format: /skydns/{reversed-domain-labels}/{prefix}.{dns-name}
	// This becomes: www.example.com after reversing and stripping 1 label (the prefix)
	svc := &Service{
		Host:        "1.2.3.4",
		TTL:         300,
		TargetStrip: 1,
		Key:         "/skydns/com/example/www/12345678",
	}
	require.NoError(t, backend.SaveService(ctx, svc))

	// Provider should now see it
	// Key /skydns/com/example/www/12345678 -> reversed: 12345678.www.example.com
	// After TargetStrip=1: www.example.com
	records, err = provider.Records(ctx)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, "www.example.com", records[0].DNSName)
	assert.Equal(t, "1.2.3.4", records[0].Targets[0])
}
