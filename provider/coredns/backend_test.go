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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/external-dns/internal/testutils"
)

func TestGetBackendType(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected BackendType
	}{
		{
			name:     "default is etcd",
			envVars:  map[string]string{},
			expected: BackendTypeEtcd,
		},
		{
			name:     "explicit etcd",
			envVars:  map[string]string{"COREDNS_BACKEND": "etcd"},
			expected: BackendTypeEtcd,
		},
		{
			name:     "sqlite",
			envVars:  map[string]string{"COREDNS_BACKEND": "sqlite"},
			expected: BackendTypeSQLite,
		},
		{
			name:     "sqlite3",
			envVars:  map[string]string{"COREDNS_BACKEND": "sqlite3"},
			expected: BackendTypeSQLite,
		},
		{
			name:     "case insensitive",
			envVars:  map[string]string{"COREDNS_BACKEND": "SQLITE"},
			expected: BackendTypeSQLite,
		},
		{
			name:     "unknown type preserved",
			envVars:  map[string]string{"COREDNS_BACKEND": "consul"},
			expected: BackendType("consul"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testutils.TestHelperEnvSetter(t, tt.envVars)
			got := GetBackendType()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestGetBackendConfig(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected BackendConfig
	}{
		{
			name:    "default config",
			envVars: map[string]string{},
			expected: BackendConfig{
				Type:       BackendTypeEtcd,
				SQLitePath: "",
			},
		},
		{
			name: "sqlite with path",
			envVars: map[string]string{
				"COREDNS_BACKEND":     "sqlite",
				"COREDNS_SQLITE_PATH": "/data/dns.db",
			},
			expected: BackendConfig{
				Type:       BackendTypeSQLite,
				SQLitePath: "/data/dns.db",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testutils.TestHelperEnvSetter(t, tt.envVars)
			got := GetBackendConfig()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestNewBackend_SQLite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	cfg := &BackendConfig{
		Type:       BackendTypeSQLite,
		SQLitePath: dbPath,
	}

	backend, err := NewBackend(cfg)
	require.NoError(t, err)
	require.NotNil(t, backend)
	defer backend.Close()

	// Verify it's a SQLite backend
	sqliteBackend, ok := backend.(*SQLiteBackend)
	require.True(t, ok)
	assert.Equal(t, dbPath, sqliteBackend.Path())
}

func TestNewBackend_SQLiteDefaultPath(t *testing.T) {
	cfg := &BackendConfig{
		Type:       BackendTypeSQLite,
		SQLitePath: "", // Empty path should use default
	}

	// This will fail because we can't write to /var/lib/external-dns in tests
	// but it tests that the default path is attempted
	_, err := NewBackend(cfg)
	// The error should be about permissions, not configuration
	if err != nil {
		assert.Contains(t, err.Error(), "/var/lib/external-dns")
	}
}

func TestNewBackend_UnknownType(t *testing.T) {
	cfg := &BackendConfig{
		Type: BackendType("unknown"),
	}

	backend, err := NewBackend(cfg)
	assert.Error(t, err)
	assert.Equal(t, ErrUnknownBackend, err)
	assert.Nil(t, backend)
}

func TestNewBackend_FromEnv(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "env.db")

	envVars := map[string]string{
		"COREDNS_BACKEND":     "sqlite",
		"COREDNS_SQLITE_PATH": dbPath,
	}
	testutils.TestHelperEnvSetter(t, envVars)

	// Pass nil to read from environment
	backend, err := NewBackend(nil)
	require.NoError(t, err)
	require.NotNil(t, backend)
	defer backend.Close()

	sqliteBackend, ok := backend.(*SQLiteBackend)
	require.True(t, ok)
	assert.Equal(t, dbPath, sqliteBackend.Path())
}
