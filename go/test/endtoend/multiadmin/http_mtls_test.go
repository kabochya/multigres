// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multiadmin

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// httpMTLSCerts holds the CA, server, and client certificate paths generated
// for TestHTTPClientCertAuth.
type httpMTLSCerts struct {
	caCert       string
	serverCert   string
	serverKey    string
	operatorCert string
	operatorKey  string
	tenantBCert  string
	tenantBKey   string
}

// generateHTTPMTLSCerts creates a CA, a server cert for multiadmin's HTTP
// listener, and two client certs signed by the same CA: "operator" (allow-
// listed) and "tenant-b" (not), mirroring scripts/dev/try-http-mtls.sh.
func generateHTTPMTLSCerts(t *testing.T, dir string) httpMTLSCerts {
	t.Helper()

	caCertFile := filepath.Join(dir, "ca.crt")
	caKeyFile := filepath.Join(dir, "ca.key")
	require.NoError(t, local.GenerateCA(caCertFile, caKeyFile))

	c := httpMTLSCerts{
		caCert:       caCertFile,
		serverCert:   filepath.Join(dir, "server.crt"),
		serverKey:    filepath.Join(dir, "server.key"),
		operatorCert: filepath.Join(dir, "operator.crt"),
		operatorKey:  filepath.Join(dir, "operator.key"),
		tenantBCert:  filepath.Join(dir, "tenant-b.crt"),
		tenantBKey:   filepath.Join(dir, "tenant-b.key"),
	}

	require.NoError(t, local.GenerateCert(caCertFile, caKeyFile, c.serverCert, c.serverKey, "multiadmin", []string{"localhost"}))
	require.NoError(t, local.GenerateCert(caCertFile, caKeyFile, c.operatorCert, c.operatorKey, "operator", nil))
	require.NoError(t, local.GenerateCert(caCertFile, caKeyFile, c.tenantBCert, c.tenantBKey, "tenant-b", nil))

	return c
}

// httpsClient builds an https client trusting caCertFile and, when certFile
// is non-empty, presenting it as a client certificate.
func httpsClient(t *testing.T, caCertFile, certFile, keyFile string) *http.Client {
	t.Helper()

	caPEM, err := os.ReadFile(caCertFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM), "failed to parse CA cert")

	tlsConfig := &tls.Config{RootCAs: pool}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		require.NoError(t, err)
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
		Timeout:   10 * time.Second,
	}
}

// getStatus issues a GET against base+path with client and returns the
// response status code.
func getStatus(t *testing.T, client *http.Client, url string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err, "GET %s", url)
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestHTTPClientCertAuth boots multiadmin with TLS and HTTP client-certificate
// enforcement enabled (--tls-cert/-key/-ca, --enable-http-mtls-auth,
// --http-auth-mtls-allowed-subjects) and drives real TLS connections against
// it, end to end, to prove:
//   - an allow-listed client cert is authorized and a same-CA cert that isn't
//     allow-listed is rejected (go/common/servenv/http_auth.go, client_cert.go)
//   - probe paths stay reachable without a client certificate
//   - the /proxy/admin self-hop presents its own client certificate rather
//     than falling back to plaintext (go/services/multiadmin/proxy.go)
func TestHTTPClientCertAuth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	certs := generateHTTPMTLSCerts(t, t.TempDir())

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultiadminExtraArgs(
			"--tls-cert", certs.serverCert,
			"--tls-key", certs.serverKey,
			"--tls-ca", certs.caCert,
			"--enable-http-mtls-auth",
			"--http-auth-mtls-allowed-subjects", "CN=operator",
			"--http-auth-mtls-allowed-subjects", "CN=multiadmin",
		),
	)
	defer cleanup()

	require.NotNil(t, setup.Multiadmin, "WithMultiadmin() should have created a multiadmin instance")
	base := fmt.Sprintf("https://localhost:%d", setup.MultiadminHttpPort)

	noCert := httpsClient(t, certs.caCert, "", "")
	operator := httpsClient(t, certs.caCert, certs.operatorCert, certs.operatorKey)
	tenantB := httpsClient(t, certs.caCert, certs.tenantBCert, certs.tenantBKey)

	t.Run("allow-listed client cert is authorized", func(t *testing.T) {
		require.Equal(t, http.StatusOK, getStatus(t, operator, base+"/config"))
	})
	t.Run("same-CA cert that isn't allow-listed is rejected", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized, getStatus(t, tenantB, base+"/config"))
	})
	t.Run("no client certificate is rejected", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized, getStatus(t, noCert, base+"/config"))
	})
	t.Run("probe paths stay exempt without a client certificate", func(t *testing.T) {
		require.Equal(t, http.StatusOK, getStatus(t, noCert, base+"/live"))
		require.Equal(t, http.StatusOK, getStatus(t, noCert, base+"/ready"))
	})
	t.Run("self-proxy hop presents its own client certificate", func(t *testing.T) {
		require.Equal(t, http.StatusOK, getStatus(t, operator, base+"/proxy/admin/global/config"))
	})
}
