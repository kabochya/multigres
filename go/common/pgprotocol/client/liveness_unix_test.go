// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build unix

package client

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pair returns a client connection and the server's end of it.
func pair(t *testing.T) (*Conn, net.Conn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		s, _ := l.Accept()
		accepted <- s
	}()
	nc, err := net.Dial("tcp", l.Addr().String())
	require.NoError(t, err)
	server := <-accepted
	t.Cleanup(func() { _ = nc.Close(); _ = server.Close() })
	return &Conn{conn: nc, bufferedReader: bufio.NewReader(nc)}, server
}

// TestLocalPortIsThePortTheServerSees: it is the port the other end sees the connection from, which
// is the client_port of the backend in pg_stat_activity.
func TestLocalPortIsThePortTheServerSees(t *testing.T) {
	c, server := pair(t)
	require.Equal(t, int32(server.RemoteAddr().(*net.TCPAddr).Port), c.LocalPort())
	require.Positive(t, c.LocalPort())
}

// TestPeerClosed: an idle connection is open until the server ends it or sends it an error, and
// looking does not take anything off the connection.
func TestPeerClosed(t *testing.T) {
	t.Run("quiet", func(t *testing.T) {
		c, _ := pair(t)
		require.False(t, c.PeerClosed())
	})
	t.Run("a notice is not an end", func(t *testing.T) {
		c, server := pair(t)
		_, err := server.Write([]byte{'N', 0, 0, 0, 4})
		require.NoError(t, err)
		require.Eventually(t, func() bool { return !c.PeerClosed() }, time.Second, 5*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		require.False(t, c.PeerClosed())
		buf := make([]byte, 5)
		_, err = c.conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, byte('N'), buf[0], "looking took nothing off the connection")
	})
	t.Run("an error is an end", func(t *testing.T) {
		c, server := pair(t)
		_, err := server.Write([]byte{'E', 0, 0, 0, 5, 0})
		require.NoError(t, err)
		require.Eventually(t, c.PeerClosed, time.Second, 5*time.Millisecond)
	})
	t.Run("a message split across segments is not an end", func(t *testing.T) {
		// The first segment of an asynchronous message is taken into the client's buffer; the
		// rest begins with a byte that is an 'E' only by chance.
		c, server := pair(t)
		_, err := server.Write([]byte{'N', 0, 0, 0, 9, 1})
		require.NoError(t, err)
		_, err = c.bufferedReader.Peek(6)
		require.NoError(t, err)
		require.Equal(t, 6, c.bufferedReader.Buffered())
		_, err = server.Write([]byte{'E', 2, 3, 4})
		require.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
		require.False(t, c.PeerClosed(), "the kernel's first byte is in the middle of a message")

		// Once the client has taken the whole message, the 'E' that follows is at a boundary.
		_, err = c.bufferedReader.Discard(6)
		require.NoError(t, err)
		require.Eventually(t, c.PeerClosed, time.Second, 5*time.Millisecond)
	})
	t.Run("a close is an end", func(t *testing.T) {
		c, server := pair(t)
		require.NoError(t, server.Close())
		require.Eventually(t, c.PeerClosed, time.Second, 5*time.Millisecond)
	})
}
