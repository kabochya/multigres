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

package client

import "net"

// LocalPort returns the local TCP port of the connection, which is the client_port the server
// shows in pg_stat_activity for this backend: with the backend's process id it names the
// connection exactly, without a clock and without reading the server's statistics. It is zero
// when the connection is not TCP (a Unix socket has no port, and the server shows -1).
func (c *Conn) LocalPort() int32 {
	if addr, ok := c.conn.LocalAddr().(*net.TCPAddr); ok {
		return int32(addr.Port) //nolint:gosec // a port fits in an int32
	}
	return 0
}

// PeerClosed reports whether the server has ended the connection or sent an error that ends
// it, without reading anything from it: an idle connection that is readable has been closed by
// the other side (end of stream, a reset) or has been sent a FATAL ErrorResponse, which is what
// the server sends a session it terminates. It is only meaningful for a connection that no
// request is using. A connection it cannot look at (a TLS connection, whose bytes are not the
// protocol's until decrypted, only an end of stream or a reset is seen), or on a platform
// without the call, is reported as open. So is one that has bytes the client already took off
// the socket into its buffer: what is left in the kernel then may start in the middle of a
// message, and its first byte says nothing about what the server sent.
func (c *Conn) PeerClosed() bool {
	if c.bufferedReader != nil && c.bufferedReader.Buffered() > 0 {
		return false
	}
	return peerClosed(c.conn)
}
