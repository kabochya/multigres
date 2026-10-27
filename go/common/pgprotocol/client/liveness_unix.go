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
	"errors"
	"net"
	"syscall"
)

// peerClosed looks at the first byte waiting on the connection without taking it.
func peerClosed(nc net.Conn) bool {
	_, tls := nc.(interface{ NetConn() net.Conn })
	if inner, ok := nc.(interface{ NetConn() net.Conn }); ok {
		nc = inner.NetConn()
	}
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	var (
		buf  [1]byte
		n    int
		rerr error
	)
	if err := raw.Read(func(fd uintptr) bool {
		n, _, rerr = syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		return true // never wait for the socket to become readable
	}); err != nil {
		return false
	}
	switch {
	case rerr == nil && n == 0:
		return true // end of stream
	case rerr == nil:
		return !tls && buf[0] == 'E' // an ErrorResponse nobody asked for
	case errors.Is(rerr, syscall.EAGAIN), errors.Is(rerr, syscall.EWOULDBLOCK), errors.Is(rerr, syscall.EINTR):
		return false // nothing waiting
	default:
		return true // a reset, or the socket is gone
	}
}
