// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package session

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lonng/nano/pkg/errcode"
	"github.com/lonng/nano/service"
)

// NetworkEntity represent low-level network instance
type NetworkEntity interface {
	Push(route string, v interface{}) error
	RPC(route string, v interface{}) error
	SendResponse(mid uint64, code errcode.Code, v interface{}) error
	Close() error
	RemoteAddr() net.Addr
}

type kickEntity interface {
	Kick([]byte) error
}

var (
	//ErrIllegalUID represents a invalid uid
	ErrIllegalUID = errors.New("illegal uid")
	// ErrUIDMismatch indicates an attempt to change an established identity.
	ErrUIDMismatch = errors.New("session uid mismatch")
	// ErrKickUnsupported indicates that the session is not backed by a client gateway connection.
	ErrKickUnsupported = errors.New("session entity does not support kick")
)

// Session represents a client session which could storage temp data during low-level
// keep connected, all data will be released when the low-level connection was broken.
// Session instance related to the client will be passed to Handler method as the first
// parameter.
type Session struct {
	sync.RWMutex                           // protect data
	id              int64                  // session global unique id
	uid             int64                  // binding user id
	lastTime        int64                  // last heartbeat time
	authTracked     uint32                 // whether the authenticated metric is active
	connectionEpoch uint64                 // gateway ownership generation for targeted delivery
	entity          NetworkEntity          // low-level network entity
	data            map[string]interface{} // session data store
	router          *Router
}

// New returns a new session instance
// a NetworkEntity is a low-level network instance
func New(entity NetworkEntity) *Session {
	return &Session{
		id:       service.Connections.SessionID(),
		entity:   entity,
		data:     make(map[string]interface{}),
		lastTime: time.Now().Unix(),
		router:   newRouter(),
	}
}

// NetworkEntity returns the low-level network agent object
func (s *Session) NetworkEntity() NetworkEntity {
	return s.entity
}

// NetworkEntity returns the service router
func (s *Session) Router() *Router {
	return s.router
}

// RPC sends message to remote server
func (s *Session) RPC(route string, v interface{}) error {
	return s.entity.RPC(route, v)
}

// Push message to client
func (s *Session) Push(route string, v interface{}) error {
	return s.entity.Push(route, v)
}

// ID returns the session id
func (s *Session) ID() int64 {
	return s.id
}

// UID returns uid that bind to current session
func (s *Session) UID() int64 {
	return atomic.LoadInt64(&s.uid)
}

// SetConnectionEpoch records the gateway ownership generation assigned by the
// distributed session directory.
func (s *Session) SetConnectionEpoch(epoch uint64) {
	atomic.StoreUint64(&s.connectionEpoch, epoch)
}

// ConnectionEpoch returns the gateway ownership generation for this session.
func (s *Session) ConnectionEpoch() uint64 {
	return atomic.LoadUint64(&s.connectionEpoch)
}

// Bind bind UID to current session
func (s *Session) Bind(uid int64) error {
	_, err := s.BindWithResult(uid)
	return err
}

// BindWithResult binds UID to the session and reports whether this call made
// the first successful transition from unauthenticated to authenticated.
func (s *Session) BindWithResult(uid int64) (bool, error) {
	if uid < 1 {
		return false, ErrIllegalUID
	}
	for {
		current := atomic.LoadInt64(&s.uid)
		if current == uid {
			return false, nil
		}
		if current != 0 {
			return false, ErrUIDMismatch
		}
		if atomic.CompareAndSwapInt64(&s.uid, 0, uid) {
			if atomic.CompareAndSwapUint32(&s.authTracked, 0, 1) {
				service.SessionStats.Authenticate()
			}
			return true, nil
		}
	}
}

// Release removes this session from authenticated-session metrics. It is
// idempotent because connection shutdown can be observed by multiple goroutines.
func (s *Session) Release() {
	if s != nil && atomic.CompareAndSwapUint32(&s.authTracked, 1, 0) {
		service.SessionStats.ReleaseAuthenticated()
	}
}

// Close terminate current session, session related data will not be released,
// all related data should be Clear explicitly in Session closed callback
func (s *Session) Close() {
	s.entity.Close()
}

// Kick sends the protocol-level kick packet and lets the gateway close the connection.
func (s *Session) Kick(data []byte) error {
	entity, ok := s.entity.(kickEntity)
	if !ok {
		return ErrKickUnsupported
	}
	return entity.Kick(data)
}

// RemoteAddr returns the remote network address.
func (s *Session) RemoteAddr() net.Addr {
	return s.entity.RemoteAddr()
}

// Remove delete data associated with the key from session storage
func (s *Session) Remove(key string) {
	s.Lock()
	defer s.Unlock()

	delete(s.data, key)
}

// Set associates value with the key in session storage
func (s *Session) Set(key string, value interface{}) {
	s.Lock()
	defer s.Unlock()

	s.data[key] = value
}

// HasKey decides whether a key has associated value
func (s *Session) HasKey(key string) bool {
	s.RLock()
	defer s.RUnlock()

	_, has := s.data[key]
	return has
}

// Int returns the value associated with the key as a int.
func (s *Session) Int(key string) int {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int)
	if !ok {
		return 0
	}
	return value
}

// Int8 returns the value associated with the key as a int8.
func (s *Session) Int8(key string) int8 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int8)
	if !ok {
		return 0
	}
	return value
}

// Int16 returns the value associated with the key as a int16.
func (s *Session) Int16(key string) int16 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int16)
	if !ok {
		return 0
	}
	return value
}

// Int32 returns the value associated with the key as a int32.
func (s *Session) Int32(key string) int32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int32)
	if !ok {
		return 0
	}
	return value
}

// Int64 returns the value associated with the key as a int64.
func (s *Session) Int64(key string) int64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int64)
	if !ok {
		return 0
	}
	return value
}

// Uint returns the value associated with the key as a uint.
func (s *Session) Uint(key string) uint {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint)
	if !ok {
		return 0
	}
	return value
}

// Uint8 returns the value associated with the key as a uint8.
func (s *Session) Uint8(key string) uint8 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint8)
	if !ok {
		return 0
	}
	return value
}

// Uint16 returns the value associated with the key as a uint16.
func (s *Session) Uint16(key string) uint16 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint16)
	if !ok {
		return 0
	}
	return value
}

// Uint32 returns the value associated with the key as a uint32.
func (s *Session) Uint32(key string) uint32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint32)
	if !ok {
		return 0
	}
	return value
}

// Uint64 returns the value associated with the key as a uint64.
func (s *Session) Uint64(key string) uint64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint64)
	if !ok {
		return 0
	}
	return value
}

// Float32 returns the value associated with the key as a float32.
func (s *Session) Float32(key string) float32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(float32)
	if !ok {
		return 0
	}
	return value
}

// Float64 returns the value associated with the key as a float64.
func (s *Session) Float64(key string) float64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(float64)
	if !ok {
		return 0
	}
	return value
}

// String returns the value associated with the key as a string.
func (s *Session) String(key string) string {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return ""
	}

	value, ok := v.(string)
	if !ok {
		return ""
	}
	return value
}

// Value returns the value associated with the key as a interface{}.
func (s *Session) Value(key string) interface{} {
	s.RLock()
	defer s.RUnlock()

	return s.data[key]
}

// State returns all session state
func (s *Session) State() map[string]interface{} {
	s.RLock()
	defer s.RUnlock()

	return s.data
}

// Restore session state after reconnect
func (s *Session) Restore(data map[string]interface{}) {
	s.Lock()
	defer s.Unlock()

	s.data = data
}

// Clear releases all data related to current session
func (s *Session) Clear() {
	s.Release()
	s.Lock()
	defer s.Unlock()

	s.uid = 0
	s.data = map[string]interface{}{}
}
