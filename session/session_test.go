package session

import (
	"errors"
	"testing"

	"github.com/lonng/nano/service"
)

func TestNewSession(t *testing.T) {
	s := New(nil)
	if s.ID() < 1 {
		t.Fail()
	}
}

func TestSession_Bind(t *testing.T) {
	const testUID int64 = 900100001
	authenticatedBefore := service.SessionStats.Authenticated()
	onlineBefore := service.SessionStats.OnlinePlayers()
	s := New(nil)
	if err := s.Bind(testUID); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind(testUID); err != nil {
		t.Fatalf("binding the same UID should be idempotent: %v", err)
	}
	if err := s.Bind(testUID + 1); !errors.Is(err, ErrUIDMismatch) {
		t.Fatalf("Bind error = %v, want ErrUIDMismatch", err)
	}
	if got := s.UID(); got != testUID {
		t.Fatalf("UID = %d, want original UID %d", got, testUID)
	}
	if got := service.SessionStats.Authenticated(); got != authenticatedBefore+1 {
		t.Fatalf("authenticated sessions = %d, want %d", got, authenticatedBefore+1)
	}
	if got := service.SessionStats.OnlinePlayers(); got != onlineBefore+1 {
		t.Fatalf("online players = %d, want %d", got, onlineBefore+1)
	}
	s.Release()
	if got := service.SessionStats.Authenticated(); got != authenticatedBefore {
		t.Fatalf("authenticated sessions after release = %d, want %d", got, authenticatedBefore)
	}
	if got := service.SessionStats.OnlinePlayers(); got != onlineBefore {
		t.Fatalf("online players after release = %d, want %d", got, onlineBefore)
	}
}

func TestSessionBindWithResultReportsOnlyFirstTransition(t *testing.T) {
	s := New(nil)
	defer s.Release()

	bound, err := s.BindWithResult(100)
	if err != nil || !bound {
		t.Fatalf("first BindWithResult() = (%v, %v), want (true, nil)", bound, err)
	}
	bound, err = s.BindWithResult(100)
	if err != nil || bound {
		t.Fatalf("repeated BindWithResult() = (%v, %v), want (false, nil)", bound, err)
	}
}

func TestSessionClearReleasesAuthenticatedMetric(t *testing.T) {
	authenticatedBefore := service.SessionStats.Authenticated()
	s := New(nil)
	if err := s.Bind(101); err != nil {
		t.Fatal(err)
	}
	s.Clear()
	if got := service.SessionStats.Authenticated(); got != authenticatedBefore {
		t.Fatalf("authenticated sessions after clear = %d, want %d", got, authenticatedBefore)
	}
}

func TestSession_HasKey(t *testing.T) {
	s := New(nil)
	key := "hello"
	value := "world"
	s.Set(key, value)
	if !s.HasKey(key) {
		t.Fail()
	}
}

func TestSession_Float32(t *testing.T) {
	s := New(nil)
	key := "hello"
	value := float32(1.2000)
	s.Set(key, value)
	if value != s.Float32(key) {
		t.Fail()
	}
}

func TestSession_Float64(t *testing.T) {
	s := New(nil)
	key := "hello"
	value := 1.2000
	s.Set(key, value)
	if value != s.Float64(key) {
		t.Fail()
	}
}

func TestSession_Int(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := 234
	s.Set(key, value)
	if value != s.Int(key) {
		t.Fail()
	}
}

func TestSession_Int8(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := int8(123)
	s.Set(key, value)
	if value != s.Int8(key) {
		t.Fail()
	}
}

func TestSession_Int16(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := int16(3245)
	s.Set(key, value)
	if value != s.Int16(key) {
		t.Fail()
	}
}

func TestSession_Int32(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := int32(5454)
	s.Set(key, value)
	if value != s.Int32(key) {
		t.Fail()
	}
}

func TestSession_Int64(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := int64(444454)
	s.Set(key, value)
	if value != s.Int64(key) {
		t.Fail()
	}
}

func TestSession_Uint(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := uint(24254)
	s.Set(key, value)
	if value != s.Uint(key) {
		t.Fail()
	}
}

func TestSession_Uint8(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := uint8(34)
	s.Set(key, value)
	if value != s.Uint8(key) {
		t.Fail()
	}
}

func TestSession_Uint16(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := uint16(4645)
	s.Set(key, value)
	if value != s.Uint16(key) {
		t.Fail()
	}
}

func TestSession_Uint32(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := uint32(12365)
	s.Set(key, value)
	if value != s.Uint32(key) {
		t.Fail()
	}
}

func TestSession_Uint64(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := uint64(1000)
	s.Set(key, value)
	if value != s.Uint64(key) {
		t.Fail()
	}
}

func TestSession_State(t *testing.T) {
	s := New(nil)
	key := "testkey"
	value := uint64(1000)
	s.Set(key, value)
	state := s.State()
	if value != state[key].(uint64) {
		t.Fail()
	}
}

func TestSession_Restore(t *testing.T) {
	s := New(nil)
	s2 := New(nil)
	key := "testkey"
	value := uint64(1000)
	s.Set(key, value)
	state := s.State()
	s2.Restore(state)
	if value != s2.Uint64(key) {
		t.Fail()
	}
}
