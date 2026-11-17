package cluster

import (
	"bytes"
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

type queuedTestScheduler struct{ tasks []scheduler.Task }

func (q *queuedTestScheduler) Schedule(f scheduler.Task) { q.tasks = append(q.tasks, f) }

type rawPayloadService struct {
	got   []byte
	calls int
}

func (r *rawPayloadService) Raw(c *session.RequestContext, b []byte) error {
	r.got = append([]byte(nil), b...)
	r.calls++
	return nil
}
func newQueuedHandlerTest(ttl time.Duration) (*LocalHandler, *session.Session, *queuedTestScheduler, *rawPayloadService) {
	q := &queuedTestScheduler{}
	r := &rawPayloadService{}
	s := session.New(&concurrentHandlerResponseEntity{})
	s.Set("q", q)
	h := NewHandler(&Node{Options: Options{RequestTimeout: ttl}}, nil)
	m, _ := reflect.TypeOf(r).MethodByName("Raw")
	h.localServices["R"] = &component.Service{SchedName: "q"}
	h.localHandlers["R.Raw"] = &component.Handler{Receiver: reflect.ValueOf(r), Method: m, IsRawArg: true}
	return h, s, q, r
}
func TestRawHandlerOwnsQueuedPayload(t *testing.T) {
	h, s, q, r := newQueuedHandlerTest(time.Second)
	d := codec.NewDecoder()
	makePacket := func(body byte) []byte {
		m := &message.Message{Type: message.Notify, Route: "R.Raw", Data: bytes.Repeat([]byte{body}, 100)}
		b, _ := m.Encode()
		p, _ := codec.Encode(packet.Data, b)
		return p
	}
	pp, _ := d.Decode(makePacket('A'))
	m, _ := message.Decode(pp[0].Data)
	h.localProcess(context.Background(), h.localHandlers[m.Route], 0, s, m)
	d.Decode(makePacket('B'))
	q.tasks[0]()
	if !bytes.Equal(r.got, bytes.Repeat([]byte{'A'}, 100)) {
		t.Fatalf("queued handler body overwritten: first byte %q; want A", r.got[0])
	}
}
func TestExpiredQueuedRequestDoesNotInvokeHandler(t *testing.T) {
	h, s, q, r := newQueuedHandlerTest(time.Millisecond)
	m := &message.Message{Type: message.Request, ID: 1, Route: "R.Raw", Data: []byte("x")}
	h.localProcess(context.Background(), h.localHandlers[m.Route], 1, s, m)
	time.Sleep(20 * time.Millisecond)
	q.tasks[0]()
	if r.calls != 0 {
		t.Fatalf("expired request still invoked handler %d time(s)", r.calls)
	}
}
func TestHeartbeatTimestampConcurrentAccess(t *testing.T) {
	a := &agent{}
	h := &LocalHandler{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			atomic.LoadInt64(&a.lastAt)
		}
	}()
	for i := 0; i < 10000; i++ {
		h.processPacket(a, &packet.Packet{Type: packet.Heartbeat})
	}
	<-done
}
