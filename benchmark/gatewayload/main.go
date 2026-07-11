package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/packet"
)

type config struct {
	addr        string
	transport   string
	wsPath      string
	connections int
	rate        int
	parallel    int
	dialTimeout time.Duration
	hold        time.Duration
	reportEvery time.Duration
}

type metrics struct {
	attempted atomic.Int64
	connected atomic.Int64
	active    atomic.Int64
	failed    atomic.Int64
	closed    atomic.Int64
}

type connectionSet struct {
	mu    sync.Mutex
	conns []loadConn
}

type loadConn interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
	SetDeadline(time.Time) error
}

type websocketConn struct {
	conn      *websocket.Conn
	remainder []byte
}

func (c *websocketConn) Read(p []byte) (int, error) {
	if len(c.remainder) == 0 {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return 0, err
		}
		c.remainder = data
	}
	n := copy(p, c.remainder)
	c.remainder = c.remainder[n:]
	return n, nil
}

func (c *websocketConn) Write(p []byte) (int, error) {
	if err := c.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *websocketConn) Close() error { return c.conn.Close() }
func (c *websocketConn) SetDeadline(deadline time.Time) error {
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(deadline)
}

func (s *connectionSet) add(conn loadConn) {
	s.mu.Lock()
	s.conns = append(s.conns, conn)
	s.mu.Unlock()
}

func (s *connectionSet) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
}

func main() {
	cfg := parseFlags()
	if err := validate(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "参数错误：", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var stats metrics
	var conns connectionSet
	reportDone := make(chan struct{})
	go report(ctx, cfg, &stats, reportDone)

	startedAt := time.Now()
	fmt.Printf("开始压测 transport=%s addr=%s target=%d rate=%d/s parallel=%d\n", cfg.transport, cfg.addr, cfg.connections, cfg.rate, cfg.parallel)
	ramp(ctx, cfg, &stats, &conns)
	rampElapsed := time.Since(startedAt)
	fmt.Printf("爬升结束 elapsed=%s connected=%d active=%d failed=%d\n",
		rampElapsed.Round(time.Millisecond), stats.connected.Load(), stats.active.Load(), stats.failed.Load())

	if ctx.Err() == nil && cfg.hold > 0 {
		timer := time.NewTimer(cfg.hold)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}

	stop()
	conns.closeAll()
	<-reportDone

	attempted := stats.attempted.Load()
	failureRate := float64(0)
	if attempted > 0 {
		failureRate = float64(stats.failed.Load()) / float64(attempted) * 100
	}
	fmt.Printf("最终结果 attempted=%d connected=%d active=%d failed=%d closed=%d failure_rate=%.2f%%\n",
		attempted, stats.connected.Load(), stats.active.Load(), stats.failed.Load(), stats.closed.Load(), failureRate)
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:34590", "网关 TCP 监听地址")
	flag.StringVar(&cfg.transport, "transport", "ws", "传输类型：tcp 或 ws")
	flag.StringVar(&cfg.wsPath, "ws-path", "/nano", "WebSocket 路径")
	flag.IntVar(&cfg.connections, "connections", 10000, "目标连接数")
	flag.IntVar(&cfg.rate, "rate", 1000, "每秒发起的连接数")
	flag.IntVar(&cfg.parallel, "parallel", 256, "最大并行拨号数")
	flag.DurationVar(&cfg.dialTimeout, "dial-timeout", 5*time.Second, "连接和握手超时")
	flag.DurationVar(&cfg.hold, "hold", 2*time.Minute, "爬升完成后的保持时间，0 表示等待 Ctrl+C")
	flag.DurationVar(&cfg.reportEvery, "report", time.Second, "指标输出间隔")
	flag.Parse()
	return cfg
}

func validate(cfg config) error {
	if cfg.addr == "" {
		return errors.New("addr 不能为空")
	}
	if cfg.transport != "tcp" && cfg.transport != "ws" {
		return errors.New("transport 只能是 tcp 或 ws")
	}
	if cfg.connections <= 0 || cfg.rate <= 0 || cfg.parallel <= 0 {
		return errors.New("connections、rate、parallel 必须大于 0")
	}
	if cfg.dialTimeout <= 0 || cfg.reportEvery <= 0 || cfg.hold < 0 {
		return errors.New("时间参数无效")
	}
	return nil
}

func ramp(ctx context.Context, cfg config, stats *metrics, conns *connectionSet) {
	const ticksPerSecond = 10
	sem := make(chan struct{}, cfg.parallel)
	ticker := time.NewTicker(time.Second / ticksPerSecond)
	defer ticker.Stop()
	var wg sync.WaitGroup
	launched := 0
	tokens := 0
	for launched < cfg.connections {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
		}
		tokens += cfg.rate
		count := tokens / ticksPerSecond
		tokens %= ticksPerSecond
		if count == 0 {
			continue
		}
		if remaining := cfg.connections - launched; count > remaining {
			count = remaining
		}
		for i := 0; i < count; i++ {
			launched++
			stats.attempted.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				conn, err := connect(ctx, cfg, cfg.dialTimeout)
				<-sem
				if err != nil {
					stats.failed.Add(1)
					return
				}
				conns.add(conn)
				stats.connected.Add(1)
				stats.active.Add(1)
				go readLoop(ctx, conn, stats)
			}()
		}
	}
	wg.Wait()
}

func connect(ctx context.Context, cfg config, timeout time.Duration) (loadConn, error) {
	var conn loadConn
	var err error
	if cfg.transport == "ws" {
		dialer := websocket.Dialer{HandshakeTimeout: timeout}
		ws, _, dialErr := dialer.DialContext(ctx, "ws://"+cfg.addr+cfg.wsPath, nil)
		if dialErr != nil {
			return nil, dialErr
		}
		conn = &websocketConn{conn: ws}
	} else {
		dialer := net.Dialer{Timeout: timeout}
		conn, err = dialer.DialContext(ctx, "tcp", cfg.addr)
	}
	if err != nil {
		return nil, err
	}
	fail := func(err error) (loadConn, error) {
		_ = conn.Close()
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return fail(err)
	}
	handshake, err := codec.Encode(packet.Handshake, nil)
	if err != nil {
		return fail(err)
	}
	if _, err := conn.Write(handshake); err != nil {
		return fail(err)
	}
	decoder := codec.NewDecoder()
	buf := make([]byte, 2048)
	for {
		n, readErr := conn.Read(buf)
		if readErr != nil {
			return fail(readErr)
		}
		packets, decodeErr := decoder.Decode(buf[:n])
		if decodeErr != nil {
			return fail(decodeErr)
		}
		for _, p := range packets {
			if p.Type != packet.Handshake {
				continue
			}
			ack, encodeErr := codec.Encode(packet.HandshakeAck, nil)
			if encodeErr != nil {
				return fail(encodeErr)
			}
			if _, writeErr := conn.Write(ack); writeErr != nil {
				return fail(writeErr)
			}
			if err := conn.SetDeadline(time.Time{}); err != nil {
				return fail(err)
			}
			return conn, nil
		}
	}
}

func readLoop(ctx context.Context, conn loadConn, stats *metrics) {
	defer func() {
		stats.active.Add(-1)
		stats.closed.Add(1)
		_ = conn.Close()
	}()
	buf := make([]byte, 2048)
	for {
		if _, err := conn.Read(buf); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func report(ctx context.Context, cfg config, stats *metrics, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(cfg.reportEvery)
	defer ticker.Stop()
	started := time.Now()
	var previousConnected int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		connected := stats.connected.Load()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		fmt.Printf("elapsed=%-8s attempted=%-8d connected=%-8d active=%-8d failed=%-6d closed=%-6d connect_rate=%-7.0f/s loadgen_heap=%.1fMiB goroutines=%d\n",
			time.Since(started).Round(time.Second), stats.attempted.Load(), connected, stats.active.Load(),
			stats.failed.Load(), stats.closed.Load(), float64(connected-previousConnected)/cfg.reportEvery.Seconds(),
			float64(mem.HeapAlloc)/(1024*1024), runtime.NumGoroutine())
		previousConnected = connected
	}
}
