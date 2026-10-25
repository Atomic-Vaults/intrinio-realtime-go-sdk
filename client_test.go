package intrinio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeServer is a MANUAL provider endpoint: it authorizes every key, rejects the first rejectDials websocket
// upgrades, records the channels clients join and can drop connections or push messages to them.
type fakeServer struct {
	*httptest.Server
	rejectDials atomic.Int32
	dials       atomic.Int32
	tokens      atomic.Int32
	open        atomic.Int32
	mu          sync.Mutex
	headers     []http.Header
	conns       []*websocket.Conn
	joins       chan string
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	s := &fakeServer{joins: make(chan string, 1000)}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "token-%d", s.tokens.Add(1))
	})
	mux.HandleFunc("/socket/websocket", func(w http.ResponseWriter, r *http.Request) {
		s.dials.Add(1)
		if s.rejectDials.Add(-1) >= 0 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.headers = append(s.headers, r.Header.Clone())
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		s.open.Add(1)
		defer s.open.Add(-1)
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if len(data) > 2 && data[0] == 74 {
				s.joins <- string(data[2:])
			}
		}
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func (s *fakeServer) config() Config {
	return Config{ApiKey: "key", Provider: MANUAL, IPAddress: strings.TrimPrefix(s.URL, "http://")}
}

func (s *fakeServer) connectedHeaders() []http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]http.Header(nil), s.headers...)
}

func (s *fakeServer) latestConn() *websocket.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[len(s.conns)-1]
}

// dropLatest closes the latest connection from the server side
func (s *fakeServer) dropLatest() {
	s.latestConn().Close()
}

func (s *fakeServer) send(t *testing.T, data []byte) {
	t.Helper()
	if err := s.latestConn().WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("send: %v", err)
	}
}

// quoteMessage is a websocket message carrying one equities quote
func quoteMessage(quoteType QuoteType, symbol string, price float32, at time.Time) []byte {
	msg := []byte{byte(quoteType), 0, byte(len(symbol))}
	msg = append(msg, symbol...)
	msg = append(msg, 0) // source
	msg = binary.LittleEndian.AppendUint16(msg, uint16('Q'))
	msg = binary.LittleEndian.AppendUint32(msg, math.Float32bits(price))
	msg = binary.LittleEndian.AppendUint32(msg, 100)
	msg = binary.LittleEndian.AppendUint64(msg, uint64(at.UnixNano()))
	msg = append(msg, 0) // conditions
	msg[1] = byte(len(msg))
	return append([]byte{1}, msg...)
}

func withoutBackoff(t *testing.T) {
	t.Helper()
	originalBackoffs, originalDelay := selfHealBackoffs, reconnectDelay
	selfHealBackoffs, reconnectDelay = [5]int{}, 0
	t.Cleanup(func() { selfHealBackoffs, reconnectDelay = originalBackoffs, originalDelay })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func receiveJoin(t *testing.T, s *fakeServer) string {
	t.Helper()
	select {
	case symbol := <-s.joins:
		return symbol
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a join")
		return ""
	}
}

func TestStartRetriesAFailedDial(t *testing.T) {
	withoutBackoff(t)
	s := newFakeServer(t)
	s.rejectDials.Store(2)
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) {})

	// a failed first dial used to leave the client without a connection and crash the process in read()
	client.Start()
	if dials := s.dials.Load(); dials != 3 {
		t.Fatalf("dials = %d, want 3", dials)
	}
	// a rejected dial may be a rejected token, so every retry authorizes again
	if tokens := s.tokens.Load(); tokens != 3 {
		t.Fatalf("tokens = %d, want 3", tokens)
	}
	client.JoinMany([]string{"AMD"})
	if symbol := receiveJoin(t, s); symbol != "AMD" {
		t.Fatalf("joined %q, want AMD", symbol)
	}
	client.Stop()
}

func TestStartReturnsWhenStoppedBeforeConnecting(t *testing.T) {
	withoutBackoff(t)
	s := newFakeServer(t)
	s.rejectDials.Store(1 << 30)
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) {})

	started := make(chan error)
	go func() { started <- client.StartContext(context.Background()) }()
	waitFor(t, "a dial", func() bool { return s.dials.Load() > 0 })
	client.Stop()
	select {
	case err := <-started:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("StartContext = %v, want ErrStopped", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

func TestStartContextGivesUpWhenCancelledDuringABackoff(t *testing.T) {
	s := newFakeServer(t)
	s.rejectDials.Store(1 << 30)
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) {})

	// the production backoff sleeps 10s after the first failed dial; cancelling must not wait for it
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error)
	go func() { started <- client.StartContext(ctx) }()
	waitFor(t, "a dial", func() bool { return s.dials.Load() > 0 })
	cancel()
	select {
	case err := <-started:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StartContext = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StartContext did not return promptly after the context was cancelled")
	}
	if dials := s.dials.Load(); dials != 1 {
		t.Fatalf("dials = %d, want 1", dials)
	}
}

func TestReconnectRejoinsWithTheInitialHeaders(t *testing.T) {
	withoutBackoff(t)
	s := newFakeServer(t)
	disconnects := atomic.Int32{}
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) {})
	client.OnDisconnect(func() { disconnects.Add(1) })
	client.Start()
	client.JoinMany([]string{"AMD"})
	receiveJoin(t, s)

	// drop the connection: read() fails and the client reconnects, its first attempt rejected
	s.rejectDials.Store(1)
	s.dropLatest()
	if symbol := receiveJoin(t, s); symbol != "AMD" {
		t.Fatalf("rejoined %q, want AMD", symbol)
	}
	if n := disconnects.Load(); n != 1 {
		t.Fatalf("disconnects = %d, want 1", n)
	}
	// a rejected dial may be a rejected token, so the retry authorized again
	if tokens := s.tokens.Load(); tokens != 2 {
		t.Fatalf("tokens = %d, want 2", tokens)
	}
	headers := s.connectedHeaders()
	if len(headers) != 2 {
		t.Fatalf("connections = %d, want 2", len(headers))
	}
	for _, name := range []string{"UseNewEquitiesFormat", "Client-Information"} {
		if initial, reconnected := headers[0].Get(name), headers[1].Get(name); initial == "" || initial != reconnected {
			t.Errorf("%s header: initial %q, reconnected %q", name, initial, reconnected)
		}
	}
	client.Stop()
}

func TestJoinWhileDisconnectedJoinsOnReconnect(t *testing.T) {
	withoutBackoff(t)
	reconnectDelay = 200 * time.Millisecond
	s := newFakeServer(t)
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) {})
	client.Start()
	client.JoinMany([]string{"AMD"})
	receiveJoin(t, s)

	s.dropLatest()
	waitFor(t, "the drop to be seen", func() bool {
		client.subscriptionsMu.Lock()
		defer client.subscriptionsMu.Unlock()
		return client.isClosed
	})
	// joining while the connection is down returns at once and is sent with the rejoin
	joined := make(chan struct{})
	go func() {
		client.JoinMany([]string{"TSLA"})
		close(joined)
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("JoinMany blocked while the connection was down")
	}
	got := map[string]bool{receiveJoin(t, s): true, receiveJoin(t, s): true}
	if !got["AMD"] || !got["TSLA"] {
		t.Fatalf("rejoined %v, want AMD and TSLA", got)
	}
	client.Stop()
}

// Stopping a client while it was waiting to reconnect left that reconnect running, and starting the client again
// revived it: it dialed another socket and woke the stopped run's reader, so two readers (or writers) shared a socket
// and the client's goroutines panicked.
func TestStopDuringReconnectThenStartDoesNotReviveTheStoppedRun(t *testing.T) {
	withoutBackoff(t)
	reconnectDelay = 300 * time.Millisecond
	s := newFakeServer(t)
	quotes := atomic.Int32{}
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) { quotes.Add(1) })
	client.Start()
	client.JoinMany([]string{"AMD"})
	receiveJoin(t, s)

	s.dropLatest() // the client now waits reconnectDelay before redialing
	waitFor(t, "the drop to be seen", func() bool {
		client.subscriptionsMu.Lock()
		defer client.subscriptionsMu.Unlock()
		return client.isClosed
	})
	client.Stop()
	client.Start()
	client.JoinMany([]string{"AMD"})
	receiveJoin(t, s)

	// well past the stopped run's reconnect delay: only the restarted connection exists
	time.Sleep(3 * reconnectDelay)
	if dials := s.dials.Load(); dials != 2 {
		t.Fatalf("dials = %d, want 2 (the stopped run reconnected)", dials)
	}
	if open := s.open.Load(); open != 1 {
		t.Fatalf("open connections = %d, want 1", open)
	}
	// and a quote is handled exactly once
	s.send(t, quoteMessage(BID, "AMD", 200.5, time.Now()))
	waitFor(t, "the quote", func() bool { return quotes.Load() == 1 })
	time.Sleep(100 * time.Millisecond)
	if n := quotes.Load(); n != 1 {
		t.Fatalf("quotes handled = %d, want 1", n)
	}
	client.Stop()
	waitFor(t, "the server to see the close", func() bool { return s.open.Load() == 0 })
}

func TestStopAndStartBackToBack(t *testing.T) {
	withoutBackoff(t)
	s := newFakeServer(t)
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) {})
	for i := 0; i < 5; i++ {
		client.Start()
		client.JoinMany([]string{"AMD"})
		receiveJoin(t, s)
		client.Stop()
	}
	waitFor(t, "every connection to close", func() bool { return s.open.Load() == 0 })
}

func TestQuotesAreHandledWithoutWaitingForAPoll(t *testing.T) {
	withoutBackoff(t)
	s := newFakeServer(t)
	handled := make(chan time.Time, 1)
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) { handled <- time.Now() })
	client.Start()
	client.JoinMany([]string{"AMD"})
	receiveJoin(t, s)

	// let the workers go idle; they used to sleep a second whenever their queue was empty
	time.Sleep(1500 * time.Millisecond)
	sent := time.Now()
	s.send(t, quoteMessage(ASK, "AMD", 200.75, sent))
	select {
	case at := <-handled:
		if latency := at.Sub(sent); latency > 200*time.Millisecond {
			t.Fatalf("quote handled after %v", latency)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("quote not handled")
	}
	client.Stop()
}

func TestMalformedMessagesAreDropped(t *testing.T) {
	withoutBackoff(t)
	s := newFakeServer(t)
	quotes := make(chan EquityQuote, 1)
	client := NewEquitiesClient(s.config(), nil, func(q EquityQuote) { quotes <- q })
	client.Start()
	client.JoinMany([]string{"AMD"})
	receiveJoin(t, s)

	s.send(t, []byte{})
	s.send(t, quoteMessage(BID, "AMD", 1, time.Now())[:10]) // truncated
	s.send(t, quoteMessage(BID, "AMD", 200.5, time.Now()))
	select {
	case q := <-quotes:
		if q.Symbol != "AMD" || q.Price != 200.5 || q.Type != BID {
			t.Fatalf("quote = %+v", q)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("valid quote after malformed ones not handled")
	}
	client.Stop()
}

// Unsynchronized, the subscriptions map made the runtime abort with "concurrent map read and map write". The client's
// wsConn field is still shared without synchronization, so -race reports that, not the map.
func TestSubscriptionsAreSafeForConcurrentUse(t *testing.T) {
	withoutBackoff(t)
	s := newFakeServer(t)
	client := NewEquitiesClient(s.config(), nil, func(EquityQuote) {})
	client.Start()
	go func() { // keep draining joins so the writer never backs up
		for range s.joins {
		}
	}()

	// the caller joins and leaves while a reconnect rejoins the subscriptions
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				symbol := fmt.Sprintf("S%d_%d", g, i)
				client.JoinMany([]string{symbol, symbol + "X"})
				client.Join(symbol + "Y")
				client.Leave(symbol)
				if i%50 == 0 {
					client.LeaveAll()
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			client.tryResetWebSocket()
		}
	}()
	wg.Wait()
}
