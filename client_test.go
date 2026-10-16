package intrinio

import (
	"fmt"
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
// upgrades and records the channels clients join.
type fakeServer struct {
	*httptest.Server
	rejectDials atomic.Int32
	dials       atomic.Int32
	tokens      atomic.Int32
	mu          sync.Mutex
	headers     []http.Header
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
		s.mu.Lock()
		s.headers = append(s.headers, r.Header.Clone())
		s.mu.Unlock()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
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

func withoutBackoff(t *testing.T) {
	t.Helper()
	original := selfHealBackoffs
	selfHealBackoffs = [5]int{}
	t.Cleanup(func() { selfHealBackoffs = original })
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

	started := make(chan struct{})
	go func() {
		client.Start()
		close(started)
	}()
	waitFor(t, "a dial", func() bool { return s.dials.Load() > 0 })
	client.Stop()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Stop")
	}
}

// Unsynchronized, the subscriptions map made the runtime abort with "concurrent map read and map write". The client's
// isClosed and wsConn fields are still shared without synchronization, so -race reports those, not the map.
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
			go func() { <-client.reconnected }()
			client.tryResetWebSocket()
		}
	}()
	wg.Wait()
}
