package intrinio

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var selfHealBackoffs [5]int = [5]int{10, 30, 60, 300, 600}

var reconnectDelay = 10 * time.Second

// stopGracePeriod is how long Stop waits for the server to close the socket after the close message before closing it
var stopGracePeriod = 5 * time.Second

// ErrStopped is returned by StartContext when Stop is called before the client connects
var ErrStopped = errors.New("intrinio client stopped")

const (
	HEARTBEAT_INTERVAL       int = 20
	MAX_OPTIONS_QUEUE_DEPTH  int = 20000
	MAX_EQUITIES_QUEUE_DEPTH int = 10000
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// doBackoff calls fn until it succeeds, sleeping the self-heal backoffs in between, and gives up once stop is closed,
// including before the first attempt. It reports whether fn succeeded.
func doBackoff(fn func() bool, stop <-chan struct{}) bool {
	for i := 0; ; i = min(i+1, len(selfHealBackoffs)-1) {
		select {
		case <-stop:
			return false
		default:
		}
		if fn() {
			return true
		}
		if !sleepOrStop(time.Duration(selfHealBackoffs[i])*time.Second, stop) {
			return false
		}
	}
}

// sleepOrStop sleeps for d and reports false instead if stop is closed first
func sleepOrStop(d time.Duration, stop <-chan struct{}) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-stop:
		return false
	}
}

type Client struct {
	token           string
	tokenUpdateTime time.Time
	dataMsgCount    uint64
	txtMsgCount     uint32
	workerCount     int
	subscriptions   map[string]bool
	subscriptionsMu sync.Mutex // shared by the caller's joins and leaves and the reconnect's rejoin; also guards isClosed changes
	isClosed        bool
	lifecycleMu     sync.Mutex     // guards stop and adding to runWg
	stop            chan struct{}  // the running run's stop signal, closed by Stop
	runWg           sync.WaitGroup // every goroutine of the running run: workers, read, write and reconnect
	readChannel     chan []byte
	writeChannel    chan []byte
	httpClient      *http.Client
	wsConn          *websocket.Conn
	heartbeat       *time.Ticker
	config          Config
	work            func(stop <-chan struct{})
	onDisconnect    func()
	composeJoinMsg  func(string) []byte
	composeLeaveMsg func(string) []byte
}

func NewOptionsClient(
	c Config,
	onTrade func(OptionTrade),
	onQuote func(OptionQuote),
	onRefresh func(OptionRefresh),
	onUnusualActivity func(OptionUnusualActivity)) *Client {
	client := &Client{
		isClosed:      true,
		workerCount:   1,
		readChannel:   make(chan []byte, MAX_OPTIONS_QUEUE_DEPTH),
		writeChannel:  make(chan []byte, 1000),
		subscriptions: make(map[string]bool),
		httpClient:    http.DefaultClient,
		config:        c,
	}
	if onTrade != nil {
		client.workerCount++
	}
	if onQuote != nil {
		client.workerCount += 8
	}
	client.work = func(stop <-chan struct{}) {
		for {
			select {
			case data := <-client.readChannel:
				workOnOptions(data, onTrade, onQuote, onRefresh, onUnusualActivity)
			case <-stop:
				return
			}
		}
	}
	client.composeJoinMsg = func(symbol string) []byte {
		return composeOptionJoinMsg(
			onTrade != nil,
			onQuote != nil,
			onRefresh != nil,
			onUnusualActivity != nil,
			symbol)
	}
	client.composeLeaveMsg = composeOptionLeaveMsg
	return client
}

func NewEquitiesClient(
	c Config,
	onTrade func(EquityTrade),
	onQuote func(EquityQuote)) *Client {
	client := &Client{
		isClosed:      true,
		workerCount:   2,
		readChannel:   make(chan []byte, MAX_EQUITIES_QUEUE_DEPTH),
		writeChannel:  make(chan []byte, 1000),
		subscriptions: make(map[string]bool),
		httpClient:    http.DefaultClient,
		config:        c,
	}
	if onQuote != nil {
		client.workerCount += 2
	}
	client.work = func(stop <-chan struct{}) {
		for {
			select {
			case data := <-client.readChannel:
				workOnEquities(data, onTrade, onQuote)
			case <-stop:
				return
			}
		}
	}
	client.composeJoinMsg = func(symbol string) []byte {
		return composeEquityJoinMsg(
			onTrade != nil,
			onQuote != nil,
			symbol)
	}
	client.composeLeaveMsg = composeEquityLeaveMsg
	return client
}

func (client *Client) trySetToken() bool {
	log.Print("Client - Authorizing...")
	authUrl := client.config.getAuthUrl()
	req, httpNewReqErr := http.NewRequest("GET", authUrl, nil)
	if httpNewReqErr != nil {
		log.Printf("Client - Authorization Failure: %v\n", httpNewReqErr)
		return false
	}
	req.Header.Add("Client-Information", "IntrinioRealtimeOptionsGoSDKv3.1")
	resp, httpDoErr := client.httpClient.Do(req)
	if httpDoErr != nil {
		log.Printf("Client - Authorization Failure: %v\n", httpDoErr)
		return false
	}
	if resp.StatusCode != 200 {
		log.Printf("Client - Authorization Failure: %v\n", resp.Status)
		return false
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		log.Printf("Client - Authorization Failure: %v\n", readErr)
		return false
	}
	client.token = string(body)
	client.tokenUpdateTime = time.Now()
	log.Print("Client - Authorization successful")
	return true
}

// getToken returns the cached token, authorizing again (with backoff) once it is a day old. It reports false if stop
// is closed before that succeeds.
func (client *Client) getToken(stop <-chan struct{}) (string, bool) {
	if time.Since(client.tokenUpdateTime) < (24 * time.Hour) {
		return client.token, true
	}
	return client.token, doBackoff(client.trySetToken, stop)
}

// wsHeader selects the message format the parsers expect, on the first connection and every reconnection alike
func wsHeader() map[string][]string {
	return map[string][]string{"UseNewEquitiesFormat": {"v2"}, "Client-Information": {"IntrinioRealtimeOptionsGoSDKv3.1"}}
}

func (client *Client) initWebSocket(token string) bool {
	log.Println("Client - Connecting...")
	wsUrl := client.config.getWSUrl(token)
	dialer := websocket.Dialer{
		ReadBufferSize:  10240,
		WriteBufferSize: 128,
	}
	conn, resp, dialErr := dialer.Dial(wsUrl, wsHeader())
	if dialErr != nil {
		log.Printf("Client - Connection failure: %v\n", dialErr)
		return false
	}
	log.Printf("Client - Status: %s\n", resp.Status)
	client.wsConn = conn
	if reflect.ValueOf(client.heartbeat).IsZero() {
		//log.Println("Client - Starting heartbeat")
		client.heartbeat = time.NewTicker(20 * time.Second)
	}
	return true
}

// markOpen queues joins for the subscriptions that the closed connection did not carry and lets new joins through.
// Holding subscriptionsMu across both keeps a concurrent join from being skipped by both.
func (client *Client) markOpen() {
	client.subscriptionsMu.Lock()
	defer client.subscriptionsMu.Unlock()
	for key := range client.subscriptions {
		client.writeChannel <- client.composeJoinMsg(key)
	}
	client.isClosed = false
}

func (client *Client) tryResetWebSocket() bool {
	wsUrl := client.config.getWSUrl(client.token)
	dialer := websocket.Dialer{
		ReadBufferSize:  10240,
		WriteBufferSize: 128,
	}
	conn, resp, dialErr := dialer.Dial(wsUrl, wsHeader())
	if dialErr != nil {
		log.Printf("Client - Reconnection failure: %v\n", dialErr)
		return false
	}
	log.Printf("Client - Status: %s\n", resp.Status)
	client.wsConn = conn
	log.Printf("Client - Rejoining")
	client.markOpen()
	return true
}

// reconnect redials until it succeeds, then closes reconnected; it gives up once the run is stopped
func (client *Client) reconnect(stop <-chan struct{}, reconnected chan<- struct{}) {
	defer client.runWg.Done()
	client.wsConn.Close()
	if !sleepOrStop(reconnectDelay, stop) {
		return
	}
	if doBackoff(func() bool {
		log.Println("Client - Reconnecting...")
		if time.Since(client.tokenUpdateTime) >= (24*time.Hour) && !client.trySetToken() {
			return false
		}
		if client.tryResetWebSocket() {
			return true
		}
		client.tokenUpdateTime = time.Time{} // the token may have been rejected, authorize again before the next dial
		return false
	}, stop) {
		close(reconnected)
	}
}

func (client *Client) write(stop <-chan struct{}) {
	defer client.runWg.Done()
	for {
		select {
		case <-stop:
			client.drainWrites()
			time.Sleep(500 * time.Millisecond)
			log.Println("Client - Sending close message")
			client.wsConn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(time.Second*2))
			return
		default:
		}
		if client.isClosed {
			sleepOrStop(time.Second, stop)
			continue
		}
		select {
		case <-client.heartbeat.C:
			client.wsConn.WriteMessage(websocket.BinaryMessage, []byte{})
			client.LogStats()
		case data := <-client.writeChannel:
			client.wsConn.WriteMessage(websocket.BinaryMessage, data)
		case <-stop:
		}
	}
}

// drainWrites sends the queued messages (e.g. the leaves of a Stop) on a best effort basis
func (client *Client) drainWrites() {
	for {
		select {
		case data := <-client.writeChannel:
			client.wsConn.WriteMessage(websocket.BinaryMessage, data)
		default:
			return
		}
	}
}

func (client *Client) read(stop <-chan struct{}) {
	defer client.runWg.Done()
	var highWatermark int = cap(client.readChannel) * 9 / 10
	var queueFull bool = false
	for {
		msgType, data, err := client.wsConn.ReadMessage()
		if err != nil {
			client.isClosed = true
			log.Printf("Client - Received message '%v'\n", err)
			select {
			case <-stop:
				return
			default:
			}
			if client.onDisconnect != nil {
				client.onDisconnect()
			}
			reconnected := make(chan struct{})
			client.runWg.Add(1) // counted while read itself is, so a concurrent Stop still waits for it
			go client.reconnect(stop, reconnected)
			select {
			case <-reconnected:
				log.Println("Client - Reconnected")
			case <-stop:
				return
			}
		} else if msgType == websocket.BinaryMessage {
			client.dataMsgCount++
			select {
			case client.readChannel <- data:
				if queueFull && len(client.readChannel) < highWatermark {
					queueFull = false
					log.Println("Client - read channel draining")
				}
			default:
				if !queueFull {
					log.Println("Client - read channel full")
					queueFull = true
				}
			}
		} else if msgType == websocket.TextMessage {
			client.txtMsgCount++
			log.Printf("Client - %s\n", string(data))
		}
	}
}

// Start connects and starts streaming, retrying until it is connected or stopped.
func (client *Client) Start() {
	_ = client.StartContext(context.Background())
}

// StartContext connects and starts streaming, retrying with the self-heal backoff until it is connected. If ctx is done
// or Stop is called first it gives up and returns ctx's error or ErrStopped, and the client is not running.
func (client *Client) StartContext(ctx context.Context) error {
	client.lifecycleMu.Lock()
	stop := make(chan struct{})
	client.stop = stop
	client.lifecycleMu.Unlock()

	connectStop := make(chan struct{})
	connected := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
		case <-stop:
		case <-connected:
			return
		}
		close(connectStop)
	}()
	// Keep dialing until connected: read() and write() need a connection, and reading from a client that never
	// connected dereferences a nil one in its own goroutine, which no caller can recover and crashes the process.
	ok := doBackoff(func() bool {
		token, authorized := client.getToken(connectStop)
		if !authorized {
			return false
		}
		if client.initWebSocket(token) {
			return true
		}
		client.tokenUpdateTime = time.Time{} // the token may have been rejected, authorize again before the next dial
		return false
	}, connectStop)
	close(connected)

	client.lifecycleMu.Lock()
	defer client.lifecycleMu.Unlock()
	select {
	case <-stop:
		ok = false
	default:
	}
	if !ok || ctx.Err() != nil {
		if ok {
			client.wsConn.Close() // connected just as the start was abandoned
		}
		if client.stop == stop {
			client.stop = nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrStopped
	}
	client.markOpen()
	client.runWg.Add(client.workerCount + 2)
	for w := 0; w < client.workerCount; w++ {
		go func() {
			defer client.runWg.Done()
			client.work(stop)
		}()
	}
	go client.read(stop)
	go client.write(stop)
	return nil
}

// OnDisconnect sets a function the client calls, on its read goroutine, whenever the connection drops before it
// reconnects. Set it before starting the client.
func (client *Client) OnDisconnect(fn func()) {
	client.onDisconnect = fn
}

// Join subscribes to symbol. While the connection is down it is joined once the client reconnects.
func (client *Client) Join(symbol string) {
	s := strings.TrimSpace(symbol)
	if s != "" {
		client.subscriptionsMu.Lock()
		defer client.subscriptionsMu.Unlock()
		if !client.subscriptions[symbol] {
			client.subscriptions[symbol] = true
			if !client.isClosed {
				client.writeChannel <- client.composeJoinMsg(symbol)
			}
		}
	}
}

// JoinMany subscribes to symbols. While the connection is down they are joined once the client reconnects.
func (client *Client) JoinMany(symbols []string) {
	client.subscriptionsMu.Lock()
	defer client.subscriptionsMu.Unlock()
	for i := 0; i < len(symbols); i++ {
		s := strings.TrimSpace(symbols[i])
		if s != "" && !client.subscriptions[symbols[i]] {
			client.subscriptions[symbols[i]] = true
			if !client.isClosed {
				client.writeChannel <- client.composeJoinMsg(symbols[i])
			}
		}
	}
}

func (client *Client) JoinLobby() {
	client.subscriptionsMu.Lock()
	defer client.subscriptionsMu.Unlock()
	if !client.subscriptions["$FIREHOSE"] {
		client.subscriptions["$FIREHOSE"] = true
		if !client.isClosed {
			client.writeChannel <- client.composeJoinMsg("$FIREHOSE")
		}
	} else {
		log.Print("Client - lobby channel already joined")
	}
}

func (client *Client) LeaveAll() {
	client.subscriptionsMu.Lock()
	defer client.subscriptionsMu.Unlock()
	for key := range client.subscriptions {
		client.writeChannel <- client.composeLeaveMsg(key)
		delete(client.subscriptions, key)
	}
}

func (client *Client) Leave(symbol string) {
	s := strings.TrimSpace(symbol)
	if s != "" {
		client.subscriptionsMu.Lock()
		defer client.subscriptionsMu.Unlock()
		if client.subscriptions[symbol] {
			client.writeChannel <- client.composeLeaveMsg(symbol)
			delete(client.subscriptions, symbol)
		}
	}
}

func (client *Client) LeaveMany(symbols []string) {
	for i := 0; i < len(symbols); i++ {
		client.Leave(symbols[i])
	}
}

func (client *Client) LeaveLobby(composeLeave func(string)) {
	client.subscriptionsMu.Lock()
	defer client.subscriptionsMu.Unlock()
	if client.subscriptions["$FIREHOSE"] {
		client.writeChannel <- client.composeLeaveMsg("$FIREHOSE")
		delete(client.subscriptions, "$FIREHOSE")
	}
}

// Stop leaves every subscription, stops the client and waits for all of its goroutines, including a pending reconnect,
// so the client can be started again. A StartContext still connecting returns ErrStopped.
func (client *Client) Stop() {
	log.Println("Client - Stopping...")
	client.LeaveAll()
	client.lifecycleMu.Lock()
	if client.stop != nil {
		close(client.stop)
		client.stop = nil
	}
	client.lifecycleMu.Unlock()

	// write() sends the close message and the server then closes the socket, ending read(); close it ourselves if the
	// server does not answer so a dead connection cannot hold Stop
	stopped := make(chan struct{})
	go func() {
		client.runWg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(stopGracePeriod):
		client.wsConn.Close()
		<-stopped
	}
	if client.wsConn != nil {
		client.wsConn.Close()
	}
	client.isClosed = true
	// drop what the stopped run left queued so a later Start neither delivers stale messages nor sends stale leaves
	for {
		select {
		case <-client.readChannel:
		case <-client.writeChannel:
		default:
			//client.LogStats()
			log.Println("Client - Stopped")
			return
		}
	}
}

func (client *Client) LogStats() {
	log.Printf("Client - Data Message Count: %d, Queue Depth: %d", client.dataMsgCount, len(client.readChannel))
}
