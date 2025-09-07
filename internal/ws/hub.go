package ws

import (
    "compress/flate"
    "net/http"
    "sync"
    "sync/atomic"
    "time"

    "github.com/gorilla/websocket"
)

// Message is a generic JSON map payload to send to clients.
type Message map[string]any

// Client represents a single websocket client.
type Client struct {
    Conn *websocket.Conn
    Send chan Message
    Node string
}

// Hub manages websocket clients and broadcasts.
type Hub struct {
    mu        sync.RWMutex
    clients   map[*Client]struct{}
    nodeIndex map[string]map[*Client]struct{}
    MaxConns  int
    QueueSize int
    Workers   int
    WriteTimeoutSec int
    CloseOnError bool
    // metrics
    ClientsGauge          int64
    BroadcastTotal        int64
    SendErrorsTotal       int64
    QueueDropTotal        int64
    BroadcastNodesTotal   int64
    BroadcastDurationMs   int64
    closed    int32
}

// NewHub creates a new Hub with limits.
func NewHub(maxConns, queueSize, workers int) *Hub {
    if workers <= 0 { workers = 4 }
    return &Hub{clients: map[*Client]struct{}{}, nodeIndex: map[string]map[*Client]struct{}{}, MaxConns: maxConns, QueueSize: queueSize, Workers: workers, WriteTimeoutSec: 5, CloseOnError: true}
}

// Add adds a client to the hub.
func (h *Hub) Add(c *Client) {
    h.mu.Lock()
    h.clients[c] = struct{}{}
    atomic.AddInt64(&h.ClientsGauge, 1)
    h.mu.Unlock()
}

// Remove removes a client from the hub.
func (h *Hub) Remove(c *Client) {
    h.mu.Lock()
    if _, ok := h.clients[c]; ok {
        delete(h.clients, c)
        atomic.AddInt64(&h.ClientsGauge, -1)
    }
    if c.Node != "" {
        if set, ok := h.nodeIndex[c.Node]; ok {
            delete(set, c)
            if len(set) == 0 { delete(h.nodeIndex, c.Node) }
        }
    }
    h.mu.Unlock()
}

// IndexClient registers a client's node in the index.
func (h *Hub) IndexClient(c *Client, node string) {
    h.mu.Lock()
    c.Node = node
    set := h.nodeIndex[node]
    if set == nil { set = make(map[*Client]struct{}) }
    set[c] = struct{}{}
    h.nodeIndex[node] = set
    h.mu.Unlock()
}

// SnapshotClients returns a copy of all clients for iteration without holding locks.
func (h *Hub) SnapshotClients() []*Client {
    h.mu.RLock()
    out := make([]*Client, 0, len(h.clients))
    for c := range h.clients { out = append(out, c) }
    h.mu.RUnlock()
    return out
}

// SnapshotNode returns a copy set of clients for a node.
func (h *Hub) SnapshotNode(node string) []*Client {
    h.mu.RLock()
    set := h.nodeIndex[node]
    var out []*Client
    for c := range set { out = append(out, c) }
    h.mu.RUnlock()
    return out
}

// SnapshotNodeNames returns a copy of all node names currently indexed.
func (h *Hub) SnapshotNodeNames() []string {
    h.mu.RLock()
    names := make([]string, 0, len(h.nodeIndex))
    for n := range h.nodeIndex { names = append(names, n) }
    h.mu.RUnlock()
    return names
}

// Broadcast broadcasts a message to all clients using a worker pool.
func (h *Hub) Broadcast(v Message) {
    if atomic.LoadInt32(&h.closed) == 1 { return }
    atomic.AddInt64(&h.BroadcastTotal, 1)
    clients := h.SnapshotClients()
    start := time.Now()
    workers := h.Workers
    if workers <= 0 { workers = 4 }
    jobs := make(chan *Client)
    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for cl := range jobs {
                select {
                case cl.Send <- v:
                default:
                    atomic.AddInt64(&h.QueueDropTotal, 1)
                }
            }
        }()
    }
    for _, c := range clients { jobs <- c }
    close(jobs)
    wg.Wait()
    atomic.StoreInt64(&h.BroadcastDurationMs, time.Since(start).Milliseconds())
}

// SendToNode sends a message to all clients of a node.
func (h *Hub) SendToNode(node string, v Message) {
    if atomic.LoadInt32(&h.closed) == 1 { return }
    clients := h.SnapshotNode(node)
    for _, cl := range clients {
        select {
        case cl.Send <- v:
        default:
            atomic.AddInt64(&h.QueueDropTotal, 1)
        }
    }
}

// Upgrader returns a websocket upgrader with compression.
func Upgrader(checkOrigin func(*http.Request) bool) websocket.Upgrader {
    u := websocket.Upgrader{CheckOrigin: checkOrigin}
    u.EnableCompression = true
    return u
}

// InitClient prepares a client connection for writes and pings.
func InitClient(c *websocket.Conn, queueSize int, readSec int) *Client {
    cl := &Client{Conn: c, Send: make(chan Message, queueSize)}
    c.EnableWriteCompression(true)
    _ = c.SetCompressionLevel(flate.BestSpeed)
    c.SetReadLimit(1 << 20)
    c.SetReadDeadline(time.Now().Add(time.Duration(readSec) * time.Second))
    c.SetPongHandler(func(string) error { c.SetReadDeadline(time.Now().Add(time.Duration(readSec) * time.Second)); return nil })
    return cl
}

// Writer pumps messages from the client's queue to the websocket.
func (cl *Client) Writer(h *Hub) {
    for msg := range cl.Send {
        if h.WriteTimeoutSec > 0 { _ = cl.Conn.SetWriteDeadline(time.Now().Add(time.Duration(h.WriteTimeoutSec) * time.Second)) }
        if err := cl.Conn.WriteJSON(msg); err != nil {
            atomic.AddInt64(&h.SendErrorsTotal, 1)
            if h.CloseOnError { break } else { continue }
        }
    }
}

// Close drains clients and prevents future broadcasts
func (h *Hub) Close() {
    if !atomic.CompareAndSwapInt32(&h.closed, 0, 1) { return }
    h.mu.Lock()
    for c := range h.clients { close(c.Send); _ = c.Conn.Close() }
    h.clients = map[*Client]struct{}{}
    h.nodeIndex = map[string]map[*Client]struct{}{}
    h.mu.Unlock()
}


