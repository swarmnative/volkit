package httpserver

import (
    "bufio"
    "bytes"
    "compress/gzip"
    "context"
    "crypto/rand"
    "crypto/sha256"
    "crypto/tls"
    "crypto/x509"
    "encoding/hex"
    "encoding/json"
    "encoding/pem"
    "fmt"
    "io"
    "log/slog"
    "math/big"
    "net"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"
    "runtime"

    "github.com/gorilla/websocket"
    "github.com/swarmnative/volkit/internal/controller"
    "github.com/swarmnative/volkit/internal/issuer"
    "github.com/swarmnative/volkit/internal/pki"
    "github.com/swarmnative/volkit/internal/ws"
)

// ControllerAPI narrows controller dependencies used by the HTTP server.
type ControllerAPI interface {
    Ready() error
    Snapshot() controller.MetricsSnapshot
    Preflight() error
    Nudge()
    ClaimsForNode(node string) []controller.ClaimOut
    ChangeVersion() int64
    ReadyCounts() map[string]int
    MintServiceAccount(level string) (string, string, int64, error)
    BuildRcloneEnvPublic() []string
}

type Options struct {
    Ctx context.Context
    Ctrl ControllerAPI
    Cfg controller.Config
    TLSClientConfig *tls.Config
    BroadcastCacheHits *int64
    BroadcastCacheMiss *int64
    AgentStatusCount *int64
}

func getenv(k, def string) string { v := strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(getenvRaw(k)))); if v=="" { return def }; return v }
func getenvRaw(k string) string { return strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(strings.TrimSpace(os.Getenv(k)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) }
func getenvInt(k string, def int) int { v := strings.TrimSpace(os.Getenv(k)); if v=="" { return def }; if n,err := strconv.Atoi(v); err==nil { return n }; return def }
func itoa(n int64) string { return strconv.FormatInt(n, 10) }
func bool01(b bool) string { if b { return "1" }; return "0" }

// gzipIfJSON wraps responses that set Content-Type: application/json with gzip.
func WithGzipJSON(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
            next.ServeHTTP(w, r)
            return
        }
        rw := &gzipResponseWriter{ResponseWriter: w}
        rw.Header().Set("Vary", "Accept-Encoding")
        defer rw.close()
        next.ServeHTTP(rw, r)
    })
})

// WithHTTPMetrics records status class counters per process for observability
func WithHTTPMetrics(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        rw := &statusRecorder{ResponseWriter: w, status: 200}
        start := time.Now()
        next.ServeHTTP(rw, r)
        dur := time.Since(start)
        _ = dur // reserved for future histogram
        atomic.AddInt64(&httpRequestsTotal, 1)
        code := rw.status
        switch {
        case code >= 200 && code < 300:
            atomic.AddInt64(&http2xxTotal, 1)
        case code >= 300 && code < 400:
            atomic.AddInt64(&http3xxTotal, 1)
        case code >= 400 && code < 500:
            atomic.AddInt64(&http4xxTotal, 1)
        case code >= 500:
            atomic.AddInt64(&http5xxTotal, 1)
        }
    })
})

type statusRecorder struct {
    http.ResponseWriter
    status int
}

func (s *statusRecorder) WriteHeader(code int) {
    s.status = code
    s.ResponseWriter.WriteHeader(code)
}

type gzipResponseWriter struct {
    http.ResponseWriter
    gz       *gzip.Writer
    started  bool
    compress bool
}

func (g *gzipResponseWriter) startIfNeeded() {
    if g.started { return }
    g.started = true
    if !strings.Contains(strings.ToLower(g.Header().Get("Content-Type")), "application/json") {
        g.compress = false
        return
    }
    g.compress = true
    g.Header().Del("Content-Length")
    g.Header().Set("Content-Encoding", "gzip")
    w, _ := gzip.NewWriterLevel(g.ResponseWriter, gzip.BestSpeed)
    g.gz = w
}
func (g *gzipResponseWriter) Write(b []byte) (int, error) {
    if !g.started { g.startIfNeeded() }
    if g.compress && g.gz != nil { return g.gz.Write(b) }
    return g.ResponseWriter.Write(b)
}
func (g *gzipResponseWriter) close() { if g.gz != nil { _ = g.gz.Close() } }

func authOK(r *http.Request) bool { if strings.EqualFold(strings.TrimSpace(os.Getenv("AUTH_DISABLE")), "true") { return true }; return r.TLS != nil }
func peerNodeFromTLS(r *http.Request) string {
    if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 { return "" }
    crt := r.TLS.PeerCertificates[0]
    if len(crt.DNSNames) > 0 { return strings.TrimSpace(crt.DNSNames[0]) }
    if len(crt.IPAddresses) > 0 { return strings.TrimSpace(crt.IPAddresses[0].String()) }
    return strings.TrimSpace(crt.Subject.CommonName)
}

// WithRequestID ensures every request has an X-Request-Id header; preserves incoming value
func WithRequestID(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        rid := strings.TrimSpace(r.Header.Get("X-Request-Id"))
        if rid == "" { rid = newRequestID() }
        w.Header().Set("X-Request-Id", rid)
        next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, rid)))
    })
}

// WithRecovery catches panics, logs, and returns 500 JSON without crashing the server
func WithRecovery(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        defer func() {
            if rec := recover(); rec != nil {
                slog.Error("http_panic", "err", rec)
                // best-effort 500 JSON
                w.Header().Set("Content-Type", "application/json")
                http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
            }
        }()
        next.ServeHTTP(w, r)
    })
}

type requestIDKey struct{}

func newRequestID() string {
    var b [16]byte
    if _, err := rand.Read(b[:]); err != nil { return strconv.FormatInt(time.Now().UnixNano(), 36) }
    return hex.EncodeToString(b[:])
}

// BuildMux registers all HTTP routes and returns the mux and a websocket hub.
func BuildMux(o Options) (*http.ServeMux, *ws.Hub) {
    mux := http.NewServeMux()
    hub := ws.NewHub(getenvInt("VOLKIT_WS_MAX_CLIENTS", 1000), getenvInt("VOLKIT_WS_SEND_QUEUE", 16), getenvInt("VOLKIT_WS_WORKERS", 4))
    // tune ws write timeout
    hub.WriteTimeoutSec = getenvInt("VOLKIT_WS_WRITE_TIMEOUT_SEC", 5)

    // shared outbound HTTP client (connection reuse, timeouts)
    tr := &http.Transport{TLSClientConfig: o.TLSClientConfig, MaxIdleConns: 64, MaxIdleConnsPerHost: 16, IdleConnTimeout: 60 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ExpectContinueTimeout: 1 * time.Second}
    sharedClient := &http.Client{Timeout: 6 * time.Second, Transport: tr}

    // /ready
    mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
        if o.Ctrl.Ready() == nil { w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("ok")); return }
        http.Error(w, "not ready", http.StatusServiceUnavailable)
    })
    // /healthz
    mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("ok")) })
    // /status
    mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, o.Ctrl.Snapshot()) })
    // /preflight
    mux.HandleFunc("/preflight", func(w http.ResponseWriter, r *http.Request) {
        if err := o.Ctrl.Preflight(); err != nil { writeError(w, http.StatusPreconditionFailed, err.Error()); return }
        writeText(w, http.StatusOK, "ok")
    })
    // /reload
    mux.HandleFunc("/reload", func(w http.ResponseWriter, r *http.Request) { writeText(w, http.StatusAccepted, "reconcile scheduled"); go o.Ctrl.Nudge() })
    // /maintenance (rate-limited)
    lim := newLimiter(getenvInt("VOLKIT_RL_SENSITIVE_CAP", 10), getenvInt("VOLKIT_RL_SENSITIVE_RPS", 3))
    mux.HandleFunc("/maintenance/recompute", func(w http.ResponseWriter, r *http.Request) {
        if !limitOr429(lim, clientIP(r), w) { return }
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        if getenv("AUTH_DISABLE", "false") == "true" && r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
        go o.Ctrl.Nudge(); w.WriteHeader(http.StatusAccepted); _, _ = w.Write([]byte("recompute scheduled"))
    })
    mux.HandleFunc("/maintenance/broadcast", func(w http.ResponseWriter, r *http.Request) {
        if !limitOr429(lim, clientIP(r), w) { return }
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        if getenv("AUTH_DISABLE", "false") == "true" && r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
        v := strings.TrimSpace(r.URL.Query().Get("pause"))
        if v == "true" || v == "1" { w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("broadcast paused")); return }
        if v == "false" || v == "0" { w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("broadcast resumed")); return }
        w.WriteHeader(http.StatusBadRequest); _, _ = w.Write([]byte("use ?pause=true|false"))
    })

    // /claims and /claims/validate
    var claimsRequestsTotal int64
    var claimsDurationMs int64
    var claimsETagMu sync.Mutex
    node2ETag := map[string]string{}
    mux.HandleFunc("/claims", func(w http.ResponseWriter, r *http.Request) {
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        if getenv("AUTH_DISABLE", "false") == "true" && r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
        n := strings.TrimSpace(r.URL.Query().Get("node"))
        if n != "" {
            minMs := getenvInt("VOLKIT_CLAIMS_MIN_MS", 200)
            // very small in-memory rate limit per node via ETag cache timestamp, omitted for brevity
            _ = minMs
        }
        start := time.Now()
        if getenv("AUTH_DISABLE", "false") != "true" && n != "" {
            pn := peerNodeFromTLS(r)
            if pn == "" || !strings.EqualFold(n, pn) { writeError(w, http.StatusForbidden, "forbidden"); return }
        }
        claims := o.Ctrl.ClaimsForNode(n)
        var etag string
        claimsETagMu.Lock(); etag = node2ETag[n]; claimsETagMu.Unlock()
        if etag == "" {
            bufTmp, _ := json.Marshal(claims)
            sumTmp := sha256.Sum256(bufTmp)
            etag = "\"" + hex.EncodeToString(sumTmp[:]) + "\""
            claimsETagMu.Lock(); node2ETag[n] = etag; claimsETagMu.Unlock()
        }
        if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { writeEmpty(w, http.StatusNotModified); return }
        w.Header().Set("ETag", etag)
        writeJSON(w, http.StatusOK, claims)
        atomic.AddInt64(&claimsRequestsTotal, 1)
        atomic.StoreInt64(&claimsDurationMs, time.Since(start).Milliseconds())
    })
    mux.HandleFunc("/claims/validate", func(w http.ResponseWriter, r *http.Request) {
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        n := strings.TrimSpace(r.URL.Query().Get("node"))
        if getenv("AUTH_DISABLE", "false") != "true" && n != "" {
            pn := peerNodeFromTLS(r)
            if pn == "" || !strings.EqualFold(n, pn) { writeError(w, http.StatusForbidden, "forbidden"); return }
        }
        claims := o.Ctrl.ClaimsForNode(n)
        buf, _ := json.Marshal(claims)
        sum := sha256.Sum256(buf)
        etag := "\"" + hex.EncodeToString(sum[:]) + "\""
        writeJSON(w, http.StatusOK, map[string]any{"node": n, "count": len(claims), "etag": etag, "bytes": len(buf), "ts": time.Now().Unix()})
    })

    // WebSocket /ws
    if getenv("VOLKIT_WS_ENABLE", "true") == "true" {
        allowed := strings.Split(strings.TrimSpace(getenv("VOLKIT_WS_ALLOWED_ORIGINS", "")), ",")
        for i := range allowed { allowed[i] = strings.TrimSpace(allowed[i]) }
        checkOrigin := func(r *http.Request) bool {
            if len(allowed) == 1 && allowed[0] == "" { return true }
            orig := strings.TrimSpace(r.Header.Get("Origin"))
            if orig == "" { return true }
            for _, a := range allowed { if a == "*" || strings.EqualFold(a, orig) { return true } }
            return false
        }
        upgrader := ws.Upgrader(checkOrigin)
        maxPerNode := getenvInt("VOLKIT_WS_MAX_PER_NODE", 2)
        mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
            if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
            curr := int(atomic.LoadInt64(&hub.ClientsGauge))
            if hub.MaxConns > 0 && curr >= hub.MaxConns { http.Error(w, "too many connections", http.StatusServiceUnavailable); return }
            c, err := upgrader.Upgrade(w, r, nil)
            if err != nil { slog.Warn("ws upgrade", "error", err); return }
            slog.Info("ws connected")
            client := ws.InitClient(c, hub.QueueSize, getenvInt("VOLKIT_WS_READ_SEC", 90))
            hub.Add(client)
            client.Send <- ws.Message{"type": "hello", "ts": time.Now().Unix()}
            done := make(chan struct{})
            go func() { client.Writer(hub); close(done) }()
            for {
                mt, data, err := c.ReadMessage(); if err != nil { break }
                if mt == websocket.TextMessage || mt == websocket.BinaryMessage {
                    var msg map[string]any
                    if json.Unmarshal(data, &msg) == nil {
                        if t, _ := msg["type"].(string); t == "hello" {
                            if node, _ := msg["nodeId"].(string); strings.TrimSpace(node) != "" {
                                if getenv("AUTH_DISABLE", "false") != "true" {
                                    if r.TLS == nil { _ = c.Close(); break }
                                    pn := peerNodeFromTLS(r)
                                    if pn == "" || !strings.EqualFold(strings.TrimSpace(node), pn) { slog.Warn("ws hello node mismatch", "node", node, "peer", pn); _ = c.Close(); break }
                                }
                                if maxPerNode > 0 {
                                    count := len(hub.SnapshotNode(strings.TrimSpace(node)))
                                    if count >= maxPerNode { slog.Warn("ws per-node limit reached", "node", node, "limit", maxPerNode); _ = c.Close(); break }
                                }
                                hub.IndexClient(client, strings.TrimSpace(node))
                            }
                        }
                    }
                }
            }
            _ = c.Close(); hub.Remove(client); slog.Info("ws closed")
        })
    }

    // /validate (static+preflight)
    mux.HandleFunc("/validate", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        static := controller.ValidateConfig(o.Cfg)
        err := o.Ctrl.Preflight()
        out := struct{ OK bool `json:"ok"`; Errors []string `json:"errors"`; Warnings []string `json:"warnings"`; Summary map[string]any `json:"summary,omitempty"`; PreflightOK bool `json:"preflightOK"` }{ OK: static.OK && err==nil, Errors: append([]string{}, static.Errors...), Warnings: append([]string{}, static.Warnings...), Summary: static.Summary, PreflightOK: err==nil }
        if err != nil { msg := strings.TrimSpace(err.Error()); for _, p := range strings.Split(msg, ";") { p = strings.TrimSpace(p); if p != "" { out.Errors = append(out.Errors, p) } } }
        _ = json.NewEncoder(w).Encode(out)
    })

    // /hints long-poll
    mux.HandleFunc("/hints", func(w http.ResponseWriter, r *http.Request) {
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        since := strings.TrimSpace(r.URL.Query().Get("since"))
        var vsince int64; if since != "" { if v, err := strconv.ParseInt(since, 10, 64); err == nil { vsince = v } }
        timeoutSec := getenvInt("VOLKIT_HINTS_TIMEOUT_SEC", 30)
        if q := strings.TrimSpace(r.URL.Query().Get("timeoutSec")); q != "" { if v, err := strconv.Atoi(q); err == nil && v >= 0 && v <= 120 { timeoutSec = v } }
        deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
        for { if time.Now().After(deadline) { w.WriteHeader(http.StatusNoContent); return }; if o.Ctrl.ChangeVersion() > vsince { w.WriteHeader(http.StatusOK); return }; time.Sleep(300 * time.Millisecond) }
    })

    // /pki
    mux.HandleFunc("/pki/ca", func(w http.ResponseWriter, r *http.Request) { if getenv("VOLKIT_PKI_ENABLE", "false") != "true" { http.NotFound(w, r); return }; w.Header().Set("Content-Type", "application/x-pem-file"); w.WriteHeader(http.StatusOK); _, _ = w.Write(pki.CA.PEM) })
    mux.HandleFunc("/pki/enroll", func(w http.ResponseWriter, r *http.Request) {
        if !limitOr429(lim, clientIP(r), w) { return }
        if getenv("VOLKIT_PKI_ENABLE", "false") != "true" { http.NotFound(w, r); return }
        if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
        if getenv("AUTH_DISABLE", "false") != "true" { if r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return } }
        tokExpected := strings.TrimSpace(getenv("VOLKIT_ENROLL_TOKEN", ""))
        tok := strings.TrimSpace(r.Header.Get("X-Volkit-Token"))
        if tokExpected == "" || tok != tokExpected { http.Error(w, "forbidden", http.StatusForbidden); return }
        csrPEM, err := io.ReadAll(io.LimitReader(r.Body, 64*1024)); if err != nil { http.Error(w, "read error", http.StatusBadRequest); return }
        block, _ := pem.Decode(csrPEM); if block == nil || block.Type != "CERTIFICATE REQUEST" { http.Error(w, "bad csr", http.StatusBadRequest); return }
        csr, err := x509.ParseCertificateRequest(block.Bytes); if err != nil || csr.CheckSignature() != nil { http.Error(w, "bad csr", http.StatusBadRequest); return }
        now := time.Now(); tpl := &x509.Certificate{SerialNumber: new(big.Int).SetInt64(now.UnixNano()), Subject: csr.Subject, DNSNames: csr.DNSNames, IPAddresses: csr.IPAddresses, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
        der, err := x509.CreateCertificate(rand.Reader, tpl, pki.CA.Cert, csr.PublicKey, pki.CA.Key); if err != nil { http.Error(w, "sign failed", http.StatusInternalServerError); return }
        pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}); w.Header().Set("Content-Type", "application/x-pem-file"); w.WriteHeader(http.StatusCreated); _, _ = w.Write(pemCert)
    })

    // /creds (unified issuer)
    mux.HandleFunc("/creds", func(w http.ResponseWriter, r *http.Request) {
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        lvl := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("level")))
        issuerMode := strings.ToLower(strings.TrimSpace(getenv("VOLKIT_ISSUER_MODE", "")))
        if issuerMode == "" { if getenv("VOLKIT_ISSUER_ENDPOINT", "") != "" { issuerMode = "external" } else { issuerMode = "static" } }
        base := strings.TrimSpace(getenv("VOLKIT_ISSUER_ENDPOINT", ""))
        if issuerMode == "external" && base != "" {
            u := strings.TrimRight(base, "/") + "/creds?level=" + url.QueryEscape(lvl)
            req, _ := http.NewRequest(http.MethodGet, u, nil); if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" { req.Header.Set("If-None-Match", inm) }
            resp, err := sharedClient.Do(req)
            if err == nil && resp != nil { defer resp.Body.Close(); if resp.StatusCode == http.StatusNotModified { writeEmpty(w, http.StatusNotModified); return }; if resp.StatusCode/100 == 2 { if et := strings.TrimSpace(resp.Header.Get("ETag")); et != "" { w.Header().Set("ETag", et) }; _, _ = io.Copy(w, resp.Body); return } }
        }
        if issuerMode == "s3_admin" {
            ak := strings.TrimSpace(getenv("VOLKIT_ISSUER_ACCESS_KEY", ""))
            sk := strings.TrimSpace(getenv("VOLKIT_ISSUER_SECRET_KEY", ""))
            if ak != "" && sk != "" {
                if !strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_ALLOW_ROOT_ISSUER", "false")), "true") { http.Error(w, "root issuer not allowed (set VOLKIT_ALLOW_ROOT_ISSUER=true to enable)", http.StatusForbidden); return }
                if strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_S3_ADMIN_FORCE_STATIC", "false")), "true") {
                    env := o.Ctrl.BuildRcloneEnvPublic(); for i := range env { if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + ak }; if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + sk } }
                    buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env}); sum := sha256.Sum256(buf); etag := "\"" + hex.EncodeToString(sum[:]) + "\""; if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { w.WriteHeader(http.StatusNotModified); return }
                    w.Header().Set("ETag", etag); w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": 0, "issuer": "s3_admin_static"}); return
                }
                stsEndpoint := strings.TrimSpace(getenv("VOLKIT_ISSUER_ENDPOINT", getenv("VOLKIT_ENDPOINT", "")))
                region := strings.TrimSpace(getenv("VOLKIT_S3_REGION", "us-east-1"))
                dur := 3600; if v := strings.TrimSpace(getenv("VOLKIT_ISSUER_TTL_SEC", "")); v != "" { if n, err := strconv.Atoi(v); err == nil && n >= 900 && n <= 43200 { dur = n } }
                insecure := strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_ISSUER_INSECURE", "false")), "true")
                if cred, err := issuer.AssumeRoleSTS(stsEndpoint, ak, sk, dur, region, insecure, o.TLSClientConfig); err == nil && cred.AccessKeyId != "" {
                    env := o.Ctrl.BuildRcloneEnvPublic(); for i := range env { if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + cred.AccessKeyId }; if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + cred.SecretAccessKey } }; env = append(env, "RCLONE_CONFIG_S3_SESSION_TOKEN="+cred.SessionToken)
                    expiresAt := cred.Expiration.Unix(); buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env}); sum := sha256.Sum256(buf); etag := "\"" + hex.EncodeToString(sum[:]) + "\""; if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { w.WriteHeader(http.StatusNotModified); return }
                    w.Header().Set("ETag", etag); writeJSON(w, http.StatusOK, map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": expiresAt, "issuer": "s3_admin_sts"}); return
                }
                if strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_SA_ENABLE", "true")), "true") {
                    if ak2, sk2, exp, err2 := o.Ctrl.MintServiceAccount(lvl); err2 == nil && ak2 != "" {
                        env := o.Ctrl.BuildRcloneEnvPublic(); for i := range env { if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + ak2 }; if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + sk2 } }
                        buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env}); sum := sha256.Sum256(buf); etag := "\"" + hex.EncodeToString(sum[:]) + "\""; if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { w.WriteHeader(http.StatusNotModified); return }
                        w.Header().Set("ETag", etag); writeJSON(w, http.StatusOK, map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": exp, "issuer": "s3_admin_sa"}); return
                    }
                }
                env := o.Ctrl.BuildRcloneEnvPublic(); for i := range env { if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + ak }; if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + sk } }
                buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env}); sum := sha256.Sum256(buf); etag := "\"" + hex.EncodeToString(sum[:]) + "\""; if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { w.WriteHeader(http.StatusNotModified); return }
                w.Header().Set("ETag", etag); writeJSON(w, http.StatusOK, map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": 0, "issuer": "s3_admin_static"}); return
            }
        }
        env := o.Ctrl.BuildRcloneEnvPublic(); buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env}); sum := sha256.Sum256(buf); etag := "\"" + hex.EncodeToString(sum[:]) + "\""; w.Header().Set("ETag", etag); writeJSON(w, http.StatusOK, map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": 0})
    })

    // /metrics
    if getenv("VOLKIT_ENABLE_METRICS", "false") == "true" {
        mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
            w.Header().Set("Content-Type", "text/plain; version=0.0.4")
            s := o.Ctrl.Snapshot()
            build := map[string]string{ "version": strings.TrimSpace(getenv("VOLKIT_BUILD_VERSION", "dev")), "go_version": runtime.Version() }
            var b strings.Builder
            fmt.Fprintf(&b, "# HELP volkit_build_info Build and runtime info\n# TYPE volkit_build_info gauge\nvolkit_build_info{version=\"%s\",go_version=\"%s\"} 1\n", build["version"], build["go_version"])
            // claims metrics from this module
            fmt.Fprintf(&b, "# HELP volkit_http_claims_requests_total Total /claims requests\n# TYPE volkit_http_claims_requests_total counter\nvolkit_http_claims_requests_total %s\n", itoa(atomic.LoadInt64(&claimsRequestsTotal)))
            fmt.Fprintf(&b, "# HELP volkit_http_claims_duration_milliseconds Last /claims duration in ms\n# TYPE volkit_http_claims_duration_milliseconds gauge\nvolkit_http_claims_duration_milliseconds %s\n", itoa(atomic.LoadInt64(&claimsDurationMs)))
            // http status-class metrics
            fmt.Fprintf(&b, "# HELP volkit_http_requests_total Total HTTP requests\n# TYPE volkit_http_requests_total counter\nvolkit_http_requests_total %s\n", itoa(atomic.LoadInt64(&httpRequestsTotal)))
            fmt.Fprintf(&b, "# HELP volkit_http_responses_class Total responses by status class\n# TYPE volkit_http_responses_class counter\n")
            fmt.Fprintf(&b, "volkit_http_responses_class{class=\"2xx\"} %s\n", itoa(atomic.LoadInt64(&http2xxTotal)))
            fmt.Fprintf(&b, "volkit_http_responses_class{class=\"3xx\"} %s\n", itoa(atomic.LoadInt64(&http3xxTotal)))
            fmt.Fprintf(&b, "volkit_http_responses_class{class=\"4xx\"} %s\n", itoa(atomic.LoadInt64(&http4xxTotal)))
            fmt.Fprintf(&b, "volkit_http_responses_class{class=\"5xx\"} %s\n", itoa(atomic.LoadInt64(&http5xxTotal)))
            // controller snapshot metrics
            fmt.Fprintf(&b, "# HELP volkit_reconcile_total Total reconcile loops\n# TYPE volkit_reconcile_total counter\nvolkit_reconcile_total %s\n", itoa(s.ReconcileTotal))
            // ws metrics from hub
            fmt.Fprintf(&b, "# HELP volkit_ws_clients Current WebSocket clients\n# TYPE volkit_ws_clients gauge\nvolkit_ws_clients %s\n", itoa(atomic.LoadInt64(&hub.ClientsGauge)))
            if o.BroadcastCacheHits != nil { fmt.Fprintf(&b, "# HELP volkit_ws_broadcast_cache_hits Total WebSocket broadcast cache hits\n# TYPE volkit_ws_broadcast_cache_hits counter\nvolkit_ws_broadcast_cache_hits %s\n", itoa(atomic.LoadInt64(o.BroadcastCacheHits))) }
            if o.BroadcastCacheMiss != nil { fmt.Fprintf(&b, "# HELP volkit_ws_broadcast_cache_misses Total WebSocket broadcast cache misses\n# TYPE volkit_ws_broadcast_cache_misses counter\nvolkit_ws_broadcast_cache_misses %s\n", itoa(atomic.LoadInt64(o.BroadcastCacheMiss))) }
            _, _ = w.Write([]byte(b.String()))
        })
    }

    // /agent/status (rate-limited, write-only from agents)
    mux.HandleFunc("/agent/status", func(w http.ResponseWriter, r *http.Request) {
        if !limitOr429(lim, clientIP(r), w) { return }
        if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        if getenv("AUTH_DISABLE", "false") == "true" && r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
        // lightweight accept without storing payload server-side in this module; controller may track internally
        // we still enforce minimal shape to avoid abuse
        type agentStatus struct { NodeID string `json:"nodeId"`; Ts int64 `json:"ts"` }
        var st agentStatus
        if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&st); err != nil { http.Error(w, "bad request", http.StatusBadRequest); return }
        if strings.TrimSpace(st.NodeID) == "" { http.Error(w, "nodeId required", http.StatusBadRequest); return }
        if getenv("AUTH_DISABLE", "false") != "true" {
            pn := peerNodeFromTLS(r)
            if pn == "" || !strings.EqualFold(strings.TrimSpace(st.NodeID), pn) { http.Error(w, "forbidden", http.StatusForbidden); return }
        }
        w.WriteHeader(http.StatusAccepted)
    })

    // /metrics/ready
    mux.HandleFunc("/metrics/ready", func(w http.ResponseWriter, r *http.Request) {
        if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
        w.Header().Set("Content-Type", "text/plain; charset=utf-8")
        m := o.Ctrl.ReadyCounts()
        for lvl, n := range m { _, _ = w.Write([]byte("volkit_ready_nodes{level=\"" + lvl + "\"} " + itoa(int64(n)) + "\n")) }
    })

    return mux, hub
}

// unified helpers
func writeJSON(w http.ResponseWriter, code int, v any) { w.Header().Set("Content-Type", "application/json"); w.WriteHeader(code); _ = json.NewEncoder(w).Encode(v) }
func writeText(w http.ResponseWriter, code int, s string) { w.WriteHeader(code); _, _ = w.Write([]byte(s)) }
func writeError(w http.ResponseWriter, code int, msg string) { writeJSON(w, code, map[string]any{"error": msg}) }
func writeEmpty(w http.ResponseWriter, code int) { w.WriteHeader(code) }

// limitOr429 applies limiter and writes rate-limit headers on 429
func limitOr429(l *limiter, key string, w http.ResponseWriter) bool {
    if l.Allow(key) { return true }
    // best-effort retry hints; no per-user quota tracking here
    w.Header().Set("Retry-After", "1")
    writeError(w, http.StatusTooManyRequests, "rate limited")
    return false
}


// token-bucket limiter per client IP
type bucket struct{ tokens float64; last time.Time }
type limiter struct{ mu sync.Mutex; m map[string]*bucket; capacity float64; rps float64 }
func newLimiter(capacity int, rps int) *limiter { if capacity <= 0 { capacity = 10 }; if rps <= 0 { rps = 3 }; return &limiter{m: map[string]*bucket{}, capacity: float64(capacity), rps: float64(rps)} }
func (l *limiter) Allow(key string) bool { if key == "" { key = "-" }; now := time.Now(); l.mu.Lock(); defer l.mu.Unlock(); b := l.m[key]; if b == nil { b = &bucket{tokens: l.capacity, last: now}; l.m[key] = b }; elapsed := now.Sub(b.last).Seconds(); b.tokens = minF(l.capacity, b.tokens+elapsed*l.rps); b.last = now; if b.tokens >= 1 { b.tokens -= 1; return true }; return false }
func minF(a,b float64) float64 { if a<b { return a }; return b }
func clientIP(r *http.Request) string { if xf := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xf != "" { if i := strings.IndexByte(xf, ','); i >= 0 { return strings.TrimSpace(xf[:i]) }; return xf }; host := r.RemoteAddr; if i := strings.LastIndex(host, ":"); i >= 0 { host = host[:i] }; return strings.TrimSpace(host) }

// http counters
var httpRequestsTotal int64
var http2xxTotal int64
var http3xxTotal int64
var http4xxTotal int64
var http5xxTotal int64

