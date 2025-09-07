package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/swarmnative/volkit/internal/controller"
	"github.com/swarmnative/volkit/internal/httpserver"
	"github.com/swarmnative/volkit/internal/issuer"
	"github.com/swarmnative/volkit/internal/pki"
	"github.com/swarmnative/volkit/internal/ws"
	"crypto/rand"
	"encoding/pem"
	"math/big"
)

func main() {
	// init JSON slog
	level := parseLogLevel(getenv("VOLKIT_LOG_LEVEL", "info"))
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	// normalize unified issuer mode
	issuerMode := strings.ToLower(strings.TrimSpace(getenv("VOLKIT_ISSUER_MODE", "")))
	if issuerMode == "" {
		if getenv("VOLKIT_ISSUER_ENDPOINT", "") != "" { issuerMode = "external" } else { issuerMode = "static" }
	}

	cfg := controller.Config{
		MinioEndpointsCSV:   getenv("VOLKIT_ENDPOINTS", "http://s3.local:9000"),
		S3Provider:          getenv("VOLKIT_PROVIDER", ""),
		S3Endpoint:          getenv("VOLKIT_ENDPOINT", "http://s3.local:9000"),
		RcloneRemote:        getenv("VOLKIT_RCLONE_REMOTE", "S3:bucket"),
		RcloneExtraArgs:     getenv("VOLKIT_RCLONE_ARGS", ""),
		Mountpoint:          getenv("VOLKIT_MOUNTPOINT", "/mnt/s3"),
		AccessKeyFile:       getenv("VOLKIT_ACCESS_KEY_FILE", "/run/secrets/s3_access_key"),
		SecretKeyFile:       getenv("VOLKIT_SECRET_KEY_FILE", "/run/secrets/s3_secret_key"),
		MounterImage:        getenv("VOLKIT_RCLONE_IMAGE", getenv("VOLKIT_DEFAULT_RCLONE_IMAGE", "rclone/rclone:latest")),
		HelperImage:         getenv("VOLKIT_NSENTER_HELPER_IMAGE", ""),
		ReadyFile:           ".ready",
		PollInterval:        15 * time.Second,
		MounterUpdateMode:   getenv("VOLKIT_RCLONE_UPDATE_MODE", defaultUpdateMode()),
		MounterPullInterval: parseDurationOr("24h"),
		UnmountOnExit:       getenv("VOLKIT_UNMOUNT_ON_EXIT", "true") == "true",
		AutoCreateBucket:    getenv("VOLKIT_AUTOCREATE_BUCKET", "false") == "true",
		AutoCreatePrefix:    getenv("VOLKIT_AUTOCREATE_PREFIX", "false") == "true",
		ReadOnly:            getenv("VOLKIT_READ_ONLY", "false") == "true",
		AllowOther:          getenv("VOLKIT_ALLOW_OTHER", "false") == "true",
		EnableProxy:         getenv("VOLKIT_PROXY_ENABLE", "false") == "true",
		LocalLBEnabled:      getenv("VOLKIT_PROXY_LOCAL_LB", "false") == "true",
		ProxyPort:           getenv("VOLKIT_PROXY_PORT", "8081"),
		ProxyNetwork:        getenv("VOLKIT_PROXY_NETWORK", ""),
		LabelPrefix:         getenv("VOLKIT_LABEL_PREFIX", getenv("LABEL_PREFIX", "")),
		LabelStrict:         getenv("VOLKIT_LABEL_STRICT", "false") == "true",
		StrictReady:         getenv("VOLKIT_STRICT_READY", "false") == "true",
		Preset:              getenv("VOLKIT_PRESET", ""),
		AutoClaimFromMounts: getenv("VOLKIT_AUT_CLAIM_FROM_MOUNTS", "false") == "true",
		ClaimAllowlistRegex: getenv("VOLKIT_CLAIM_ALLOWLIST_REGEX", ""),
		ImageCleanupEnabled: getenv("VOLKIT_IMAGE_CLEANUP_ENABLED", "true") == "true",
		ImageRetentionDays:  getenvInt("VOLKIT_IMAGE_RETENTION_DAYS", 14),
		ImageKeepRecent:     getenvInt("VOLKIT_IMAGE_KEEP_RECENT", 2),
		ManagerDockerHost:   getenv("VOLKIT_MANAGER_DOCKER_HOST", ""),
		Mode:                getenv("VOLKIT_MODE", "all"),          // all | coordinator | agent
		CoordinatorURL:      getenv("VOLKIT_COORDINATOR_URL", ""),   // e.g., http://coordinator:8080
		CreatePlaceholder:   getenv("VOLKIT_CREATE_PLACEHOLDER", "off"), // off | coordinator
		PlaceholderMinInterval: parseDuration(getenv("VOLKIT_PLACEHOLDER_MIN_INTERVAL", "10s")),
		ControlObjectRelPath: getenv("VOLKIT_CONTROL_OBJECT", ".volkit/.ready"),
		VolumeReclaimEnabled: getenv("VOLKIT_VOLUME_RECLAIM", "true") == "true",
		VolumeNamePrefix:     getenv("VOLKIT_VOLUME_NAME_PREFIX", "volkit-"),
		CredDir:              getenv("VOLKIT_CRED_DIR", "/run/volkit/creds"),
		CredRotateInterval:   parseDuration(getenv("VOLKIT_CRED_ROTATE_INTERVAL", "0")),
		// unified issuer config
		CredsURL:             getenv("VOLKIT_ISSUER_ENDPOINT", getenv("VOLKIT_CREDS_URL", "")),
		NodeLevel:            getenv("VOLKIT_NODE_LEVEL", getenv("NODE_LEVEL", "")),
	}

	// strict VOLKIT_* startup self-checks
	if err := checkStartupEnv(); err != nil { slog.Error("env_check", "error", err); os.Exit(2) }

	// --validate-config fast path
	if hasArg("--validate-config") {
		vr := controller.ValidateConfig(cfg)
		_ = json.NewEncoder(os.Stdout).Encode(vr)
		if vr.OK {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}

	// effective config summary (masked)
	vr := controller.ValidateConfig(cfg)
	slog.Info("effective_config", slog.String("summary", mustJSON(vr.Summary)))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ctrl, err := controller.New(ctx, cfg)
	if err != nil {
		slog.Error("init controller", "error", err)
		os.Exit(1)
	}

	mux, hub := httpserver.BuildMux(httpserver.Options{Ctx: ctx, Ctrl: ctrl, Cfg: cfg, TLSClientConfig: tlsClientConfig, BroadcastCacheHits: &wsBroadcastCacheHits, BroadcastCacheMiss: &wsBroadcastCacheMiss, AgentStatusCount: &agentStatusCount})

	// simple in-memory rate limiter for lightweight endpoints
	var rlMu sync.Mutex
	lastHit := map[string]time.Time{}
	tryRateLimit := func(key string, minInterval time.Duration) bool {
		rlMu.Lock(); defer rlMu.Unlock()
		if t, ok := lastHit[key]; ok {
			if time.Since(t) < minInterval { return false }
		}
		lastHit[key] = time.Now()
		return true
	}

	// initialize internal CA material for /pki/enroll (file-based CA or ephemeral dev CA)
	pki.LoadCA()

	// simplified auth: file-based mTLS or disabled for dev, with optional node binding checks
	authOK := func(r *http.Request) bool {
		if getenv("AUTH_DISABLE", "false") == "true" { return true }
		// require TLS when not disabled
		return r.TLS != nil
	}
	peerNodeFromTLS := func(r *http.Request) string {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 { return "" }
		crt := r.TLS.PeerCertificates[0]
		// prefer SAN DNSNames/IPs, fallback CN
		if len(crt.DNSNames) > 0 { return strings.TrimSpace(crt.DNSNames[0]) }
		if len(crt.IPAddresses) > 0 { return strings.TrimSpace(crt.IPAddresses[0].String()) }
		return strings.TrimSpace(crt.Subject.CommonName)
	}

	// Optional file-based mTLS client config (no external dependency)
	var tlsClientConfig *tls.Config
	{
		clientCert := strings.TrimSpace(getenv("VOLKIT_TLS_CLIENT_CERT_FILE", ""))
		clientKey := strings.TrimSpace(getenv("VOLKIT_TLS_CLIENT_KEY_FILE", ""))
		serverCA := strings.TrimSpace(getenv("VOLKIT_TLS_SERVER_CA_FILE", ""))
		if clientCert != "" || clientKey != "" || serverCA != "" {
			cfg := &tls.Config{MinVersion: tls.VersionTLS12}
			if serverCA != "" {
				pem, err := os.ReadFile(serverCA)
				if err == nil {
					pool := x509.NewCertPool()
					if pool.AppendCertsFromPEM(pem) { cfg.RootCAs = pool }
				}
			}
			if clientCert != "" && clientKey != "" {
				if pair, err := tls.LoadX509KeyPair(clientCert, clientKey); err == nil {
					cfg.Certificates = []tls.Certificate{pair}
				}
			}
			tlsClientConfig = cfg
		}
	}

	// global broadcast pause switch (runtime maintenance)
	var broadcastPaused int32
	var (
		wsClientsGauge       int64
		wsBroadcastCacheHits  int64
		wsBroadcastCacheMiss  int64
		agentStatusCount     int64
		agentStatusRateLimit int64
		wsBroadcastTotal int64
		wsSendErrorsTotal int64
		wsQueueDropTotal int64
		wsBroadcastNodesTotal int64
		wsBroadcastDurationMs int64
	)

	// per-node claims ETag cache (in-memory, version-scoped)
	type nodeETagEntry struct {
		Version    int64
		ETag       string
		ComputedAt int64
		Count      int
	}
	var (
		etagMu     sync.Mutex
		nodeETags  = map[string]nodeETagEntry{}
	)
	addConn := func(h *ws.Hub, c *ws.Client) {
		h.Mu.Lock(); defer h.Mu.Unlock()
		if h.Clients == nil { h.Clients = make(map[*ws.Client]struct{}) }
		h.Clients[c] = struct{}{}
		atomic.AddInt64(&wsClientsGauge, 1)
	}
	removeConn := func(h *ws.Hub, c *ws.Client) {
		h.Mu.Lock(); defer h.Mu.Unlock()
		if _, ok := h.Clients[c]; ok {
			delete(h.Clients, c)
			atomic.AddInt64(&wsClientsGauge, -1)
		}
		if c.Node != "" && h.NodeIndex != nil {
			if set, ok := h.NodeIndex[c.Node]; ok {
				delete(set, c)
				if len(set) == 0 { delete(h.NodeIndex, c.Node) }
			}
		}
	}
	broadcastJSON := func(h *ws.Hub, v map[string]any) {
		atomic.AddInt64(&wsBroadcastTotal, 1)
		h.Mu.Lock(); defer h.Mu.Unlock()
		for cl := range h.Clients {
			select {
			case cl.Send <- v:
				// ok
			default:
				// queue full, drop
				atomic.AddInt64(&wsQueueDropTotal, 1)
			}
		}
	}
	sendToNode := func(h *ws.Hub, node string, v map[string]any) {
		h.Mu.Lock(); defer h.Mu.Unlock()
		if h.NodeIndex == nil { return }
		set := h.NodeIndex[node]
		for cl := range set {
			select {
			case cl.Send <- v:
				// ok
			default:
				atomic.AddInt64(&wsQueueDropTotal, 1)
			}
		}
	}
	maxPerNode := getenvInt("VOLKIT_WS_MAX_PER_NODE", 2)
	// optional WebSocket endpoint for coordinator -> agent events
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
		mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
			// Unified auth for WS
			if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
			// capacity check
			curr := int(atomic.LoadInt64(&hub.ClientsGauge))
			if hub.MaxConns > 0 && curr >= hub.MaxConns {
				http.Error(w, "too many connections", http.StatusServiceUnavailable)
				return
			}
			c, err := upgrader.Upgrade(w, r, nil)
			if err != nil { slog.Warn("ws upgrade", "error", err); return }
			slog.Info("ws connected")
			client := ws.InitClient(c, hub.QueueSize, getenvInt("VOLKIT_WS_READ_SEC", 90))
			hub.Add(client)
			// On connect, send a hello and initial hint
			client.Send <- ws.Message{"type": "hello", "ts": time.Now().Unix()}
			// writer
			done := make(chan struct{})
			go func() { client.Writer(hub); close(done) }()
			// reader (keepalive)
			for {
				mt, data, err := c.ReadMessage()
				if err != nil { break }
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
								// per-node connection limit
								if maxPerNode > 0 {
									count := len(hub.SnapshotNode(strings.TrimSpace(node)))
									if count >= maxPerNode {
										slog.Warn("ws per-node limit reached", "node", node, "limit", maxPerNode)
										_ = c.Close(); break
									}
								}
								hub.IndexClient(client, strings.TrimSpace(node))
							}
						}
					}
				}
			}
			_ = c.Close()
			hub.Remove(client)
			slog.Info("ws closed")
		})
	}

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if ctrl.Ready() == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ctrl.Snapshot())
	})
	mux.HandleFunc("/preflight", func(w http.ResponseWriter, r *http.Request) {
		if err := ctrl.Preflight(); err != nil {
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/reload", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("reconcile scheduled"))
		go ctrl.Nudge()
	})
	mux.HandleFunc("/maintenance/recompute", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
		if getenv("AUTH_DISABLE", "false") == "true" && r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
		go ctrl.Nudge()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("recompute scheduled"))
	})
	mux.HandleFunc("/maintenance/broadcast", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
		if getenv("AUTH_DISABLE", "false") == "true" && r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
		v := strings.TrimSpace(r.URL.Query().Get("pause"))
		if v == "true" || v == "1" {
			atomic.StoreInt32(&broadcastPaused, 1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("broadcast paused"))
			return
		}
		if v == "false" || v == "0" {
			atomic.StoreInt32(&broadcastPaused, 0)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("broadcast resumed"))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("use ?pause=true|false"))
	})
	// Simple per-node ETag memory to avoid recomputing in hot path
	var claimsETagMu sync.Mutex
	node2ETag := map[string]string{}
	mux.HandleFunc("/claims", func(w http.ResponseWriter, r *http.Request) {
		// unified auth
		if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
		// harden: if AUTH_DISABLE=true but no TLS, deny /claims (sensitive)
		if getenv("AUTH_DISABLE", "false") == "true" && r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
		n := strings.TrimSpace(r.URL.Query().Get("node"))
		// per-node rate limit
		if n != "" {
			minMs := getenvInt("VOLKIT_CLAIMS_MIN_MS", 200)
			key := "claims:" + n
			if !tryRateLimit(key, time.Duration(minMs)*time.Millisecond) { http.Error(w, "too many requests", http.StatusTooManyRequests); return }
		}
		start := time.Now()
		// enforce node binding when TLS is used and not disabled
		if getenv("AUTH_DISABLE", "false") != "true" && n != "" {
			pn := peerNodeFromTLS(r)
			if pn == "" || !strings.EqualFold(n, pn) { http.Error(w, "forbidden", http.StatusForbidden); return }
		}
		claims := ctrl.ClaimsForNode(n)
		// ETag support
		var etag string
		claimsETagMu.Lock()
		etag = node2ETag[n]
		claimsETagMu.Unlock()
		if etag == "" {
			bufTmp, _ := json.Marshal(claims)
			sumTmp := sha256.Sum256(bufTmp)
			etag = "\"" + hex.EncodeToString(sumTmp[:]) + "\""
			claimsETagMu.Lock(); node2ETag[n] = etag; claimsETagMu.Unlock()
		}
		if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		// write body lazily if needed
		buf, _ := json.Marshal(claims)
		_, _ = w.Write(buf)
		atomic.AddInt64(&claimsRequestsTotal, 1)
		atomic.StoreInt64(&claimsDurationMs, time.Since(start).Milliseconds())
	})
	mux.HandleFunc("/claims/validate", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
		n := strings.TrimSpace(r.URL.Query().Get("node"))
		if getenv("AUTH_DISABLE", "false") != "true" && n != "" {
			pn := peerNodeFromTLS(r)
			if pn == "" || !strings.EqualFold(n, pn) { http.Error(w, "forbidden", http.StatusForbidden); return }
		}
		claims := ctrl.ClaimsForNode(n)
		buf, _ := json.Marshal(claims)
		sum := sha256.Sum256(buf)
		etag := "\"" + hex.EncodeToString(sum[:]) + "\""
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"node": n, "count": len(claims), "etag": etag, "bytes": len(buf), "ts": time.Now().Unix()})
	})
	if getenv("VOLKIT_ENABLE_METRICS", "false") == "true" {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			// minimal text exposition
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			s := ctrl.Snapshot()
			build := map[string]string{
				"version": strings.TrimSpace(getenv("VOLKIT_BUILD_VERSION", "dev")),
				"go_version": runtime.Version(),
			}
			_, _ = w.Write([]byte(
				"# HELP volkit_build_info Build and runtime info\n" +
				"# TYPE volkit_build_info gauge\n" +
				"volkit_build_info{version=\"" + build["version"] + "\",go_version=\"" + build["go_version"] + "\"} 1\n" +
				"# HELP volkit_http_claims_requests_total Total /claims requests\n" +
				"# TYPE volkit_http_claims_requests_total counter\n" +
				"volkit_http_claims_requests_total " + itoa(atomic.LoadInt64(&claimsRequestsTotal)) + "\n" +
				"# HELP volkit_http_claims_duration_milliseconds Last /claims duration in ms\n" +
				"# TYPE volkit_http_claims_duration_milliseconds gauge\n" +
				"volkit_http_claims_duration_milliseconds " + itoa(atomic.LoadInt64(&claimsDurationMs)) + "\n" +
				"# HELP volkit_reconcile_total Total reconcile loops\n" +
					"# TYPE volkit_reconcile_total counter\n" +
					"volkit_reconcile_total " + itoa(s.ReconcileTotal) + "\n" +
					"# HELP volkit_reconcile_errors Total reconcile errors\n" +
					"# TYPE volkit_reconcile_errors counter\n" +
					"volkit_reconcile_errors " + itoa(s.ReconcileErrors) + "\n" +
					"# HELP volkit_mounter_running Whether rclone mounter is running\n" +
					"# TYPE volkit_mounter_running gauge\n" +
					"volkit_mounter_running " + bool01(s.MounterRunning) + "\n" +
					"# HELP volkit_mount_writable Whether mountpoint is writable\n" +
					"# TYPE volkit_mount_writable gauge\n" +
					"volkit_mount_writable " + bool01(s.MountWritable) + "\n" +
					"# HELP volkit_heal_attempts_total Total heal attempts\n" +
					"# TYPE volkit_heal_attempts_total counter\n" +
					"volkit_heal_attempts_total " + itoa(s.HealAttemptsTotal) + "\n" +
					"# HELP volkit_heal_success_total Total heal success\n" +
					"# TYPE volkit_heal_success total counter\n" +
					"volkit_heal_success_total " + itoa(s.HealSuccessTotal) + "\n" +
					"# HELP volkit_last_heal_success_timestamp Seconds since epoch of last heal success\n" +
					"# TYPE volkit_last_heal_success_timestamp gauge\n" +
					"volkit_last_heal_success_timestamp " + itoa(s.LastHealSuccessUnix) + "\n" +
					"# HELP volkit_orphan_cleanup_total Total orphaned mounters cleaned\n" +
					"# TYPE volkit_orphan_cleanup_total counter\n" +
					"volkit_orphan_cleanup_total " + itoa(s.OrphanCleanupTotal) + "\n" +
					"# HELP volkit_volume_created_total Total named volumes created\n" +
					"# TYPE volkit_volume_created_total counter\n" +
					"volkit_volume_created_total " + itoa(s.VolumeCreatedTotal) + "\n" +
					"# HELP volkit_volume_reclaimed_total Total named volumes reclaimed\n" +
					"# TYPE volkit_volume_reclaimed_total counter\n" +
					"volkit_volume_reclaimed_total " + itoa(s.VolumeReclaimedTotal) + "\n" +
					"# HELP volkit_volume_drift_total Managed volumes with unexpected device root\n" +
					"# TYPE volkit_volume_drift_total counter\n" +
					"volkit_volume_drift_total " + itoa(s.VolumeDriftTotal) + "\n" +
					"# HELP volkit_reconcile_duration_milliseconds Last reconcile duration in ms\n" +
					"# TYPE volkit_reconcile_duration_milliseconds gauge\n" +
					"volkit_reconcile_duration_milliseconds " + itoa(ctrl.Snapshot().ReconcileDurationMs) + "\n" +
					"# HELP volkit_mounter_created_total Total mounter containers created\n" +
					"# TYPE volkit_mounter_created_total counter\n" +
					""))
		})
	}
	// hints moved into httpserver

	// Serve CA certificate for agents to bootstrap (read-only)
	mux.HandleFunc("/pki/ca", func(w http.ResponseWriter, r *http.Request) {
		if getenv("VOLKIT_PKI_ENABLE", "false") != "true" { http.NotFound(w, r); return }
		w.Header().Set("Content-Type", "application/x-pem-file")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(pki.CA.PEM)
	})
	// enroll endpoint (dev/testing only; gated)
	mux.HandleFunc("/pki/enroll", func(w http.ResponseWriter, r *http.Request) {
		if getenv("VOLKIT_PKI_ENABLE", "false") != "true" { http.NotFound(w, r); return }
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		if getenv("AUTH_DISABLE", "false") != "true" {
			// Require TLS and token
			if r.TLS == nil { http.Error(w, "tls required", http.StatusForbidden); return }
		}
		tokExpected := strings.TrimSpace(getenv("VOLKIT_ENROLL_TOKEN", ""))
		tok := strings.TrimSpace(r.Header.Get("X-Volkit-Token"))
		if tokExpected == "" || tok != tokExpected { http.Error(w, "forbidden", http.StatusForbidden); return }
		// read CSR (PEM)
		csrPEM, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
		if err != nil { http.Error(w, "read error", http.StatusBadRequest); return }
		block, _ := pem.Decode(csrPEM)
		if block == nil || block.Type != "CERTIFICATE REQUEST" { http.Error(w, "bad csr", http.StatusBadRequest); return }
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil || csr.CheckSignature() != nil { http.Error(w, "bad csr", http.StatusBadRequest); return }
		// sign short-lived cert
		now := time.Now()
		tpl := &x509.Certificate{SerialNumber: new(big.Int).SetInt64(now.UnixNano()), Subject: csr.Subject, DNSNames: csr.DNSNames, IPAddresses: csr.IPAddresses, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		der, err := x509.CreateCertificate(rand.Reader, tpl, pki.CA.Cert, csr.PublicKey, pki.CA.Key)
		if err != nil { http.Error(w, "sign failed", http.StatusInternalServerError); return }
		pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		w.Header().Set("Content-Type", "application/x-pem-file")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(pemCert)
	})

	// default to TLS-only unless explicitly allowed
	handler := httpserver.WithRecovery(httpserver.WithRequestID(httpserver.WithHTTPMetrics(httpserver.WithGzipJSON(mux))))
	srv := &http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	// Choose one of: SPIRE mTLS, file-based TLS, or plain HTTP
	go func() {
		// File-based TLS path (supports Docker secrets)
		serverCert := strings.TrimSpace(getenv("VOLKIT_TLS_SERVER_CERT_FILE", ""))
		serverKey := strings.TrimSpace(getenv("VOLKIT_TLS_SERVER_KEY_FILE", ""))
		clientCA := strings.TrimSpace(getenv("VOLKIT_TLS_CLIENT_CA_FILE", ""))
		if serverCert != "" && serverKey != "" {
			cfg := &tls.Config{MinVersion: tls.VersionTLS12}
			if clientCA != "" {
				pem, err := os.ReadFile(clientCA)
				if err == nil {
					pool := x509.NewCertPool()
					if pool.AppendCertsFromPEM(pem) {
						cfg.ClientCAs = pool
						cfg.ClientAuth = tls.RequireAndVerifyClientCert
					}
				}
			}
			pair, err := tls.LoadX509KeyPair(serverCert, serverKey)
			if err != nil { slog.Error("load server cert", "error", err); os.Exit(1) }
			cfg.Certificates = []tls.Certificate{pair}
			handlerTLS := httpserver.WithRecovery(httpserver.WithRequestID(httpserver.WithHTTPMetrics(httpserver.WithGzipJSON(mux))))
			srv = &http.Server{Addr: ":8080", Handler: handlerTLS, TLSConfig: cfg, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
			slog.Info("https (file mTLS) listening", slog.String("addr", ":8080"))
			if err := srv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				slog.Error("https server", "error", err)
				os.Exit(1)
			}
			return
		}
		// Plain HTTP only when explicitly enabled
		if getenv("VOLKIT_HTTP_ENABLE", "false") == "true" {
			slog.Info("http listening", slog.String("addr", ":8080"))
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("http server", "error", err)
				os.Exit(1)
			}
		} else {
			slog.Error("no TLS configured and VOLKIT_HTTP_ENABLE!=true; refusing to listen on plaintext", "addr", ":8080")
		}
	}()

	// if WebSocket enabled, broadcast claims_changed on ChangeVersion bump
	if getenv("VOLKIT_WS_ENABLE", "true") == "true" {
		go func() {
			var last int64 = ctrl.ChangeVersion()
			baseMin := getenvInt("VOLKIT_WS_BROADCAST_MIN_MS", 300)
			maxMin := getenvInt("VOLKIT_WS_BROADCAST_MAX_MS", 2000)
			lastAt := time.Now().Add(-time.Duration(baseMin) * time.Millisecond)
			pending := false
			var timer *time.Timer
			broadcastPerNode := func(ver int64) {
				start := time.Now()
				nodes := hub.SnapshotNodeNames()
				for _, node := range nodes {
					if atomic.LoadInt32(&broadcastPaused) == 1 { continue }
					var etag string
					etagMu.Lock()
					ent, ok := nodeETags[node]
					etagMu.Unlock()
					if ok && ent.Version == ver && ent.ETag != "" {
						etag = ent.ETag
						atomic.AddInt64(&wsBroadcastCacheHits, 1)
					} else {
						claims := ctrl.ClaimsForNode(node)
						if len(claims) == 0 { continue }
						buf, _ := json.Marshal(claims)
						sum := sha256.Sum256(buf)
						etag = "\"" + hex.EncodeToString(sum[:]) + "\""
						etagMu.Lock()
						nodeETags[node] = nodeETagEntry{Version: ver, ETag: etag, ComputedAt: time.Now().Unix()}
						etagMu.Unlock()
						atomic.AddInt64(&wsBroadcastCacheMiss, 1)
					}
					hub.SendToNode(node, ws.Message{"type": "claims_changed", "version": ver, "node": node, "etag": etag, "ts": time.Now().Unix()})
				}
				atomic.AddInt64(&hub.BroadcastNodesTotal, int64(len(nodes)))
				atomic.StoreInt64(&hub.BroadcastDurationMs, time.Since(start).Milliseconds())
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				time.Sleep(200 * time.Millisecond)
				if getenv("VOLKIT_MAINTENANCE", "false") == "true" { continue }
				cur := ctrl.ChangeVersion()
				if cur > last {
					last = cur
					clients := int(atomic.LoadInt64(&hub.ClientsGauge))
					minMs := baseMin + clients*10
					if minMs > maxMin { minMs = maxMin }
					if time.Since(lastAt) >= time.Duration(minMs)*time.Millisecond {
						broadcastPerNode(cur)
						lastAt = time.Now()
						pending = false
					} else if !pending {
						delay := time.Duration(minMs)*time.Millisecond - time.Since(lastAt)
						pending = true
						ver := last
						timer = time.AfterFunc(delay, func() {
							broadcastPerNode(ver)
							lastAt = time.Now()
							pending = false
						})
					} else {
						_ = timer
					}
				}
			}
		}()
	}

	// Agent-side proxy auto mode: probe backends and adjust HAProxy via runtime API
	modeEff := strings.ToLower(strings.TrimSpace(getenv("VOLKIT_MODE", "all")))
	allowAutoOnCoordinator := strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_PROXY_AUTO_ALLOW_COORDINATOR", "false")), "true")
	if ((modeEff != "coordinator") || allowAutoOnCoordinator) && getenv("VOLKIT_PROXY_ENABLE", "false") == "true" && getenv("VOLKIT_PROXY_AUTO", "false") == "true" {
		go func() {
			// config (hard-coded to reduce config surface)
			probeURL := "/minio/health/ready"
			windowSec := 45
			minDwellSec := 30
			backoffBase, backoffMin, backoffMax := 5, 1, 60
			backoff := backoffBase
			// backend names from env (services csv)
			backendsCsv := strings.TrimSpace(getenv("VOLKIT_PROXY_LOCAL_SERVICES", ""))
			backends := splitCSV(backendsCsv)
			if len(backends) == 0 { return }
			// simple memory of rtt and failures per backend
			type stat struct { rttMsP95 int64; failPct int; cnt int; last int64 }
			stats := map[string]*stat{}
			for _, b := range backends { stats[b] = &stat{} }
            // runtime API socket (optional)
			runtimeAddr := strings.TrimSpace(getenv("VOLKIT_HAP_RUNTIME", "/var/run/haproxy.sock"))
			lastMode := "A" // A: nearest, B: weighted, C: balance
			lastChange := time.Now().Add(-time.Duration(minDwellSec) * time.Second)
			for {
				// probe sequentially (lightweight)
				for _, b := range backends {
					start := time.Now()
					ok := false
					// assume backend resolves via service name in same overlay
					u := fmt.Sprintf("http://%s:%s%s", b, strings.TrimSpace(getenv("VOLKIT_PROXY_BACKEND_PORT", "9000")), probeURL)
					ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
					req, _ := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
					resp, err := http.DefaultClient.Do(req)
					if err == nil && resp.StatusCode/100 == 2 { ok = true }
					if resp != nil && resp.Body != nil { _ = resp.Body.Close() }
					cancel()
					dur := time.Since(start).Milliseconds()
					s := stats[b]
					s.cnt++
					if ok { s.last = time.Now().Unix() } else { s.failPct += 10 }
					// crude p95 approx: track max of recent small sample
					if dur > s.rttMsP95 { s.rttMsP95 = dur }
				}
				// window decay
				for _, s := range stats { s.failPct = s.failPct * 8 / 10; s.rttMsP95 = s.rttMsP95 * 8 / 10 }
				// decide mode
				best := backends[0]
				bestScore := int64(1<<60)
				for _, b := range backends {
					s := stats[b]
					score := s.rttMsP95*5 + int64(s.failPct)*40
					if score < bestScore { bestScore = score; best = b }
				}
				healthyBest := bestScore < 150*5 && stats[best].failPct < 20
				mode := lastMode
				if healthyBest {
					mode = "A"
				} else if bestScore < 300*5 && stats[best].failPct < 50 {
					mode = "B"
				} else {
					mode = "C"
				}
				// adaptive backoff: healthy -> exponential backoff; anomaly -> fast probe
				if mode == "A" {
					if backoff < backoffMax { backoff *= 2; if backoff > backoffMax { backoff = backoffMax } }
				} else {
					backoff = backoffMin
				}
				if mode != lastMode && time.Since(lastChange).Seconds() >= float64(minDwellSec) {
					// apply via runtime API if available
					if fi, err := os.Stat(runtimeAddr); err == nil && (fi.Mode()&os.ModeSocket) != 0 {
						conn, err := net.Dial("unix", runtimeAddr)
						if err == nil {
							w := bufio.NewWriter(conn)
							// set all to backup in A except best
							for _, b := range backends {
								if mode == "A" {
									fmt.Fprintf(w, "set server be_minio/%s weight %d\n", b, 10)
									if b == best { fmt.Fprintf(w, "enable server be_minio/%s\n", b) } else { fmt.Fprintf(w, "disable server be_minio/%s\n", b) }
								} else if mode == "B" {
									wBest, wOther := 10, 2
									fmt.Fprintf(w, "enable server be_minio/%s\n", best)
									fmt.Fprintf(w, "set server be_minio/%s weight %d\n", best, wBest)
									for _, o := range backends { if o == best { continue }; fmt.Fprintf(w, "enable server be_minio/%s\nset server be_minio/%s weight %d\n", o, o, wOther) }
								} else {
									for _, b2 := range backends { fmt.Fprintf(w, "enable server be_minio/%s\nset server be_minio/%s weight %d\n", b2, b2, 5) }
								}
							w.Flush()
							_ = conn.Close()
						}
					}
					lastMode = mode
					lastChange = time.Now()
				}
				time.Sleep(time.Duration(backoff) * time.Second)
			}
		}()
	}

	// agent: wait hints then pull claims (simplified loop)
	if getenv("VOLKIT_MODE", "all") == "agent" && getenv("VOLKIT_COORDINATOR_URL", "") != "" {
		// optional WebSocket client preferred; fallback to long-poll hints
		if getenv("VOLKIT_WS_ENABLE", "true") == "true" {
			go func() {
				// agent-side nudge debounce to merge triggers
				var lastNudge time.Time
				minNudge := time.Duration(getenvInt("VOLKIT_AGENT_NUDGE_MIN_MS", 200)) * time.Millisecond
				for {
					u := strings.TrimRight(getenv("VOLKIT_COORDINATOR_URL", ""), "/") + "/ws"
					dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
					if tlsClientConfig != nil { dialer.TLSClientConfig = tlsClientConfig }
					headers := http.Header{}
					c, _, err := dialer.Dial(u, headers)
					if err != nil { time.Sleep(3 * time.Second); continue }
					// ping/pong setup
					c.SetReadLimit(1 << 20)
					c.SetReadDeadline(time.Now().Add(90 * time.Second))
					c.SetPongHandler(func(string) error { c.SetReadDeadline(time.Now().Add(90 * time.Second)); return nil })
					// identify
					_ = c.WriteJSON(map[string]any{"type": "hello", "nodeId": getenv("HOSTNAME", ""), "ts": time.Now().Unix()})
					var lastVersion int64 = 0
					for {
						var msg map[string]any
						if err := c.ReadJSON(&msg); err != nil { c.Close(); break }
						if t, _ := msg["type"].(string); t != "" {
							switch t {
							case "claims_changed":
								if v, ok := msg["version"].(float64); ok {
									ver := int64(v)
									if ver <= lastVersion { continue }
									lastVersion = ver
								}
								if et, _ := msg["etag"].(string); strings.TrimSpace(et) != "" {
									if strings.TrimSpace(ctrl.LastClaimsETag()) == strings.TrimSpace(et) { continue }
								}
								// precheck hints(timeout=0) to avoid needless fetch if already up-to-date
								u2 := strings.TrimRight(getenv("VOLKIT_COORDINATOR_URL", ""), "/") + "/hints?since=" + strconv.FormatInt(lastVersion, 10) + "&timeoutSec=0"
								req2, _ := http.NewRequest(http.MethodGet, u2, nil)
								resp2, err2 := (&http.Client{ Timeout: 3 * time.Second }).Do(req2)
								if tlsClientConfig != nil {
									resp2, err2 = (&http.Client{ Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsClientConfig} }).Do(req2)
								}
								if err2 == nil { io.Copy(io.Discard, resp2.Body); resp2.Body.Close() }
								// regardless, nudge; debounce to merge triggers
								if time.Since(lastNudge) >= minNudge {
									ctrl.Nudge()
									lastNudge = time.Now()
								}
							case "reload":
								if time.Since(lastNudge) >= minNudge {
									ctrl.Nudge()
									lastNudge = time.Now()
								}
							case "ping":
								_ = c.WriteJSON(map[string]any{"type": "pong", "ts": time.Now().Unix()})
							}
						}
					}
				}
			}()
		}
		go func() {
			var backoff = time.Second
			var last int64 = 0
			var lastNudge time.Time
			minNudge := time.Duration(getenvInt("VOLKIT_AGENT_NUDGE_MIN_MS", 200)) * time.Millisecond
			for {
				select { case <-ctx.Done(): return; default: }
				// long-poll hints
				// jitter to avoid thundering herd
				jitterMs := rand.Intn(400)
				time.Sleep(time.Duration(jitterMs) * time.Millisecond)
				u := strings.TrimRight(getenv("VOLKIT_COORDINATOR_URL", ""), "/") + 
					"/hints?since=" + strconv.FormatInt(last, 10) + 
					"&timeoutSec=30"
				req, _ := http.NewRequest(http.MethodGet, u, nil)
				client := &http.Client{ Timeout: 35 * time.Second }
				if tlsClientConfig != nil { client.Transport = &http.Transport{TLSClientConfig: tlsClientConfig} }
				resp, err := client.Do(req)
				if err != nil {
					time.Sleep(backoff)
					if backoff < 60*time.Second { backoff *= 2 }
					continue
				}
				io.Copy(io.Discard, resp.Body); resp.Body.Close()
				backoff = time.Second
				if time.Since(lastNudge) >= minNudge {
					ctrl.Nudge() // trigger reconcile -> agent分支 fetch claims
					lastNudge = time.Now()
				}
				last = time.Now().Unix()
			}
		}()

		// periodic heartbeat status POST
		go func() {
			idle := 20
			maxIdle := 60
			lastVer := ctrl.ChangeVersion()
			for {
				// build status payload from local snapshot
				s := ctrl.Snapshot()
				// compute ready prefixes (agent-side)
				var readyPrefixes []string
				if getenv("VOLKIT_MODE", "all") == "agent" && getenv("VOLKIT_COORDINATOR_URL", "") != "" {
					readyPrefixes = ctrl.ReadyPrefixesLocal(ctrl.AgentClaimsForLocalNode())
				}
				payload := map[string]any{
					"nodeId": getenv("HOSTNAME", ""),
					"ts": time.Now().Unix(),
					"mounterRunning": s.MounterRunning,
					"mountWritable": s.MountWritable,
					"claimsETag": strings.TrimSpace(ctrl.LastClaimsETag()),
					"readyPrefixes": readyPrefixes,
				}
				b, _ := json.Marshal(payload)
				u := strings.TrimRight(getenv("VOLKIT_COORDINATOR_URL", ""), "/") + "/agent/status"
				req, _ := http.NewRequest(http.MethodPost, u, bytes.NewReader(b))
				req.Header.Set("Content-Type", "application/json")
				client := &http.Client{ Timeout: 5 * time.Second }
				if tlsClientConfig != nil { client.Transport = &http.Transport{TLSClientConfig: tlsClientConfig} }
				if resp, err := client.Do(req); err == nil { io.Copy(io.Discard, resp.Body); resp.Body.Close() }
				// adaptive: if controller changed, reset; else increase idle up to max
				cur := ctrl.ChangeVersion()
				if cur != lastVer { idle = 20; lastVer = cur } else if idle < maxIdle { idle += 5 }
				time.Sleep(time.Duration(idle+rand.Intn(5)) * time.Second)
			}
		}()

		// creds watcher: poll /creds etag and nudge on change
		go func() {
			lastETag := ""
			minSec := getenvInt("VOLKIT_CREDS_MIN_SEC", 60)
			if minSec < 15 { minSec = 15 }
			level := strings.ToLower(strings.TrimSpace(getenv("VOLKIT_NODE_LEVEL", getenv("NODE_LEVEL", ""))))
			if level == "" { level = "default" }
			for {
				select { case <-ctx.Done(): return; default: }
				time.Sleep(time.Duration(minSec+rand.Intn(10)) * time.Second)
				base := strings.TrimSpace(getenv("VOLKIT_ISSUER_ENDPOINT", ""))
				if base == "" { base = strings.TrimSpace(getenv("VOLKIT_COORDINATOR_URL", "")) }
				if base == "" { continue }
				u := strings.TrimRight(base, "/") + "/creds?level=" + url.QueryEscape(level)
				req, _ := http.NewRequest(http.MethodGet, u, nil)
				if lastETag != "" { req.Header.Set("If-None-Match", lastETag) }
				client := &http.Client{ Timeout: 5 * time.Second }
				if tlsClientConfig != nil { client.Transport = &http.Transport{TLSClientConfig: tlsClientConfig} }
				resp, err := client.Do(req)
				if err != nil { continue }
				if resp.StatusCode == http.StatusNotModified { io.Copy(io.Discard, resp.Body); resp.Body.Close(); continue }
				if resp.StatusCode/100 == 2 {
					et := strings.TrimSpace(resp.Header.Get("ETag"))
					io.Copy(io.Discard, resp.Body); resp.Body.Close()
					if et != "" && et != lastETag {
						lastETag = et
						ctrl.Nudge() // trigger reconcile -> ensureMounter will pick up new creds
					}
				} else {
					io.Copy(io.Discard, resp.Body); resp.Body.Close()
				}
			}
		}()
	}

	mux.HandleFunc("/creds", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(r) { http.Error(w, "unauthorized", http.StatusUnauthorized); return }
		lvl := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("level")))
		// Prefer external issuer when configured; s3_admin; fallback to static env
		base := strings.TrimSpace(getenv("VOLKIT_ISSUER_ENDPOINT", ""))
		if issuerMode == "external" && base != "" {
			u := strings.TrimRight(base, "/") + "/creds?level=" + url.QueryEscape(lvl)
			req, _ := http.NewRequest(http.MethodGet, u, nil)
			if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" { req.Header.Set("If-None-Match", inm) }
			client := &http.Client{ Timeout: 6 * time.Second }
			if tlsClientConfig != nil { client.Transport = &http.Transport{TLSClientConfig: tlsClientConfig} }
			resp, err := client.Do(req)
			if err == nil && resp != nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusNotModified {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				if resp.StatusCode/100 == 2 {
					if et := strings.TrimSpace(resp.Header.Get("ETag")); et != "" { w.Header().Set("ETag", et) }
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.Copy(w, resp.Body)
					return
				}
			}
			// fall through to static when issuer fails
		}
		// s3_admin: prefer STS AssumeRole with issuer AK/SK; fallback to static override
		if issuerMode == "s3_admin" {
			ak := strings.TrimSpace(getenv("VOLKIT_ISSUER_ACCESS_KEY", ""))
			sk := strings.TrimSpace(getenv("VOLKIT_ISSUER_SECRET_KEY", ""))
			if ak != "" && sk != "" {
				allowRoot := strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_ALLOW_ROOT_ISSUER", "false")), "true")
				if !allowRoot {
					http.Error(w, "root issuer not allowed (set VOLKIT_ALLOW_ROOT_ISSUER=true to enable)", http.StatusForbidden)
					return
				}
				// optional forcing static (no STS), using root AK/SK directly
				if strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_S3_ADMIN_FORCE_STATIC", "false")), "true") {
					env := ctrl.(*controller.Controller).BuildRcloneEnvPublic()
					for i := range env {
						if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + ak }
						if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + sk }
					}
					buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env})
					sum := sha256.Sum256(buf)
					etag := "\"" + hex.EncodeToString(sum[:]) + "\""
					if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { w.WriteHeader(http.StatusNotModified); return }
					w.Header().Set("ETag", etag)
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": 0, "issuer": "s3_admin_static"})
					return
				}
				// Try STS AssumeRole
				atomic.AddInt64(&issuerSTSAttempts, 1)
				stsEndpoint := strings.TrimSpace(getenv("VOLKIT_ISSUER_ENDPOINT", getenv("VOLKIT_ENDPOINT", "")))
				region := strings.TrimSpace(getenv("VOLKIT_S3_REGION", "us-east-1"))
				dur := 3600
				if v := strings.TrimSpace(getenv("VOLKIT_ISSUER_TTL_SEC", "")); v != "" { if n, err := strconv.Atoi(v); err == nil && n >= 900 && n <= 43200 { dur = n } }
				insecure := strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_ISSUER_INSECURE", "false")), "true")
				if cred, err := issuer.AssumeRoleSTS(stsEndpoint, ak, sk, dur, region, insecure, tlsClientConfig); err == nil && cred.AccessKeyId != "" {
					atomic.AddInt64(&issuerSTSSuccess, 1)
					// build env with session token
					env := ctrl.(*controller.Controller).BuildRcloneEnvPublic()
					for i := range env {
						if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + cred.AccessKeyId }
						if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + cred.SecretAccessKey }
					}
					env = append(env, "RCLONE_CONFIG_S3_SESSION_TOKEN="+cred.SessionToken)
					expiresAt := cred.Expiration.Unix()
					buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env})
					sum := sha256.Sum256(buf)
					etag := "\"" + hex.EncodeToString(sum[:]) + "\""
					if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { w.WriteHeader(http.StatusNotModified); return }
					w.Header().Set("ETag", etag)
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": expiresAt, "issuer": "s3_admin_sts"})
					return
				} else {
					atomic.AddInt64(&issuerSTSFailures, 1)
				}
				// Fallback to Service Account via controller helper if enabled
				if strings.EqualFold(strings.TrimSpace(getenv("VOLKIT_SA_ENABLE", "true")), "true") {
					atomic.AddInt64(&issuerSAAttempts, 1)
					if ak2, sk2, exp, err2 := ctrl.(*controller.Controller).MintServiceAccount(lvl); err2 == nil && ak2 != "" {
						atomic.AddInt64(&issuerSASuccess, 1)
						env := ctrl.(*controller.Controller).BuildRcloneEnvPublic()
						for i := range env {
							if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + ak2 }
							if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + sk2 }
						}
						buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env})
						sum := sha256.Sum256(buf)
						etag := "\"" + hex.EncodeToString(sum[:]) + "\""
						if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == etag { w.WriteHeader(http.StatusNotModified); return }
						w.Header().Set("ETag", etag)
						w.Header().Set("Content-Type", "application/json")
						_ = json.NewEncoder(w).Encode(map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": exp, "issuer": "s3_admin_sa"})
						return
					} else {
						atomic.AddInt64(&issuerSAFailures, 1)
					}
				}
				// Fallback to static override when STS/SA fail
				env := ctrl.(*controller.Controller).BuildRcloneEnvPublic()
				for i := range env {
					if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { env[i] = "RCLONE_CONFIG_S3_ACCESS_KEY_ID=" + ak }
					if strings.HasPrefix(env[i], "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { env[i] = "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=" + sk }
				}
				buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env})
				sum := sha256.Sum256(buf)
				tag := "\"" + hex.EncodeToString(sum[:]) + "\""
				if inm := strings.TrimSpace(r.Header.Get("If-None-Match")); inm != "" && inm == tag { w.WriteHeader(http.StatusNotModified); return }
				w.Header().Set("ETag", tag)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"level": lvl, "rcloneEnv": env, "etag": tag, "expiresAt": 0, "issuer": "s3_admin_static"})
				return
			}
		}
		// static fallback (fully compatible)
		env := ctrl.(*controller.Controller).BuildRcloneEnvPublic()
		buf, _ := json.Marshal(struct{ RcloneEnv []string `json:"rcloneEnv"` }{RcloneEnv: env})
		sum := sha256.Sum256(buf)
		etag := "\"" + hex.EncodeToString(sum[:]) + "\""
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"level": lvl, "rcloneEnv": env, "etag": etag, "expiresAt": 0})
	})

	ctrl.Run()
	if hub != nil { hub.Close() }
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	_ = srv.Shutdown(ctx2)
}

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
func bool01(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func getenvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n
	}
	return def
}

func hasArg(flag string) bool {
	for _, a := range os.Args[1:] {
		if a == flag {
			return true
		}
	}
	return false
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func parseDurationOr(def string) time.Duration {
	v := os.Getenv("VOLKIT_RCLONE_PULL_INTERVAL")
	if v == "" {
		d, _ := time.ParseDuration(def)
		return d
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	d, _ := time.ParseDuration(def)
	return d
}

func defaultUpdateMode() string {
	// default to on_change; user can set never
	if v := os.Getenv("VOLKIT_RCLONE_UPDATE_MODE"); v != "" {
		return v
	}
	return "on_change"
}

// checkStartupEnv validates environment constraints and returns error on violation
func checkStartupEnv() error {
	if strings.EqualFold(getenv("VOLKIT_MODE", "all"), "agent") && strings.TrimSpace(getenv("VOLKIT_COORDINATOR_URL", "")) == "" {
		return fmt.Errorf("VOLKIT_COORDINATOR_URL required when VOLKIT_MODE=agent")
	}
	if v := getenvInt("VOLKIT_WS_WRITE_TIMEOUT_SEC", 5); v < 1 || v > 120 { return fmt.Errorf("VOLKIT_WS_WRITE_TIMEOUT_SEC out of range [1,120]: %d", v) }
	if v := getenvInt("VOLKIT_RL_SENSITIVE_CAP", 10); v < 1 || v > 10000 { return fmt.Errorf("VOLKIT_RL_SENSITIVE_CAP out of range [1,10000]: %d", v) }
	if v := getenvInt("VOLKIT_RL_SENSITIVE_RPS", 3); v < 1 || v > 10000 { return fmt.Errorf("VOLKIT_RL_SENSITIVE_RPS out of range [1,10000]: %d", v) }
	if strings.EqualFold(getenv("VOLKIT_ALLOW_ROOT_ISSUER", "false"), "true") && strings.EqualFold(getenv("VOLKIT_ISSUER_MODE", ""), "static") {
		slog.Warn("ALLOW_ROOT_ISSUER has no effect when ISSUER_MODE=static")
	}
	return nil
}
