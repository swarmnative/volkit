package main

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "encoding/json"
    "log/slog"
    "net/http"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "syscall"
    "time"

    "github.com/swarmnative/volkit/internal/controller"
    "github.com/swarmnative/volkit/internal/httpserver"
)

func main() {
    level := parseLogLevel(getenv("VOLKIT_LOG_LEVEL", "info"))
    slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

    // Build config
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
        Mode:                getenv("VOLKIT_MODE", "all"),
        CoordinatorURL:      getenv("VOLKIT_COORDINATOR_URL", ""),
        CreatePlaceholder:   getenv("VOLKIT_CREATE_PLACEHOLDER", "off"),
        PlaceholderMinInterval: parseDuration(getenv("VOLKIT_PLACEHOLDER_MIN_INTERVAL", "10s")),
        ControlObjectRelPath: getenv("VOLKIT_CONTROL_OBJECT", ".volkit/.ready"),
        VolumeReclaimEnabled: getenv("VOLKIT_VOLUME_RECLAIM", "true") == "true",
        VolumeNamePrefix:     getenv("VOLKIT_VOLUME_NAME_PREFIX", "volkit-"),
        CredDir:              getenv("VOLKIT_CRED_DIR", "/run/volkit/creds"),
        CredRotateInterval:   parseDuration(getenv("VOLKIT_CRED_ROTATE_INTERVAL", "0")),
        CredsURL:             getenv("VOLKIT_ISSUER_ENDPOINT", getenv("VOLKIT_CREDS_URL", "")),
        NodeLevel:            getenv("VOLKIT_NODE_LEVEL", getenv("NODE_LEVEL", "")),
    }

    if err := checkStartupEnv(); err != nil { slog.Error("env_check", "error", err); os.Exit(2) }

    // build optional TLS client config for outbound client
    var tlsClientConfig *tls.Config
    if ca := strings.TrimSpace(getenv("VOLKIT_TLS_SERVER_CA_FILE", "")); ca != "" {
        if pem, err := os.ReadFile(ca); err == nil {
            pool := x509.NewCertPool(); if pool.AppendCertsFromPEM(pem) { tlsClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12} }
        }
    }

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    ctrl, err := controller.New(ctx, cfg)
    if err != nil { slog.Error("init controller", "error", err); os.Exit(1) }

    mux, hub := httpserver.BuildMux(httpserver.Options{Ctx: ctx, Ctrl: ctrl, Cfg: cfg, TLSClientConfig: tlsClientConfig})

    handler := httpserver.WithRecovery(httpserver.WithRequestID(httpserver.WithHTTPMetrics(httpserver.WithGzipJSON(mux))))
    srv := &http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}

    go func() {
        if strings.EqualFold(getenv("VOLKIT_HTTP_ENABLE", "false"), "true") {
            slog.Info("http listening", slog.String("addr", ":8080"))
            if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed { slog.Error("http server", "error", err); os.Exit(1) }
            return
        }
        // TLS required unless explicitly enabled plain HTTP
        cert := strings.TrimSpace(getenv("VOLKIT_TLS_SERVER_CERT_FILE", ""))
        key := strings.TrimSpace(getenv("VOLKIT_TLS_SERVER_KEY_FILE", ""))
        if cert == "" || key == "" { slog.Error("no TLS configured and VOLKIT_HTTP_ENABLE!=true; refusing to listen on plaintext", "addr", ":8080"); return }
        cfg := &tls.Config{MinVersion: tls.VersionTLS12}
        if pair, err := tls.LoadX509KeyPair(cert, key); err == nil { cfg.Certificates = []tls.Certificate{pair} } else { slog.Error("load server cert", "error", err); os.Exit(1) }
        srv.TLSConfig = cfg
        slog.Info("https listening", slog.String("addr", ":8080"))
        if err := srv.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed { slog.Error("https server", "error", err); os.Exit(1) }
    }()

    ctrl.Run()
    if hub != nil { hub.Close() }
    ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
    defer cancel2()
    _ = srv.Shutdown(ctx2)
}

func getenv(k, def string) string { v := os.Getenv(k); if v == "" { return def }; return v }
func getenvInt(k string, def int) int { v := os.Getenv(k); if v=="" { return def }; if n,err := strconv.Atoi(v); err==nil { return n }; return def }
func parseDurationOr(def string) time.Duration { if v := os.Getenv("VOLKIT_RCLONE_PULL_INTERVAL"); v == "" { d,_ := time.ParseDuration(def); return d }; if d,err := time.ParseDuration(os.Getenv("VOLKIT_RCLONE_PULL_INTERVAL")); err==nil { return d }; d,_ := time.ParseDuration(def); return d }
func parseDuration(v string) time.Duration { d, _ := time.ParseDuration(strings.TrimSpace(v)); return d }
func defaultUpdateMode() string { if v := os.Getenv("VOLKIT_RCLONE_UPDATE_MODE"); v != "" { return v }; return "on_change" }
func parseLogLevel(s string) slog.Level { switch strings.ToLower(strings.TrimSpace(s)) { case "debug": return slog.LevelDebug; case "warn","warning": return slog.LevelWarn; case "error": return slog.LevelError; default: return slog.LevelInfo } }

func checkStartupEnv() error {
    if strings.EqualFold(getenv("VOLKIT_MODE", "all"), "agent") && strings.TrimSpace(getenv("VOLKIT_COORDINATOR_URL", "")) == "" { return fmt.Errorf("VOLKIT_COORDINATOR_URL required when VOLKIT_MODE=agent") }
    if v := getenvInt("VOLKIT_WS_WRITE_TIMEOUT_SEC", 5); v < 1 || v > 120 { return fmt.Errorf("VOLKIT_WS_WRITE_TIMEOUT_SEC out of range [1,120]: %d", v) }
    if v := getenvInt("VOLKIT_RL_SENSITIVE_CAP", 10); v < 1 || v > 10000 { return fmt.Errorf("VOLKIT_RL_SENSITIVE_CAP out of range [1,10000]: %d", v) }
    if v := getenvInt("VOLKIT_RL_SENSITIVE_RPS", 3); v < 1 || v > 10000 { return fmt.Errorf("VOLKIT_RL_SENSITIVE_RPS out of range [1,10000]: %d", v) }
    return nil
}


