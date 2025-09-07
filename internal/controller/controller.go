package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"errors"
    "sync"
	"crypto/tls"
	"crypto/x509"
	"github.com/docker/docker/api/types/volume"
)

type Config struct {
	MinioEndpointsCSV   string
	S3Provider          string
	S3Endpoint          string
	RcloneRemote        string
	RcloneExtraArgs     string
	Mountpoint          string
	AccessKeyFile       string
	SecretKeyFile       string
	MounterImage        string
	HelperImage         string
	ReadyFile           string
	PollInterval        time.Duration
	MounterUpdateMode   string // never | periodic | on_change
	MounterPullInterval time.Duration
	UnmountOnExit       bool
	AutoCreateBucket    bool
	AutoCreatePrefix    bool
	ReadOnly            bool
	AllowOther          bool
	EnableProxy         bool
	LocalLBEnabled      bool
	ProxyPort           string
	ProxyNetwork        string
	LabelPrefix         string
	LabelStrict         bool
	StrictReady         bool
	Preset              string
	// Claim discovery enhancements
	AutoClaimFromMounts    bool   // infer prefix from ServiceSpec.Mounts when enabled and no explicit labels
	ClaimAllowlistRegex    string // optional whitelist for inferred prefixes
	// Optional image retention controls (no-op if unused)
	ImageCleanupEnabled bool
	ImageRetentionDays  int
	ImageKeepRecent     int
	// Optional remote manager Docker host for reading Service specs from workers
	ManagerDockerHost   string
	// Coordinator/Agent split mode
	Mode                string // all | coordinator | agent
	CoordinatorURL      string // http://coordinator:8080
	CreatePlaceholder   string // off | coordinator
	PlaceholderMinInterval time.Duration // rate limit for placeholder writes
	CoordinatorLeaderOnly bool // only leader writes placeholder
	// CoordinatorTLSCAFile string
	// Control object path used to assert remote readiness (relative to prefix)
	ControlObjectRelPath string
	// Local named volume management toggles
	VolumeReclaimEnabled bool
	VolumeNamePrefix     string
	// Credentials file injection (host tmpfs) and rotation
	CredDir             string
	CredRotateInterval  time.Duration
	// Optional external creds endpoint (controller fetches and distributes without secrets persistence)
	CredsURL            string
	// Node level for this agent/controller instance (e.g., bronze|silver). Used to select creds
	NodeLevel           string
}

type Controller struct {
	ctx           context.Context
	cli           *client.Client
	managerCli    *client.Client
	cfg           Config
	lastImagePull time.Time
	lastImageID   string
	// metrics
	reconcileTotal      int64
	reconcileErrors     int64
	lastMounterRunning  bool
	lastMountWritable   bool
	lastReconcileMs     int64
	healAttemptsTotal   int64
	healSuccessTotal    int64
	orphanCleanupTotal  int64
	volumeCreatedTotal  int64
	volumeReclaimedTotal int64
	credsFetchOK        int64
	credsFetchErrors    int64
	credsBackoffTotal   int64
	volumeDriftTotal    int64
	servicesBlockedTotal   int64
	servicesUnblockedTotal int64
	lastHealSuccessUnix int64
	mounterCreatedTotal int64
	metricsMu           sync.Mutex
	// events
	eventCh chan struct{}
	// cache
	selfImageRef string
	// docker short-ttl caches
	cacheMu           sync.Mutex
	svcCacheUntil     time.Time
	cachedServices    []types.Service
	nodeCacheUntil    time.Time
	cachedNodes       []swarm.Node
	// rate limiting for placeholder writes
	lastPlaceholderWrite map[string]time.Time
	lastChangeVersion    int64
	lastClaimsETag       string
	// image readiness flags
	helperImageReady   bool
	mounterImageReady  bool
	// per-node claims cache scoped to lastChangeVersion
	claimsCacheMu  sync.Mutex
	claimsCacheVer int64
	claimsByNode   map[string][]ClaimOut
	// additional metrics
	claimsCacheHits       int64
	claimsCacheMiss       int64
	lastClaimsComputeMs   int64
	lastServiceListMs     int64
	lastNodeListMs        int64
	dockerApiErrors       int64
	// dependency graph and affected nodes
	depMu          sync.Mutex
	svcToNodes     map[string][]string
	svcAffectsAll  map[string]bool
	affMu          sync.Mutex
	affectedAll    bool
	affectedNodes  map[string]struct{}
	// histogram buckets
	claimsBucketsUpper        []int
	claimsBuckets             []int64
	claimsSumMs               int64
	claimsCount               int64
	dockerSvcBucketsUpper     []int
	dockerSvcBuckets          []int64
	dockerSvcSumMs            int64
	dockerSvcCount            int64
	dockerNodeBucketsUpper    []int
	dockerNodeBuckets         []int64
	dockerNodeSumMs           int64
	dockerNodeCount           int64
	// node -> ready prefixes set
	readyMu   sync.Mutex
	nodeReady map[string]map[string]struct{}
	// per-service migration cooldown
	migMu        sync.Mutex
	lastSvcMigrate map[string]int64
	// mounter failure backoff
	mounterFailCount    int
	mounterBackoffUntil time.Time
	mounterBackoffTotal int64
	// creds cache/backoff per level
	credsMu         sync.Mutex
	credsByLevel    map[string]struct{ env []string; etag string }
	credBackoffTill map[string]time.Time
	// service account pruning counters
	lastSAPrune   time.Time
	saPrunedTotal int64
	saPruneErrors int64
}

// ChangeVersion returns current change version for hinting
func (c *Controller) ChangeVersion() int64 { return c.lastChangeVersion }
// BumpChange updates change version to now
func (c *Controller) BumpChange() {
    c.lastChangeVersion = time.Now().Unix()
    c.claimsCacheMu.Lock()
    c.claimsCacheVer = 0
    c.claimsByNode = nil
    c.claimsCacheMu.Unlock()
}

// LastClaimsETag returns the last seen ETag from the most recent /claims fetch
// in agent mode. Empty when not yet fetched or when coordinator didn't send one.
func (c *Controller) LastClaimsETag() string { return c.lastClaimsETag }

func New(ctx context.Context, cfg Config) (*Controller, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	var mcli *client.Client
	if strings.TrimSpace(cfg.ManagerDockerHost) != "" {
		if c2, err := client.NewClientWithOpts(client.WithHost(cfg.ManagerDockerHost), client.WithAPIVersionNegotiation()); err == nil {
			mcli = c2
		} else {
			slog.Warn("manager docker host client init failed", "host", cfg.ManagerDockerHost, "error", err)
		}
	}
	return &Controller{
		ctx:                  ctx,
		cli:                  cli,
		managerCli:           mcli,
		cfg:                  cfg,
		eventCh:              make(chan struct{}, 1),
		selfImageRef:         "",
		lastPlaceholderWrite: make(map[string]time.Time),
		lastChangeVersion:    time.Now().Unix(),
		lastClaimsETag:       "",
		svcToNodes:           make(map[string][]string),
		svcAffectsAll:        make(map[string]bool),
		affectedNodes:        make(map[string]struct{}),
		claimsBucketsUpper:   []int{1,5,10,25,50,100,250,500,1000,2500},
		claimsBuckets:        make([]int64, 10),
		dockerSvcBucketsUpper: []int{1,5,10,25,50,100,250,500,1000,2500},
		dockerSvcBuckets:     make([]int64, 10),
		dockerNodeBucketsUpper: []int{1,5,10,25,50,100,250,500,1000,2500},
		dockerNodeBuckets:    make([]int64, 10),
		nodeReady:            make(map[string]map[string]struct{}),
		migMu:                sync.Mutex{},
		lastSvcMigrate:       make(map[string]int64),
		credsMu:              sync.Mutex{},
		credsByLevel:         make(map[string]struct{ env []string; etag string }),
		credBackoffTill:      make(map[string]time.Time),
	}, nil
}

// record histogram samples (milliseconds)
func (c *Controller) observeSvcDur(ms int) {
    c.metricsMu.Lock()
    for i, up := range c.dockerSvcBucketsUpper { if ms <= up { c.dockerSvcBuckets[i]++; break } }
    c.dockerSvcSumMs += int64(ms); c.dockerSvcCount++
    c.metricsMu.Unlock()
}

func (c *Controller) observeNodeDur(ms int) {
    c.metricsMu.Lock()
    for i, up := range c.dockerNodeBucketsUpper { if ms <= up { c.dockerNodeBuckets[i]++; break } }
    c.dockerNodeSumMs += int64(ms); c.dockerNodeCount++
    c.metricsMu.Unlock()
}

func (c *Controller) observeClaimsDur(ms int) {
    c.metricsMu.Lock()
    for i, up := range c.claimsBucketsUpper { if ms <= up { c.claimsBuckets[i]++; break } }
    c.claimsSumMs += int64(ms); c.claimsCount++
    c.metricsMu.Unlock()
}

// affected-nodes helpers
func (c *Controller) markAffectedAll() {
    c.affMu.Lock(); c.affectedAll = true; c.affMu.Unlock()
}

func (c *Controller) markAffectedNode(n string) {
    n = strings.TrimSpace(n)
    if n == "" { return }
    c.affMu.Lock(); if c.affectedNodes == nil { c.affectedNodes = make(map[string]struct{}) }; c.affectedNodes[n] = struct{}{}; c.affMu.Unlock()
}

// AffectedNodesOnce returns current affected set and clears it. If all==true, callers should treat as full broadcast.
func (c *Controller) AffectedNodesOnce() (nodes []string, all bool) {
    c.affMu.Lock()
    all = c.affectedAll
    if !all && len(c.affectedNodes) > 0 {
        nodes = make([]string, 0, len(c.affectedNodes))
        for n := range c.affectedNodes { nodes = append(nodes, n) }
    }
    c.affectedAll = false
    c.affectedNodes = make(map[string]struct{})
    c.affMu.Unlock()
    return
}

func (c *Controller) Run() {
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	// optional debug server
	if strings.EqualFold(strings.TrimSpace(os.Getenv("VOLKIT_DEBUG_SERVER")), "true") {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK); _, _ = w.Write([]byte("ok")) })
			mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(c.Snapshot())
			})
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			addr := strings.TrimSpace(os.Getenv("VOLKIT_DEBUG_ADDR"))
			if addr == "" { addr = ":6060" }
			_ = http.ListenAndServe(addr, mux)
		}()
	}
	go c.watchDockerEvents()
	for {
		start := time.Now()
		if err := c.reconcile(); err != nil {
			c.metricsMu.Lock()
			c.reconcileErrors++
			c.metricsMu.Unlock()
			slog.Error("reconcile error", "error", err)
		}
		c.metricsMu.Lock()
		c.lastReconcileMs = time.Since(start).Milliseconds()
		c.metricsMu.Unlock()
		// periodic SA prune
		if strings.EqualFold(strings.TrimSpace(os.Getenv("VOLKIT_SA_PRUNE")), "true") {
			if time.Since(c.lastSAPrune) > 10*time.Minute {
				if err := c.pruneServiceAccounts(); err != nil { c.metricsMu.Lock(); c.saPruneErrors++; c.metricsMu.Unlock() } else { c.lastSAPrune = time.Now() }
			}
		}
		select {
		case <-c.ctx.Done():
			return
		case <-c.eventCh:
			// debounce/coalesce burst of events within a small window (configurable)
			debounce := 400 * time.Millisecond
			if v := strings.TrimSpace(os.Getenv("VOLKIT_EVENT_DEBOUNCE_MS")); v != "" {
				if ms, err := strconv.Atoi(v); err == nil && ms >= 0 && ms <= 5000 {
					debounce = time.Duration(ms) * time.Millisecond
				}
			}
		deb:
			for {
				select {
				case <-c.eventCh:
					// keep draining
				case <-time.After(debounce):
					break deb
				}
			}
			continue
		case <-ticker.C:
		}
	}
}

func (c *Controller) watchDockerEvents() {
	f := filters.NewArgs()
	f.Add("type", "service")
	f.Add("type", "node")
	msgs, errs := c.cli.Events(c.ctx, types.EventsOptions{Filters: f})
	backoff := time.Second
	for {
		select {
		case <-c.ctx.Done():
			return
		case m := <-msgs:
			// only bump for service/node changes
			if m.Type == events.ServiceEventType || m.Type == events.NodeEventType {
				c.BumpChange()
			}
			// invalidate caches by event type
			c.cacheMu.Lock()
			switch m.Type {
			case events.ServiceEventType:
				c.svcCacheUntil = time.Time{}; c.cachedServices = nil
			case events.NodeEventType:
				c.nodeCacheUntil = time.Time{}; c.cachedNodes = nil
			default:
				c.svcCacheUntil = time.Time{}; c.cachedServices = nil
				c.nodeCacheUntil = time.Time{}; c.cachedNodes = nil
			}
			c.cacheMu.Unlock()
			select { case c.eventCh <- struct{}{}: default: }
		case <-errs:
			// exponential backoff with jitter
			sleep := backoff + time.Duration(rand.Int63n(int64(backoff/2)))
			if sleep > 30*time.Second { sleep = 30 * time.Second }
			time.Sleep(sleep)
			if backoff < 30*time.Second { backoff *= 2 }
			msgs, errs = c.cli.Events(c.ctx, types.EventsOptions{Filters: f})
		}
	}
}

func (c *Controller) timeoutCtx(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.ctx, d)
}

func (c *Controller) Ready() error {
	// mountpoint exists
	if err := os.MkdirAll(c.cfg.Mountpoint, 0o755); err != nil {
		return err
	}
	// in read-only mode, skip write probe
	if !c.cfg.ReadOnly {
		test := filepath.Join(c.cfg.Mountpoint, c.cfg.ReadyFile)
		if err := os.WriteFile(test, []byte(time.Now().Format(time.RFC3339)), 0o644); err != nil {
			return err
		}
		_ = os.Remove(test)
	}
	// optional strict remote check
	if c.cfg.StrictReady {
		u := strings.TrimSpace(c.resolveEndpointForMounter())
		if u != "" {
			ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil || (resp.StatusCode >= 500 && resp.StatusCode != 404) {
				if resp != nil && resp.Body != nil { _ = resp.Body.Close() }
				return fmt.Errorf("remote not ready: %v (url=%s)", err, u)
			}
			if resp != nil && resp.Body != nil { _ = resp.Body.Close() }
		}
	}
	return nil
}

func (c *Controller) reconcile() error {
	c.metricsMu.Lock(); c.reconcileTotal++; c.metricsMu.Unlock()
	// Ensure mountpoint directory exists
	_ = os.MkdirAll(c.cfg.Mountpoint, 0o755)

	// Try to ensure rshared on host (best-effort)
	if err := c.ensureRShared(); err != nil {
		slog.Warn("ensure rshared failed", "error", err)
	}

	// Auto-pull mounter image according to mode
	if strings.TrimSpace(c.cfg.MounterUpdateMode) == "never" {
		// no-op
	} else {
		if err := c.pullMounterImageIfChanged(); err != nil {
			slog.Warn("pull mounter image (on_change)", "error", err)
		}
	}

	// Ensure mounter container exists
	if err := c.ensureMounter(); err != nil {
		return err
	}

	// If mount is stuck, try cleanup (best-effort)
	if err := c.checkAndHealMount(); err != nil {
		slog.Warn("heal mount", "error", err)
	} else {
		c.metricsMu.Lock()
		c.healAttemptsTotal++
		if testRW(c.cfg.Mountpoint) == nil {
			c.healSuccessTotal++
			c.lastHealSuccessUnix = time.Now().Unix()
		}
		c.metricsMu.Unlock()
	}

	// Declarative claim provisioning: create requested prefixes under mountpoint
	if err := c.provisionClaims(); err != nil {
		slog.Warn("provision claims", "error", err)
	}

	// update readiness labels and maybe unblock services
	c.updateLevelReadyLabels()
	c.maybeUnblockServices()

	// Emit status to logs
	c.logStatus()
	// Cleanup orphaned rclone containers (best-effort)
	if err := c.cleanupOrphanedMounters(); err != nil {
		slog.Warn("cleanup orphaned mounters", "error", err)
	}
	return nil
}

// ensureImagePresent makes sure the given image reference is available locally.
func (c *Controller) ensureImagePresent(img string) error {
	img = strings.TrimSpace(img)
	if img == "" {
		return fmt.Errorf("empty image reference")
	}
	if _, _, err := c.cli.ImageInspectWithRaw(c.ctx, img); err == nil {
		return nil
	}
	ctx, cancel := c.timeoutCtx(60 * time.Second)
	rc, err := c.cli.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		cancel()
		return err
	}
	defer rc.Close()
	_, _ = io.Copy(io.Discard, rc)
	cancel()
	// verify
	if _, _, err := c.cli.ImageInspectWithRaw(c.ctx, img); err != nil {
		return err
	}
	return nil
}

func (c *Controller) ensureHelperImage() error {
	if c.helperImageReady { return nil }
	if err := c.ensureImagePresent(c.helperImageRef()); err != nil { return err }
	c.helperImageReady = true
	return nil
}

func (c *Controller) ensureMounterImage() error {
	if c.mounterImageReady { return nil }
	if err := c.ensureImagePresent(c.cfg.MounterImage); err != nil { return err }
	c.mounterImageReady = true
	return nil
}

func (c *Controller) ensureMounter() error {
	name := c.mounterName()
	// Precompute desired env/cmd for drift detection
	_, _ = os.ReadFile(c.cfg.AccessKeyFile)
	_, _ = os.ReadFile(c.cfg.SecretKeyFile)
	// detect level
	level := strings.ToLower(strings.TrimSpace(c.cfg.NodeLevel))
	if level == "" { level = "default" }
	// fetch current level credentials
	env, _, _ := c.fetchLevelCreds(level)
	// write creds file on host tmpfs
	credFile, err := c.writeCredsEnv(level, env)
	if err != nil { slog.Warn("write creds", "error", err) }
	cmd := []string{"mount", c.cfg.RcloneRemote, c.cfg.Mountpoint, "--config", "/dev/null"}
	if c.cfg.AllowOther { cmd = append(cmd, "--allow-other") } else { cmd = append(cmd, "--allow-root") }
	cmd = append(cmd, "--vfs-cache-mode=writes", "--dir-cache-time=12h")
	cmd = append(cmd, c.buildPresetArgs()...)
	if c.cfg.ReadOnly { cmd = append(cmd, "--read-only") }
	if strings.TrimSpace(c.cfg.RcloneExtraArgs) != "" { cmd = append(cmd, parseArgs(c.cfg.RcloneExtraArgs)...) }
	desiredHash := c.computeMounterSpecHash(env, cmd)

	// find by name
	args := filters.NewArgs()
	args.Add("name", name)
	ctx, cancel := c.timeoutCtx(10 * time.Second)
	defer cancel()
	conts, err := c.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return err
	}
	// If exists and image changed (after pull) or endpoint drifted, recreate
	desiredImageID := c.cachedImageID()
	if len(conts) > 0 {
		id := conts[0].ID
		ictx, icancel := c.timeoutCtx(5 * time.Second)
		inspect, err := c.cli.ContainerInspect(ictx, id)
		icancel()
		if err == nil {
			// spec-hash drift: recreate when labels mismatch
			currentHash := ""
			if inspect.Config != nil && inspect.Config.Labels != nil {
				currentHash = strings.TrimSpace(inspect.Config.Labels["swarmnative.mounter.spec"]) 
			}
			if desiredHash != "" && currentHash != "" && !strings.EqualFold(desiredHash, currentHash) {
				rctx, rcancel := c.timeoutCtx(10 * time.Second)
				_ = c.cli.ContainerRemove(rctx, id, container.RemoveOptions{Force: true})
				rcancel()
			} else if desiredImageID != "" && inspect.Image != desiredImageID {
				rctx, rcancel := c.timeoutCtx(10 * time.Second)
				_ = c.cli.ContainerRemove(rctx, id, container.RemoveOptions{Force: true})
				rcancel()
			} else if inspect.State != nil && inspect.State.Running {
				// Endpoint drift detection
				desired := strings.TrimSpace(c.resolveEndpointForMounter())
				current := ""
				if inspect.Config != nil {
					for _, e := range inspect.Config.Env {
						if strings.HasPrefix(e, "RCLONE_CONFIG_S3_ENDPOINT=") {
							current = strings.TrimPrefix(e, "RCLONE_CONFIG_S3_ENDPOINT=")
							break
						}
					}
				}
				if desired != "" && current != "" && !strings.EqualFold(desired, current) {
					rctx, rcancel := c.timeoutCtx(10 * time.Second)
					_ = c.cli.ContainerRemove(rctx, id, container.RemoveOptions{Force: true})
					rcancel()
				} else {
					return nil
				}
			} else {
				sctx, scancel := c.timeoutCtx(10 * time.Second)
				if err := c.cli.ContainerStart(sctx, id, container.StartOptions{}); err == nil {
					scancel()
					return nil
				}
				scancel()
				r2ctx, r2cancel := c.timeoutCtx(10 * time.Second)
				_ = c.cli.ContainerRemove(r2ctx, id, container.RemoveOptions{Force: true})
				r2cancel()
			}
		}
	}

	// Backoff on consecutive failures
	if time.Now().Before(c.mounterBackoffUntil) {
		return fmt.Errorf("mounter backoff active")
	}
	// Before creating a fresh mounter, ensure no stale mount remains
	if err := c.unmountIfMounted(); err != nil {
		slog.Warn("pre-create unmount failed", "error", err)
	}

	// Networking: attach to overlay network when provided (for controller overlay IP access)
	var netCfg *network.NetworkingConfig
	if strings.TrimSpace(c.cfg.ProxyNetwork) != "" {
		netCfg = &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{
			c.cfg.ProxyNetwork: {},
		}}
	} else {
		netCfg = &network.NetworkingConfig{}
	}

	// ensure mounter image exists
	if err := c.ensureImagePresent(c.cfg.MounterImage); err != nil {
		return fmt.Errorf("ensure mounter image: %w", err)
	}

	// build command via creds file sourcing
	var runCmd []string
	binds := []string{"/dev/fuse:/dev/fuse", fmt.Sprintf("%s:%s:rshared", c.cfg.Mountpoint, c.cfg.Mountpoint)}
	if strings.TrimSpace(credFile) != "" { containerCred := filepath.Join("/run", filepath.Base(credFile)); runCmd = c.mounterCommandWithCredFile(containerCred); binds = append(binds, fmt.Sprintf("%s:%s:ro", filepath.Dir(credFile), "/run")) } else { runCmd = append([]string{"rclone"}, cmd...) }

	// security/resource knobs via env (defaults hardened)
	readOnlyRoot := strings.ToLower(strings.TrimSpace(os.Getenv("VOLKIT_MOUNTER_READONLY_ROOTFS")))
	rofs := readOnlyRoot != "false"
	nnp := strings.ToLower(strings.TrimSpace(os.Getenv("VOLKIT_MOUNTER_NO_NEW_PRIVILEGES"))) != "false"
	allowCapsCsv := strings.TrimSpace(os.Getenv("VOLKIT_MOUNTER_CAPS_ALLOW"))
	if allowCapsCsv == "" { allowCapsCsv = "SYS_ADMIN" }
	var capAdd []string
	for _, one := range strings.Split(allowCapsCsv, ",") { v := strings.TrimSpace(one); if v != "" { capAdd = append(capAdd, v) } }
	// seccomp/apparmor profiles (optional)
	seccompProfile := strings.TrimSpace(os.Getenv("VOLKIT_SECCOMP_PROFILE"))
	apparmorProfile := strings.TrimSpace(os.Getenv("VOLKIT_APPARMOR_PROFILE"))
	var secOpts []string
	if nnp { secOpts = append(secOpts, "no-new-privileges:true") }
	if seccompProfile != "" { secOpts = append(secOpts, fmt.Sprintf("seccomp=%s", seccompProfile)) }
	if apparmorProfile != "" { secOpts = append(secOpts, fmt.Sprintf("apparmor=%s", apparmorProfile)) }
	// tmpfs size for /tmp
	tmpfsSize := strings.TrimSpace(os.Getenv("VOLKIT_MOUNTER_TMPFS_SIZE"))
	if tmpfsSize == "" { tmpfsSize = "16m" }
	// resources
	var pidsLimitPtr *int64
	if v := strings.TrimSpace(os.Getenv("VOLKIT_MOUNTER_PIDS_LIMIT")); v != "" { if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 { pidsLimitPtr = &n } }
	var memBytes int64
	if v := strings.TrimSpace(os.Getenv("VOLKIT_MOUNTER_MEMORY_BYTES")); v != "" { if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 { memBytes = n } }
	var nanoCPUs int64
	if v := strings.TrimSpace(os.Getenv("VOLKIT_MOUNTER_NANO_CPUS")); v != "" { if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 { nanoCPUs = n } }

	cctx, ccancel := c.timeoutCtx(20 * time.Second)
	resp, err := c.cli.ContainerCreate(cctx,
		&container.Config{
			Image: c.cfg.MounterImage,
			Env:   nil, // avoid exposing creds via env
			Cmd:   runCmd,
			Labels: map[string]string{
				"swarmnative.mounter":      "managed",
				"swarmnative.mounter.spec": desiredHash,
			},
		},
		&container.HostConfig{
			Privileged:  false,
			CapAdd:      capAdd,
			CapDrop:     []string{"ALL"},
			NetworkMode: c.selfNetworkMode(),
			RestartPolicy: container.RestartPolicy{
				Name: "always",
			},
			Binds: binds,
			ReadonlyRootfs: rofs,
			SecurityOpt: secOpts,
			Tmpfs: map[string]string{"/tmp": "rw,nosuid,nodev,noexec,size=" + tmpfsSize},
			Resources: container.Resources{
				Devices: []container.DeviceMapping{{PathOnHost: "/dev/fuse", PathInContainer: "/dev/fuse", CgroupPermissions: "mrw"}},
				PidsLimit: pidsLimitPtr,
				Memory:    memBytes,
				NanoCPUs:  nanoCPUs,
			},
		},
		netCfg,
		nil,
		name,
	)
	ccancel()
	if err != nil {
		c.mounterFailCount++
		// simple exponential backoff up to 2 minutes
		wait := time.Duration(1<<min(c.mounterFailCount, 7)) * time.Second
		until := time.Now().Add(wait)
		c.mounterBackoffUntil = until
		c.metricsMu.Lock(); c.mounterBackoffTotal++; c.metricsMu.Unlock()
		return fmt.Errorf("create mounter: %w", err)
	}
	sctx2, scancel2 := c.timeoutCtx(15 * time.Second)
	if err := c.cli.ContainerStart(sctx2, resp.ID, container.StartOptions{}); err != nil {
		scancel2()
		c.mounterFailCount++
		wait := time.Duration(1<<min(c.mounterFailCount, 7)) * time.Second
		c.mounterBackoffUntil = time.Now().Add(wait)
		c.metricsMu.Lock(); c.mounterBackoffTotal++; c.metricsMu.Unlock()
		return fmt.Errorf("start mounter: %w", err)
	}
	scancel2()
	// success -> reset fail/backoff
	c.mounterFailCount = 0
	c.mounterBackoffUntil = time.Time{}
	c.metricsMu.Lock(); c.mounterCreatedTotal++; c.metricsMu.Unlock()
	return nil
}

func min(a, b int) int { if a < b { return a }; return b }

// isMounted checks whether a path is currently a mountpoint (best-effort by reading /proc/self/mountinfo)
func isMounted(path string) bool {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil { return false }
	lines := strings.Split(string(b), "\n")
	for _, ln := range lines {
		if ln == "" { continue }
		fields := strings.Fields(ln)
		if len(fields) >= 5 {
			if fields[4] == path { return true }
		}
	}
	return false
}

// unmountIfMounted lazily unmounts the configured mountpoint when it is currently mounted.
func (c *Controller) unmountIfMounted() error {
	if !isMounted(c.cfg.Mountpoint) {
		return nil
	}
	sh := fmt.Sprintf("(nsenter -t 1 -m -- fusermount -uz %[1]s || true); (nsenter -t 1 -m -- umount -l %[1]s || true)", c.cfg.Mountpoint)
	if err := c.ensureHelperImage(); err != nil { return err }
	cont, err := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.helperImageRef(), Cmd: []string{"sh", "-c", sh}},
		&container.HostConfig{Privileged: true, PidMode: "host", Binds: []string{fmt.Sprintf("%s:%s", c.cfg.Mountpoint, c.cfg.Mountpoint)}},
		&network.NetworkingConfig{}, nil, c.helperName("preunmount"))
	if err != nil { return err }
	defer func() { _ = c.cli.ContainerRemove(c.ctx, cont.ID, container.RemoveOptions{Force: true}) }()
	if err := c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{}); err != nil { return err }
	wctx, wcancel := c.timeoutCtx(10 * time.Second)
	statusCh, errCh := c.cli.ContainerWait(wctx, cont.ID, container.WaitConditionNotRunning)
	select {
	case <-statusCh:
	case err := <-errCh:
		if err != nil {
			if rc, lgErr := c.cli.ContainerLogs(context.Background(), cont.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: "40"}); lgErr == nil {
				defer rc.Close()
				b, _ := io.ReadAll(io.LimitReader(rc, 64*1024))
				slog.Warn("preunmount helper failed", "error", err, "logs", string(b))
			}
			wcancel()
			return err
		}
	}
	wcancel()
	return nil
}

func (c *Controller) pullMounterImageIfDue() error {
	if time.Since(c.lastImagePull) < c.cfg.MounterPullInterval {
		return nil
	}
	ictx, icancel := c.timeoutCtx(60 * time.Second)
	rc, err := c.cli.ImagePull(ictx, c.cfg.MounterImage, image.PullOptions{})
	if err != nil {
		icancel()
		return err
	}
	defer rc.Close()
	_, _ = io.Copy(io.Discard, rc)
	c.lastImagePull = time.Now()
	if ii, _, err := c.cli.ImageInspectWithRaw(ictx, c.cfg.MounterImage); err == nil {
		c.lastImageID = ii.ID
	}
	icancel()
	return nil
}

func (c *Controller) pullMounterImageIfChanged() error {
	// Check current image id
	current := c.cachedImageID()
	// Pull new
	ipctx, ipcancel := c.timeoutCtx(60 * time.Second)
	rc, err := c.cli.ImagePull(ipctx, c.cfg.MounterImage, image.PullOptions{})
	if err != nil {
		ipcancel()
		return err
	}
	defer rc.Close()
	_, _ = io.Copy(io.Discard, rc)
	c.lastImagePull = time.Now()
	// Inspect new id
	if ii, _, err := c.cli.ImageInspectWithRaw(ipctx, c.cfg.MounterImage); err == nil {
		if current != "" && ii.ID == current {
			// unchanged
			ipcancel()
			return nil
		}
		c.lastImageID = ii.ID
	}
	ipcancel()
	return nil
}

func (c *Controller) cachedImageID() string {
	if c.lastImageID != "" {
		return c.lastImageID
	}
	if ii, _, err := c.cli.ImageInspectWithRaw(c.ctx, c.cfg.MounterImage); err == nil {
		c.lastImageID = ii.ID
		return c.lastImageID
	}
	return ""
}

func (c *Controller) ensureRShared() error {
	// Use host namespace via nsenter available in main image (util-linux preinstalled)
	sh := fmt.Sprintf("nsenter -t 1 -m -- mount --make-rshared %s || mount --make-rshared %s", c.cfg.Mountpoint, c.cfg.Mountpoint)
	// ensure helper image exists
	if err := c.ensureImagePresent(c.helperImageRef()); err != nil {
		return err
	}
	cont, err := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.helperImageRef(), Cmd: []string{"sh", "-c", sh}},
		&container.HostConfig{Privileged: true, PidMode: "host", Binds: []string{fmt.Sprintf("%s:%s", c.cfg.Mountpoint, c.cfg.Mountpoint)}},
		&network.NetworkingConfig{}, nil, c.helperName("rshared-helper"))
	if err != nil {
		return err
	}
	defer func() { _ = c.cli.ContainerRemove(c.ctx, cont.ID, container.RemoveOptions{Force: true}) }()
	if err := c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{}); err != nil {
		return err
	}
	// wait with timeout and log capture on failure
	wctx, wcancel := c.timeoutCtx(10 * time.Second)
	statusCh, errCh := c.cli.ContainerWait(wctx, cont.ID, container.WaitConditionNotRunning)
	select {
	case <-statusCh:
		// ok
	case err := <-errCh:
		// capture logs best-effort
		if err != nil {
			if rc, lgErr := c.cli.ContainerLogs(context.Background(), cont.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: "40"}); lgErr == nil {
				defer rc.Close()
				b, _ := io.ReadAll(io.LimitReader(rc, 64*1024))
				slog.Warn("rshared helper failed", "error", err, "logs", string(b))
			}
			wcancel()
			return err
		}
	}
	wcancel()
	return nil
}

func (c *Controller) checkAndHealMount() error {
	// If mountpoint exists but not usable, try lazy unmount via helper
	if err := testRW(c.cfg.Mountpoint); err == nil {
		return nil
	}
	sh := fmt.Sprintf("(nsenter -t 1 -m -- fusermount -uz %[1]s || true); (nsenter -t 1 -m -- umount -l %[1]s || true)", c.cfg.Mountpoint)
	// ensure helper image exists
	if err := c.ensureImagePresent(c.helperImageRef()); err != nil {
		return err
	}
	cont, err := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.helperImageRef(), Cmd: []string{"sh", "-c", sh}},
		&container.HostConfig{Privileged: true, PidMode: "host", Binds: []string{fmt.Sprintf("%s:%s", c.cfg.Mountpoint, c.cfg.Mountpoint)}},
		&network.NetworkingConfig{}, nil, c.helperName("umount-helper"))
	if err != nil {
		return err
	}
	defer func() { _ = c.cli.ContainerRemove(c.ctx, cont.ID, container.RemoveOptions{Force: true}) }()
	if err := c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{}); err != nil {
		return err
	}
	wctx, wcancel := c.timeoutCtx(15 * time.Second)
	statusCh, errCh := c.cli.ContainerWait(wctx, cont.ID, container.WaitConditionNotRunning)
	select {
	case <-statusCh:
		// ok
	case err := <-errCh:
		if err != nil {
			if rc, lgErr := c.cli.ContainerLogs(context.Background(), cont.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: "80"}); lgErr == nil {
				defer rc.Close()
				b, _ := io.ReadAll(io.LimitReader(rc, 64*1024))
				slog.Warn("umount helper failed", "error", err, "logs", string(b))
			}
			wcancel()
			return err
		}
	}
	wcancel()
	return nil
}

func parseArgs(s string) []string {
	// basic shell-like parsing: handles double quotes and escaped quotes, no env expansion
	var out []string
	s = strings.TrimSpace(s)
	if s == "" { return out }
	var buf strings.Builder
	inQuote := false
	esc := false
	flush := func() {
		if buf.Len() > 0 {
			out = append(out, buf.String())
			buf.Reset()
		}
	}
	for _, r := range s {
		if esc {
			buf.WriteRune(r)
			esc = false
			continue
		}
		switch r {
		case '\\':
			esc = true
		case '"':
			inQuote = !inQuote
		case ' ', '\t':
			if inQuote { buf.WriteRune(r) } else { flush() }
		default:
			buf.WriteRune(r)
		}
	}
	flush()
	return out
}

func (c *Controller) logStatus() {
	// container state
	name := c.mounterName()
	args := filters.NewArgs()
	args.Add("name", name)
	conts, err := c.cli.ContainerList(c.ctx, container.ListOptions{All: true, Filters: args})
	running := false
	if err == nil && len(conts) > 0 {
		id := conts[0].ID
		if inspect, err := c.cli.ContainerInspect(c.ctx, id); err == nil && inspect.State != nil {
			running = inspect.State.Running
		}
	}
	mountOK := testRW(c.cfg.Mountpoint) == nil
	c.metricsMu.Lock()
	c.lastMounterRunning = running
	c.lastMountWritable = mountOK
	c.metricsMu.Unlock()
	slog.Info("status", "mounter_running", running, "mount_writable", mountOK, "last_image_pull", c.lastImagePull.Format(time.RFC3339))
}

// --- Declarative volume (prefix) provisioning via service/container labels ---

// parseLabels 仅接受 `volkit.*` 标签。
func (c *Controller) parseLabels(labels map[string]string) map[string]string {
	allowed := map[string]struct{}{
		"volkit.enabled": {},
		"volkit.bucket":  {},
		"volkit.prefix":  {},
		"volkit.class":   {},
		"volkit.reclaim": {},
		"volkit.access":  {},
		"volkit.args":    {},
		"volkit.claims":  {},
	}
	values := map[string]struct {
		v       string
		pref    bool
		pfxName string
	}{}
	for k, v := range labels {
		base := k
		prefix := ""
		if i := strings.Index(k, "/"); i >= 0 { prefix = k[:i]; base = k[i+1:] }
		// 兼容索引化：允许 volkit.N.prefix
		b := base
		if dot := strings.Index(base, "."); dot > 0 { b = base[:dot] }
		if _, ok := allowed[b]; !ok { continue }
		canonical := base
		fromPref := prefix != ""
		if old, ok := values[canonical]; ok {
			if !old.pref && fromPref { values[canonical] = struct{ v string; pref bool; pfxName string }{v: v, pref: true, pfxName: prefix} }
			continue
		}
		values[canonical] = struct{ v string; pref bool; pfxName string }{v: v, pref: fromPref, pfxName: prefix}
	}
	out := make(map[string]string, len(values))
	for k, meta := range values { out[k] = meta.v }
	return out
}

type claimSpec struct {
	enabled bool
	bucket  string
	prefix  string
	mode    string   // local | remote | both (empty -> local)
	nodes   []string // explicit node names (optional)
	selector string  // simple node selector, e.g. role==gpu or role in [edge,gpu]
	level   string   // gold|silver|bronze (optional)
	class   string
	reclaim string // Retain|Delete
	access  string // rw|ro
	args    string // extra rclone args suggestion (not enforced per-service)
	uid     int
	gid     int
	chmod   string
	mountpt string
}

// ClaimOut is the JSON structure returned by /claims
type ClaimOut struct {
	Bucket     string `json:"bucket"`
	Prefix     string `json:"prefix"`
	Mode       string `json:"mode,omitempty"`
	Level      string `json:"level,omitempty"`
	UID        int    `json:"uid,omitempty"`
	GID        int    `json:"gid,omitempty"`
	Chmod      string `json:"chmod,omitempty"`
	Mountpoint string `json:"mountpoint,omitempty"`
}

// ClaimsForNode aggregates Service claims and returns those targeted to a node.
func (c *Controller) ClaimsForNode(node string) []ClaimOut {
	var result []ClaimOut
	curVer := c.ChangeVersion()
	// cache hit
	c.claimsCacheMu.Lock()
	if c.claimsByNode != nil && c.claimsCacheVer == curVer {
		if v, ok := c.claimsByNode[node]; ok {
			out := make([]ClaimOut, len(v))
			copy(out, v)
			c.claimsCacheMu.Unlock()
			c.metricsMu.Lock(); c.claimsCacheHits++; c.metricsMu.Unlock()
			return out
		}
	}
	c.claimsCacheMu.Unlock()
	start := time.Now()
	svcs, err := c.serviceList()
	if err != nil {
		slog.Warn("claims list services", "error", err)
		return result
	}
	name2labels := c.nodeLabelsByName()
	for _, svc := range svcs {
		labels := svc.Spec.Labels
		claims := c.parseIndexedClaims(labels)
		for _, cl := range claims {
			if !cl.enabled || strings.TrimSpace(cl.prefix) == "" {
				continue
			}
			if len(cl.nodes) > 0 && node != "" {
				matched := false
				for _, n := range cl.nodes {
					if strings.EqualFold(strings.TrimSpace(n), node) { matched = true; break }
				}
				if !matched { continue }
			}
			if strings.TrimSpace(cl.selector) != "" && node != "" {
				if !c.nodeMatchesSelector(node, cl.selector, name2labels) { continue }
			}
			result = append(result, ClaimOut{Bucket: cl.bucket, Prefix: cl.prefix, Mode: cl.mode, Level: cl.level, UID: cl.uid, GID: cl.gid, Chmod: cl.chmod, Mountpoint: cl.mountpt})
		}
	}
	// store in cache
	c.claimsCacheMu.Lock()
	if c.claimsByNode == nil || c.claimsCacheVer != curVer {
		c.claimsByNode = make(map[string][]ClaimOut)
		c.claimsCacheVer = curVer
	}
	cpy := make([]ClaimOut, len(result))
	copy(cpy, result)
	c.claimsByNode[node] = cpy
	c.claimsCacheMu.Unlock()
	c.metricsMu.Lock(); c.claimsCacheMiss++; c.lastClaimsComputeMs = time.Since(start).Milliseconds(); c.metricsMu.Unlock()
	return result
}

func (c *Controller) serviceList() ([]types.Service, error) {
	// short TTL cache (configurable; default 800ms)
	ttl := 800 * time.Millisecond
	if v := strings.TrimSpace(os.Getenv("VOLKIT_CACHE_TTL_MS")); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms >= 0 && ms <= 10000 {
			ttl = time.Duration(ms) * time.Millisecond
		}
	}
	c.cacheMu.Lock()
	if time.Now().Before(c.svcCacheUntil) && c.cachedServices != nil {
		sv := make([]types.Service, len(c.cachedServices))
		copy(sv, c.cachedServices)
		c.cacheMu.Unlock()
		return sv, nil
	}
	c.cacheMu.Unlock()
	cliRef := c.cli
	if c.managerCli != nil { cliRef = c.managerCli }
	ctx, cancel := context.WithTimeout(c.ctx, 1500*time.Millisecond)
	defer cancel()
	start := time.Now()
	svcs, err := cliRef.ServiceList(ctx, types.ServiceListOptions{})
	dur := time.Since(start).Milliseconds()
	c.metricsMu.Lock(); c.lastServiceListMs = dur; c.metricsMu.Unlock()
	c.observeSvcDur(int(dur))
	if err != nil { c.metricsMu.Lock(); c.dockerApiErrors++; c.metricsMu.Unlock(); return nil, err }
	c.cacheMu.Lock(); c.cachedServices = svcs; c.svcCacheUntil = time.Now().Add(ttl); c.cacheMu.Unlock()
	// 客户端过滤：仅保留含 volkit.* 标签的服务
	var out []types.Service
	for _, s := range svcs {
		lbl := s.Spec.Labels
		if lbl == nil { continue }
		if _, ok := lbl["volkit.enabled"]; ok { out = append(out, s); continue }
		if _, ok := lbl["volkit.claims"]; ok { out = append(out, s); continue }
		pick := false
		for k := range lbl { if strings.HasPrefix(k, "volkit.") && strings.Contains(k, ".prefix") { pick = true; break } }
		if pick { out = append(out, s) }
	}
	return out, nil
}

// parseIndexedClaims 仅支持 volkit.* 与 volkit.N.*
func (c *Controller) parseIndexedClaims(labels map[string]string) []claimSpec {
	m := c.parseLabels(labels)
	var out []claimSpec
	// multi-claim indexed
	cnt := atoi(m["volkit.claims"]) 
	if cnt > 0 {
		for i := 1; i <= cnt; i++ {
			pref := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.prefix", i)])
			buck := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.bucket", i)])
			mode := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.mode", i)])
			nodesCsv := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.nodes", i)])
			selector := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.nodeSelector", i)])
			level := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.level", i)])
			uid := atoi(labels[fmt.Sprintf("volkit.%d.uid", i)])
			gid := atoi(labels[fmt.Sprintf("volkit.%d.gid", i)])
			chmod := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.chmod", i)])
			mp := strings.TrimSpace(labels[fmt.Sprintf("volkit.%d.mountpoint", i)])
			if pref == "" { continue }
			var nodes []string
			if nodesCsv != "" { nodes = splitCSV(nodesCsv) }
			if selector == "" && level != "" { selector = fmt.Sprintf("trust==%s", level) }
			out = append(out, claimSpec{enabled: true, bucket: buck, prefix: strings.Trim(pref, "/"), mode: strings.ToLower(mode), nodes: nodes, selector: selector, level: strings.ToLower(level), uid: uid, gid: gid, chmod: chmod, mountpt: mp})
		}
		return out
	}
	// single-claim
	if strings.EqualFold(m["volkit.enabled"], "true") {
		level := strings.TrimSpace(labels["volkit.level"]) 
		selector := strings.TrimSpace(labels["volkit.nodeSelector"]) 
		if selector == "" && level != "" { selector = fmt.Sprintf("trust==%s", level) }
		out = append(out, claimSpec{
			enabled: true,
			bucket:  m["volkit.bucket"],
			prefix:  strings.Trim(m["volkit.prefix"], "/"),
			mode:    strings.ToLower(strings.TrimSpace(labels["volkit.mode"])) ,
			nodes:   splitCSV(labels["volkit.nodes"]),
			selector: selector,
			level:   strings.ToLower(level),
			uid:     atoi(labels["volkit.uid"]),
			gid:     atoi(labels["volkit.gid"]),
			chmod:   strings.TrimSpace(labels["volkit.chmod"]),
			mountpt: strings.TrimSpace(labels["volkit.mountpoint"]),
		})
	}
	return out
}

// node label helpers
func (c *Controller) nodeLabelsByName() map[string]map[string]string {
	// short TTL cache (configurable; default 800ms)
	ttl := 800 * time.Millisecond
	if v := strings.TrimSpace(os.Getenv("VOLKIT_CACHE_TTL_MS")); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms >= 0 && ms <= 10000 {
			ttl = time.Duration(ms) * time.Millisecond
		}
	}
	c.cacheMu.Lock()
	var nodes []swarm.Node
	if time.Now().Before(c.nodeCacheUntil) && c.cachedNodes != nil {
		nodes = make([]swarm.Node, len(c.cachedNodes))
		copy(nodes, c.cachedNodes)
		c.cacheMu.Unlock()
	} else {
		c.cacheMu.Unlock()
		cliRef := c.cli
		if c.managerCli != nil { cliRef = c.managerCli }
		ctx, cancel := context.WithTimeout(c.ctx, 1500*time.Millisecond)
		defer cancel()
		start := time.Now()
		var err error
		nodes, err = cliRef.NodeList(ctx, types.NodeListOptions{})
		dur := time.Since(start).Milliseconds()
		c.metricsMu.Lock(); c.lastNodeListMs = dur; c.metricsMu.Unlock()
		c.observeNodeDur(int(dur))
		if err != nil { return map[string]map[string]string{} }
		c.cacheMu.Lock(); c.cachedNodes = nodes; c.nodeCacheUntil = time.Now().Add(ttl); c.cacheMu.Unlock()
	}
	res := make(map[string]map[string]string, len(nodes))
	for _, n := range nodes {
		name := strings.TrimSpace(n.Description.Hostname)
		if name == "" { name = n.ID }
		m := map[string]string{}
		if n.Spec.Labels != nil { for k, v := range n.Spec.Labels { m[k] = v } }
		res[name] = m
	}
	return res
}

func (c *Controller) nodeMatchesSelector(node string, selector string, name2labels map[string]map[string]string) bool {
	if strings.TrimSpace(selector) == "" { return true }
	labels := name2labels[node]
	if labels == nil { return false }
	sel := strings.TrimSpace(selector)
	if strings.Contains(sel, " in ") {
		parts := strings.SplitN(sel, " in ", 2)
		key := strings.TrimSpace(parts[0])
		vals := strings.Trim(parts[1], " []")
		list := splitCSV(vals)
		v := labels[key]
		for _, one := range list { if one == v { return true } }
		return false
	}
	if strings.Contains(sel, "==") {
		parts := strings.SplitN(sel, "==", 2)
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		return labels[key] == val
	}
	// unknown syntax -> fail closed
	return false
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" { return nil }
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" { out = append(out, p) }
	}
	return out
}

func atoi(s string) int { i, _ := strconv.Atoi(strings.TrimSpace(s)); return i }

func (c *Controller) provisionClaims() error {
	// coordinator mode: aggregate all service claims and optionally create remote placeholders (single point)
	if strings.EqualFold(c.cfg.Mode, "coordinator") {
		// list services
		svcs, err := c.serviceList()
		if err != nil {
			slog.Warn("coordinator list services", "error", err)
			return nil
		}
		// optional allowlist regex
		var allow *regexp.Regexp
		if strings.TrimSpace(c.cfg.ClaimAllowlistRegex) != "" {
			if re, err := regexp.Compile(c.cfg.ClaimAllowlistRegex); err == nil { allow = re } else { slog.Warn("invalid allowlist regex", "regex", c.cfg.ClaimAllowlistRegex, "error", err) }
		}
		for _, svc := range svcs {
			labels := svc.Spec.Labels
			claims := c.parseIndexedClaims(labels)
			for _, s := range claims {
				if !s.enabled || strings.TrimSpace(s.prefix) == "" { continue }
				if allow != nil && !allow.MatchString(s.prefix) { continue }
				mode := strings.ToLower(strings.TrimSpace(s.mode))
				if mode != "remote" && mode != "both" && mode != "" { continue }
				if !c.cfg.ReadOnly {
					if c.cfg.AutoCreateBucket && strings.TrimSpace(s.bucket) != "" { _ = c.runRcloneCmd([]string{"mkdir", fmt.Sprintf("S3:%s", s.bucket)}) }
					if c.cfg.AutoCreatePrefix && strings.TrimSpace(s.bucket) != "" {
						remotePath := fmt.Sprintf("S3:%s/%s", s.bucket, strings.Trim(s.prefix, "/"))
						_ = c.runRcloneCmd([]string{"mkdir", remotePath})
					}
					if strings.EqualFold(c.cfg.CreatePlaceholder, "coordinator") && strings.TrimSpace(s.bucket) != "" {
						if c.cfg.CoordinatorLeaderOnly && !c.isSwarmLeader() { continue }
						if c.allowPlaceholder(s.bucket, s.prefix) {
							rel := strings.TrimSpace(c.cfg.ControlObjectRelPath)
							if rel == "" { rel = ".volkit/.ready" }
							placeholder := fmt.Sprintf("S3:%s/%s/%s", s.bucket, strings.Trim(s.prefix, "/"), strings.TrimLeft(rel, "/"))
							_ = c.runRcloneCmd([]string{"touch", placeholder})
						}
					}
				}
			}
		}
		return nil
	}

	// agent mode pulling from coordinator
	if strings.EqualFold(c.cfg.Mode, "agent") && strings.TrimSpace(c.cfg.CoordinatorURL) != "" {
		me, _ := os.Hostname()
		claims, _, err := c.fetchClaims(me)
		if err != nil { slog.Warn("fetch claims", "error", err); return nil }
		// desired named volumes for this node (by name)
		desiredVolumes := map[string]struct{}{}
		for _, cl := range claims {
			mode := strings.ToLower(strings.TrimSpace(cl.Mode))
			if mode == "" || mode == "local" || mode == "both" {
				base := c.cfg.Mountpoint
				if strings.TrimSpace(cl.Mountpoint) != "" { base = cl.Mountpoint }
				p := filepath.Join(base, filepath.Clean("/"+cl.Prefix))
				if err := os.MkdirAll(p, 0o755); err != nil { slog.Warn("claim mkdir", "path", p, "error", err) }
				if cl.Chmod != "" { if m, err := strconv.ParseInt(cl.Chmod, 8, 32); err == nil { _ = os.Chmod(p, fs.FileMode(m)) } }
				if cl.UID > 0 || cl.GID > 0 { _ = os.Chown(p, cl.UID, cl.GID) }
				// create named volume only when mount is writable and control object is present
				if testRW(base) == nil {
					rel := strings.TrimSpace(c.cfg.ControlObjectRelPath)
					if rel == "" { rel = ".volkit/.ready" }
					ctrlPath := filepath.Join(p, filepath.Clean("/"+rel))
					if fi, err := os.Stat(ctrlPath); err == nil && !fi.IsDir() {
						vname := c.volumeNameForPrefix(cl.Prefix)
						opts := volume.CreateOptions{ Name: vname, Driver: "local", DriverOpts: map[string]string{"type": "none", "o": "bind", "device": p}, Labels: map[string]string{"volkit.managed": "true", "volkit.prefix": strings.Trim(cl.Prefix, "/") } }
						if _, err := c.cli.VolumeCreate(c.ctx, opts); err != nil {
							slog.Warn("volume create", "name", vname, "device", p, "error", err)
						} else {
							desiredVolumes[vname] = struct{}{}
							c.metricsMu.Lock(); c.volumeCreatedTotal++; c.metricsMu.Unlock()
						}
					}
				}
			}
		}
		// reclaim stale volkit volumes on this node when enabled
		if c.cfg.VolumeReclaimEnabled {
			if list, err := c.cli.VolumeList(c.ctx, volume.ListOptions{}); err == nil {
				prefix := strings.TrimSpace(c.cfg.VolumeNamePrefix)
				if prefix == "" { prefix = "volkit-" }
				for _, v := range list.Volumes {
					if !strings.HasPrefix(v.Name, prefix) { continue }
					// extra safety: only remove when label marks managed and device matches under mountpoint
					if v.Labels["volkit.managed"] != "true" { continue }
					dev := strings.TrimSpace(v.Options["device"])
					root := strings.TrimRight(c.cfg.Mountpoint, "/")+"/"
					if dev == "" || !strings.HasPrefix(dev, root) {
						// drift: managed volume points outside expected root
						slog.Warn("volume drift detected", "name", v.Name, "device", dev, "expected_prefix", root)
						c.metricsMu.Lock(); c.volumeDriftTotal++; c.metricsMu.Unlock()
						continue
					}
					if _, ok := desiredVolumes[v.Name]; ok { continue }
					if err := c.cli.VolumeRemove(c.ctx, v.Name, true); err != nil {
						slog.Warn("volume remove", "name", v.Name, "error", err)
					} else {
						c.metricsMu.Lock(); c.orphanCleanupTotal++; c.volumeReclaimedTotal++; c.metricsMu.Unlock()
					}
				}
			}
		}
		return nil
	}

	// default: legacy local behavior
	// ensure mount is writable first
	if err := testRW(c.cfg.Mountpoint); err != nil { return err }
	svSpecs, err := c.collectServiceClaimSpecs()
	if err != nil { slog.Warn("collect service claims", "error", err); svSpecs = nil }
	for _, s := range svSpecs {
		if !s.enabled || s.prefix == "" { continue }
		if err := c.ensureRemotePaths(s); err != nil { slog.Warn("claim ensure remote", "bucket", s.bucket, "prefix", s.prefix, "error", err) }
		p := filepath.Join(c.cfg.Mountpoint, filepath.Clean("/"+s.prefix))
		if err := os.MkdirAll(p, 0o755); err != nil { slog.Warn("claim mkdir", "path", p, "error", err) }
	}
	return nil
}

// fetchClaims issues GET /claims, supports If-None-Match and 304
func (c *Controller) fetchClaims(node string) ([]ClaimOut, bool, error) {
	u := strings.TrimRight(c.cfg.CoordinatorURL, "/") + "/claims?node=" + url.QueryEscape(node)
	ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if et := strings.TrimSpace(c.lastClaimsETag); et != "" { req.Header.Set("If-None-Match", et) }
	resp, err := c.httpClient().Do(req)
	if err != nil { return nil, false, err }
	defer func() { if resp.Body != nil { _ = resp.Body.Close() } }()
	if resp.StatusCode == http.StatusNotModified { return nil, true, nil }
	if resp.StatusCode/100 != 2 { return nil, false, fmt.Errorf("http %d", resp.StatusCode) }
	if et := strings.TrimSpace(resp.Header.Get("ETag")); et != "" { c.lastClaimsETag = et }
	var items []ClaimOut
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil { return nil, false, err }
	return items, false, nil
}

func (c *Controller) collectClaimSpecs(conts []types.Container) []claimSpec {
	var out []claimSpec
	for _, ct := range conts {
		if len(ct.Labels) == 0 {
			continue
		}
		var cs claimSpec
		m := c.parseLabels(ct.Labels)
		if v, ok := m["volkit.enabled"]; ok {
			cs.enabled = strings.EqualFold(v, "true")
		}
		if v, ok := m["volkit.bucket"]; ok {
			cs.bucket = v
		}
		if v, ok := m["volkit.prefix"]; ok {
			cs.prefix = strings.Trim(v, "/")
		}
		if v, ok := m["volkit.class"]; ok {
			cs.class = v
		}
		if v, ok := m["volkit.reclaim"]; ok {
			cs.reclaim = v
		}
		if v, ok := m["volkit.access"]; ok {
			cs.access = v
		}
		if v, ok := m["volkit.args"]; ok {
			cs.args = v
		}
		if cs.enabled {
			out = append(out, cs)
		}
	}
	return out
}

// collectServiceClaimSpecs builds claim specs from Swarm Service labels (preferred),
// and optionally infers prefixes from ServiceSpec.Mounts when enabled and no explicit prefix.
func (c *Controller) collectServiceClaimSpecs() ([]claimSpec, error) {
    var out []claimSpec
    // Prefer manager client if configured
    cliRef := c.cli
    if c.managerCli != nil {
        cliRef = c.managerCli
    }
    svcs, err := cliRef.ServiceList(c.ctx, types.ServiceListOptions{})
    if err != nil {
        return nil, err
    }
    // compile optional allowlist regex
    var allow *regexp.Regexp
    if strings.TrimSpace(c.cfg.ClaimAllowlistRegex) != "" {
        if re, err := regexp.Compile(c.cfg.ClaimAllowlistRegex); err == nil {
            allow = re
        } else {
            slog.Warn("invalid allowlist regex", "regex", c.cfg.ClaimAllowlistRegex, "error", err)
        }
    }
    for _, svc := range svcs {
        m := c.parseLabels(svc.Spec.Labels)
        var cs claimSpec
        if v, ok := m["volkit.enabled"]; ok {
            cs.enabled = strings.EqualFold(v, "true")
        }
        if v, ok := m["volkit.bucket"]; ok { cs.bucket = v }
        if v, ok := m["volkit.prefix"]; ok { cs.prefix = strings.Trim(v, "/") }
        if v, ok := m["volkit.class"]; ok { cs.class = v }
        if v, ok := m["volkit.reclaim"]; ok { cs.reclaim = v }
        if v, ok := m["volkit.access"]; ok { cs.access = v }
        if v, ok := m["volkit.args"]; ok { cs.args = v }

        // If enabled and no explicit prefix, infer from mounts under our mountpoint
        if cs.enabled && cs.prefix == "" && c.cfg.AutoClaimFromMounts {
            mounts := svc.Spec.TaskTemplate.ContainerSpec.Mounts
            for _, mnt := range mounts {
                // Avoid SDK const dependency differences; compare by string value
                if strings.EqualFold(string(mnt.Type), "bind") {
                    src := strings.TrimSpace(mnt.Source)
                    mp := strings.TrimRight(c.cfg.Mountpoint, "/") + "/"
                    if strings.HasPrefix(src, mp) {
                        pref := strings.Trim(strings.TrimPrefix(src, mp), "/")
                        if pref != "" {
                            if allow != nil && !allow.MatchString(pref) {
                                continue
                            }
                            cs.prefix = pref
                            break
                        }
                    }
                }
            }
        }

        if cs.enabled && cs.prefix != "" {
            out = append(out, cs)
        }
    }
    return out, nil
}

func (c *Controller) buildRcloneEnv() []string {
	// credentials: env overrides file
	access := strings.TrimSpace(os.Getenv("VOLKIT_ACCESS_KEY"))
	secret := strings.TrimSpace(os.Getenv("VOLKIT_SECRET_KEY"))
	token := strings.TrimSpace(os.Getenv("VOLKIT_SESSION_TOKEN"))
	if access == "" {
		if b, err := os.ReadFile(c.cfg.AccessKeyFile); err == nil {
			access = strings.TrimSpace(string(b))
		}
	}
	if secret == "" {
		if b, err := os.ReadFile(c.cfg.SecretKeyFile); err == nil {
			secret = strings.TrimSpace(string(b))
		}
	}
	env := []string{
		"RCLONE_CONFIG_S3_TYPE=s3",
		fmt.Sprintf("RCLONE_CONFIG_S3_ACCESS_KEY_ID=%s", access),
		fmt.Sprintf("RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=%s", secret),
		fmt.Sprintf("RCLONE_CONFIG_S3_ENDPOINT=%s", c.resolveEndpointForMounter()),
	}
	if token != "" { env = append(env, fmt.Sprintf("RCLONE_CONFIG_S3_SESSION_TOKEN=%s", token)) }
	if strings.TrimSpace(c.cfg.S3Provider) != "" {
		env = append(env, fmt.Sprintf("RCLONE_CONFIG_S3_PROVIDER=%s", c.cfg.S3Provider))
	}
	return env
}

// BuildRcloneEnvPublic exposes the current rclone env (used by /creds)
func (c *Controller) BuildRcloneEnvPublic() []string { return c.buildRcloneEnv() }

func (c *Controller) ensureRemotePaths(s claimSpec) error {
	// Only act when configured
	if !(c.cfg.AutoCreateBucket || c.cfg.AutoCreatePrefix) {
		return nil
	}
	// read-only mode skips remote creation
	if c.cfg.ReadOnly {
		return nil
	}
	// require bucket name for remote operations
	if strings.TrimSpace(s.bucket) == "" {
		return nil
	}
	// mkdir bucket
	if c.cfg.AutoCreateBucket {
		if err := c.runRcloneCmd([]string{"mkdir", fmt.Sprintf("S3:%s", s.bucket)}); err != nil {
			// ignore errors like already exists
			slog.Warn("mkdir bucket", "bucket", s.bucket, "error", err)
		}
	}
	if c.cfg.AutoCreatePrefix && strings.TrimSpace(s.prefix) != "" {
		remotePath := fmt.Sprintf("S3:%s/%s", s.bucket, strings.Trim(s.prefix, "/"))
		if err := c.runRcloneCmd([]string{"mkdir", remotePath}); err != nil {
			slog.Warn("mkdir prefix", "path", remotePath, "error", err)
		}
		// also create control object when configured (legacy/local path)
		rel := strings.TrimSpace(c.cfg.ControlObjectRelPath)
		if rel == "" { rel = ".volkit/.ready" }
		placeholder := fmt.Sprintf("S3:%s/%s/%s", s.bucket, strings.Trim(s.prefix, "/"), strings.TrimLeft(rel, "/"))
		_ = c.runRcloneCmd([]string{"touch", placeholder})
	}
	return nil
}

func (c *Controller) runRcloneCmd(cmd []string) error {
	name := c.helperName("rclone-run")
	env := c.buildRcloneEnv()
	// Ensure helper can reach the S3 endpoint: attach to overlay network if provided
	var netCfg *network.NetworkingConfig
	if strings.TrimSpace(c.cfg.ProxyNetwork) != "" {
		netCfg = &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{
			c.cfg.ProxyNetwork: {},
		}}
	} else {
		netCfg = &network.NetworkingConfig{}
	}
	// ensure mounter image exists (used to run rclone cmd)
	if err := c.ensureImagePresent(c.cfg.MounterImage); err != nil {
		return err
	}
	cont, err := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.cfg.MounterImage, Env: env, Cmd: cmd},
		&container.HostConfig{NetworkMode: c.selfNetworkMode()},
		netCfg, nil, name)
	if err != nil {
		return err
	}
	defer func() { _ = c.cli.ContainerRemove(context.Background(), cont.ID, container.RemoveOptions{Force: true}) }()
	if err := c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{}); err != nil {
		return err
	}
	// wait for completion with timeout and include logs on failure
	wctx, wcancel := c.timeoutCtx(120 * time.Second)
	statusCh, errCh := c.cli.ContainerWait(wctx, cont.ID, container.WaitConditionNotRunning)
	select {
	case st := <-statusCh:
		if st.StatusCode != 0 {
			if rc, lgErr := c.cli.ContainerLogs(context.Background(), cont.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: "120"}); lgErr == nil {
				defer rc.Close()
				b, _ := io.ReadAll(io.LimitReader(rc, 128*1024))
				slog.Warn("rclone run non-zero exit", "cmd", strings.Join(cmd, " "), "code", st.StatusCode, "logs", string(b))
			}
			wcancel()
			return fmt.Errorf("rclone exit code %d", st.StatusCode)
		}
	case err := <-errCh:
		if err != nil {
			if rc, lgErr := c.cli.ContainerLogs(context.Background(), cont.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true, Tail: "120"}); lgErr == nil {
				defer rc.Close()
				b, _ := io.ReadAll(io.LimitReader(rc, 128*1024))
				slog.Warn("rclone run failed", "cmd", strings.Join(cmd, " "), "error", err, "logs", string(b))
			}
			wcancel()
			return err
		}
	}
	wcancel()
	return nil
}

// MintServiceAccount creates a short-lived MinIO Service Account using root credentials via mc
// Returns (accessKey, secretKey, expiresAtUnix)
func (c *Controller) MintServiceAccount(level string) (string, string, int64, error) {
    if !strings.EqualFold(strings.TrimSpace(os.Getenv("VOLKIT_ALLOW_ROOT_ISSUER")), "true") {
        return "", "", 0, fmt.Errorf("root issuer not allowed")
    }
    rootAK := strings.TrimSpace(os.Getenv("VOLKIT_ISSUER_ACCESS_KEY"))
    rootSK := strings.TrimSpace(os.Getenv("VOLKIT_ISSUER_SECRET_KEY"))
    if rootAK == "" || rootSK == "" {
        return "", "", 0, fmt.Errorf("missing root issuer keys")
    }
    img := strings.TrimSpace(os.Getenv("VOLKIT_MC_IMAGE"))
    if img == "" { img = "minio/mc:latest" }
    if err := c.ensureImagePresent(img); err != nil { return "", "", 0, err }
    // expiry
    ttlSec := 3600
    if v := strings.TrimSpace(os.Getenv("VOLKIT_ISSUER_TTL_SEC")); v != "" {
        if n, err := strconv.Atoi(v); err == nil && n >= 300 && n <= 43200 { ttlSec = n }
    }
    // name prefix
    pfx := strings.TrimSpace(os.Getenv("VOLKIT_SA_PREFIX"))
    if pfx == "" { pfx = "volkit-" }
    name := fmt.Sprintf("%s%s-%d", pfx, strings.ToLower(strings.TrimSpace(level)), time.Now().Unix())
    // build expiry duration string (minutes)
    var expiry string
    if ttlSec%3600 == 0 { expiry = fmt.Sprintf("%dh", ttlSec/3600) } else if ttlSec%60 == 0 { expiry = fmt.Sprintf("%dm", ttlSec/60) } else { expiry = fmt.Sprintf("%ds", ttlSec) }
    endpoint := c.resolveEndpointForMounter()
    // Run mc in helper container and emit JSON only
    sh := fmt.Sprintf(`set -e
mc alias set minio %s %s %s >/dev/null
mc admin user svcacct add --expiry %s --name %s --json minio %s
`, endpoint, rootAK, rootSK, expiry, name, rootAK)
    cont, err := c.cli.ContainerCreate(c.ctx,
        &container.Config{Image: img, Cmd: []string{"sh", "-lc", sh}},
        &container.HostConfig{NetworkMode: c.selfNetworkMode()},
        &network.NetworkingConfig{}, nil, c.helperName("mc-sa"))
    if err != nil { return "", "", 0, err }
    defer func() { _ = c.cli.ContainerRemove(context.Background(), cont.ID, container.RemoveOptions{Force: true}) }()
    if err := c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{}); err != nil { return "", "", 0, err }
    _, _ = c.cli.ContainerWait(c.ctx, cont.ID, container.WaitConditionNotRunning)
    // read logs
    rc, lgErr := c.cli.ContainerLogs(context.Background(), cont.ID, container.LogsOptions{ShowStdout: true, ShowStderr: false, Tail: "10"})
    if lgErr != nil { return "", "", 0, fmt.Errorf("mc logs: %v", lgErr) }
    defer rc.Close()
    b, _ := io.ReadAll(io.LimitReader(rc, 64*1024))
    // Parse JSON
    type resp struct { AccessKey string `json:"accessKey"`; SecretKey string `json:"secretKey"`; Expiration string `json:"expiration"` }
    var r resp
    if err := json.Unmarshal(b, &r); err != nil { return "", "", 0, fmt.Errorf("parse mc output: %v body=%s", err, string(b)) }
    if strings.TrimSpace(r.AccessKey) == "" || strings.TrimSpace(r.SecretKey) == "" { return "", "", 0, fmt.Errorf("mc returned empty keys: %s", string(b)) }
    var expires int64
    if strings.TrimSpace(r.Expiration) != "" {
        if t, e := time.Parse(time.RFC3339, strings.TrimSpace(r.Expiration)); e == nil { expires = t.Unix() }
    }
    return strings.TrimSpace(r.AccessKey), strings.TrimSpace(r.SecretKey), expires, nil
}

// pruneServiceAccounts removes expired Service Accounts created by volkit after a grace period
func (c *Controller) pruneServiceAccounts() error {
    if !strings.EqualFold(strings.TrimSpace(os.Getenv("VOLKIT_SA_PRUNE")), "true") { return nil }
    img := strings.TrimSpace(os.Getenv("VOLKIT_MC_IMAGE"))
    if img == "" { img = "minio/mc:latest" }
    if err := c.ensureImagePresent(img); err != nil { return err }
    rootAK := strings.TrimSpace(os.Getenv("VOLKIT_ISSUER_ACCESS_KEY"))
    rootSK := strings.TrimSpace(os.Getenv("VOLKIT_ISSUER_SECRET_KEY"))
    if rootAK == "" || rootSK == "" { return fmt.Errorf("missing root issuer keys") }
    grace := 600
    if v := strings.TrimSpace(os.Getenv("VOLKIT_SA_PRUNE_GRACE_SEC")); v != "" { if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 86400 { grace = n } }
    pfx := strings.TrimSpace(os.Getenv("VOLKIT_SA_PREFIX")); if pfx == "" { pfx = "volkit-" }
    endpoint := c.resolveEndpointForMounter()
    // list all service accounts for root and delete expired ones by name prefix and expiration < now-grace
    sh := fmt.Sprintf(`set -e
mc alias set minio %s %s %s >/dev/null
mc admin user svcacct list --json minio %s
`, endpoint, rootAK, rootSK, rootAK)
    cont, err := c.cli.ContainerCreate(c.ctx,
        &container.Config{Image: img, Cmd: []string{"sh", "-lc", sh}},
        &container.HostConfig{NetworkMode: c.selfNetworkMode()},
        &network.NetworkingConfig{}, nil, c.helperName("mc-sa-list"))
    if err != nil { return err }
    defer func() { _ = c.cli.ContainerRemove(context.Background(), cont.ID, container.RemoveOptions{Force: true}) }()
    if err := c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{}); err != nil { return err }
    _, _ = c.cli.ContainerWait(c.ctx, cont.ID, container.WaitConditionNotRunning)
    rc, lgErr := c.cli.ContainerLogs(context.Background(), cont.ID, container.LogsOptions{ShowStdout: true, ShowStderr: false, Tail: "200"})
    if lgErr != nil { return lgErr }
    defer rc.Close()
    data, _ := io.ReadAll(io.LimitReader(rc, 256*1024))
    // mc may return multiple JSON lines; iterate
    lines := strings.Split(string(data), "\n")
    now := time.Now().Add(-time.Duration(grace) * time.Second)
    toDelete := []string{}
    for _, ln := range lines {
        ln = strings.TrimSpace(ln); if ln == "" { continue }
        var m map[string]any
        if json.Unmarshal([]byte(ln), &m) == nil {
            name, _ := m["name"].(string)
            expStr, _ := m["expiration"].(string)
            if strings.HasPrefix(name, pfx) && strings.TrimSpace(expStr) != "" {
                if t, e := time.Parse(time.RFC3339, strings.TrimSpace(expStr)); e == nil {
                    if t.Before(now) { toDelete = append(toDelete, name) }
                }
            }
        }
    }
    if len(toDelete) == 0 { return nil }
    // delete them
    var b strings.Builder
    fmt.Fprintf(&b, "set -e\nmc alias set minio %s %s %s >/dev/null\n", endpoint, rootAK, rootSK)
    for _, n := range toDelete { fmt.Fprintf(&b, "mc admin user svcacct rm minio %s || true\n", n) }
    cont2, err2 := c.cli.ContainerCreate(c.ctx,
        &container.Config{Image: img, Cmd: []string{"sh", "-lc", b.String()}},
        &container.HostConfig{NetworkMode: c.selfNetworkMode()},
        &network.NetworkingConfig{}, nil, c.helperName("mc-sa-prune"))
    if err2 != nil { return err2 }
    defer func() { _ = c.cli.ContainerRemove(context.Background(), cont2.ID, container.RemoveOptions{Force: true}) }()
    if err := c.cli.ContainerStart(c.ctx, cont2.ID, container.StartOptions{}); err != nil { return err }
    _, _ = c.cli.ContainerWait(c.ctx, cont2.ID, container.WaitConditionNotRunning)
    c.metricsMu.Lock(); c.saPrunedTotal += int64(len(toDelete)); c.metricsMu.Unlock()
    return nil
}


// helperImageRef returns the image to use for helper containers (nsenter/unmount etc.).
// Prefers explicitly configured HelperImage; falls back to MounterImage as a best-effort.
func (c *Controller) helperImageRef() string {
    img := strings.TrimSpace(c.cfg.HelperImage)
    if img != "" {
        return img
    }
    return strings.TrimSpace(c.cfg.MounterImage)
}

// selfNetworkMode attempts to attach helper/mounter containers to the same
// network namespace as this controller container when possible.
func (c *Controller) selfNetworkMode() container.NetworkMode {
    if id := c.selfContainerID(); id != "" {
        return container.NetworkMode("container:" + id)
    }
    return container.NetworkMode("bridge")
}

// selfContainerID parses /proc/self/cgroup to discover this container's ID.
func (c *Controller) selfContainerID() string {
    if data, err := os.ReadFile("/proc/self/cgroup"); err == nil {
        lines := strings.Split(string(data), "\n")
        for _, ln := range lines {
            if ln == "" { continue }
            parts := strings.SplitN(ln, ":", 3)
            path := ln
            if len(parts) == 3 { path = parts[2] }
            if i := strings.LastIndex(path, "/"); i >= 0 {
                id := strings.TrimSpace(path[i+1:])
                id = strings.TrimSuffix(id, ".scope")
                id = strings.TrimPrefix(id, "docker-")
                if len(id) >= 12 { return id }
            }
        }
    }
    return ""
}

// cleanupOrphanedMounters removes exited/created rclone mounter containers that
// are managed by this controller (identified by name prefix and label).
func (c *Controller) cleanupOrphanedMounters() error {
	// Identify by name prefix and label
	args := filters.NewArgs()
	args.Add("name", "rclone-mounter-")
	args.Add("label", "swarmnative.mounter=managed")
	// Include non-running containers
	conts, err := c.cli.ContainerList(c.ctx, container.ListOptions{All: true, Filters: args})
	if err != nil {
		return err
	}
	removed := 0
	for _, ct := range conts {
		if ct.State == "running" || ct.State == "restarting" {
			continue
		}
		// best-effort remove
		_ = c.cli.ContainerRemove(c.ctx, ct.ID, container.RemoveOptions{Force: true})
		removed++
	}
	if removed > 0 {
		c.metricsMu.Lock(); c.orphanCleanupTotal += int64(removed); c.metricsMu.Unlock()
	}
	return nil
}

// MetricsSnapshot is a read-only copy of controller metrics/state for exposition.
type MetricsSnapshot struct {
	ReconcileTotal      int64
	ReconcileErrors     int64
	MounterRunning      bool
	MountWritable       bool
	HealAttemptsTotal   int64
	HealSuccessTotal    int64
	LastHealSuccessUnix int64
	OrphanCleanupTotal  int64
	VolumeCreatedTotal  int64
	VolumeReclaimedTotal int64
	VolumeDriftTotal    int64
	ReconcileDurationMs int64
	MounterCreatedTotal int64
	// new metrics
	ClaimsCacheHits     int64
	ClaimsCacheMiss     int64
	ClaimsComputeMs     int64
	DockerServiceListMs int64
	DockerNodeListMs    int64
	DockerApiErrors     int64
	CredsFetchOK        int64
	CredsFetchErrors    int64
	CredsBackoffTotal   int64
	MounterBackoffTotal int64
	ServicesBlockedTotal   int64
	ServicesUnblockedTotal int64
	SAPrunedTotal int64
	SAPruneErrors int64
}

func (c *Controller) Snapshot() MetricsSnapshot {
	c.metricsMu.Lock()
	snap := MetricsSnapshot{
		ReconcileTotal:      c.reconcileTotal,
		ReconcileErrors:     c.reconcileErrors,
		MounterRunning:      c.lastMounterRunning,
		MountWritable:       c.lastMountWritable,
		HealAttemptsTotal:   c.healAttemptsTotal,
		HealSuccessTotal:    c.healSuccessTotal,
		LastHealSuccessUnix: c.lastHealSuccessUnix,
		OrphanCleanupTotal:  c.orphanCleanupTotal,
		VolumeCreatedTotal:  c.volumeCreatedTotal,
		VolumeReclaimedTotal: c.volumeReclaimedTotal,
		VolumeDriftTotal:    c.volumeDriftTotal,
		ReconcileDurationMs: c.lastReconcileMs,
		MounterCreatedTotal: c.mounterCreatedTotal,
		ClaimsCacheHits:     c.claimsCacheHits,
		ClaimsCacheMiss:     c.claimsCacheMiss,
		ClaimsComputeMs:     c.lastClaimsComputeMs,
		DockerServiceListMs: c.lastServiceListMs,
		DockerNodeListMs:    c.lastNodeListMs,
		DockerApiErrors:     c.dockerApiErrors,
		CredsFetchOK:        c.credsFetchOK,
		CredsFetchErrors:    c.credsFetchErrors,
		CredsBackoffTotal:   c.credsBackoffTotal,
		MounterBackoffTotal: c.mounterBackoffTotal,
		ServicesBlockedTotal:   c.servicesBlockedTotal,
		ServicesUnblockedTotal: c.servicesUnblockedTotal,
		SAPrunedTotal: c.saPrunedTotal,
		SAPruneErrors: c.saPruneErrors,
	}
	c.metricsMu.Unlock()
	return snap
}

// ReadyCounts returns a map of level => number of nodes currently labeled ready for that level
func (c *Controller) ReadyCounts() map[string]int {
    nodes := c.nodeLabelsByName()
    out := map[string]int{}
    for _, labs := range nodes {
        for k, v := range labs {
            if !strings.HasPrefix(k, "volkit.ready.") { continue }
            if !strings.EqualFold(strings.TrimSpace(v), "true") { continue }
            lvl := strings.TrimPrefix(k, "volkit.ready.")
            out[lvl]++
        }
    }
    return out
}

// Cleanup attempts to lazy-unmount and remove the mounter container on shutdown
func (c *Controller) Cleanup() {
	if !c.cfg.UnmountOnExit {
		return
	}
	// lazy unmount via helper
	_ = c.checkAndHealMount()
	// stop & remove mounter if exists
	args := filters.NewArgs()
	args.Add("name", c.mounterName())
	conts, err := c.cli.ContainerList(c.ctx, container.ListOptions{All: true, Filters: args})
	if err == nil && len(conts) > 0 {
		id := conts[0].ID
		_ = c.cli.ContainerRemove(context.Background(), id, container.RemoveOptions{Force: true})
	}
}

func (c *Controller) Nudge() {
	select {
	case c.eventCh <- struct{}{}:
	default:
	}
}

func (c *Controller) Preflight() error {
	var errs []string
	// Docker API reachable
	if _, err := c.cli.Ping(c.ctx); err != nil {
		errs = append(errs, fmt.Sprintf("docker ping failed: %v", err))
	}
	// Credentials resolved
	env := c.buildRcloneEnv()
	hasAK := false
	hasSK := false
	for _, e := range env {
		if strings.HasPrefix(e, "RCLONE_CONFIG_S3_ACCESS_KEY_ID=") { hasAK = true }
		if strings.HasPrefix(e, "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=") { hasSK = true }
	}
	if !hasAK || !hasSK {
		errs = append(errs, "missing access/secret credentials (set VOLKIT_ACCESS_KEY/SECRET_KEY or mount secret files)")
	}
	// Helper image nsenter availability (best-effort)
	name := c.helperName("nsenter-check")
	cont, err := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.helperImageRef(), Cmd: []string{"sh", "-lc", "nsenter --version >/dev/null 2>&1 || exit 1"}},
		&container.HostConfig{}, &network.NetworkingConfig{}, nil, name)
	if err == nil {
		_ = c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{})
		_, werr := c.cli.ContainerWait(c.ctx, cont.ID, container.WaitConditionNotRunning)
		if werr != nil {
			errs = append(errs, "helper image may lack nsenter (set VOLKIT_NSENTER_HELPER_IMAGE to this image)")
		}
		_ = c.cli.ContainerRemove(c.ctx, cont.ID, container.RemoveOptions{Force: true})
	} else {
		errs = append(errs, fmt.Sprintf("cannot create helper for nsenter check: %v", err))
	}
	// Mounter image rclone presence check (best-effort)
	if mcont, merr := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.cfg.MounterImage, Cmd: []string{"sh", "-lc", "rclone version >/dev/null 2>&1"}},
		&container.HostConfig{}, &network.NetworkingConfig{}, nil, c.helperName("rclone-check")); merr == nil {
		_ = c.cli.ContainerStart(c.ctx, mcont.ID, container.StartOptions{})
		st, _ := c.cli.ContainerWait(c.ctx, mcont.ID, container.WaitConditionNotRunning)
		if st.StatusCode != 0 {
			errs = append(errs, "mounter image may not include rclone (set VOLKIT_RCLONE_IMAGE to a valid rclone image)")
		}
		_ = c.cli.ContainerRemove(c.ctx, mcont.ID, container.RemoveOptions{Force: true})
	}
	// SYS_ADMIN capability availability warning for rootless engines
	if pcont, perr := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.helperImageRef(), Cmd: []string{"true"}},
		&container.HostConfig{CapAdd: []string{"SYS_ADMIN"}}, &network.NetworkingConfig{}, nil, c.helperName("cap-check")); perr == nil {
		// created successfully; cleanup
		_ = c.cli.ContainerRemove(c.ctx, pcont.ID, container.RemoveOptions{Force: true})
	} else {
		errs = append(errs, "engine may be rootless or missing SYS_ADMIN; rclone fuse mount may not work")
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (c *Controller) mounterName() string {
	return "rclone-mounter-" + sanitizeHostname()
}

func (c *Controller) helperName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, sanitizeHostname())
}

func sanitizeHostname() string {
	hn, err := os.Hostname()
	if err != nil || hn == "" {
		hn = "unknown"
	}
	// replace non-alphanumeric with hyphen
	b := make([]rune, 0, len(hn))
	for _, r := range hn {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' {
			b = append(b, r)
		} else {
			b = append(b, '-')
		}
	}
	return strings.Trim(string(b), "-")
}

func (c *Controller) resolveEndpointForMounter() string {
	// If proxy and overlay network set, directly use controller overlay IP for node-local access
	if c.cfg.EnableProxy && strings.TrimSpace(c.cfg.ProxyNetwork) != "" {
		if ip := c.selfIPOnOverlay(c.cfg.ProxyNetwork); strings.TrimSpace(ip) != "" {
			return fmt.Sprintf("http://%s:%s", ip, strings.TrimSpace(c.cfg.ProxyPort))
		}
	}
	// Fallbacks: alias (legacy) then configured endpoint
	if c.cfg.EnableProxy && c.cfg.LocalLBEnabled && strings.TrimSpace(c.cfg.ProxyNetwork) != "" {
		return fmt.Sprintf("http://%s:%s", c.localAlias(), strings.TrimSpace(c.cfg.ProxyPort))
	}
	return c.cfg.S3Endpoint
}

// selfIPOnOverlay returns the IPv4 of this controller on the given overlay network.
func (c *Controller) selfIPOnOverlay(netName string) string {
	// Try hostname -> inspect
	if hn, err := os.Hostname(); err == nil && strings.TrimSpace(hn) != "" {
		if insp, err := c.cli.ContainerInspect(c.ctx, hn); err == nil {
			if insp.NetworkSettings != nil && insp.NetworkSettings.Networks != nil {
				if ep, ok := insp.NetworkSettings.Networks[netName]; ok && ep != nil {
					if ip := strings.TrimSpace(ep.IPAddress); ip != "" {
						return ip
					}
				}
			}
		}
	}
	// Fallback: parse cgroup to get container ID, then inspect
	if data, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, ln := range lines {
			if ln == "" { continue }
			parts := strings.SplitN(ln, ":", 3)
			path := ln
			if len(parts) == 3 { path = parts[2] }
			if i := strings.LastIndex(path, "/"); i >= 0 {
				id := strings.TrimSpace(path[i+1:])
				id = strings.TrimSuffix(id, ".scope")
				id = strings.TrimPrefix(id, "docker-")
				if len(id) >= 12 {
					if insp, err := c.cli.ContainerInspect(c.ctx, id); err == nil {
						if insp.NetworkSettings != nil && insp.NetworkSettings.Networks != nil {
							if ep, ok := insp.NetworkSettings.Networks[netName]; ok && ep != nil {
								if ip := strings.TrimSpace(ep.IPAddress); ip != "" {
									return ip
								}
							}
						}
					}
				}
			}
		}
	}
	return ""
}

func (c *Controller) localAlias() string {
	return "volkit-lb-" + sanitizeHostname()
}

// volumeNameForPrefix returns a deterministic local volume name for a given prefix
func (c *Controller) volumeNameForPrefix(prefix string) string {
	base := strings.TrimSpace(prefix)
	base = strings.Trim(strings.ReplaceAll(base, "/", "-"), "-")
	if base == "" { base = "root" }
	pfx := strings.TrimSpace(c.cfg.VolumeNamePrefix)
	if pfx == "" { pfx = "volkit-" }
	if len(base) > 64 { base = base[len(base)-64:] }
	return pfx + base
}

func testRW(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return err
	}
	f := filepath.Join(path, ".rw-test")
	if err := os.WriteFile(f, []byte("ok"), fs.FileMode(0o644)); err != nil {
		return err
	}
	_ = os.Remove(f)
	return nil
}

// ValidationResult provides structured validation outcome.
type ValidationResult struct {
	OK       bool              `json:"ok"`
	Errors   []string          `json:"errors"`
	Warnings []string          `json:"warnings"`
	Summary  map[string]any    `json:"summary,omitempty"`
}

// ValidateConfig performs static validation of configuration and environment hints.
// Heavy host/engine checks belong to Preflight().
func ValidateConfig(cfg Config) ValidationResult {
	var errs []string
	var warns []string

	trim := strings.TrimSpace
	if trim(cfg.Mountpoint) == "" {
		errs = append(errs, "mountpoint is empty (set VOLKIT_MOUNTPOINT)")
	}
	if trim(cfg.RcloneRemote) == "" {
		warns = append(warns, "rclone remote empty (set VOLKIT_RCLONE_REMOTE like S3:bucket)")
	}
	if trim(cfg.S3Endpoint) == "" {
		warns = append(warns, "S3 endpoint empty (set VOLKIT_ENDPOINT)")
	}
	// plaintext HTTP notice
	if strings.EqualFold(trim(os.Getenv("VOLKIT_HTTP_ENABLE")), "true") {
		warns = append(warns, "plaintext HTTP enabled; not recommended for production")
	}
	// TLS files sanity
	serverCert := trim(os.Getenv("VOLKIT_TLS_SERVER_CERT_FILE"))
	serverKey := trim(os.Getenv("VOLKIT_TLS_SERVER_KEY_FILE"))
	if (serverCert == "") != (serverKey == "") {
		warns = append(warns, "partial TLS server files configured (both cert and key required)")
	} else if serverCert != "" && serverKey != "" {
		if _, err := os.Stat(serverCert); err != nil { errs = append(errs, "server cert not readable: "+err.Error()) }
		if _, err := os.Stat(serverKey); err != nil { errs = append(errs, "server key not readable: "+err.Error()) }
	}
	for _, f := range []string{cfg.AccessKeyFile, cfg.SecretKeyFile} {
		if trim(f) != "" {
			if _, err := os.Stat(f); err != nil { warns = append(warns, "secret file missing: "+f) }
		}
	}
	issuerMode := strings.ToLower(trim(os.Getenv("VOLKIT_ISSUER_MODE")))
	if issuerMode == "s3_admin" && !strings.EqualFold(trim(os.Getenv("VOLKIT_ALLOW_ROOT_ISSUER")), "true") {
		warns = append(warns, "s3_admin issuer requires VOLKIT_ALLOW_ROOT_ISSUER=true; otherwise denied")
	}
	// host tools/caps cannot be validated statically here
	warns = append(warns, "host tools/caps check deferred to /preflight (nsenter, fusermount, SYS_ADMIN)")

	ok := len(errs) == 0
	summary := map[string]any{
		"endpoint": trim(cfg.S3Endpoint),
		"mountpoint": trim(cfg.Mountpoint),
		"rcloneImage": trim(cfg.MounterImage),
		"helperImage": trim(cfg.HelperImage),
		"allowOther": cfg.AllowOther,
		"readOnly": cfg.ReadOnly,
		"issuerMode": issuerMode,
		"httpEnabled": strings.EqualFold(trim(os.Getenv("VOLKIT_HTTP_ENABLE")), "true"),
	}
	return ValidationResult{OK: ok, Errors: errs, Warnings: warns, Summary: summary}
}

// httpClient builds a private HTTP client for coordinator calls, honoring optional TLS files
func (c *Controller) httpClient() *http.Client {
	// Optional file-based mTLS client config (supports Docker secrets)
	clientCert := strings.TrimSpace(os.Getenv("VOLKIT_TLS_CLIENT_CERT_FILE"))
	clientKey := strings.TrimSpace(os.Getenv("VOLKIT_TLS_CLIENT_KEY_FILE"))
	serverCA := strings.TrimSpace(os.Getenv("VOLKIT_TLS_SERVER_CA_FILE"))
	if clientCert == "" && clientKey == "" && serverCA == "" {
		return &http.Client{ Timeout: 30 * time.Second }
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if serverCA != "" {
		if pem, err := os.ReadFile(serverCA); err == nil {
			pool := x509.NewCertPool()
			if pool.AppendCertsFromPEM(pem) { cfg.RootCAs = pool }
		}
	}
	if clientCert != "" && clientKey != "" {
		if pair, err := tls.LoadX509KeyPair(clientCert, clientKey); err == nil {
			cfg.Certificates = []tls.Certificate{pair}
		}
	}
	return &http.Client{ Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg} }
}

func (c *Controller) HandleNodeReadyUpdate(node string, readyPrefixes []string) {
	node = strings.TrimSpace(node)
	if node == "" { return }
	set := make(map[string]struct{}, len(readyPrefixes))
	for _, p := range readyPrefixes {
		p2 := strings.Trim(strings.TrimSpace(p), "/")
		if p2 != "" { set[p2] = struct{}{} }
	}
	c.readyMu.Lock()
	c.nodeReady[node] = set
	c.readyMu.Unlock()
	// mark change to let any long-poll/hints wake up
	c.BumpChange()
	// evaluate per-level readiness and trigger migrations if needed
	c.updateLevelReadyLabels()
}

// svcHash returns a short stable hash for a service based on its ID and required prefixes
func svcHash(id string, prefixes []string) string {
	base := strings.TrimSpace(id)
	list := append([]string(nil), prefixes...)
	sort.Strings(list)
	h := sha256.Sum256([]byte(base + "|" + strings.Join(list, ",")))
	return fmt.Sprintf("%x", h[:])[:8]
}

// updateLevelReadyLabels evaluates per-level readiness and toggles node.labels.volkit.ready.<level>.
// When a node loses readiness for a level, it triggers a rolling update (ForceUpdate++) for all services of that level (cooldown guarded).
func (c *Controller) updateLevelReadyLabels() {
	// list services
	svcs, err := c.serviceList()
	if err != nil { slog.Warn("ready labels: list services", "error", err); return }
	// build level -> required prefixes and level -> services
	type svcRef struct{ id, name, level string }
	levelReq := map[string]map[string]struct{}{}
	levelSvcs := map[string][]svcRef{}
	for _, s := range svcs {
		claims := c.parseIndexedClaims(s.Spec.Labels)
		// derive level per claim; if empty, try labels[volkit.level] or selector trust==X; else default "default"
		for _, cl := range claims {
			if !cl.enabled || strings.TrimSpace(cl.prefix) == "" { continue }
			lvl := strings.TrimSpace(cl.level)
			if lvl == "" {
				// fallback: volkit.level
				lvl = strings.TrimSpace(strings.ToLower(s.Spec.Labels["volkit.level"]))
			}
			if lvl == "" { lvl = "default" }
			lvl = strings.ToLower(lvl)
			if levelReq[lvl] == nil { levelReq[lvl] = map[string]struct{}{} }
			levelReq[lvl][strings.Trim(cl.prefix, "/")] = struct{}{}
			// register service once per level
			if _, ok := func() (bool) { for _, r := range levelSvcs[lvl] { if r.id==s.ID { return true } } ; return false }(); !ok {
				levelSvcs[lvl] = append(levelSvcs[lvl], svcRef{id: s.ID, name: s.Spec.Name, level: lvl})
			}
		}
	}
	// snapshot node ready sets
	c.readyMu.Lock()
	nr := make(map[string]map[string]struct{}, len(c.nodeReady))
	for n, set := range c.nodeReady { tmp := make(map[string]struct{}, len(set)); for k := range set { tmp[k] = struct{}{} }; nr[n] = tmp }
	c.readyMu.Unlock()
	// config: cooldown and regain toggle
	cool := int64(30)
	if v := strings.TrimSpace(os.Getenv("VOLKIT_READY_MIGRATE_COOLDOWN_SEC")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 3600 { cool = int64(n) }
	}
	regainForce := strings.EqualFold(strings.TrimSpace(os.Getenv("VOLKIT_READY_MIGRATE_ON_REGAIN")), "true")
	// evaluate per node, per level
	nodes := c.nodeLabelsByName()
	for node := range nodes {
		set := nr[node]
		for lvl, req := range levelReq {
			ready := true
			if len(set) == 0 { ready = false } else {
				for p := range req { if _, ok := set[p]; !ok { ready = false; break } }
			}
			labelKey := "volkit.ready." + lvl
			cur := false
			if labels := nodes[node]; labels != nil { if v, ok := labels[labelKey]; ok && strings.EqualFold(v, "true") { cur = true } }
			if ready == cur { continue }
			// toggle label on node
			cli := c.cli; if c.managerCli != nil { cli = c.managerCli }
			// find node ID by name
			var targetID string
			// ensure cachedNodes has content; if not, refresh via nodeLabelsByName already did a list
			for _, n := range c.cachedNodes { name := strings.TrimSpace(n.Description.Hostname); if name == "" { name = n.ID }; if name == node { targetID = n.ID; break } }
			if targetID == "" { continue }
			nctx, ncancel := c.timeoutCtx(5 * time.Second)
			inspect, err := cli.NodeInspectWithRaw(nctx, targetID)
			ncancel()
			if err != nil { continue }
			spec := inspect.Spec
			if spec.Annotations.Labels == nil { spec.Annotations.Labels = map[string]string{} }
			if ready { spec.Annotations.Labels[labelKey] = "true" } else { delete(spec.Annotations.Labels, labelKey) }
			uctx, ucancel := c.timeoutCtx(5 * time.Second)
			_ = cli.NodeUpdate(uctx, targetID, inspect.Meta.Version, spec)
			ucancel()
			// migrations: on lose readiness always; on regain readiness optionally (global or per-service label)
			for _, ref := range levelSvcs[lvl] {
				if ready {
					effRegain := regainForce
					sctx, scancel := c.timeoutCtx(6 * time.Second)
					s, _, err := cli.ServiceInspectWithRaw(sctx, ref.id, types.ServiceInspectOptions{})
					if err == nil {
						if v := strings.TrimSpace(strings.ToLower(s.Spec.Labels["volkit.forceUpdateOnRegain"])); v != "" {
							effRegain = (v == "true")
						}
					}
					if !effRegain { scancel(); continue }
					// cooldown
					now := time.Now().Unix()
					c.migMu.Lock(); last := c.lastSvcMigrate[ref.id]; if now-last >= cool { c.lastSvcMigrate[ref.id] = now } else { now = 0 }
					c.migMu.Unlock()
					if now > 0 && err == nil {
						sp := s.Spec; sp.TaskTemplate.ForceUpdate++
						_ = cli.ServiceUpdate(sctx, s.ID, s.Meta.Version, sp, types.ServiceUpdateOptions{})
					}
					scancel()
					continue
				}
				// not ready: always migrate (subject to cooldown)
				now := time.Now().Unix()
				c.migMu.Lock(); last := c.lastSvcMigrate[ref.id]; if now-last >= cool { c.lastSvcMigrate[ref.id] = now } else { now = 0 }
				c.migMu.Unlock()
				if now > 0 {
					sctx, scancel := c.timeoutCtx(6 * time.Second)
					if s, _, err := cli.ServiceInspectWithRaw(sctx, ref.id, types.ServiceInspectOptions{}); err == nil {
						sp := s.Spec; sp.TaskTemplate.ForceUpdate++
						_ = cli.ServiceUpdate(sctx, s.ID, s.Meta.Version, sp, types.ServiceUpdateOptions{})
					}
					scancel()
				}
			}
		}
	}
}

// maybeUnblockServices removes the blocking placement when at least one ready node exists for the service level
func (c *Controller) maybeUnblockServices() {
    svcs, err := c.serviceList()
    if err != nil { return }
    nodes := c.nodeLabelsByName()
    cli := c.cli; if c.managerCli != nil { cli = c.managerCli }
    for _, s := range svcs {
        plc := s.Spec.TaskTemplate.Placement
        if plc == nil || len(plc.Constraints) == 0 { continue }
        hasBlock := false
        for _, cons := range plc.Constraints { if strings.TrimSpace(cons) == "node.labels.volkit.block==true" { hasBlock = true; break } }
        if !hasBlock { continue }
        // derive level
        lvl := strings.ToLower(strings.TrimSpace(s.Spec.Labels["volkit.level"]))
        if lvl == "" {
            claims := c.parseIndexedClaims(s.Spec.Labels)
            for _, cl := range claims { if strings.TrimSpace(cl.level) != "" { lvl = strings.ToLower(strings.TrimSpace(cl.level)); break } }
            if lvl == "" { lvl = "default" }
        }
        // check any ready node
        labelKey := "volkit.ready." + lvl
        anyReady := false
        for _, labs := range nodes { if v, ok := labs[labelKey]; ok && strings.EqualFold(v, "true") { anyReady = true; break } }
        if !anyReady { continue }
        // remove block constraint and update service
        var out []string
        for _, cons := range plc.Constraints { if strings.TrimSpace(cons) != "node.labels.volkit.block==true" { out = append(out, cons) } }
        if len(out) == len(plc.Constraints) { continue }
        sctx, scancel := c.timeoutCtx(8 * time.Second)
        current, _, ierr := cli.ServiceInspectWithRaw(sctx, s.ID, types.ServiceInspectOptions{})
        if ierr == nil {
            spec := current.Spec
            if spec.TaskTemplate.Placement == nil { spec.TaskTemplate.Placement = &swarm.Placement{} }
            spec.TaskTemplate.Placement.Constraints = out
            _ = cli.ServiceUpdate(sctx, current.ID, current.Meta.Version, spec, types.ServiceUpdateOptions{})
            c.metricsMu.Lock(); c.servicesUnblockedTotal++; c.metricsMu.Unlock()
        }
        scancel()
    }
}

// writeCredsEnv writes a creds env file on the host tmpfs and returns the mounted path
func (c *Controller) writeCredsEnv(level string, rcloneEnv []string) (string, error) {
	dir := strings.TrimSpace(c.cfg.CredDir)
	if dir == "" { dir = "/run/volkit/creds" }
	// materialize via helper container to ensure host FS permissions (0600)
	if err := c.ensureHelperImage(); err != nil { return "", err }
	path := filepath.Join(dir, level+".env")
	// compose content from current env resolution
	var b strings.Builder
	for _, kv := range rcloneEnv { if i := strings.IndexByte(kv, '='); i > 0 { k := kv[:i]; v := kv[i+1:]; fmt.Fprintf(&b, "export %s='%s'\n", k, strings.ReplaceAll(v, "'", "'\\''")) } }
	content := b.String()
	sh := fmt.Sprintf("mkdir -p %s && umask 177 && cat > %s <<'EOF'\n%s\nEOF\n", dir, path, content)
	cont, err := c.cli.ContainerCreate(c.ctx,
		&container.Config{Image: c.helperImageRef(), Cmd: []string{"sh", "-lc", sh}},
		&container.HostConfig{Binds: []string{fmt.Sprintf("%s:%s", dir, dir)}},
		&network.NetworkingConfig{}, nil, c.helperName("creds-write"))
	if err != nil { return "", err }
	defer func() { _ = c.cli.ContainerRemove(c.ctx, cont.ID, container.RemoveOptions{Force: true}) }()
	if err := c.cli.ContainerStart(c.ctx, cont.ID, container.StartOptions{}); err != nil { return "", err }
	_, _ = c.cli.ContainerWait(c.ctx, cont.ID, container.WaitConditionNotRunning)
	return path, nil
}

func (c *Controller) mounterCommandWithCredFile(credFile string) []string {
	cmd := []string{"sh", "-lc", fmt.Sprintf(". %s; exec rclone mount %s %s --config /dev/null --vfs-cache-mode=writes --dir-cache-time=12h %s", credFile, c.cfg.RcloneRemote, c.cfg.Mountpoint, strings.Join(c.buildPresetArgs(), " "))}
	return cmd
}

// fetchLevelCreds returns rclone env entries for a given level; in this phase it uses existing buildRcloneEnv
// Future: replace with backend call to MinIO/AWS to mint long/short term credentials
func (c *Controller) fetchLevelCreds(level string) (env []string, etag string, err error) {
	level = strings.ToLower(strings.TrimSpace(level))
	issuer := strings.TrimSpace(c.cfg.CredsURL)
	// short backoff per level on recent errors
	c.credsMu.Lock()
	if until, ok := c.credBackoffTill[level]; ok && time.Now().Before(until) {
		if cached, ok2 := c.credsByLevel[level]; ok2 { env, etag, err = cached.env, cached.etag, nil; c.credsMu.Unlock(); c.metricsMu.Lock(); c.credsBackoffTotal++; c.metricsMu.Unlock(); return }
		c.credsMu.Unlock(); c.metricsMu.Lock(); c.credsBackoffTotal++; c.metricsMu.Unlock(); return c.buildRcloneEnv(), "static", nil
	}
	c.credsMu.Unlock()
	if issuer == "" && strings.EqualFold(c.cfg.Mode, "agent") && strings.TrimSpace(c.cfg.CoordinatorURL) != "" {
		issuer = strings.TrimRight(strings.TrimSpace(c.cfg.CoordinatorURL), "/")
	}
	if issuer != "" {
		u := strings.TrimRight(issuer, "/") + "/creds?level=" + url.QueryEscape(level)
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		resp, e := c.httpClient().Do(req)
		if e == nil && resp != nil && resp.StatusCode/100 == 2 {
			var body struct{ RcloneEnv []string `json:"rcloneEnv"`; ETag string `json:"etag"` }
			_ = json.NewDecoder(resp.Body).Decode(&body)
			if resp.Body != nil { _ = resp.Body.Close() }
			if len(body.RcloneEnv) > 0 {
				etag = strings.TrimSpace(body.ETag)
				env = body.RcloneEnv
				c.metricsMu.Lock(); c.credsFetchOK++; c.metricsMu.Unlock()
				c.credsMu.Lock(); c.credsByLevel[level] = struct{ env []string; etag string }{env: env, etag: etag}; c.credsMu.Unlock()
				return env, etag, nil
			}
		}
		if resp != nil && resp.Body != nil { _ = resp.Body.Close() }
		// backoff on error/miss
		c.metricsMu.Lock(); c.credsFetchErrors++; c.metricsMu.Unlock()
		backoffSec := 30
		if v := strings.TrimSpace(os.Getenv("VOLKIT_CREDS_BACKOFF_SEC")); v != "" { if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 600 { backoffSec = n } }
		c.credsMu.Lock(); c.credBackoffTill[level] = time.Now().Add(time.Duration(backoffSec) * time.Second); c.credsMu.Unlock()
	}
	// fallback: reuse local static config
	return c.buildRcloneEnv(), "static", nil
}

// AgentClaimsForLocalNode fetches claims for this node in agent mode via coordinator
func (c *Controller) AgentClaimsForLocalNode() []ClaimOut {
	if !strings.EqualFold(c.cfg.Mode, "agent") || strings.TrimSpace(c.cfg.CoordinatorURL) == "" {
		return nil
	}
	me, _ := os.Hostname()
	claims, _, err := c.fetchClaims(strings.TrimSpace(me))
	if err != nil { return nil }
	return claims
}

// ReadyPrefixesLocal returns prefixes that are locally ready (mount ok and control object present)
func (c *Controller) ReadyPrefixesLocal(claims []ClaimOut) []string {
	rel := strings.TrimSpace(c.cfg.ControlObjectRelPath)
	if rel == "" { rel = ".volkit/.ready" }
	var out []string
	baseDefault := c.cfg.Mountpoint
	if strings.TrimSpace(baseDefault) == "" { baseDefault = "/mnt/s3" }
	for _, cl := range claims {
		pfx := strings.Trim(strings.TrimSpace(cl.Prefix), "/")
		if pfx == "" { continue }
		base := baseDefault
		if strings.TrimSpace(cl.Mountpoint) != "" { base = cl.Mountpoint }
		if testRW(base) != nil { continue }
		full := filepath.Join(base, filepath.Clean("/"+pfx))
		ctrlPath := filepath.Join(full, filepath.Clean("/"+rel))
		if fi, err := os.Stat(ctrlPath); err == nil && !fi.IsDir() {
			out = append(out, pfx)
		}
	}
	return out
}

// computeMounterSpecHash returns a stable short hash representing the desired mounter spec
// Inputs include: credentials env (order-preserved), rclone command args, endpoint, mountpoint,
// selected image id (if known), and read-only/allow-other toggles.
func (c *Controller) computeMounterSpecHash(env []string, cmd []string) string {
	// Normalize key parts
	endpoint := strings.TrimSpace(c.resolveEndpointForMounter())
	allow := "root"
	if c.cfg.AllowOther { allow = "other" }
	ro := "rw"
	if c.cfg.ReadOnly { ro = "ro" }
	imageID := strings.TrimSpace(c.cachedImageID())
	// Compose
	var b strings.Builder
	b.WriteString("EP=")
	b.WriteString(endpoint)
	b.WriteString("\nMP=")
	b.WriteString(strings.TrimSpace(c.cfg.Mountpoint))
	b.WriteString("\nALLOW=")
	b.WriteString(allow)
	b.WriteString("\nMODE=")
	b.WriteString(ro)
	if imageID != "" { b.WriteString("\nIMG="); b.WriteString(imageID) }
	b.WriteString("\nENV=\n")
	for _, e := range env { b.WriteString(e); b.WriteByte('\n') }
	b.WriteString("CMD=\n")
	for _, a := range cmd { b.WriteString(a); b.WriteByte('\n') }
	sum := sha256.Sum256([]byte(b.String()))
	return fmt.Sprintf("%x", sum[:])[:16]
}