package httpserver

import (
    "crypto/tls"
    "crypto/x509"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "os"
    "strings"
    "testing"
    "time"

    "github.com/swarmnative/volkit/internal/controller"
)

type fakeCtrl struct{}
func (f *fakeCtrl) Ready() error { return nil }
func (f *fakeCtrl) Snapshot() controller.MetricsSnapshot { return controller.MetricsSnapshot{} }
func (f *fakeCtrl) Preflight() error { return nil }
func (f *fakeCtrl) Nudge() {}
func (f *fakeCtrl) ClaimsForNode(node string) []controller.ClaimOut { return []controller.ClaimOut{{Bucket:"b", Prefix:"p"}} }
func (f *fakeCtrl) ChangeVersion() int64 { return time.Now().Unix() }
func (f *fakeCtrl) ReadyCounts() map[string]int { return map[string]int{"default":1} }
func (f *fakeCtrl) MintServiceAccount(level string) (string,string,int64,error) { return "","",0, nil }
func (f *fakeCtrl) BuildRcloneEnvPublic() []string { return []string{"RCLONE_CONFIG_S3_TYPE=s3","RCLONE_CONFIG_S3_ACCESS_KEY_ID=old","RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=old"} }

func TestValidateHandler(t *testing.T) {
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{Mountpoint:"/mnt/s3"}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/validate", nil)
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("status=%d", rr.Code) }
    var m map[string]any
    if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil { t.Fatalf("json: %v", err) }
    if _, ok := m["ok"]; !ok { t.Fatal("missing ok") }
    if _, ok := m["preflightOK"]; !ok { t.Fatal("missing preflightOK") }
}

func TestClaimsETag(t *testing.T) {
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/claims?node=n1", nil)
    // simulate TLS by setting a non-nil TLS field to pass authOK when AUTH_DISABLE=false
    req.TLS = &tls.ConnectionState{}
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("status=%d", rr.Code) }
    etag := strings.TrimSpace(rr.Header().Get("ETag"))
    if etag == "" { t.Fatal("missing ETag") }
    rr2 := httptest.NewRecorder()
    req2 := httptest.NewRequest(http.MethodGet, "/claims?node=n1", nil)
    req2.Header.Set("If-None-Match", etag)
    req2.TLS = &tls.ConnectionState{}
    mux.ServeHTTP(rr2, req2)
    if rr2.Code != http.StatusNotModified { t.Fatalf("etag not honored: %d", rr2.Code) }
}

func TestPKIEndpoints(t *testing.T) {
    os.Setenv("VOLKIT_PKI_ENABLE", "true")
    t.Cleanup(func(){ os.Unsetenv("VOLKIT_PKI_ENABLE") })
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    // /pki/ca
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/pki/ca", nil)
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("/pki/ca status=%d", rr.Code) }
    if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "application/x-pem-file") { t.Fatalf("content-type: %s", ct) }
    // /pki/enroll unauthorized without TLS
    rr2 := httptest.NewRecorder()
    req2 := httptest.NewRequest(http.MethodPost, "/pki/enroll", strings.NewReader("bad"))
    mux.ServeHTTP(rr2, req2)
    if rr2.Code != http.StatusForbidden && rr2.Code != http.StatusMethodNotAllowed { t.Fatalf("/pki/enroll expected forbidden/method, got %d", rr2.Code) }
}

func TestCredsStaticFallback(t *testing.T) {
    // external issuer not set; expect static env shape
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/creds?level=default", nil)
    req.TLS = &tls.ConnectionState{}
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("/creds status=%d", rr.Code) }
    var m map[string]any
    if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil { t.Fatalf("json: %v", err) }
    if _, ok := m["rcloneEnv"]; !ok { t.Fatal("missing rcloneEnv") }
}

func TestCredsS3AdminForceStatic(t *testing.T) {
    os.Setenv("VOLKIT_ISSUER_MODE", "s3_admin")
    os.Setenv("VOLKIT_ALLOW_ROOT_ISSUER", "true")
    os.Setenv("VOLKIT_S3_ADMIN_FORCE_STATIC", "true")
    os.Setenv("VOLKIT_ISSUER_ACCESS_KEY", "AK")
    os.Setenv("VOLKIT_ISSUER_SECRET_KEY", "SK")
    t.Cleanup(func(){
        os.Unsetenv("VOLKIT_ISSUER_MODE")
        os.Unsetenv("VOLKIT_ALLOW_ROOT_ISSUER")
        os.Unsetenv("VOLKIT_S3_ADMIN_FORCE_STATIC")
        os.Unsetenv("VOLKIT_ISSUER_ACCESS_KEY")
        os.Unsetenv("VOLKIT_ISSUER_SECRET_KEY")
    })
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/creds?level=default", nil)
    req.TLS = &tls.ConnectionState{}
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("/creds status=%d", rr.Code) }
    var resp struct{ Issuer string `json:"issuer"`; RcloneEnv []string `json:"rcloneEnv"` }
    if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil { t.Fatalf("json: %v", err) }
    if resp.Issuer != "s3_admin_static" { t.Fatalf("issuer=%s", resp.Issuer) }
    joined := strings.Join(resp.RcloneEnv, ",")
    if !strings.Contains(joined, "RCLONE_CONFIG_S3_ACCESS_KEY_ID=AK") || !strings.Contains(joined, "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=SK") {
        t.Fatalf("env not overridden: %v", resp.RcloneEnv)
    }
}

func TestHintsTimeoutNoChange(t *testing.T) {
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/hints?since=999999999999&timeoutSec=0", nil)
    req.TLS = &tls.ConnectionState{}
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusNoContent && rr.Code != http.StatusOK { t.Fatalf("/hints code=%d", rr.Code) }
}

func TestMetricsBasic(t *testing.T) {
    os.Setenv("VOLKIT_ENABLE_METRICS", "true")
    t.Cleanup(func(){ os.Unsetenv("VOLKIT_ENABLE_METRICS") })
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("/metrics status=%d", rr.Code) }
    if !strings.Contains(rr.Body.String(), "volkit_build_info") {
        t.Fatalf("metrics content unexpected: %s", rr.Body.String())
    }
}

func TestCredsSTS_Success(t *testing.T) {
    // Simulate STS endpoint with a valid AssumeRoleResponse
    stsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/xml")
        _, _ = w.Write([]byte(
            `<AssumeRoleResponse><AssumeRoleResult><Credentials>`+
            `<AccessKeyId>AKSTS</AccessKeyId>`+
            `<SecretAccessKey>SKSTS</SecretAccessKey>`+
            `<SessionToken>TOKEN</SessionToken>`+
            `<Expiration>2099-01-01T00:00:00Z</Expiration>`+
            `</Credentials></AssumeRoleResult></AssumeRoleResponse>`))
    }))
    defer stsSrv.Close()
    os.Setenv("VOLKIT_ISSUER_MODE", "s3_admin")
    os.Setenv("VOLKIT_ALLOW_ROOT_ISSUER", "true")
    os.Setenv("VOLKIT_ISSUER_ENDPOINT", stsSrv.URL)
    os.Setenv("VOLKIT_ISSUER_ACCESS_KEY", "rootAK")
    os.Setenv("VOLKIT_ISSUER_SECRET_KEY", "rootSK")
    os.Setenv("AUTH_DISABLE", "true")
    t.Cleanup(func(){
        os.Unsetenv("VOLKIT_ISSUER_MODE")
        os.Unsetenv("VOLKIT_ALLOW_ROOT_ISSUER")
        os.Unsetenv("VOLKIT_ISSUER_ENDPOINT")
        os.Unsetenv("VOLKIT_ISSUER_ACCESS_KEY")
        os.Unsetenv("VOLKIT_ISSUER_SECRET_KEY")
        os.Unsetenv("AUTH_DISABLE")
    })
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/creds?level=default", nil)
    // allowed due to AUTH_DISABLE=true without TLS
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("/creds sts status=%d body=%s", rr.Code, rr.Body.String()) }
    var resp struct{ Issuer string `json:"issuer"`; RcloneEnv []string `json:"rcloneEnv"` }
    if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil { t.Fatalf("json: %v", err) }
    if resp.Issuer != "s3_admin_sts" { t.Fatalf("issuer=%s", resp.Issuer) }
    joined := strings.Join(resp.RcloneEnv, ",")
    if !strings.Contains(joined, "RCLONE_CONFIG_S3_ACCESS_KEY_ID=AKSTS") ||
       !strings.Contains(joined, "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=SKSTS") ||
       !strings.Contains(joined, "RCLONE_CONFIG_S3_SESSION_TOKEN=TOKEN") { t.Fatalf("sts env not applied: %v", resp.RcloneEnv) }
}

type fakeCtrlSA struct{ fakeCtrl }
func (f *fakeCtrlSA) MintServiceAccount(level string) (string,string,int64,error) { return "AKSA","SKSA", 1893456000, nil }

func TestCredsSTS_Fail_SA_Success(t *testing.T) {
    // STS returns error -> fallback SA success
    stsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "err", http.StatusInternalServerError) }))
    defer stsSrv.Close()
    os.Setenv("VOLKIT_ISSUER_MODE", "s3_admin")
    os.Setenv("VOLKIT_ALLOW_ROOT_ISSUER", "true")
    os.Setenv("VOLKIT_ISSUER_ENDPOINT", stsSrv.URL)
    os.Setenv("VOLKIT_ISSUER_ACCESS_KEY", "rootAK")
    os.Setenv("VOLKIT_ISSUER_SECRET_KEY", "rootSK")
    os.Setenv("AUTH_DISABLE", "true")
    t.Cleanup(func(){ os.Unsetenv("VOLKIT_ISSUER_MODE"); os.Unsetenv("VOLKIT_ALLOW_ROOT_ISSUER"); os.Unsetenv("VOLKIT_ISSUER_ENDPOINT"); os.Unsetenv("VOLKIT_ISSUER_ACCESS_KEY"); os.Unsetenv("VOLKIT_ISSUER_SECRET_KEY"); os.Unsetenv("AUTH_DISABLE") })
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrlSA{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/creds?level=bronze", nil)
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("/creds sa status=%d body=%s", rr.Code, rr.Body.String()) }
    var resp struct{ Issuer string `json:"issuer"`; RcloneEnv []string `json:"rcloneEnv"` }
    if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil { t.Fatalf("json: %v", err) }
    if resp.Issuer != "s3_admin_sa" { t.Fatalf("issuer=%s", resp.Issuer) }
    joined := strings.Join(resp.RcloneEnv, ",")
    if !strings.Contains(joined, "RCLONE_CONFIG_S3_ACCESS_KEY_ID=AKSA") || !strings.Contains(joined, "RCLONE_CONFIG_S3_SECRET_ACCESS_KEY=SKSA") { t.Fatalf("sa env not applied: %v", resp.RcloneEnv) }
}

func TestCredsSTS_Fail_SA_Fail_StaticFallback(t *testing.T) {
    stsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "err", http.StatusInternalServerError) }))
    defer stsSrv.Close()
    os.Setenv("VOLKIT_ISSUER_MODE", "s3_admin")
    os.Setenv("VOLKIT_ALLOW_ROOT_ISSUER", "true")
    os.Setenv("VOLKIT_ISSUER_ENDPOINT", stsSrv.URL)
    os.Setenv("VOLKIT_ISSUER_ACCESS_KEY", "rootAK")
    os.Setenv("VOLKIT_ISSUER_SECRET_KEY", "rootSK")
    os.Setenv("AUTH_DISABLE", "true")
    t.Cleanup(func(){ os.Unsetenv("VOLKIT_ISSUER_MODE"); os.Unsetenv("VOLKIT_ALLOW_ROOT_ISSUER"); os.Unsetenv("VOLKIT_ISSUER_ENDPOINT"); os.Unsetenv("VOLKIT_ISSUER_ACCESS_KEY"); os.Unsetenv("VOLKIT_ISSUER_SECRET_KEY"); os.Unsetenv("AUTH_DISABLE") })
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/creds?level=silver", nil)
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusOK { t.Fatalf("/creds static status=%d body=%s", rr.Code, rr.Body.String()) }
    var resp struct{ Issuer string `json:"issuer"`; RcloneEnv []string `json:"rcloneEnv"` }
    if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil { t.Fatalf("json: %v", err) }
    if resp.Issuer != "s3_admin_static" { t.Fatalf("issuer=%s", resp.Issuer) }
}

func TestMaintenanceAuthMatrix(t *testing.T) {
    // AUTH_DISABLE=true but no TLS -> forbidden
    os.Setenv("AUTH_DISABLE", "true")
    t.Cleanup(func(){ os.Unsetenv("AUTH_DISABLE") })
    mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/maintenance/recompute", nil)
    mux.ServeHTTP(rr, req)
    if rr.Code != http.StatusForbidden { t.Fatalf("expected 403, got %d", rr.Code) }
    // With TLS -> accepted
    rr2 := httptest.NewRecorder()
    req2 := httptest.NewRequest(http.MethodGet, "/maintenance/recompute", nil)
    req2.TLS = &tls.ConnectionState{}
    mux.ServeHTTP(rr2, req2)
    if rr2.Code != http.StatusAccepted { t.Fatalf("expected 202, got %d", rr2.Code) }
    // AUTH_DISABLE=false and no TLS -> unauthorized
    os.Unsetenv("AUTH_DISABLE")
    rr3 := httptest.NewRecorder()
    req3 := httptest.NewRequest(http.MethodGet, "/maintenance/recompute", nil)
    mux.ServeHTTP(rr3, req3)
    if rr3.Code != http.StatusUnauthorized { t.Fatalf("expected 401, got %d", rr3.Code) }
}

func TestAgentStatus_MethodAndAuth(t *testing.T) {
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// GET not allowed
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/agent/status", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed { t.Fatalf("expected 405, got %d", rr.Code) }
	// POST without TLS and AUTH_DISABLE=false -> 401
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/agent/status", strings.NewReader(`{"nodeId":"n1","ts":1}`))
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusUnauthorized { t.Fatalf("expected 401, got %d", rr2.Code) }
	// AUTH_DISABLE=true but no TLS -> 403
	os.Setenv("AUTH_DISABLE", "true")
	t.Cleanup(func(){ os.Unsetenv("AUTH_DISABLE") })
	rr3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodPost, "/agent/status", strings.NewReader(`{"nodeId":"n1","ts":1}`))
	mux.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusForbidden { t.Fatalf("expected 403, got %d", rr3.Code) }
	// AUTH_DISABLE=true with TLS -> 202
	rr4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodPost, "/agent/status", strings.NewReader(`{"nodeId":"n1","ts":1}`))
	req4.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr4, req4)
	if rr4.Code != http.StatusAccepted { t.Fatalf("expected 202, got %d", rr4.Code) }
}

func TestAgentStatus_RateLimit(t *testing.T) {
	os.Setenv("VOLKIT_RL_SENSITIVE_CAP", "1")
	os.Setenv("VOLKIT_RL_SENSITIVE_RPS", "1")
	os.Setenv("AUTH_DISABLE", "true")
	t.Cleanup(func(){ os.Unsetenv("VOLKIT_RL_SENSITIVE_CAP"); os.Unsetenv("VOLKIT_RL_SENSITIVE_RPS"); os.Unsetenv("AUTH_DISABLE") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// first ok
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/agent/status", strings.NewReader(`{"nodeId":"n1","ts":1}`))
	req1.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusAccepted { t.Fatalf("expected 202, got %d", rr1.Code) }
	// immediate second should be 429
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/agent/status", strings.NewReader(`{"nodeId":"n1","ts":2}`))
	req2.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests { t.Fatalf("expected 429, got %d", rr2.Code) }
}

func TestMaintenance_Recompute_RateLimit(t *testing.T) {
	os.Setenv("VOLKIT_RL_SENSITIVE_CAP", "1")
	os.Setenv("VOLKIT_RL_SENSITIVE_RPS", "1")
	os.Setenv("AUTH_DISABLE", "true")
	t.Cleanup(func(){ os.Unsetenv("VOLKIT_RL_SENSITIVE_CAP"); os.Unsetenv("VOLKIT_RL_SENSITIVE_RPS"); os.Unsetenv("AUTH_DISABLE") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// first ok (with TLS required for sensitive endpoint when AUTH_DISABLE=true)
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/maintenance/recompute", nil)
	req1.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusAccepted { t.Fatalf("expected 202, got %d", rr1.Code) }
	// second should be 429
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/maintenance/recompute", nil)
	req2.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests { t.Fatalf("expected 429, got %d", rr2.Code) }
}

func TestHintsImmediateChange(t *testing.T) {
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/hints?since=0&timeoutSec=30", nil)
	req.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK { t.Fatalf("/hints expected 200, got %d", rr.Code) }
}

func TestMetricsReady_AuthMatrix(t *testing.T) {
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// no TLS, AUTH_DISABLE default -> 401
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics/ready", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized { t.Fatalf("/metrics/ready expected 401, got %d", rr.Code) }
	// TLS present -> 200
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/metrics/ready", nil)
	req2.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK { t.Fatalf("/metrics/ready expected 200, got %d", rr2.Code) }
}

func TestPKIEnroll_RateLimit(t *testing.T) {
	os.Setenv("VOLKIT_PKI_ENABLE", "true")
	os.Setenv("VOLKIT_ENROLL_TOKEN", "tok")
	os.Setenv("VOLKIT_RL_SENSITIVE_CAP", "1")
	os.Setenv("VOLKIT_RL_SENSITIVE_RPS", "1")
	t.Cleanup(func(){ os.Unsetenv("VOLKIT_PKI_ENABLE"); os.Unsetenv("VOLKIT_ENROLL_TOKEN"); os.Unsetenv("VOLKIT_RL_SENSITIVE_CAP"); os.Unsetenv("VOLKIT_RL_SENSITIVE_RPS") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// first POST with TLS and token -> processes and returns 400 (bad csr) but passes RL
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/pki/enroll", strings.NewReader("bad"))
	req1.TLS = &tls.ConnectionState{}
	req1.Header.Set("X-Volkit-Token", "tok")
	mux.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusBadRequest && rr1.Code != http.StatusCreated { t.Fatalf("/pki/enroll first expected 400/201, got %d", rr1.Code) }
	// immediate second -> rate limited 429
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/pki/enroll", strings.NewReader("bad"))
	req2.TLS = &tls.ConnectionState{}
	req2.Header.Set("X-Volkit-Token", "tok")
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests { t.Fatalf("/pki/enroll expected 429, got %d", rr2.Code) }
}

func TestClaims_TLSRequiredWhenAuthDisabled(t *testing.T) {
	os.Setenv("AUTH_DISABLE", "true")
	t.Cleanup(func(){ os.Unsetenv("AUTH_DISABLE") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/claims", nil)
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden { t.Fatalf("expected 403 when AUTH_DISABLE=true without TLS, got %d", rr.Code) }
}

func TestClaims_NodeBindingMismatch(t *testing.T) {
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/claims?node=n1", nil)
	// Simulate TLS present but peer node different; our server checks peer via TLS cert
	// We cannot craft full cert in unit test; use tls.ConnectionState with empty PeerCertificates to force pn==""
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{}}
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden { t.Fatalf("expected 403 on node binding mismatch, got %d", rr.Code) }
}

func TestCreds_ExternalIssuer_ETag304(t *testing.T) {
	// external issuer server that always returns same body with ETag
	etag := "\"abc\""
	issuerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ifNone := strings.TrimSpace(r.Header.Get("If-None-Match"))
		if ifNone == etag { w.WriteHeader(http.StatusNotModified); return }
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"rcloneEnv":["RCLONE_CONFIG_S3_TYPE=s3"]}`))
	}))
	defer issuerSrv.Close()
	os.Setenv("VOLKIT_ISSUER_MODE", "external")
	os.Setenv("VOLKIT_ISSUER_ENDPOINT", issuerSrv.URL)
	t.Cleanup(func(){ os.Unsetenv("VOLKIT_ISSUER_MODE"); os.Unsetenv("VOLKIT_ISSUER_ENDPOINT") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// First request should get 200 and ETag
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodGet, "/creds?level=default", nil)
	req1.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK { t.Fatalf("first creds expected 200, got %d", rr1.Code) }
	et := strings.TrimSpace(rr1.Header().Get("ETag"))
	if et == "" { t.Fatalf("missing ETag from external issuer") }
	// Second with If-None-Match should return 304
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/creds?level=default", nil)
	req2.Header.Set("If-None-Match", et)
	req2.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusNotModified { t.Fatalf("expected 304, got %d", rr2.Code) }
}

func TestMetrics_ContainsHTTPClassCounters(t *testing.T) {
	os.Setenv("VOLKIT_ENABLE_METRICS", "true")
	t.Cleanup(func(){ os.Unsetenv("VOLKIT_ENABLE_METRICS") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// touch a couple of endpoints to increment counters
	rr0 := httptest.NewRecorder(); req0 := httptest.NewRequest(http.MethodGet, "/ready", nil); mux.ServeHTTP(rr0, req0)
	rr1 := httptest.NewRecorder(); req1 := httptest.NewRequest(http.MethodGet, "/claims", nil); mux.ServeHTTP(rr1, req1) // expect 401 or 403
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mux.ServeHTTP(rr, req)
	body := rr.Body.String()
	if !strings.Contains(body, "volkit_http_requests_total") { t.Fatalf("missing volkit_http_requests_total") }
	if !strings.Contains(body, "volkit_http_responses_class{class=\"2xx\"}") { t.Fatalf("missing 2xx counter") }
	if !strings.Contains(body, "volkit_http_responses_class{class=\"4xx\"}") && !strings.Contains(body, "volkit_http_responses_class{class=\"3xx\"}") { t.Fatalf("missing class counters") }
}

func TestReadyHasRequestID(t *testing.T) {
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	h := WithRequestID(mux)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK { t.Fatalf("/ready expected 200, got %d", rr.Code) }
	if strings.TrimSpace(rr.Header().Get("X-Request-Id")) == "" { t.Fatalf("missing X-Request-Id header") }
}

func TestWithRecovery_PanicReturns500(t *testing.T) {
	boom := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("boom") })
	wrapped := WithRecovery(boom)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	wrapped.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError { t.Fatalf("expected 500, got %d", rr.Code) }
	if !strings.Contains(rr.Body.String(), "internal server error") { t.Fatalf("unexpected body: %s", rr.Body.String()) }
}

func TestAgentStatus_RateLimit_RetryAfterHeader(t *testing.T) {
	os.Setenv("VOLKIT_RL_SENSITIVE_CAP", "1")
	os.Setenv("VOLKIT_RL_SENSITIVE_RPS", "1")
	os.Setenv("AUTH_DISABLE", "true")
	t.Cleanup(func(){ os.Unsetenv("VOLKIT_RL_SENSITIVE_CAP"); os.Unsetenv("VOLKIT_RL_SENSITIVE_RPS"); os.Unsetenv("AUTH_DISABLE") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// first accepted
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/agent/status", strings.NewReader(`{"nodeId":"n1","ts":1}`))
	req1.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusAccepted { t.Fatalf("expected 202, got %d", rr1.Code) }
	// second limited -> 429 with Retry-After
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/agent/status", strings.NewReader(`{"nodeId":"n1","ts":2}`))
	req2.TLS = &tls.ConnectionState{}
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests { t.Fatalf("expected 429, got %d", rr2.Code) }
	if v := strings.TrimSpace(rr2.Header().Get("Retry-After")); v == "" { t.Fatalf("missing Retry-After header") }
}

func TestPKIEnroll_RateLimit_RetryAfterHeader(t *testing.T) {
	os.Setenv("VOLKIT_PKI_ENABLE", "true")
	os.Setenv("VOLKIT_ENROLL_TOKEN", "tok")
	os.Setenv("VOLKIT_RL_SENSITIVE_CAP", "1")
	os.Setenv("VOLKIT_RL_SENSITIVE_RPS", "1")
	t.Cleanup(func(){ os.Unsetenv("VOLKIT_PKI_ENABLE"); os.Unsetenv("VOLKIT_ENROLL_TOKEN"); os.Unsetenv("VOLKIT_RL_SENSITIVE_CAP"); os.Unsetenv("VOLKIT_RL_SENSITIVE_RPS") })
	mux, _ := BuildMux(Options{Ctrl: &fakeCtrl{}, Cfg: controller.Config{}})
	// first request (bad csr) but passes RL
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/pki/enroll", strings.NewReader("bad"))
	req1.TLS = &tls.ConnectionState{}
	req1.Header.Set("X-Volkit-Token", "tok")
	mux.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusBadRequest && rr1.Code != http.StatusCreated { t.Fatalf("expected 400/201, got %d", rr1.Code) }
	// second limited -> 429 with Retry-After
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/pki/enroll", strings.NewReader("bad"))
	req2.TLS = &tls.ConnectionState{}
	req2.Header.Set("X-Volkit-Token", "tok")
	mux.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests { t.Fatalf("expected 429, got %d", rr2.Code) }
	if v := strings.TrimSpace(rr2.Header().Get("Retry-After")); v == "" { t.Fatalf("missing Retry-After header") }
}


