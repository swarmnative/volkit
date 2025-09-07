package issuer

import (
    "crypto/hmac"
    "crypto/sha256"
    "crypto/tls"
    "encoding/hex"
    "encoding/xml"
    "fmt"
    "net"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
)

type STSCredentials struct {
    AccessKeyId     string
    SecretAccessKey string
    SessionToken    string
    Expiration      time.Time
}

type assumeRoleResponse struct {
    XMLName xml.Name `xml:"AssumeRoleResponse"`
    Result  struct {
        Credentials struct {
            AccessKeyId     string    `xml:"AccessKeyId"`
            SecretAccessKey string    `xml:"SecretAccessKey"`
            SessionToken    string    `xml:"SessionToken"`
            Expiration      string    `xml:"Expiration"`
        } `xml:"Credentials"`
    } `xml:"AssumeRoleResult"`
}

// AssumeRoleSTS performs a minimal AssumeRole compatible with certain MinIO deployments.
func AssumeRoleSTS(endpoint, ak, sk string, durationSec int, region string, insecure bool, tlsCfg *tls.Config) (STSCredentials, error) {
    var out STSCredentials
    base := strings.TrimSpace(endpoint)
    if base == "" { return out, fmt.Errorf("empty sts endpoint") }
    u, err := url.Parse(base)
    if err != nil { return out, err }
    q := url.Values{}
    q.Set("Action", "AssumeRole")
    q.Set("Version", "2011-06-15")
    q.Set("DurationSeconds", strconv.Itoa(durationSec))
    q.Set("RoleSessionName", fmt.Sprintf("volkit-%d", time.Now().Unix()))
    ts := time.Now().UTC().Format(time.RFC3339)
    mac := hmac.New(sha256.New, []byte(sk))
    mac.Write([]byte(ts))
    sig := hex.EncodeToString(mac.Sum(nil))
    reqURL := *u
    reqURL.Path = "/"
    reqURL.RawQuery = q.Encode()
    req, _ := http.NewRequest(http.MethodGet, reqURL.String(), nil)
    req.Header.Set("X-Amz-Date", ts)
    req.Header.Set("X-Volkit-STS-AK", ak)
    req.Header.Set("X-Volkit-STS-Signature", sig)
    tr := &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConns: 64, MaxIdleConnsPerHost: 16, IdleConnTimeout: 60 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ExpectContinueTimeout: 1 * time.Second, DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
    if insecure { tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} }
    client := &http.Client{ Timeout: 8 * time.Second, Transport: tr }
    resp, err := client.Do(req)
    if err != nil { return out, err }
    defer resp.Body.Close()
    if resp.StatusCode/100 != 2 { return out, fmt.Errorf("sts http %d", resp.StatusCode) }
    var ar assumeRoleResponse
    if err := xml.NewDecoder(resp.Body).Decode(&ar); err != nil { return out, err }
    exp, _ := time.Parse(time.RFC3339, strings.TrimSpace(ar.Result.Credentials.Expiration))
    out = STSCredentials{
        AccessKeyId:     strings.TrimSpace(ar.Result.Credentials.AccessKeyId),
        SecretAccessKey: strings.TrimSpace(ar.Result.Credentials.SecretAccessKey),
        SessionToken:    strings.TrimSpace(ar.Result.Credentials.SessionToken),
        Expiration:      exp,
    }
    return out, nil
}

package issuer

import (
    "crypto/hmac"
    "crypto/sha256"
    "crypto/tls"
    "encoding/hex"
    "encoding/xml"
    "fmt"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
)

// Credentials represents temporary STS credentials.
type Credentials struct {
    AccessKeyId     string
    SecretAccessKey string
    SessionToken    string
    Expiration      time.Time
}

type assumeRoleResponse struct {
    XMLName xml.Name `xml:"AssumeRoleResponse"`
    Result  struct {
        Credentials struct {
            AccessKeyId     string    `xml:"AccessKeyId"`
            SecretAccessKey string    `xml:"SecretAccessKey"`
            SessionToken    string    `xml:"SessionToken"`
            Expiration      string    `xml:"Expiration"`
        } `xml:"Credentials"`
    } `xml:"AssumeRoleResult"`
}

// AssumeRoleSTS performs a minimal AssumeRole against a MinIO-compatible STS endpoint.
// Note: This is a simplified HMAC timestamp header flow for trusted overlays; prefer SigV4 for production-grade AWS.
func AssumeRoleSTS(endpoint, ak, sk string, durationSec int, region string, insecure bool, tlsCfg *tls.Config) (Credentials, error) {
    var out Credentials
    base := strings.TrimSpace(endpoint)
    if base == "" { return out, fmt.Errorf("empty sts endpoint") }
    u, err := url.Parse(base)
    if err != nil { return out, err }
    q := url.Values{}
    q.Set("Action", "AssumeRole")
    q.Set("Version", "2011-06-15")
    q.Set("DurationSeconds", strconv.Itoa(durationSec))
    q.Set("RoleSessionName", fmt.Sprintf("volkit-%d", time.Now().Unix()))
    ts := time.Now().UTC().Format(time.RFC3339)
    mac := hmac.New(sha256.New, []byte(sk))
    mac.Write([]byte(ts))
    sig := hex.EncodeToString(mac.Sum(nil))
    reqURL := *u
    reqURL.Path = "/"
    reqURL.RawQuery = q.Encode()
    req, _ := http.NewRequest(http.MethodGet, reqURL.String(), nil)
    req.Header.Set("X-Amz-Date", ts)
    req.Header.Set("X-Volkit-STS-AK", ak)
    req.Header.Set("X-Volkit-STS-Signature", sig)
    client := &http.Client{ Timeout: 8 * time.Second }
    if tlsCfg != nil { client.Transport = &http.Transport{TLSClientConfig: tlsCfg} }
    if insecure && client.Transport == nil { client.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} }
    resp, err := client.Do(req)
    if err != nil { return out, err }
    defer resp.Body.Close()
    if resp.StatusCode/100 != 2 { return out, fmt.Errorf("sts http %d", resp.StatusCode) }
    var ar assumeRoleResponse
    if err := xml.NewDecoder(resp.Body).Decode(&ar); err != nil { return out, err }
    exp, _ := time.Parse(time.RFC3339, strings.TrimSpace(ar.Result.Credentials.Expiration))
    out = Credentials{
        AccessKeyId:     strings.TrimSpace(ar.Result.Credentials.AccessKeyId),
        SecretAccessKey: strings.TrimSpace(ar.Result.Credentials.SecretAccessKey),
        SessionToken:    strings.TrimSpace(ar.Result.Credentials.SessionToken),
        Expiration:      exp,
    }
    return out, nil
}


