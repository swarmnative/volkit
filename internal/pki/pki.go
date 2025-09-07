package pki

import (
    "crypto/rand"
    "crypto/rsa"
    "crypto/x509"
    "crypto/x509/pkix"
    "encoding/pem"
    "math/big"
    "os"
    "strings"
    "time"
)

// CAMaterial holds an in-memory CA certificate and key.
type CAMaterial struct {
    Cert *x509.Certificate
    Key  *rsa.PrivateKey
    PEM  []byte
}

// CA is the initialized CA material used for dev/file-based PKI operations.
var CA CAMaterial

// LoadCA initializes the CA from files when provided, or generates a short-lived ephemeral CA for dev.
func LoadCA() {
    caCertFile := strings.TrimSpace(os.Getenv("VOLKIT_CA_CERT_FILE"))
    caKeyFile := strings.TrimSpace(os.Getenv("VOLKIT_CA_KEY_FILE"))
    if caCertFile != "" && caKeyFile != "" {
        if certPEM, err := os.ReadFile(caCertFile); err == nil {
            if keyPEM, err2 := os.ReadFile(caKeyFile); err2 == nil {
                block, _ := pem.Decode(certPEM)
                if block != nil {
                    if cert, err3 := x509.ParseCertificate(block.Bytes); err3 == nil {
                        kblock, _ := pem.Decode(keyPEM)
                        if kblock != nil {
                            if key, err4 := x509.ParsePKCS1PrivateKey(kblock.Bytes); err4 == nil {
                                CA = CAMaterial{Cert: cert, Key: key, PEM: certPEM}
                            }
                        }
                    }
                }
            }
        }
    }
    // fallback: generate ephemeral CA (dev only)
    if CA.Cert == nil || CA.Key == nil {
        key, _ := rsa.GenerateKey(rand.Reader, 2048)
        tpl := &x509.Certificate{SerialNumber: new(big.Int).SetInt64(time.Now().UnixNano()), Subject: pkix.Name{CommonName: "volkit-dev-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true}
        der, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
        pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
        CA = CAMaterial{Cert: tpl, Key: key, PEM: pemCert}
    }
}


