// Package pki holds a simple in-process CA material for development.
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

// Store holds CA material in memory.
type Store struct {
    Cert *x509.Certificate
    Key  *rsa.PrivateKey
    PEM  []byte
}

// LoadFromEnv loads CA from VOLKIT_CA_CERT_FILE and VOLKIT_CA_KEY_FILE, or generates a dev CA.
func (s *Store) LoadFromEnv() {
    caCertFile := strings.TrimSpace(os.Getenv("VOLKIT_CA_CERT_FILE"))
    caKeyFile := strings.TrimSpace(os.Getenv("VOLKIT_CA_KEY_FILE"))
    if caCertFile != "" && caKeyFile != "" {
        if certPEM, err := os.ReadFile(caCertFile); err == nil {
            if keyPEM, err2 := os.ReadFile(caKeyFile); err2 == nil {
                if block, _ := pem.Decode(certPEM); block != nil {
                    if cert, err3 := x509.ParseCertificate(block.Bytes); err3 == nil {
                        if kblock, _ := pem.Decode(keyPEM); kblock != nil {
                            if key, err4 := x509.ParsePKCS1PrivateKey(kblock.Bytes); err4 == nil {
                                s.Cert = cert
                                s.Key = key
                                s.PEM = certPEM
                            }
                        }
                    }
                }
            }
        }
    }
    if s.Cert == nil || s.Key == nil {
        key, _ := rsa.GenerateKey(rand.Reader, 2048)
        tpl := &x509.Certificate{SerialNumber: newSerial(), Subject: pkix.Name{CommonName: "volkit-dev-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true}
        der, _ := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
        pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
        s.Cert = tpl
        s.Key = key
        s.PEM = pemCert
    }
}

func newSerial() *big.Int {
    // simple time-based serial
    return new(big.Int).SetInt64(time.Now().UnixNano())
}


