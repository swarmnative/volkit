package ws

import "testing"

func TestHubAddRemove(t *testing.T) {
    h := NewHub(10, 1, 1)
    c := &Client{Send: make(chan Message, 1)}
    h.Add(c)
    if h.ClientsGauge != 1 { t.Fatalf("gauge=%d", h.ClientsGauge) }
    h.IndexClient(c, "n1")
    if len(h.SnapshotNode("n1")) != 1 { t.Fatal("index failed") }
    h.Remove(c)
    if h.ClientsGauge != 0 { t.Fatalf("gauge=%d", h.ClientsGauge) }
}


