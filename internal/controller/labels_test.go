package controller

import "testing"

func TestParseLabelsIndexed(t *testing.T) {
    c := &Controller{}
    in := map[string]string{
        "volkit.claims": "2",
        "volkit.1.prefix": "team/app",
        "volkit.2.prefix": "other",
        "volkit.1.level": "gold",
    }
    out := c.parseIndexedClaims(in)
    if len(out) != 2 { t.Fatalf("len=%d", len(out)) }
    if out[0].prefix != "team/app" || out[0].level != "gold" { t.Fatalf("unexpected %#v", out[0]) }
}

func TestNodeSelector(t *testing.T) {
    c := &Controller{}
    n := map[string]map[string]string{"nodeA": {"role": "gpu", "trust": "gold"}}
    if !c.nodeMatchesSelector("nodeA", "role==gpu", n) { t.Fatal("expect match") }
    if c.nodeMatchesSelector("nodeA", "role in [edge,cpu]", n) { t.Fatal("expect not match") }
}


