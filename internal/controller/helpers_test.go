package controller

import "testing"

func TestSplitCSV(t *testing.T) {
    got := splitCSV("a, b,,c ")
    want := []string{"a","b","c"}
    if len(got) != len(want) { t.Fatalf("len=%d want=%d", len(got), len(want)) }
    for i:=range want { if got[i] != want[i] { t.Fatalf("got[%d]=%q want %q", i, got[i], want[i]) } }
}

func TestVolumeNameForPrefix(t *testing.T) {
    c := &Controller{ cfg: Config{ VolumeNamePrefix: "volkit-" } }
    if v := c.volumeNameForPrefix("/team/app/data/"); v != "volkit-team-app-data" { t.Fatalf("got %q", v) }
}


