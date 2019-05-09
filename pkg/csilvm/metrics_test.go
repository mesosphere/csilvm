package csilvm

import (
	"context"
	"testing"

	"github.com/uber-go/tally"
)

func TestMetricsUptime(t *testing.T) {
	// We set an empty prefix as it adds noise to the metric names.
	const prefix = ""
	scope := tally.NewTestScope(prefix, nil)

	var vgname string
	func() {
		vgname = testvgname()
		pvname, pvclean := testpv()
		defer pvclean()
		client, clean := startTest(vgname, []string{pvname}, Metrics(scope))
		defer clean()
		req := testGetPluginInfoRequest()
		_, err := client.GetPluginInfo(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Check that the uptime metric is reported and has the 'volume-group'
	// tag.
	snap := scope.Snapshot()
	timers := timerMap(snap.Timers())

	uptimeTimer, ok := timers.get("uptime")
	if !ok {
		t.Fatalf("The uptime timer could not be found")
	}
	vgnameTag, ok := uptimeTimer.Tags()["volume-group"]
	if !ok {
		t.Fatalf("The volume-group tag could not be found")
	}
	if vgnameTag != vgname {
		t.Fatalf("Expected %q but got %q", vgname, vgnameTag)
	}
}

type timerMap map[string]tally.TimerSnapshot

// get finds the TimerSnapshot by name as the default map's key encodes
// service prefix and tags, too, which makes it inconvenient for looking up
// timers.
func (m timerMap) get(name string) (tally.TimerSnapshot, bool) {
	for _, timerSnapshot := range m {
		if timerSnapshot.Name() == name {
			return timerSnapshot, true
		}
	}
	return nil, false
}

type counterMap map[string]tally.CounterSnapshot

// get finds the CounterSnapshot by name as the default map's key encodes
// service prefix and tags, too, which makes it inconvenient for looking up
// counters.
func (m counterMap) get(name string) (tally.CounterSnapshot, bool) {
	for _, counterSnapshot := range m {
		if counterSnapshot.Name() == name {
			return counterSnapshot, true
		}
	}
	return nil, false
}

type gaugeMap map[string]tally.GaugeSnapshot

// get finds the GaugeSnapshot by name as the default map's key encodes
// service prefix and tags, too, which makes it inconvenient for looking up
// gauges.
func (m gaugeMap) get(name string) (tally.GaugeSnapshot, bool) {
	for _, gaugeSnapshot := range m {
		if gaugeSnapshot.Name() == name {
			return gaugeSnapshot, true
		}
	}
	return nil, false
}
