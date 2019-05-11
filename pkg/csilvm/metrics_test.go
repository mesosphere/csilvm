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
	gauges := gaugeMap(snap.Gauges())

	uptimeGauge := gauges.mustGet(t, "uptime")
	vgnameTag, ok := uptimeGauge.Tags()["volume-group"]
	if !ok {
		t.Fatalf("The volume-group tag could not be found")
	}
	if vgnameTag != vgname {
		t.Fatalf("Expected %q but got %q", vgname, vgnameTag)
	}
}

func TestMetricsInterceptor(t *testing.T) {
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

		// A requests that succeeds
		getPluginInfoReq := testGetPluginInfoRequest()
		_, err := client.GetPluginInfo(context.Background(), getPluginInfoReq)
		if err != nil {
			t.Fatal(err)
		}

		// Another requests that succeeds
		_, err = client.GetPluginInfo(context.Background(), getPluginInfoReq)
		if err != nil {
			t.Fatal(err)
		}

		// A single request that fails
		createVolumeReq := testCreateVolumeRequest()
		// Check that trying to create a volume with the same name but
		// incompatible capacity_range fails.
		createVolumeReq.CapacityRange.RequiredBytes += 1
		_, err = client.CreateVolume(context.Background(), createVolumeReq)
		if err == nil {
			t.Fatalf("Expected error but got nil")
		}
	}()

	// Check that requests metrics are reported.
	snap := scope.Snapshot()
	counters := counterMap(snap.Counters())
	timers := timerMap(snap.Timers())

	// Check GetPluginInfo metrics
	getPluginInfoFilter := filterMetricsTags(map[string]string{
		"method": "/csi.v0.Identity/GetPluginInfo",
	})
	getPluginInfoFilterSuccess := filterMetricsTags(map[string]string{
		"method":      "/csi.v0.Identity/GetPluginInfo",
		"result_type": resultTypeSuccess,
	})
	getPluginInfoFilterError := filterMetricsTags(map[string]string{
		"method":      "/csi.v0.Identity/GetPluginInfo",
		"result_type": resultTypeError,
	})
	success := counters.mustGet(t, "requests", getPluginInfoFilterSuccess)
	if success.Value() != 2 {
		t.Fatalf("expected 2 but got %d", 2)
	}
	_, ok := counters.get("requests.failure", getPluginInfoFilterError)
	if ok {
		t.Fatalf("The requests.failure counter was not expected")
	}
	latency := timers.mustGet(t, "requests.latency", getPluginInfoFilter)
	if int64(len(latency.Values())) != 2 {
		t.Fatalf("expected %d but got %d", 2, len(latency.Values()))
	}
	if latency.Values()[0] <= 0 {
		t.Fatalf("The requests.latency timer did not report a latency: %v", latency)
	}

	// Check CreateVolume metrics
	createVolumeFilter := filterMetricsTags(map[string]string{
		"method": "/csi.v0.Controller/CreateVolume",
	})
	createVolumeFilterSuccess := filterMetricsTags(map[string]string{
		"method":      "/csi.v0.Controller/CreateVolume",
		"result_type": resultTypeSuccess,
	})
	createVolumeFilterError := filterMetricsTags(map[string]string{
		"method":      "/csi.v0.Controller/CreateVolume",
		"result_type": resultTypeError,
	})
	_, ok = counters.get("requests", createVolumeFilterSuccess)
	if ok {
		t.Fatalf("The requests counter was not expected")
	}
	failure := counters.mustGet(t, "requests", createVolumeFilterError)
	if failure.Value() != 1 {
		t.Fatalf("expected 1 but got %d", failure.Value())
	}
	latency = timers.mustGet(t, "requests.latency", createVolumeFilter)
	if int64(len(latency.Values())) != 1 {
		t.Fatalf("expected %d but got %d", 1, len(latency.Values()))
	}
	if latency.Values()[0] <= 0 {
		t.Fatalf("The requests.latency timer did not report a latency: %v", latency)
	}
}

func TestReportStorageMetrics(t *testing.T) {
	// We set an empty prefix as it adds noise to the metric names.
	const prefix = ""
	scope := tally.NewTestScope(prefix, nil)

	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, Metrics(scope))
	defer clean()

	type expect struct {
		volumes int
		total   int
		free    int
		used    int
	}
	check := func(snap tally.Snapshot, exp expect) {
		gauges := gaugeMap(snap.Gauges())
		volumes := int(gauges.mustGet(t, "volumes").Value())
		if volumes != exp.volumes {
			t.Fatalf("expected %d but got %d", exp.volumes, volumes)
		}
		total := int(gauges.mustGet(t, "bytes-total").Value())
		if total != exp.total {
			t.Fatalf("expected %d but got %d", exp.total, total)
		}
		free := int(gauges.mustGet(t, "bytes-free").Value())
		if free != exp.free {
			t.Fatalf("expected %d but got %d", exp.free, free)
		}
		used := int(gauges.mustGet(t, "bytes-used").Value())
		if used != exp.used {
			t.Fatalf("expected %d but got %d", exp.used, used)
		}
		if exp.free+exp.used != exp.total {
			t.Fatalf("expected %d but got %d", exp.free+exp.used, exp.total)
		}
	}

	// Check storage metrics before a volume was created.
	check(scope.Snapshot(), expect{
		volumes: 0,
		total:   100663296,
		free:    100663296,
		used:    0,
	})

	// A single request that fails
	createVolumeReq := testCreateVolumeRequest()
	_, err := client.CreateVolume(context.Background(), createVolumeReq)
	if err != nil {
		t.Fatal(err)
	}

	// Check storage metrics after a volume was created.
	check(scope.Snapshot(), expect{
		volumes: 1,
		total:   100663296,
		free:    16777216,
		used:    83886080,
	})
}

type getOpts struct {
	tags map[string]string
}

type getOpt func(*getOpts)

func filterMetricsTags(tags map[string]string) getOpt {
	return func(opts *getOpts) {
		if tags == nil {
			return
		}
		opts.tags = tags
	}
}

type timerMap map[string]tally.TimerSnapshot

// get finds the TimerSnapshot by name as the default map's key encodes
// service prefix and tags, too, which makes it inconvenient for looking up
// timers.
func (m timerMap) get(name string, opts ...getOpt) (tally.TimerSnapshot, bool) {
	defaultOpts := getOpts{
		tags: make(map[string]string),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&defaultOpts)
		}
	}
timersLoop:
	for _, timerSnapshot := range m {
		if timerSnapshot.Name() == name {
			// Check that all tags being filtered on are present on the timer.
			for fkey, fval := range defaultOpts.tags {
				val, ok := timerSnapshot.Tags()[fkey]
				if !ok || fval != val {
					continue timersLoop
				}
			}
			return timerSnapshot, true
		}
	}
	return nil, false
}

func (m timerMap) mustGet(t *testing.T, name string, opts ...getOpt) tally.TimerSnapshot {
	timer, ok := m.get(name, opts...)
	if !ok {
		t.Fatalf("cannot find timer %q", name)
	}
	return timer
}

type counterMap map[string]tally.CounterSnapshot

// get finds the CounterSnapshot by name as the default map's key encodes
// service prefix and tags, too, which makes it inconvenient for looking up
// counters.
func (m counterMap) get(name string, opts ...getOpt) (tally.CounterSnapshot, bool) {
	defaultOpts := getOpts{
		tags: make(map[string]string),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&defaultOpts)
		}
	}
countersLoop:
	for _, counterSnapshot := range m {
		if counterSnapshot.Name() == name {
			// Check that all tags being filtered on are present on the counter.
			for fkey, fval := range defaultOpts.tags {
				val, ok := counterSnapshot.Tags()[fkey]
				if !ok || fval != val {
					continue countersLoop
				}
			}
			return counterSnapshot, true
		}
	}
	return nil, false
}

func (m counterMap) mustGet(t *testing.T, name string, opts ...getOpt) tally.CounterSnapshot {
	counter, ok := m.get(name, opts...)
	if !ok {
		t.Fatalf("cannot find counter %q", name)
	}
	return counter
}

type gaugeMap map[string]tally.GaugeSnapshot

// get finds the GaugeSnapshot by name as the default map's key encodes
// service prefix and tags, too, which makes it inconvenient for looking up
// gauges.
func (m gaugeMap) get(name string, opts ...getOpt) (tally.GaugeSnapshot, bool) {
	defaultOpts := getOpts{
		tags: make(map[string]string),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&defaultOpts)
		}
	}
gaugesLoop:
	for _, gaugeSnapshot := range m {
		if gaugeSnapshot.Name() == name {
			// Check that all tags being filtered on are present on the gauge.
			for fkey, fval := range defaultOpts.tags {
				val, ok := gaugeSnapshot.Tags()[fkey]
				if !ok || fval != val {
					continue gaugesLoop
				}
			}
			return gaugeSnapshot, true
		}
	}
	return nil, false
}

func (m gaugeMap) mustGet(t *testing.T, name string, opts ...getOpt) tally.GaugeSnapshot {
	gauge, ok := m.get(name, opts...)
	if !ok {
		t.Fatalf("cannot find gauge %q", name)
	}
	return gauge
}
