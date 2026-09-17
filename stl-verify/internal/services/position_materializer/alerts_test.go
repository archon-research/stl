package position_materializer

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The alerts are only as good as their agreement with what this package emits, and nothing else
// checks that. Two rules shipped broken: one selected a status this worker never records, the other
// grouped by a label that does not exist, so it named no view. Both were silent.
//
// The contract is collected from a real Telemetry, not written down here, so renaming an instrument,
// an attribute or a status in telemetry.go fails these tests instead of leaving an alert dead.
type emitted struct {
	metrics  map[string]map[string]bool // Prometheus name (dots as underscores) -> its attribute keys
	statuses map[string]bool
}

// Set by the collector rather than the instrument, so grouping on them is legitimate.
var infraLabels = map[string]bool{
	"service_name": true, "cluster": true, "k8s_namespace_name": true,
	"pod": true, "namespace": true, "job": true, "instance": true,
}

// collectEmitted exercises every recording path once and reads back what the SDK exported. It runs
// on an unseeded Telemetry, so every attribute it reports came from a recording path, not the seed.
func collectEmitted(t *testing.T) emitted {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	tel, err := NewTelemetryWithProvider(mp, nil)
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	for _, status := range runStatuses {
		tel.RecordRun(context.Background(), "materialize_a", status, 0)
	}
	tel.SetRefused(map[string]int64{"public.position_a": 0})
	tel.SetCacheRows(map[string]int64{"position_current": 0})
	tel.RecordReadFailure(context.Background(), readRefused)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	out := emitted{metrics: map[string]map[string]bool{}, statuses: map[string]bool{}}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			keys := map[string]bool{}
			out.metrics[strings.ReplaceAll(m.Name, ".", "_")] = keys
			for _, set := range attributeSets(t, m) {
				out.addAttrs(keys, set)
			}
		}
	}
	return out
}

// attributeSets returns the attribute set of every data point of m.
func attributeSets(t *testing.T, m metricdata.Metrics) []attribute.Set {
	t.Helper()
	var sets []attribute.Set
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		for _, dp := range data.DataPoints {
			sets = append(sets, dp.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, dp := range data.DataPoints {
			sets = append(sets, dp.Attributes)
		}
	default:
		t.Fatalf("metric %q is %T; teach attributeSets its data points", m.Name, m.Data)
	}
	return sets
}

func (e emitted) addAttrs(keys map[string]bool, set attribute.Set) {
	for _, kv := range set.ToSlice() {
		keys[string(kv.Key)] = true
		if kv.Key == "status" {
			e.statuses[kv.Value.AsString()] = true
		}
	}
}

func alertsFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join("..", "..", "..", "..", "alerts", "vector-cronjobs.yaml")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("reading %s: %v", p, err)
	}
	return string(b)
}

func TestAlerts_UseMetricsThisWorkerEmits(t *testing.T) {
	body := alertsFile(t)
	emittedMetrics := collectEmitted(t).metrics
	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`position_materializer_[a-z_]+`).FindAllString(body, -1) {
		seen[m] = true
		if _, ok := emittedMetrics[m]; !ok {
			t.Errorf("an alert selects %q, which this worker never emits", m)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no alert selects a position_materializer_ metric; the check read nothing")
	}
	for m := range emittedMetrics {
		if !seen[m] {
			t.Logf("note: %s is emitted but no alert reads it", m)
		}
	}
}

func TestAlerts_GroupByLabelsThatExist(t *testing.T) {
	body := alertsFile(t)
	e := collectEmitted(t)
	checked := 0
	// Only the rules that read this worker's metrics; the file covers every cronjob.
	for block := range strings.SplitSeq(body, "- alert:") {
		// The first chunk is everything before the first rule, which names no alert.
		if !strings.Contains(block, "position_materializer_") || !strings.Contains(block, "expr:") {
			continue
		}
		checked++
		name := strings.TrimSpace(strings.SplitN(block, "\n", 2)[0])
		checkGroupByLabels(t, name, block, e)
		checkStatusMatchers(t, name, block, e)
	}
	if checked == 0 {
		t.Fatal("no alert rule reads a position_materializer_ metric; the check read nothing")
	}
}

// checkGroupByLabels requires every grouped label on every metric the rule selects: a label only a
// sibling metric carries collapses this one into a single series that names nothing.
func checkGroupByLabels(t *testing.T, name, block string, e emitted) {
	t.Helper()
	selected := regexp.MustCompile(`position_materializer_[a-z_]+`).FindAllString(block, -1)
	for _, by := range regexp.MustCompile(`by \(([^)]*)\)`).FindAllStringSubmatch(block, -1) {
		for label := range strings.SplitSeq(by[1], ",") {
			label = strings.TrimSpace(label)
			if label == "" || infraLabels[label] {
				continue
			}
			for _, m := range selected {
				if !e.metrics[m][label] {
					t.Errorf("%s groups by %q, which %s does not carry", name, label, m)
				}
			}
		}
	}
}

// checkStatusMatchers reads any matcher, so status=~"ok|error" and status!="canceled" are checked too.
func checkStatusMatchers(t *testing.T, name, block string, e emitted) {
	t.Helper()
	for _, sel := range regexp.MustCompile(`status(?:=|!=|=~|!~)"([^"]*)"`).FindAllStringSubmatch(block, -1) {
		for status := range strings.SplitSeq(sel[1], "|") {
			if !e.statuses[status] {
				t.Errorf(`%s matches status %q; this worker records only %s`, name, status, sortedKeys(e.statuses))
			}
		}
	}
}

func sortedKeys(m map[string]bool) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// VectorCronjobWorkerDown is the only rule that sees a pod that never starts: a dead worker emits no
// cronjob_runs_total, so the metric-based rules above cannot. It keys on the kube-state-metrics
// deployment label and its comment asks every scheduled cronjob to add its Deployment name to BOTH
// regexes. The name is spelled out, not read from a constant, so a rename here cannot pass silently.
func TestAlerts_WorkerDownCoversThisDeployment(t *testing.T) {
	body := alertsFile(t)
	rule := ""
	for block := range strings.SplitSeq(body, "- alert:") {
		if strings.HasPrefix(strings.TrimSpace(block), "VectorCronjobWorkerDown") {
			rule = block
			break
		}
	}
	if rule == "" {
		t.Fatal("VectorCronjobWorkerDown is not in the alerts file")
	}
	regexes := regexp.MustCompile(`deployment=~"([^"]*)"`).FindAllStringSubmatch(rule, -1)
	if len(regexes) != 2 {
		t.Fatalf("VectorCronjobWorkerDown carries %d deployment regexes, want 2 (available and desired)", len(regexes))
	}
	for i, m := range regexes {
		if !regexp.MustCompile(`(^|\|)position-materializer(\||$)`).MatchString(m[1]) {
			t.Errorf("deployment regex %d of VectorCronjobWorkerDown omits position-materializer: %q", i+1, m[1])
		}
	}
}
