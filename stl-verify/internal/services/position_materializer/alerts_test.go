package position_materializer

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The alerts are only as good as their agreement with what this package emits, and nothing else
// checks that. Two rules shipped broken: one selected a status this worker never records, the other
// grouped by a label that does not exist, so it named no view. Both were silent.
//
// Emitted here, so this list is the contract: instrument names with dots as underscores, the
// attribute keys RecordRun and RecordRefused set, and the status values the service passes.
var (
	emittedMetrics = map[string]bool{
		"position_materializer_projection_runs_total": true,
		"position_materializer_rows_changed_total":    true,
		"position_materializer_positions_refused":     true,
	}
	emittedAttrs = map[string]bool{
		"materializer": true,
		"status":       true,
		"projection":   true,
	}
	emittedStatuses = map[string]bool{"ok": true, "error": true}
	// Set by the collector rather than the instrument, so grouping on them is legitimate.
	infraLabels = map[string]bool{
		"service_name": true, "cluster": true, "k8s_namespace_name": true,
		"pod": true, "namespace": true, "job": true, "instance": true,
	}
)

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
	seen := map[string]bool{}
	for _, m := range regexp.MustCompile(`position_materializer_[a-z_]+`).FindAllString(body, -1) {
		seen[m] = true
		if !emittedMetrics[m] {
			t.Errorf("an alert selects %q, which this worker never emits", m)
		}
	}
	for m := range emittedMetrics {
		if !seen[m] {
			t.Logf("note: %s is emitted but no alert reads it", m)
		}
	}
}

func TestAlerts_GroupByLabelsThatExist(t *testing.T) {
	body := alertsFile(t)
	// Only the rules that read this worker's metrics; the file covers every cronjob.
	for block := range strings.SplitSeq(body, "- alert:") {
		// The first chunk is everything before the first rule, which names no alert.
		if !strings.Contains(block, "position_materializer_") || !strings.Contains(block, "expr:") {
			continue
		}
		name := strings.TrimSpace(strings.SplitN(block, "\n", 2)[0])
		for _, by := range regexp.MustCompile(`by \(([^)]*)\)`).FindAllStringSubmatch(block, -1) {
			for label := range strings.SplitSeq(by[1], ",") {
				label = strings.TrimSpace(label)
				if label == "" || emittedAttrs[label] || infraLabels[label] {
					continue
				}
				t.Errorf("%s groups by %q, which is neither an emitted attribute nor an infra label", name, label)
			}
		}
		for _, sel := range regexp.MustCompile(`status="([a-z]+)"`).FindAllStringSubmatch(block, -1) {
			if !emittedStatuses[sel[1]] {
				t.Errorf(`%s selects status=%q; this worker records only %s`, name, sel[1], sortedKeys(emittedStatuses))
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
