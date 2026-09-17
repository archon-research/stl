package lifecycle_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
)

const gracePeriodField = "terminationGracePeriodSeconds"

// The two Python entry points. Every other k8s/base Deployment ships a Go
// binary and is held to the constant whether or not it runs lifecycle.Run —
// most reach shutdown through Temporal — so no service argues its own number.
var deploymentsOutsideTheGoShutdownChain = map[string]bool{
	"core-model-runner": true,
	"python-api":        true,
}

type manifestFields struct {
	kind  string
	name  string
	grace *int64
}

type manifestKey struct {
	indent int
	key    string
}

// The manifests are hand-written mappings, so a key's path follows from indentation alone.
func readManifests(t *testing.T, path string) []manifestFields {
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var docs []manifestFields
	var cur manifestFields
	var stack []manifestKey
	flush := func() {
		if cur != (manifestFields{}) {
			docs = append(docs, cur)
		}
		cur = manifestFields{}
		stack = stack[:0]
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if trimmed == "---" {
			flush()
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " "))
		rest := line[indent:]
		if strings.HasPrefix(rest, "- ") {
			indent += 2
			rest = rest[2:]
		}
		key, val, ok := strings.Cut(rest, ":")
		if !ok {
			continue
		}
		key, val = strings.TrimSpace(key), strings.TrimSpace(val)
		for len(stack) > 0 && stack[len(stack)-1].indent >= indent {
			stack = stack[:len(stack)-1]
		}
		keys := make([]string, 0, len(stack)+1)
		for _, k := range stack {
			keys = append(keys, k.key)
		}
		switch strings.Join(append(keys, key), ".") {
		case "kind":
			cur.kind = val
		case "metadata.name":
			cur.name = val
		case "spec.template.spec." + gracePeriodField:
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				t.Fatalf("%s: %s %q: %v", path, gracePeriodField, val, err)
			}
			cur.grace = &n
		}
		stack = append(stack, manifestKey{indent, key})
	}
	flush()
	return docs
}

// Overlays patch pod templates, so one could lower what k8s/base grants — as a
// standalone file, a strategic-merge block or a JSON-6902 `path:`. Rather than
// read three patch dialects, hold overlays to a flat rule: name the field only
// as `terminationGracePeriodSeconds: <seconds>`, at or above the constant.
func checkNoOverlayLowersTheGracePeriod(t *testing.T, want time.Duration) {
	root := filepath.Join("..", "..", "..", "..", "k8s", "overlays")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".yaml" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		line := 0
		for text := range strings.SplitSeq(string(raw), "\n") {
			line++
			if !strings.Contains(text, gracePeriodField) {
				continue
			}
			key, val, ok := strings.Cut(strings.TrimPrefix(strings.TrimSpace(text), "- "), ":")
			if !ok || strings.TrimSpace(key) != gracePeriodField {
				t.Errorf("%s:%d: patches %s in a form this test cannot evaluate; write it as `%s: <seconds>`",
					path, line, gracePeriodField, gracePeriodField)
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
			if err != nil {
				t.Errorf("%s:%d: %s is not a plain number of seconds: %q", path, line, gracePeriodField, strings.TrimSpace(val))
				continue
			}
			if time.Duration(n)*time.Second < want {
				t.Errorf("%s:%d: patches %s down to %ds, below PodTerminationGracePeriod %s", path, line, gracePeriodField, n, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestEveryGoWorkerDeploymentGrantsThePodGracePeriod(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "k8s", "base", "*", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	want := lifecycle.PodTerminationGracePeriod
	goDeployments := 0
	seenOutside := map[string]bool{}
	for _, path := range paths {
		for _, m := range readManifests(t, path) {
			if m.kind != "Deployment" {
				continue
			}
			if deploymentsOutsideTheGoShutdownChain[m.name] {
				seenOutside[m.name] = true
				continue
			}
			goDeployments++
			switch {
			case m.grace == nil:
				t.Errorf("%s: %s sets no %s, so the PodSpec default of 30s applies; PodTerminationGracePeriod is %s",
					path, m.name, gracePeriodField, want)
			case time.Duration(*m.grace)*time.Second < want:
				t.Errorf("%s: %s grants %ds, PodTerminationGracePeriod is %s",
					path, m.name, *m.grace, want)
			}
		}
	}

	if goDeployments == 0 {
		t.Fatal("no Go worker Deployment under k8s/base: the glob no longer reaches the manifests")
	}
	for name := range deploymentsOutsideTheGoShutdownChain {
		if !seenOutside[name] {
			t.Errorf("%s is exempted but has no Deployment under k8s/base", name)
		}
	}

	checkNoOverlayLowersTheGracePeriod(t, want)
}
