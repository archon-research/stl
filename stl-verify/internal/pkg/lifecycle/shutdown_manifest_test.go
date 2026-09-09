package lifecycle_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
)

// Python entry points: they never run lifecycle.Run, so the Go chain does not bind them.
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
		case "spec.template.spec.terminationGracePeriodSeconds":
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				t.Fatalf("%s: terminationGracePeriodSeconds %q: %v", path, val, err)
			}
			cur.grace = &n
		}
		stack = append(stack, manifestKey{indent, key})
	}
	flush()
	return docs
}

func TestEveryGoWorkerDeploymentGrantsThePodGracePeriod(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "k8s", "base", "*", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	want := lifecycle.PodTerminationGracePeriod
	deployments := 0
	seenOutside := map[string]bool{}
	for _, path := range paths {
		for _, m := range readManifests(t, path) {
			if m.kind != "Deployment" {
				continue
			}
			deployments++
			if deploymentsOutsideTheGoShutdownChain[m.name] {
				seenOutside[m.name] = true
				continue
			}
			switch {
			case m.grace == nil:
				t.Errorf("%s: %s sets no terminationGracePeriodSeconds, so the PodSpec default of 30s applies; PodTerminationGracePeriod is %s",
					path, m.name, want)
			case time.Duration(*m.grace)*time.Second < want:
				t.Errorf("%s: %s grants %ds, PodTerminationGracePeriod is %s",
					path, m.name, *m.grace, want)
			}
		}
	}

	if deployments == 0 {
		t.Fatal("no Deployment under k8s/base: the glob no longer reaches the manifests")
	}
	for name := range deploymentsOutsideTheGoShutdownChain {
		if !seenOutside[name] {
			t.Errorf("%s is exempted but has no Deployment under k8s/base", name)
		}
	}
}
