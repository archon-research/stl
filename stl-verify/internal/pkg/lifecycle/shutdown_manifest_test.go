package lifecycle_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
)

// Python entry points: they never run lifecycle.Run, so the Go chain does not bind them.
var deploymentsOutsideTheGoShutdownChain = map[string]bool{
	"core-model-runner": true,
	"python-api":        true,
}

type deploymentManifest struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec struct {
				TerminationGracePeriodSeconds *int64 `yaml:"terminationGracePeriodSeconds"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func TestEveryGoWorkerDeploymentGrantsThePodGracePeriod(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "k8s", "base", "*", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	want := int64(lifecycle.PodTerminationGracePeriod / time.Second)
	deployments := 0
	seenOutside := map[string]bool{}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		dec := yaml.NewDecoder(bytes.NewReader(raw))
		for {
			var m deploymentManifest
			err := dec.Decode(&m)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if m.Kind != "Deployment" {
				continue
			}
			deployments++
			if deploymentsOutsideTheGoShutdownChain[m.Metadata.Name] {
				seenOutside[m.Metadata.Name] = true
				continue
			}
			got := m.Spec.Template.Spec.TerminationGracePeriodSeconds
			switch {
			case got == nil:
				t.Errorf("%s: %s sets no terminationGracePeriodSeconds; the kubelet default is 30s, PodTerminationGracePeriod is %ds",
					path, m.Metadata.Name, want)
			case *got < want:
				t.Errorf("%s: %s grants %ds, PodTerminationGracePeriod is %ds",
					path, m.Metadata.Name, *got, want)
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
