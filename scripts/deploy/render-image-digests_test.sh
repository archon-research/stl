#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
RENDERER="${REPO_ROOT}/scripts/deploy/render-image-digests.sh"
IMAGE_RENDERER="${REPO_ROOT}/scripts/deploy/render-overlay-images.sh"
TEST_DIR="$(mktemp -d)"
trap 'rm -rf "$TEST_DIR"' EXIT

OVERLAY="${TEST_DIR}/kustomization.yaml"
FAKE_BIN="${TEST_DIR}/bin"
mkdir -p "$FAKE_BIN"

cat >"$OVERLAY" <<'YAML'
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

images:
  - name: alpha
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-watcher
    newTag: "0123456789abcdef0123456789abcdef01234567"
  - name: beta
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-watcher
    newTag: "0123456789abcdef0123456789abcdef01234567"
  - name: gamma
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "gamma-0123456789abcdef0123456789abcdef01234567"
  - name: transform-bootstrap
    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob
    newTag: "transform-bootstrap-0123456789abcdef0123456789abcdef01234567"

patches:
  - path: keep-me.yaml
YAML

cat >"${FAKE_BIN}/aws" <<'AWS'
#!/usr/bin/env bash
set -euo pipefail

if [ -n "${FAKE_DIGEST:-}" ]; then
  printf '%s\n' "$FAKE_DIGEST"
  exit 0
fi

case " $* " in
  *' imageTag=gamma-0123456789abcdef0123456789abcdef01234567 '*)
    printf '%s\n' 'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
    ;;
  *)
    printf '%s\n' 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    ;;
esac
AWS
chmod +x "${FAKE_BIN}/aws"

PATH="${FAKE_BIN}:$PATH" "$RENDERER" --write "$OVERLAY"
PATH="${FAKE_BIN}:$PATH" "$RENDERER" --check "$OVERLAY"
PATH="${FAKE_BIN}:$PATH" "$RENDERER" --verify "$OVERLAY"

if "$IMAGE_RENDERER" --strip "$OVERLAY" | grep -q 'configMapGenerator:'; then
  echo 'expected the deploy guard strip to remove the generated digest ConfigMap' >&2
  exit 1
fi

if PATH="${FAKE_BIN}:$PATH" FAKE_DIGEST=not-a-digest "$RENDERER" --write "$OVERLAY" >"${TEST_DIR}/invalid.out" 2>&1; then
  echo 'expected an invalid ECR digest to fail' >&2
  exit 1
fi
grep -Fq 'could not resolve a manifest digest' "${TEST_DIR}/invalid.out"

grep -Fqx 'configMapGenerator:' "$OVERLAY"
grep -Fqx '  - name: stl-verify-image-digests' "$OVERLAY"
grep -Fqx '      disableNameSuffixHash: true' "$OVERLAY"
grep -Fqx '      - alpha=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' "$OVERLAY"
grep -Fqx '      - beta=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' "$OVERLAY"
grep -Fqx '      - gamma=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' "$OVERLAY"
grep -Fqx '      - transform-bootstrap=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' "$OVERLAY"
grep -Fqx 'patches:' "$OVERLAY"

echo 'ok - renders a stable digest ConfigMap from stamped images'
