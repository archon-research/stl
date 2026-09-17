#!/usr/bin/env bash
# Mutation test harness for the wave-1 SECstore migrations.
#
# Usage:
#   ./run.sh                  # run against the branch head's test suite
#   ./run.sh --baseline       # check out main, run the pre-fix suite against the same mutations
#   ./run.sh --mutation M042  # run one mutation only (for debugging)
#
# Prerequisites:
#   - STL_TEST_POSTGRES_DSN or a running timescale/timescaledb:2.29.2-pg18 container
#   - python3 with no extra deps
#   - go 1.26+
#
# Output: RESULTS.md in this directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
RESULTS_FILE="$SCRIPT_DIR/RESULTS.md"
MUTATIONS_JSON="$SCRIPT_DIR/.mutations.json"
WORKTREE_DIR="/tmp/secstore-mutation-wt-$$"

BASELINE=false
SINGLE_MUTATION=""
TEST_TIMEOUT="5m"
TEST_PATTERN="TestSecStore"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --baseline)    BASELINE=true; shift ;;
        --mutation)    SINGLE_MUTATION="$2"; shift 2 ;;
        --timeout)     TEST_TIMEOUT="$2"; shift 2 ;;
        --pattern)     TEST_PATTERN="$2"; shift 2 ;;
        *)             echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
done

MIGRATIONS_DIR="$REPO_ROOT/stl-verify/db/migrations"
FILE1="20260904_120000_secstore_node_edge_stores_and_vocabularies.sql"
FILE2="20260904_120100_secstore_concept_taxonomy_from_ref.sql"

# --- Container management ---------------------------------------------------

ensure_postgres() {
    if [[ -n "${STL_TEST_POSTGRES_DSN:-}" ]]; then
        echo "Using existing Postgres at STL_TEST_POSTGRES_DSN"
        return
    fi

    local name="secstore-mutation-pg"
    if docker ps --format '{{.Names}}' | grep -q "^${name}$"; then
        echo "Reusing container $name"
    else
        echo "Starting $name (timescale/timescaledb:2.29.2-pg18)..."
        docker rm -f "$name" 2>/dev/null || true
        docker run -d --name "$name" \
            -e POSTGRES_USER=test \
            -e POSTGRES_PASSWORD=test \
            -e POSTGRES_DB=test \
            -p 15432:5432 \
            timescale/timescaledb:2.29.2-pg18

        echo "Waiting for Postgres..."
        local ready=false
        for _ in $(seq 1 30); do
            if docker exec "$name" pg_isready -U test -q 2>/dev/null; then
                ready=true
                break
            fi
            sleep 1
        done
        if ! $ready; then
            echo "ERROR: Postgres did not become ready in 30s" >&2
            exit 1
        fi
    fi

    export STL_TEST_POSTGRES_DSN="postgres://test:test@localhost:15432/test?sslmode=disable"
}

# --- Worktree setup ----------------------------------------------------------

setup_worktree() {
    local ref
    if $BASELINE; then
        ref="main"
        echo "Baseline mode: worktree at main (pre-fix suite)"
    else
        ref="HEAD"
        echo "Branch mode: worktree at HEAD"
    fi

    rm -rf "$WORKTREE_DIR"
    git -C "$REPO_ROOT" worktree add "$WORKTREE_DIR" "$ref" --detach 2>/dev/null
    echo "Worktree at $WORKTREE_DIR ($ref)"
}

cleanup_worktree() {
    if [[ -d "$WORKTREE_DIR" ]]; then
        git -C "$REPO_ROOT" worktree remove --force "$WORKTREE_DIR" 2>/dev/null || true
    fi
}
trap cleanup_worktree EXIT

# --- Mutation application ----------------------------------------------------

apply_mutation() {
    local mutation_json="$1"
    local wt_migrations="$WORKTREE_DIR/stl-verify/db/migrations"

    # Reset both files to their worktree originals
    if ! git -C "$WORKTREE_DIR" checkout -- \
        "stl-verify/db/migrations/$FILE1" \
        "stl-verify/db/migrations/$FILE2" 2>/dev/null; then
        echo "ERROR: git checkout failed — cannot reset migration files" >&2
        return 1
    fi

    # Apply patches via python for precision — patches sorted descending by line
    # so earlier patches don't shift indices of later ones.
    python3 -c "
import json, sys

mutation = json.loads(sys.argv[1])
migrations_dir = sys.argv[2]

filepath = migrations_dir + '/' + mutation['file']
with open(filepath, 'r') as f:
    lines = f.readlines()

for patch in sorted(mutation['patches'], key=lambda p: p['line'], reverse=True):
    line_idx = patch['line'] - 1
    if line_idx < 0 or line_idx >= len(lines):
        print(f'ERROR: line {patch[\"line\"]} out of range (file has {len(lines)} lines) in {mutation[\"file\"]}', file=sys.stderr)
        sys.exit(2)
    if lines[line_idx] == patch['old']:
        lines[line_idx] = patch['new']
    else:
        print(f'WARNING: line {patch[\"line\"]} mismatch in {mutation[\"file\"]}', file=sys.stderr)
        print(f'  expected: {patch[\"old\"]!r}', file=sys.stderr)
        print(f'  got:      {lines[line_idx]!r}', file=sys.stderr)
        sys.exit(2)

with open(filepath, 'w') as f:
    f.writelines(lines)
" "$mutation_json" "$wt_migrations"
}

# --- Test execution ----------------------------------------------------------

run_tests() {
    local wt_stl="$WORKTREE_DIR/stl-verify"
    local exit_code=0

    cd "$wt_stl"
    go test -tags=integration -run "$TEST_PATTERN" -v -timeout="$TEST_TIMEOUT" \
        ./db/migrator/ 2>&1 || exit_code=$?
    cd "$REPO_ROOT"

    return $exit_code
}

classify_result() {
    local exit_code="$1"
    local test_output="$2"

    if [[ $exit_code -eq 0 ]]; then
        echo "SURVIVED"
    elif [[ "$test_output" == *"--- FAIL:"* ]]; then
        echo "KILLED"
    elif [[ "$test_output" == *"build failed"* || "$test_output" == *"cannot load package"* ]]; then
        echo "HARNESS_ERROR"
    else
        echo "HARNESS_ERROR"
    fi
}

extract_killing_test() {
    local test_output="$1"
    grep -m1 -- '--- FAIL:' <<< "$test_output" | sed 's/.*--- FAIL: //' | awk '{print $1}' || echo ""
}

# --- Results -----------------------------------------------------------------

init_results() {
    cat > "$RESULTS_FILE" << 'EOF'
# Mutation Test Results

| ID | Family | Operator | Target | Result | Killing Test |
|----|--------|----------|--------|--------|--------------|
EOF
    if $BASELINE; then
        sed -i '1s/Results/Results (Baseline)/' "$RESULTS_FILE"
    fi
}

append_result() {
    local id="$1" family="$2" operator="$3" target="$4" result="$5" killer="$6"
    printf "| %s | %s | %s | %s | %s | %s |\n" \
        "$id" "$family" "$operator" "$target" "$result" "$killer" >> "$RESULTS_FILE"
}

append_summary() {
    local killed="$1" survived="$2" errors="$3" total="$4"
    cat >> "$RESULTS_FILE" << EOF

## Summary

- **Total**: $total
- **Killed**: $killed
- **Survived**: $survived
- **Harness errors**: $errors
- **Kill rate**: $(( killed * 100 / (total > 0 ? total : 1) ))%
EOF
    if $BASELINE; then
        echo "- **Mode**: baseline (main, pre-fix suite)" >> "$RESULTS_FILE"
    else
        echo "- **Mode**: branch HEAD" >> "$RESULTS_FILE"
    fi
    echo "- **Date**: $(date -Iseconds)" >> "$RESULTS_FILE"
    echo "- **Commit**: $(git -C "$REPO_ROOT" rev-parse --short HEAD)" >> "$RESULTS_FILE"
}

# --- Main --------------------------------------------------------------------

main() {
    ensure_postgres
    setup_worktree

    echo "Generating mutations..."
    python3 "$SCRIPT_DIR/mutate.py" "$MIGRATIONS_DIR" > "$MUTATIONS_JSON"

    local total
    total=$(python3 -c "import json; print(len(json.load(open('$MUTATIONS_JSON'))))")
    echo "Generated $total mutations"

    echo "Running unmutated control..."
    local ctrl_output ctrl_exit=0
    ctrl_output=$(run_tests 2>&1) || ctrl_exit=$?
    if [[ $ctrl_exit -ne 0 ]]; then
        echo "FATAL: unmutated tree is red (exit $ctrl_exit). Fix the suite before running mutations." >&2
        echo "$ctrl_output" | grep -E '(FAIL|FATAL|panic)' | head -10 >&2
        exit 1
    fi
    if ! grep -q -- '--- PASS:' <<< "$ctrl_output"; then
        echo "FATAL: control run produced no passing tests — -run pattern '$TEST_PATTERN' matched nothing." >&2
        exit 1
    fi
    echo "Control: green"

    init_results

    local killed=0 survived=0 errors=0 count=0

    while IFS= read -r mutation_json; do
        local id family operator target
        id=$(echo "$mutation_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
        family=$(echo "$mutation_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['family'])")
        operator=$(echo "$mutation_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['operator'])")
        target=$(echo "$mutation_json" | python3 -c "import json,sys; print(json.load(sys.stdin)['target'])")

        if [[ -n "$SINGLE_MUTATION" && "$id" != "$SINGLE_MUTATION" ]]; then
            continue
        fi

        count=$((count + 1))
        echo ""
        echo "=== [$count/$total] $id: $family/$operator on $target ==="

        local apply_exit=0
        apply_mutation "$mutation_json" || apply_exit=$?
        if [[ $apply_exit -ne 0 ]]; then
            echo "  HARNESS_ERROR: patch failed to apply"
            append_result "$id" "$family" "$operator" "$target" "HARNESS_ERROR" ""
            errors=$((errors + 1))
            continue
        fi

        local test_output exit_code=0
        test_output=$(run_tests 2>&1) || exit_code=$?

        local result
        result=$(classify_result "$exit_code" "$test_output")
        local killer=""
        if [[ "$result" == "KILLED" ]]; then
            killer=$(extract_killing_test "$test_output")
            killed=$((killed + 1))
            echo "  KILLED by $killer"
        elif [[ "$result" == "SURVIVED" ]]; then
            survived=$((survived + 1))
            echo "  SURVIVED"
        else
            errors=$((errors + 1))
            echo "  HARNESS_ERROR"
        fi

        append_result "$id" "$family" "$operator" "$target" "$result" "$killer"

    done < <(python3 -c "
import json
with open('$MUTATIONS_JSON') as f:
    for m in json.load(f):
        print(json.dumps(m))
")

    local denominator=$count
    if [[ $denominator -eq 0 ]]; then
        denominator=$total
    fi
    append_summary "$killed" "$survived" "$errors" "$denominator"

    echo ""
    echo "=== Done: $killed killed / $survived survived / $errors errors out of $total ==="
    echo "Results: $RESULTS_FILE"
}

main
