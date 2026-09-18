#!/usr/bin/env python3
"""Generate SQL mutations for the wave-1 SECstore migration files.

Each mutation is a single, atomic change to one of the two frozen migration files.
Output: JSON array of {id, family, operator, target, file, patches} where patches is
[{line, old, new}] (1-indexed lines, old/new are full line text including newline).

Usage:
    python3 mutate.py <migrations_dir>     # prints JSON to stdout
    python3 mutate.py <migrations_dir> -c  # prints count per family
"""
import json
import re
import sys
from pathlib import Path

FILE1 = "20260904_120000_secstore_node_edge_stores_and_vocabularies.sql"
FILE2 = "20260904_120100_secstore_concept_taxonomy_from_ref.sql"

mutations = []
_next_id = 0


def _is_sql_comment(line):
    return line.strip().startswith("--")


def mut(family, operator, target, filename, patches):
    global _next_id
    # A patch set touching only comment lines cannot change behaviour, so the
    # mutation is unkillable by construction and would inflate the denominator.
    # A mixed set (a deleted block containing a comment) is still a real mutation.
    if all(_is_sql_comment(p["old"]) for p in patches):
        return
    _next_id += 1
    mutations.append({
        "id": f"M{_next_id:03d}",
        "family": family,
        "operator": operator,
        "target": target,
        "file": filename,
        "patches": patches,
    })


def read_file(migrations_dir, filename):
    p = Path(migrations_dir) / filename
    return p.read_text().splitlines(keepends=True)


# ---------------------------------------------------------------------------
# Family: read_order — flip each DESC↔ASC in ORDER BY and index definitions
# ---------------------------------------------------------------------------

def gen_read_order(lines, filename):
    # Identify ORDER BY lines and CREATE INDEX lines
    for i, line in enumerate(lines, 1):
        # Skip COMMENT lines
        stripped = line.strip()
        if stripped.startswith("COMMENT") or stripped.startswith("--"):
            continue

        # Find each DESC in ORDER BY or CREATE INDEX lines
        if "DESC" in line and ("ORDER BY" in line or "CREATE INDEX" in line
                               or "processing_version" in line or "ingest_xid" in line
                               or "record_id" in line or "valid_from" in line):
            # Determine target from context
            if "CREATE INDEX" in line:
                idx_match = re.search(r"CREATE INDEX (\w+)", line)
                target_name = idx_match.group(1) if idx_match else f"index_line_{i}"
            else:
                target_name = _find_read_object(lines, i)

            # Find each DESC occurrence on this line
            for m in re.finditer(r"(\w+)\s+DESC", line):
                col = m.group(1)
                old_text = line
                new_text = line.replace(f"{col} DESC", f"{col} ASC", 1)
                if old_text != new_text:
                    mut("read_order", f"flip_{col}_DESC_to_ASC",
                        target_name, filename,
                        [{"line": i, "old": old_text, "new": new_text}])

        # Also flip implicit ASC to DESC on the outer ORDER BY's valid_from
        if "ORDER BY" in line and "valid_from DESC" not in line and "valid_from" in line:
            if "DISTINCT ON" not in line and "CREATE INDEX" not in line:
                # This would be a line like ORDER BY id, valid_from — but in our SQL
                # the outer ORDER BY always has valid_from DESC. Skip.
                pass


def _find_read_object(lines, line_num):
    """Walk backwards from line_num to find which view/function we're in."""
    for j in range(line_num - 1, 0, -1):
        l = lines[j - 1]
        if "CREATE VIEW" in l:
            m = re.search(r"CREATE VIEW (\w+)", l)
            return m.group(1) if m else f"view_line_{j}"
        if "CREATE FUNCTION" in l:
            m = re.search(r"CREATE FUNCTION (\w+)", l)
            return m.group(1) if m else f"func_line_{j}"
    return f"unknown_line_{line_num}"


# ---------------------------------------------------------------------------
# Family: read_order — step reversal (move window filter into the latest CTE)
# ---------------------------------------------------------------------------

def gen_step_reversal(lines, filename):
    """For each read object, move the WHERE valid_from/valid_to filter into the CTE."""
    # Find each read object's structure:
    # WITH latest AS (
    #   SELECT DISTINCT ON (...) *
    #   FROM sec_node
    #   ORDER BY ...
    # )
    # SELECT DISTINCT ON (id) *
    # FROM latest
    # WHERE valid_from <= ...
    #   AND ... < valid_to
    # ORDER BY ...

    i = 0
    while i < len(lines):
        line = lines[i]

        # Look for "WITH latest AS" or "WITH known AS" patterns
        if re.search(r"\bWITH\s+(latest|known)\s+AS\b", line):
            obj_name = _find_read_object(lines, i + 1)

            # Find the CTE's ORDER BY line (end of CTE)
            cte_order_line = None
            cte_close_line = None  # the ) that closes the CTE
            where_lines = []
            outer_where_start = None

            for j in range(i, min(i + 30, len(lines))):
                l = lines[j]
                # The CTE's closing ) followed by SELECT
                if l.strip() == ")":
                    cte_close_line = j
                # The ORDER BY inside the CTE
                if "ORDER BY" in l and cte_close_line is None:
                    cte_order_line = j

                # The WHERE clause of the outer query
                if cte_close_line is not None and "WHERE" in l and "valid_from" in l:
                    outer_where_start = j
                if outer_where_start is not None and ("AND" in l or "WHERE" in l) and ("valid_to" in l or "valid_from" in l):
                    where_lines.append(j)
                elif outer_where_start is not None and "ORDER BY" in l:
                    break

            if cte_order_line is not None and where_lines and cte_close_line is not None:
                # Build the step-reversal mutation: move WHERE clauses into the CTE,
                # just before the CTE's ORDER BY
                patches = []

                # Extract the WHERE conditions
                where_text = ""
                for wl in where_lines:
                    text = lines[wl].strip()
                    text = re.sub(r"^WHERE\s+", "", text)
                    text = re.sub(r"^AND\s+", "", text)
                    if where_text:
                        where_text += "\n        AND " + text
                    else:
                        where_text = text

                # Insert WHERE into CTE (before ORDER BY)
                cte_order = lines[cte_order_line]
                indent = "    "
                if "        " in cte_order:
                    indent = "        "
                new_cte_order = f"{indent}WHERE {where_text}\n{cte_order}"
                patches.append({
                    "line": cte_order_line + 1,
                    "old": cte_order,
                    "new": new_cte_order,
                })

                # Remove the WHERE lines from the outer query
                for wl in where_lines:
                    patches.append({
                        "line": wl + 1,
                        "old": lines[wl],
                        "new": "",
                    })

                mut("read_order", "step_reversal",
                    obj_name, filename, patches)
        i += 1


# ---------------------------------------------------------------------------
# Family: read_window — flip bounds and delete window WHERE
# ---------------------------------------------------------------------------

def gen_read_window(lines, filename):
    """For each read object's outer WHERE, flip <= to < and < to <=, and delete the WHERE."""
    i = 0
    while i < len(lines):
        line = lines[i]

        # Find WHERE lines with valid_from <= or < valid_to
        if ("valid_from <=" in line or "effective_at <" in line or
            "< valid_to" in line or "(now()" in line):

            obj_name = _find_read_object(lines, i + 1)

            # Flip <= to < on valid_from bound
            if "<=" in line and ("valid_from" in line or "(now()" in line):
                new_line = line.replace("<=", "<", 1)
                mut("read_window", "flip_lower_bound_inclusive_to_exclusive",
                    obj_name, filename,
                    [{"line": i + 1, "old": line, "new": new_line}])

            # Flip < to <= on valid_to bound
            if "< valid_to" in line or re.search(r"<\s+valid_to", line):
                new_line = line.replace("< valid_to", "<= valid_to", 1)
                if new_line == line:
                    new_line = re.sub(r"<\s+valid_to", "<= valid_to", line, count=1)
                if new_line != line:
                    mut("read_window", "flip_upper_bound_exclusive_to_inclusive",
                        obj_name, filename,
                        [{"line": i + 1, "old": line, "new": new_line}])

        i += 1

    # Delete entire window WHERE clause for each read object
    i = 0
    in_read = False
    while i < len(lines):
        line = lines[i]
        if "CREATE VIEW" in line or "CREATE FUNCTION" in line:
            in_read = True
            obj_name = _find_read_object(lines, i + 1)
        if in_read and "$$;" in line:
            in_read = False

        # Find the WHERE block between FROM latest and the outer ORDER BY
        if in_read and "FROM latest" in line:
            where_lines = []
            for j in range(i + 1, min(i + 10, len(lines))):
                l = lines[j]
                if "WHERE" in l or ("AND" in l and ("valid" in l or "now()" in l)):
                    where_lines.append(j)
                elif "ORDER BY" in l:
                    break

            if where_lines:
                patches = [{"line": wl + 1, "old": lines[wl], "new": ""}
                           for wl in where_lines]
                mut("read_window", "delete_window_filter",
                    obj_name, filename, patches)
        i += 1


# ---------------------------------------------------------------------------
# Family: knowledge_time — delete or move pg_visible_in_snapshot
# ---------------------------------------------------------------------------

def gen_knowledge_time(lines, filename):
    for i, line in enumerate(lines):
        if "pg_visible_in_snapshot" in line:
            obj_name = _find_read_object(lines, i + 1)

            # Delete the pg_visible_in_snapshot filter line
            # Need to also handle the WITH known AS ( SELECT * FROM ... WHERE pg_visible... )
            # Find the CTE block
            cte_start = None
            cte_end = None
            for j in range(i - 5, i + 5):
                if 0 <= j < len(lines):
                    if "WITH known AS" in lines[j]:
                        cte_start = j
                    if j > i and lines[j].strip().startswith(")"):
                        cte_end = j
                        break

            if cte_start is not None and cte_end is not None:
                # Mutation 1: delete the pg_visible_in_snapshot filter
                # Replace the known CTE with just SELECT * FROM sec_node/sec_edge
                patches = []
                for j in range(cte_start, cte_end + 1):
                    patches.append({
                        "line": j + 1,
                        "old": lines[j],
                        "new": "",
                    })
                # Also fix the FROM known reference in the latest CTE
                for j in range(cte_end, min(cte_end + 10, len(lines))):
                    if "FROM known" in lines[j]:
                        table = "sec_node" if "sec_node" in lines[i] else "sec_edge"
                        # Replace ), latest AS ( ... FROM known with ), latest AS ( ... FROM table
                        new_l = lines[j].replace("FROM known", f"FROM {table}")
                        patches.append({
                            "line": j + 1,
                            "old": lines[j],
                            "new": new_l,
                        })
                        break

                mut("knowledge_time", "delete_pg_visible_in_snapshot",
                    obj_name, filename, patches)

                # Mutation 2: move pg_visible_in_snapshot AFTER version resolution
                # This means putting it in the outer WHERE instead of in the known CTE
                # Remove the known CTE, add pg_visible filter to outer WHERE
                patches2 = list(patches)  # same CTE removal
                # Find the outer WHERE
                for j in range(cte_end, min(cte_end + 20, len(lines))):
                    if "WHERE valid_from" in lines[j] or "WHERE" in lines[j] and "effective_at" in lines[j]:
                        old_where = lines[j]
                        new_where = old_where.rstrip("\n") + "\n      AND pg_visible_in_snapshot(ingest_xid, known_at)\n"
                        patches2.append({
                            "line": j + 1,
                            "old": old_where,
                            "new": new_where,
                        })
                        break

                mut("knowledge_time", "move_pg_visible_after_resolution",
                    obj_name, filename, patches2)


# ---------------------------------------------------------------------------
# Family: tiebreak — drop columns from ORDER BY / indexes
# ---------------------------------------------------------------------------

def gen_tiebreak(lines, filename):
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("COMMENT") or stripped.startswith("--"):
            continue

        # ORDER BY or CREATE INDEX lines with the tiebreak columns
        is_order = "ORDER BY" in line or "CREATE INDEX" in line
        if not is_order:
            continue

        target = _find_read_object(lines, i + 1)
        if "CREATE INDEX" in line:
            m = re.search(r"CREATE INDEX (\w+)", line)
            target = m.group(1) if m else target

        # Drop record_id DESC
        if "record_id DESC" in line:
            new_line = re.sub(r",\s*record_id\s+DESC", "", line)
            if new_line != line:
                mut("tiebreak", "drop_record_id",
                    target, filename,
                    [{"line": i + 1, "old": line, "new": new_line}])

        # Drop ingest_xid DESC
        if "ingest_xid DESC" in line:
            new_line = re.sub(r",\s*ingest_xid\s+DESC", "", line)
            if new_line != line:
                mut("tiebreak", "drop_ingest_xid",
                    target, filename,
                    [{"line": i + 1, "old": line, "new": new_line}])

        # Swap processing_version and ingest_xid order
        if "processing_version DESC" in line and "ingest_xid DESC" in line:
            # Find the positions and swap them
            new_line = line
            new_line = new_line.replace("processing_version DESC, ingest_xid DESC",
                                        "ingest_xid DESC, processing_version DESC")
            if new_line != line:
                mut("tiebreak", "swap_pv_ingest_xid",
                    target, filename,
                    [{"line": i + 1, "old": line, "new": new_line}])


# ---------------------------------------------------------------------------
# Family: constraint — delete CHECK, REFERENCES, NOT NULL, DEFAULT
# ---------------------------------------------------------------------------

def gen_constraint(lines, filename):
    i = 0
    in_function = False
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        dollar_count = line.count("$$")
        if dollar_count % 2 == 1:
            in_function = not in_function

        # Skip comments and function bodies
        if stripped.startswith("--") or stripped.startswith("COMMENT") or in_function:
            i += 1
            continue

        # Named CONSTRAINT ... CHECK (multi-line)
        m = re.match(r"\s+CONSTRAINT\s+(\w+)\s+CHECK\s*\(", stripped)
        if m:
            constraint_name = m.group(1)
            # Find the end of this constraint (matching parens)
            depth = stripped.count("(") - stripped.count(")")
            end = i
            while depth > 0 and end < len(lines) - 1:
                end += 1
                depth += lines[end].count("(") - lines[end].count(")")

            patches = []
            for j in range(i, end + 1):
                patches.append({"line": j + 1, "old": lines[j], "new": ""})

            # Need to handle trailing comma
            if i > 0 and lines[i - 1].rstrip().endswith(","):
                # Remove trailing comma from previous line
                prev = lines[i - 1]
                patches.insert(0, {
                    "line": i,
                    "old": prev,
                    "new": prev.rstrip().rstrip(",") + "\n",
                })

            table = _find_table(lines, i)
            mut("constraint", f"delete_{constraint_name}",
                table, filename, patches)
            i = end + 1
            continue

        # Inline CHECK (processing_version >= 0)
        if "CHECK" in line and "CONSTRAINT" not in stripped and "CREATE" not in line:
            start = line.find("CHECK")
            if start >= 0:
                # Balance parens to find the full CHECK (…) including nested IN (…)
                depth = 0
                j = line.index("(", start)
                for k in range(j, len(line)):
                    if line[k] == "(":
                        depth += 1
                    elif line[k] == ")":
                        depth -= 1
                        if depth == 0:
                            # k is the closing paren
                            # remove from the whitespace before CHECK to the closing paren
                            ws_start = start
                            while ws_start > 0 and line[ws_start - 1] == " ":
                                ws_start -= 1
                            new_line = line[:ws_start] + line[k + 1:]
                            new_line = re.sub(r",\s*,", ",", new_line)
                            col_match = re.search(r"^\s+(\w+)", line)
                            col = col_match.group(1) if col_match else "unknown"
                            table = _find_table(lines, i)
                            mut("constraint", f"delete_inline_check_{col}",
                                table, filename,
                                [{"line": i + 1, "old": line, "new": new_line}])
                            break

        # REFERENCES (foreign key)
        if "REFERENCES" in line and "CONSTRAINT" not in stripped and "COMMENT" not in line:
            ref_match = re.search(r"\s+REFERENCES\s+\w+\([^)]+\)", line)
            if ref_match:
                new_line = line[:ref_match.start()] + line[ref_match.end():]
                col_match = re.search(r"^\s+(\w+)", line)
                col = col_match.group(1) if col_match else "unknown"
                table = _find_table(lines, i)
                mut("constraint", f"delete_fk_{col}",
                    table, filename,
                    [{"line": i + 1, "old": line, "new": new_line}])

        # NOT NULL (but not on GENERATED or inside CHECK or COMMENT)
        if "NOT NULL" in line and "GENERATED" not in line and "COMMENT" not in line and "CHECK" not in line:
            if re.search(r"^\s+\w+\s+.*NOT NULL", line):
                new_line = line.replace("NOT NULL", "", 1)
                col_match = re.search(r"^\s+(\w+)", line)
                col = col_match.group(1) if col_match else "unknown"
                table = _find_table(lines, i)
                mut("constraint", f"delete_not_null_{col}",
                    table, filename,
                    [{"line": i + 1, "old": line, "new": new_line}])

        # DEFAULT 'infinity'
        if "DEFAULT 'infinity'" in line:
            new_line = line.replace("DEFAULT 'infinity'", "")
            col_match = re.search(r"^\s+(\w+)", line)
            col = col_match.group(1) if col_match else "unknown"
            table = _find_table(lines, i)
            mut("constraint", f"delete_default_infinity_{col}",
                table, filename,
                [{"line": i + 1, "old": line, "new": new_line}])

        i += 1


def _find_table(lines, line_num):
    """Walk backwards to find the CREATE TABLE name."""
    for j in range(line_num, 0, -1):
        m = re.search(r"CREATE TABLE (\w+)", lines[j - 1])
        if m:
            return m.group(1)
    return "unknown"


# ---------------------------------------------------------------------------
# Family: guard — delete RAISE branches, alter pre-image, drop hash chain
# ---------------------------------------------------------------------------

def gen_guard(lines, filename):
    i = 0
    while i < len(lines):
        line = lines[i]

        # Find RAISE EXCEPTION lines inside sec_store_append_guard
        if "RAISE EXCEPTION" in line and "COMMENT" not in line:
            # Find the extent of this IF...END IF or just the RAISE
            # Walk backwards to find the IF
            if_start = None
            end_if = None
            for j in range(i - 1, max(i - 10, 0), -1):
                if re.search(r"\bIF\b", lines[j]) and "END IF" not in lines[j]:
                    if_start = j
                    break

            # Walk forward to find END IF
            for j in range(i + 1, min(i + 10, len(lines))):
                if "END IF" in lines[j]:
                    end_if = j
                    break

            if if_start is not None and end_if is not None:
                # Extract what the RAISE is about for the target name
                raise_match = re.search(r"RAISE EXCEPTION '([^']{0,60})", line)
                short_desc = raise_match.group(1)[:40] if raise_match else f"line_{i+1}"
                short_desc = re.sub(r"[^a-zA-Z0-9_]", "_", short_desc)

                patches = [{"line": j + 1, "old": lines[j], "new": ""}
                           for j in range(if_start, end_if + 1)]

                mut("guard", f"delete_raise_{short_desc}",
                    "sec_store_append_guard", filename, patches)
        i += 1

    # Delete pre-image exclusions (- 'record_id' - 'ingest_xid' etc.)
    for i, line in enumerate(lines):
        if "- 'record_id'" in line or "- 'ingest_xid'" in line or "- 'ingested_at'" in line:
            # Each exclusion: remove one - 'xxx' from the chain
            for field in ["record_id", "ingest_xid", "ingested_at", "content_hash", "edge_id"]:
                pattern = f" - '{field}'"
                if pattern in line:
                    new_line = line.replace(pattern, "", 1)
                    mut("guard", f"include_{field}_in_preimage",
                        "sec_store_append_guard", filename,
                        [{"line": i + 1, "old": line, "new": new_line}])

        # Delete supersedes_content_hash substitution
        if "supersedes_content_hash" in line and "COMMENT" not in line:
            if "jsonb_build_object" in line:
                # Find the block that does the substitution (the IF ... END IF)
                for j in range(i - 5, i):
                    if j >= 0 and "IF NEW.supersedes_record_id IS NOT NULL" in lines[j]:
                        for k in range(i, min(i + 5, len(lines))):
                            if "END IF" in lines[k]:
                                patches = [{"line": m + 1, "old": lines[m], "new": ""}
                                           for m in range(j, k + 1)]
                                mut("guard", "delete_hash_chain_substitution",
                                    "sec_store_append_guard", filename, patches)
                                break
                        break


# ---------------------------------------------------------------------------
# Family: acl — delete privilege words from REVOKE, drop immutability triggers
# ---------------------------------------------------------------------------

def gen_acl(lines, filename):
    for i, line in enumerate(lines):
        # Static REVOKE lines
        if "REVOKE" in line and "EXECUTE" not in line and "COMMENT" not in line and "format" not in line:
            for priv in ["UPDATE", "DELETE", "TRUNCATE"]:
                if priv in line:
                    new_line = re.sub(rf",?\s*{priv}", "", line)
                    new_line = re.sub(r"REVOKE\s*,", "REVOKE ", new_line)
                    new_line = re.sub(r",\s*ON", " ON", new_line)
                    if "REVOKE  ON" in new_line or "REVOKE ON" in new_line:
                        new_line = ""
                    if new_line != line:
                        mut("acl", f"remove_{priv.lower()}_from_revoke",
                            f"revoke_line_{i+1}", filename,
                            [{"line": i + 1, "old": line, "new": new_line}])

        # EXECUTE format('REVOKE ...') lines inside the DO $$ block
        if "EXECUTE format" in line and "REVOKE" in line and "COMMENT" not in line:
            for priv in ["UPDATE", "DELETE", "TRUNCATE"]:
                if priv in line:
                    new_line = re.sub(rf",?\s*{priv}", "", line)
                    new_line = re.sub(r"REVOKE\s*,", "REVOKE ", new_line)
                    new_line = re.sub(r",\s*ON", " ON", new_line)
                    if "REVOKE  ON" in new_line or "REVOKE ON" in new_line:
                        new_line = "        -- revoke removed\n"
                    if new_line != line:
                        mut("acl", f"remove_{priv.lower()}_from_format_revoke",
                            f"revoke_line_{i+1}", filename,
                            [{"line": i + 1, "old": line, "new": new_line}])

        # Delete each EXECUTE format('REVOKE ...') line entirely
        if "EXECUTE format" in line and "REVOKE" in line and "COMMENT" not in line:
            mut("acl", "delete_revoke_line",
                f"revoke_line_{i+1}", filename,
                [{"line": i + 1, "old": line, "new": "        -- revoke deleted\n"}])

        # EXECUTE format('CREATE TRIGGER ... reference_table_immutable')
        if "reference_table_immutable" in line and "COMMENT" not in line and "EXECUTE" in line:
            mut("acl", "delete_immutability_trigger",
                f"trigger_line_{i+1}", filename,
                [{"line": i + 1, "old": line, "new": "        -- trigger deleted\n"}])

        # Static CREATE TRIGGER ... reference_table_immutable (if any)
        if "reference_table_immutable" in line and "COMMENT" not in line and "EXECUTE" not in line:
            mut("acl", "delete_immutability_trigger",
                f"trigger_line_{i+1}", filename,
                [{"line": i + 1, "old": line, "new": ""}])


# ---------------------------------------------------------------------------
# Family: seed — mutations on the concept taxonomy seed file
# ---------------------------------------------------------------------------

def gen_seed(lines, filename):
    # Find node INSERT lines (they start with a space and a tuple)
    node_lines = []
    edge_lines = []
    in_node_insert = False
    in_edge_insert = False

    for i, line in enumerate(lines):
        if "INSERT INTO sec_node" in line:
            in_node_insert = True
            in_edge_insert = False
            continue
        if "INSERT INTO sec_edge" in line:
            in_edge_insert = True
            in_node_insert = False
            continue
        if line.strip().startswith("ON CONFLICT"):
            in_node_insert = False
            in_edge_insert = False
            continue

        if in_node_insert and line.strip().startswith("("):
            node_lines.append(i)
        if in_edge_insert and line.strip().startswith("("):
            edge_lines.append(i)

    # Delete one node row (pick a middle one to avoid boundary effects)
    if node_lines:
        target_idx = node_lines[len(node_lines) // 2]
        old_line = lines[target_idx]
        # Remove the line (handle trailing comma)
        new_line = ""
        mut("seed", "delete_one_node_row",
            "seed_node", filename,
            [{"line": target_idx + 1, "old": old_line, "new": new_line}])

    # Delete one edge row
    if edge_lines:
        target_idx = edge_lines[len(edge_lines) // 2]
        old_line = lines[target_idx]
        mut("seed", "delete_one_edge_row",
            "seed_edge", filename,
            [{"line": target_idx + 1, "old": old_line, "new": ""}])

    # Re-parent one edge (change dst_id)
    if edge_lines:
        target_idx = edge_lines[0]
        old_line = lines[target_idx]
        # Change the dst concept to a different one
        new_line = re.sub(r"'concept-[^']+','CONCEPT','NARROWER_THAN'",
                          "'concept-FAKE-REPARENT','CONCEPT','NARROWER_THAN'",
                          old_line, count=1)
        if new_line != old_line:
            mut("seed", "reparent_one_edge",
                "seed_edge", filename,
                [{"line": target_idx + 1, "old": old_line, "new": new_line}])

    # Change one concept_class in a node
    for idx in node_lines[:5]:
        line = lines[idx]
        if '"concept_class": "instrument_type"' in line:
            new_line = line.replace('"concept_class": "instrument_type"',
                                    '"concept_class": "FAKE_CLASS"')
            mut("seed", "change_concept_class",
                "seed_node", filename,
                [{"line": idx + 1, "old": line, "new": new_line}])
            break

    # Flip one requires_approval in change_reason_vocabulary
    # (This is in FILE1, not FILE2, but the plan says "seed" family)

    # Flip one is_terminal in node_status_vocabulary
    # (Also in FILE1)

    # Change one cardinality in rel_type_vocabulary
    # (Also in FILE1)

    # Null one cluster_key in rel_type_vocabulary
    # (Also in FILE1)


def gen_seed_vocab(lines, filename):
    """Seed mutations on vocabulary INSERT statements in FILE1."""
    for i, line in enumerate(lines):
        # requires_approval flips
        if "requires_approval" not in line and "'REPOINT'" in line:
            # This is a VALUES row in change_reason_vocabulary
            # flip true/false for requires_approval
            if ", true)" in line or ",true)" in line:
                new_line = line.replace(", true)", ", false)")
                if new_line != line:
                    mut("seed", "flip_requires_approval_true_to_false",
                        "change_reason_vocabulary", filename,
                        [{"line": i + 1, "old": line, "new": new_line}])
            elif ", false)" in line or ",false)" in line:
                new_line = line.replace(", false)", ", true)")
                if new_line != line:
                    mut("seed", "flip_requires_approval_false_to_true",
                        "change_reason_vocabulary", filename,
                        [{"line": i + 1, "old": line, "new": new_line}])

    # Find requires_approval in change_reason_vocabulary VALUES
    in_crv = False
    for i, line in enumerate(lines):
        if "INSERT INTO change_reason_vocabulary" in line:
            in_crv = True
            continue
        if in_crv and "ON CONFLICT" in line:
            in_crv = False
            continue
        if in_crv and line.strip().startswith("("):
            if ", false)" in line:
                new_line = line.replace(", false)", ", true)", 1)
                mut("seed", "flip_requires_approval",
                    "change_reason_vocabulary", filename,
                    [{"line": i + 1, "old": line, "new": new_line}])
                break
            elif ", true)" in line:
                new_line = line.replace(", true)", ", false)", 1)
                mut("seed", "flip_requires_approval",
                    "change_reason_vocabulary", filename,
                    [{"line": i + 1, "old": line, "new": new_line}])
                break

    # is_terminal flip in node_status_vocabulary
    in_nsv = False
    for i, line in enumerate(lines):
        if "INSERT INTO node_status_vocabulary" in line:
            in_nsv = True
            continue
        if in_nsv and "ON CONFLICT" in line:
            in_nsv = False
            continue
        if in_nsv and line.strip().startswith("("):
            if ", false," in line and "'ACTIVE'" in line:
                new_line = line.replace(", false,", ", true,", 1)
                mut("seed", "flip_is_terminal",
                    "node_status_vocabulary", filename,
                    [{"line": i + 1, "old": line, "new": new_line}])
                break

    # cardinality change in rel_type_vocabulary
    in_rtv = False
    for i, line in enumerate(lines):
        if "INSERT INTO rel_type_vocabulary" in line:
            in_rtv = True
            continue
        if in_rtv and "ON CONFLICT" in line:
            in_rtv = False
            continue
        if in_rtv and "'1'" in line and "NARROWER_THAN" in line:
            new_line = line.replace("'1'", "'n'", 1)
            mut("seed", "change_cardinality",
                "rel_type_vocabulary", filename,
                [{"line": i + 1, "old": line, "new": new_line}])
            break

    # Null one cluster_key (SPLIT_FROM has '{ex_date}')
    in_rtv = False
    for i, line in enumerate(lines):
        if "INSERT INTO rel_type_vocabulary" in line:
            in_rtv = True
            continue
        if in_rtv and "ON CONFLICT" in line:
            in_rtv = False
            continue
        if in_rtv and "'{ex_date}'" in line:
            new_line = line.replace("'{ex_date}'", "NULL")
            mut("seed", "null_cluster_key",
                "rel_type_vocabulary", filename,
                [{"line": i + 1, "old": line, "new": new_line}])
            break


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <migrations_dir> [-c]", file=sys.stderr)
        sys.exit(1)

    migrations_dir = sys.argv[1]
    count_only = "-c" in sys.argv

    lines1 = read_file(migrations_dir, FILE1)
    lines2 = read_file(migrations_dir, FILE2)

    gen_read_order(lines1, FILE1)
    gen_step_reversal(lines1, FILE1)
    gen_read_window(lines1, FILE1)
    gen_knowledge_time(lines1, FILE1)
    gen_tiebreak(lines1, FILE1)
    gen_constraint(lines1, FILE1)
    gen_guard(lines1, FILE1)
    gen_acl(lines1, FILE1)
    gen_seed_vocab(lines1, FILE1)
    gen_seed(lines2, FILE2)

    if count_only:
        from collections import Counter
        c = Counter(m["family"] for m in mutations)
        total = sum(c.values())
        for family, count in sorted(c.items()):
            print(f"  {family}: {count}")
        print(f"  TOTAL: {total}")
    else:
        json.dump(mutations, sys.stdout, indent=2)
        print()


if __name__ == "__main__":
    main()
