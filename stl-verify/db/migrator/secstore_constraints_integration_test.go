//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestSecStoreEveryEngineRuleRejectsItsInput(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	t.Run("sec_node_valid_chk_rejects_valid_to_before_valid_from", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('em-t-valid-chk', 'ENTITY', 'ACTIVE', '2026-06-01', '2026-05-31', 'test', 'SEED_LOAD', 'valid_to < valid_from', 'test')`)
		assertSQLState(t, err, "23514", "sec_node_valid_chk")
	})

	t.Run("sec_edge_valid_chk_rejects_valid_to_before_valid_from", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('sec-t-eval-chk', 'SECURITY', 'sec-t-eval-dst', 'SECURITY', 'HAS_UNDERLYING', '2026-06-01', '2026-05-31', 'test', 'SEED_LOAD', 'valid_to < valid_from', 'test')`)
		assertSQLState(t, err, "23514", "sec_edge_valid_chk")
	})

	t.Run("sec_node_valid_to_not_null", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('em-t-null-vt', 'ENTITY', 'ACTIVE', '2026-01-01', NULL, 'test', 'SEED_LOAD', 'explicit NULL valid_to', 'test')`)
		assertSQLState(t, err, "23502", "sec_node.valid_to NOT NULL")
	})

	t.Run("sec_edge_valid_to_not_null", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, valid_to, `+secstoreSpine+`)
			VALUES ('sec-t-null-evt', 'SECURITY', 'sec-t-null-edst', 'SECURITY', 'HAS_UNDERLYING', '2026-01-01', NULL, 'test', 'SEED_LOAD', 'explicit NULL valid_to', 'test')`)
		assertSQLState(t, err, "23502", "sec_edge.valid_to NOT NULL")
	})

	t.Run("sec_node_id_prefix_chk_rejects_mismatched_kind", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-prefix', 'ENTITY', 'ACTIVE', '2026-01-01', 'test', 'SEED_LOAD', 'sec- declared ENTITY', 'test')`)
		assertSQLState(t, err, "23514", "sec_node_id_prefix_chk: sec- with ENTITY")
	})

	t.Run("sec_node_id_prefix_chk_rejects_unprefixed_id", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ('totally-unprefixed', 'ENTITY', 'ACTIVE', '2026-01-01', 'test', 'SEED_LOAD', 'unprefixed id', 'test')`)
		assertSQLState(t, err, "23514", "sec_node_id_prefix_chk: unprefixed")
	})

	t.Run("sec_node_status_fkey_rejects_illegal_pair", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-status-fk', 'SECURITY', 'DISSOLVED', '2026-01-01', 'test', 'SEED_LOAD', 'DISSOLVED is ENTITY-only', 'test')`)
		assertSQLState(t, err, "23503", "sec_node_status_fkey: SECURITY+DISSOLVED")
	})

	t.Run("sec_node_status_fkey_accepts_legal_pair", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-status-ok', 'SECURITY', 'ACTIVE', '2026-01-01', 'test', 'SEED_LOAD', 'SECURITY+ACTIVE is legal', 'test')`)
		if err != nil {
			t.Fatalf("legal status pair must land: %v", err)
		}
	})

	t.Run("change_reason_code_fk_rejects_unknown_code_on_node", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, actor, change_reason_code, change_reason, source_system)
			VALUES ('em-t-reason-fk', 'ENTITY', 'ACTIVE', '2026-01-01', 'test', 'INVENTED_CODE', 'unknown code', 'test')`)
		assertSQLState(t, err, "23503", "change_reason_code FK on sec_node")
	})

	t.Run("change_reason_code_fk_rejects_unknown_code_on_edge", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, actor, change_reason_code, change_reason, source_system)
			VALUES ('sec-t-reason-efk', 'SECURITY', 'sec-t-reason-edst', 'SECURITY', 'HAS_UNDERLYING', '2026-01-01', 'test', 'INVENTED_CODE', 'unknown code', 'test')`)
		assertSQLState(t, err, "23503", "change_reason_code FK on sec_edge")
	})

	t.Run("sec_edge_weight_basis_chk_rejects_weight_without_basis", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, rel_weight, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-wt-nobasis', 'SECURITY', 'sec-t-wt-dst', 'SECURITY', 'HAS_UNDERLYING', 0.5, '2026-01-01', 'test', 'SEED_LOAD', 'weight without basis', 'test')`)
		assertSQLState(t, err, "23514", "sec_edge_weight_basis_chk")
	})

	t.Run("weight_basis_fk_rejects_unknown_basis", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, rel_weight, weight_basis, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-wt-badfk', 'SECURITY', 'sec-t-wt-dst2', 'SECURITY', 'HAS_UNDERLYING', 0.5, 'UNITS', '2026-01-01', 'test', 'SEED_LOAD', 'unknown basis', 'test')`)
		assertSQLState(t, err, "23503", "weight_basis FK")
	})

	t.Run("weight_with_legal_basis_lands", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, rel_weight, weight_basis, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-wt-ok', 'SECURITY', 'sec-t-wt-dst3', 'SECURITY', 'HAS_UNDERLYING', 0.5, 'VALUE', '2026-01-01', 'test', 'SEED_LOAD', 'weight with VALUE', 'test')`)
		if err != nil {
			t.Fatalf("weight with legal basis must land: %v", err)
		}
	})

	t.Run("sec_edge_run_id_fk_rejects_unknown_run", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, run_id, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-run-fk', 'SECURITY', 'sec-t-run-dst', 'SECURITY', 'HAS_UNDERLYING', 999999, '2026-01-01', 'test', 'SEED_LOAD', 'unknown run_id', 'test')`)
		assertSQLState(t, err, "23503", "sec_edge.run_id FK")
	})

	t.Run("sec_node_run_id_fk_rejects_unknown_run", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, run_id, `+secstoreSpine+`)
			VALUES ('em-t-run-fk', 'ENTITY', 'ACTIVE', '2026-01-01', 999999, 'test', 'SEED_LOAD', 'unknown run_id', 'test')`)
		assertSQLState(t, err, "23503", "sec_node.run_id FK")
	})

	t.Run("record_type_check_rejects_unknown_kind", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ('em-t-badkind', 'INVENTED', 'ACTIVE', '2026-01-01', 'test', 'SEED_LOAD', 'unknown record_type', 'test')`)
		assertSQLState(t, err, "23514", "unknown record_type")
	})

	t.Run("processing_version_check_rejects_negative", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, processing_version, `+secstoreSpine+`)
			VALUES ('em-t-neg-pv', 'ENTITY', 'ACTIVE', '2026-01-01', -1, 'test', 'SEED_LOAD', 'negative pv', 'test')`)
		assertSQLState(t, err, "23514", "processing_version >= 0")
	})

	t.Run("valid_from_finite_chk_rejects_negative_infinity", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ('em-t-neginf', 'ENTITY', 'ACTIVE', '-infinity', 'test', 'SEED_LOAD', 'neg-infinity valid_from', 'test')`)
		assertSQLState(t, err, "23514", "sec_node_valid_from_finite_chk")
	})

	t.Run("spine_not_null_actor", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, actor, change_reason_code, change_reason, source_system)
			VALUES ('em-t-null-actor', 'ENTITY', 'ACTIVE', '2026-01-01', NULL, 'SEED_LOAD', 'null actor', 'test')`)
		assertSQLState(t, err, "23502", "sec_node.actor NOT NULL")
	})

	t.Run("spine_not_null_change_reason_code", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, actor, change_reason_code, change_reason, source_system)
			VALUES ('em-t-null-crc', 'ENTITY', 'ACTIVE', '2026-01-01', 'test', NULL, 'null code', 'test')`)
		assertSQLState(t, err, "23502", "sec_node.change_reason_code NOT NULL")
	})

	t.Run("spine_not_null_change_reason", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, actor, change_reason_code, change_reason, source_system)
			VALUES ('em-t-null-cr', 'ENTITY', 'ACTIVE', '2026-01-01', 'test', 'SEED_LOAD', NULL, 'test')`)
		assertSQLState(t, err, "23502", "sec_node.change_reason NOT NULL")
	})

	t.Run("spine_not_null_source_system", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, actor, change_reason_code, change_reason, source_system)
			VALUES ('em-t-null-ss', 'ENTITY', 'ACTIVE', '2026-01-01', 'test', 'SEED_LOAD', 'null source', NULL)`)
		assertSQLState(t, err, "23502", "sec_node.source_system NOT NULL")
	})

	t.Run("family_check_rejects_unknown_family", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason)
			VALUES ('TEST_TYPE', 'invented_family', '{ENTITY}', '{ENTITY}', '1', 'draft', 'test', 'test')`)
		assertSQLState(t, err, "23514", "rel_type_vocabulary.family CHECK")
	})

	t.Run("cardinality_check_rejects_unknown_value", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason)
			VALUES ('TEST_CARD', 'composition', '{ENTITY}', '{ENTITY}', 'many', 'draft', 'test', 'test')`)
		assertSQLState(t, err, "23514", "rel_type_vocabulary.cardinality CHECK")
	})

	t.Run("maturity_check_rejects_unknown_value", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason)
			VALUES ('TEST_MAT', 'composition', '{ENTITY}', '{ENTITY}', '1', 'unknown', 'test', 'test')`)
		assertSQLState(t, err, "23514", "rel_type_vocabulary.maturity CHECK")
	})

	t.Run("cluster_key_null_element_check", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, cluster_key, maturity, description, change_reason)
			VALUES ('TEST_CK', 'composition', '{ENTITY}', '{ENTITY}', '1', '{ex_date,NULL}', 'draft', 'test', 'test')`)
		assertSQLState(t, err, "23514", "cluster_key NULL-element CHECK")
	})

	t.Run("cluster_key_empty_array_check", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, cluster_key, maturity, description, change_reason)
			VALUES ('TEST_CKE', 'composition', '{ENTITY}', '{ENTITY}', '1', '{}', 'draft', 'test', 'test')`)
		assertSQLState(t, err, "23514", "cluster_key empty array CHECK")
	})

	t.Run("sec_edge_edge_disc_shape_chk", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (edge_disc, src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			OVERRIDING SYSTEM VALUE
			VALUES ('not-valid-disc', 'sec-t-disc-chk', 'SECURITY', 'sec-t-disc-dst', 'SECURITY', 'HAS_UNDERLYING', '2026-01-01', 'test', 'SEED_LOAD', 'bad disc shape', 'test')`)
		assertSQLState(t, err, "P0001", "sec_edge guard rejects invalid edge_disc shape")
	})

	t.Run("sec_edge_valid_from_finite_chk", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-einf', 'SECURITY', 'sec-t-einf-dst', 'SECURITY', 'HAS_UNDERLYING', '-infinity', 'test', 'SEED_LOAD', 'neg-infinity edge', 'test')`)
		assertSQLState(t, err, "23514", "sec_edge_valid_from_finite_chk")
	})

	t.Run("vocabulary_immutability_update", func(t *testing.T) {
		vocabs := []string{
			"rel_type_vocabulary",
			"weight_basis_vocabulary",
			"change_reason_vocabulary",
			"concept_class_vocabulary",
			"node_status_vocabulary",
		}
		for _, v := range vocabs {
			t.Run(v, func(t *testing.T) {
				_, err := pool.Exec(ctx, fmt.Sprintf(
					"UPDATE %s SET description = 'tampered' WHERE true", v))
				assertSQLState(t, err, "P0001", v+" immutability trigger UPDATE")
			})
		}
	})

	t.Run("vocabulary_immutability_delete", func(t *testing.T) {
		// Tables with FK references from sec_node/sec_edge hit 23503 before the
		// trigger fires, so we split: FK-free tables must hit P0001 (the trigger
		// is their only defence); FK-referenced tables accept either.
		fkFree := []string{
			"weight_basis_vocabulary",
			"concept_class_vocabulary",
		}
		fkReferenced := []string{
			"rel_type_vocabulary",
			"change_reason_vocabulary",
			"node_status_vocabulary",
		}
		for _, v := range fkFree {
			t.Run(v, func(t *testing.T) {
				_, err := pool.Exec(ctx, fmt.Sprintf(
					"DELETE FROM %s WHERE false OR true", v))
				assertSQLState(t, err, "P0001", v+" immutability trigger DELETE")
			})
		}
		for _, v := range fkReferenced {
			t.Run(v, func(t *testing.T) {
				_, err := pool.Exec(ctx, fmt.Sprintf(
					"DELETE FROM %s WHERE false OR true", v))
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					if pgErr.Code != "P0001" && pgErr.Code != "23503" {
						t.Fatalf("DELETE on %s: got %s (%s), want P0001 (immutability) or 23503 (FK)", v, pgErr.Code, pgErr.Message)
					}
				} else if err == nil {
					t.Fatalf("DELETE on %s must be rejected", v)
				} else {
					t.Fatalf("DELETE on %s: unexpected error %v", v, err)
				}
			})
		}
	})

	t.Run("sec_node_edge_processing_version_check_on_edge", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, processing_version, valid_from, `+secstoreSpine+`)
			VALUES ('sec-t-negpv-e', 'SECURITY', 'sec-t-negpv-edst', 'SECURITY', 'HAS_UNDERLYING', -1, '2026-01-01', 'test', 'SEED_LOAD', 'negative pv on edge', 'test')`)
		assertSQLState(t, err, "23514", "sec_edge.processing_version >= 0")
	})

	t.Run("concept_class_maturity_check", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO concept_class_vocabulary (concept_class, maturity, description)
			VALUES ('test_class', 'unknown', 'test')`)
		assertSQLState(t, err, "23514", "concept_class_vocabulary.maturity CHECK")
	})
}

func TestSecStoreNotNullAndForeignKeyCompleteness(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// --- Vocabulary NOT NULL ---

	t.Run("rel_type_vocabulary_not_null", func(t *testing.T) {
		cases := []struct {
			col string
			sql string
		}{
			{"description", `INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason) VALUES ('TEST_NN_DESC', 'composition', '{ENTITY}', '{ENTITY}', '1', 'draft', NULL, 'test')`},
			{"src_kinds", `INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason) VALUES ('TEST_NN_SK', 'composition', NULL, '{ENTITY}', '1', 'draft', 'test', 'test')`},
			{"dst_kinds", `INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason) VALUES ('TEST_NN_DK', 'composition', '{ENTITY}', NULL, '1', 'draft', 'test', 'test')`},
			{"derived_only", `INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, derived_only, change_reason) VALUES ('TEST_NN_DO', 'composition', '{ENTITY}', '{ENTITY}', '1', 'draft', 'test', NULL, 'test')`},
			{"change_reason", `INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason) VALUES ('TEST_NN_CR', 'composition', '{ENTITY}', '{ENTITY}', '1', 'draft', 'test', NULL)`},
		}
		for _, tc := range cases {
			t.Run(tc.col, func(t *testing.T) {
				_, err := pool.Exec(ctx, tc.sql)
				assertSQLState(t, err, "23502", "rel_type_vocabulary."+tc.col+" NOT NULL")
			})
		}
	})

	t.Run("weight_basis_vocabulary_description_not_null", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO weight_basis_vocabulary (basis, description) VALUES ('TEST_BASIS', NULL)`)
		assertSQLState(t, err, "23502", "weight_basis_vocabulary.description NOT NULL")
	})

	t.Run("change_reason_vocabulary_not_null", func(t *testing.T) {
		cases := []struct {
			col string
			sql string
		}{
			{"description", `INSERT INTO change_reason_vocabulary (code, description) VALUES ('TEST_CRV_DESC', NULL)`},
			{"requires_approval", `INSERT INTO change_reason_vocabulary (code, description, requires_approval) VALUES ('TEST_CRV_RA', 'test', NULL)`},
		}
		for _, tc := range cases {
			t.Run(tc.col, func(t *testing.T) {
				_, err := pool.Exec(ctx, tc.sql)
				assertSQLState(t, err, "23502", "change_reason_vocabulary."+tc.col+" NOT NULL")
			})
		}
	})

	t.Run("concept_class_vocabulary_description_not_null", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO concept_class_vocabulary (concept_class, maturity, description) VALUES ('TEST_CCV', 'draft', NULL)`)
		assertSQLState(t, err, "23502", "concept_class_vocabulary.description NOT NULL")
	})

	t.Run("node_status_vocabulary_not_null", func(t *testing.T) {
		cases := []struct {
			col string
			sql string
		}{
			{"record_type", `INSERT INTO node_status_vocabulary (record_type, status, is_terminal, description) VALUES (NULL, 'TEST_ST', false, 'test')`},
			{"status", `INSERT INTO node_status_vocabulary (record_type, status, is_terminal, description) VALUES ('TEST_RT', NULL, false, 'test')`},
			{"is_terminal", `INSERT INTO node_status_vocabulary (record_type, status, is_terminal, description) VALUES ('TEST_RT2', 'TEST_ST2', NULL, 'test')`},
			{"description", `INSERT INTO node_status_vocabulary (record_type, status, is_terminal, description) VALUES ('TEST_RT3', 'TEST_ST3', false, NULL)`},
		}
		for _, tc := range cases {
			t.Run(tc.col, func(t *testing.T) {
				_, err := pool.Exec(ctx, tc.sql)
				assertSQLState(t, err, "23502", "node_status_vocabulary."+tc.col+" NOT NULL")
			})
		}
	})

	// --- Vocabulary FK run_id ---

	t.Run("vocabulary_run_id_fk", func(t *testing.T) {
		cases := []struct {
			table string
			sql   string
		}{
			{"rel_type_vocabulary", `INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason, run_id) VALUES ('TEST_FK_RUN', 'composition', '{ENTITY}', '{ENTITY}', '1', 'draft', 'test', 'test', 999999)`},
			{"weight_basis_vocabulary", `INSERT INTO weight_basis_vocabulary (basis, description, run_id) VALUES ('TEST_FK_RUN', 'test', 999999)`},
			{"change_reason_vocabulary", `INSERT INTO change_reason_vocabulary (code, description, run_id) VALUES ('TEST_FK_RUN', 'test', 999999)`},
			{"concept_class_vocabulary", `INSERT INTO concept_class_vocabulary (concept_class, maturity, description, run_id) VALUES ('TEST_FK_RUN', 'draft', 'test', 999999)`},
			{"node_status_vocabulary", `INSERT INTO node_status_vocabulary (record_type, status, is_terminal, description, run_id) VALUES ('TEST_FK_RT', 'TEST_FK_ST', false, 'test', 999999)`},
		}
		for _, tc := range cases {
			t.Run(tc.table, func(t *testing.T) {
				_, err := pool.Exec(ctx, tc.sql)
				assertSQLState(t, err, "23503", tc.table+".run_id FK")
			})
		}
	})

	// --- rel_type_vocabulary weight_basis FK ---

	t.Run("rel_type_vocabulary_weight_basis_fk", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO rel_type_vocabulary (rel_type, family, src_kinds, dst_kinds, cardinality, maturity, description, change_reason, weight_basis) VALUES ('TEST_WB_FK', 'composition', '{ENTITY}', '{ENTITY}', '1', 'draft', 'test', 'test', 'NONEXISTENT')`)
		assertSQLState(t, err, "23503", "rel_type_vocabulary.weight_basis FK")
	})

	// --- sec_node NOT NULL (non-PK, non-equivalent) ---

	t.Run("sec_node_status_not_null", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, valid_from, `+secstoreSpine+`)
			VALUES ('em-t-null-status', 'ENTITY', NULL, '2026-01-01', 'test', 'SEED_LOAD', 'null status', 'test')`)
		assertSQLState(t, err, "23502", "sec_node.status NOT NULL")
	})

	t.Run("sec_node_attrs_not_null", func(t *testing.T) {
		_, err := pool.Exec(ctx, `
			INSERT INTO sec_node (id, record_type, status, attrs, valid_from, `+secstoreSpine+`)
			VALUES ('em-t-null-attrs', 'ENTITY', 'ACTIVE', NULL, '2026-01-01', 'test', 'SEED_LOAD', 'null attrs', 'test')`)
		assertSQLState(t, err, "23502", "sec_node.attrs NOT NULL")
	})

	// --- sec_edge NOT NULL (non-PK, non-equivalent) ---

	t.Run("sec_edge_not_null", func(t *testing.T) {
		cases := []struct {
			col  string
			sql  string
			want []string // acceptable SQLSTATE codes
		}{
			{"src_kind", `INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, actor, change_reason_code, change_reason, source_system) VALUES ('sec-t-nn-sk', NULL, 'em-t-nn-dst', 'ENTITY', 'HAS_UNDERLYING', '2026-01-01', 'test', 'SEED_LOAD', 'null src_kind', 'test')`, []string{"23502", "P0001"}},
			{"dst_kind", `INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, actor, change_reason_code, change_reason, source_system) VALUES ('sec-t-nn-dk', 'SECURITY', 'em-t-nn-dst2', NULL, 'HAS_UNDERLYING', '2026-01-01', 'test', 'SEED_LOAD', 'null dst_kind', 'test')`, []string{"23502", "P0001"}},
			{"payload", `INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, payload, valid_from, actor, change_reason_code, change_reason, source_system) VALUES ('sec-t-nn-pl', 'SECURITY', 'em-t-nn-dst3', 'SECURITY', 'HAS_UNDERLYING', NULL, '2026-01-01', 'test', 'SEED_LOAD', 'null payload', 'test')`, []string{"23502"}},
			{"actor", `INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, actor, change_reason_code, change_reason, source_system) VALUES ('sec-t-nn-ac', 'SECURITY', 'em-t-nn-dst4', 'SECURITY', 'HAS_UNDERLYING', '2026-01-01', NULL, 'SEED_LOAD', 'null actor', 'test')`, []string{"23502"}},
			{"change_reason_code", `INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, actor, change_reason_code, change_reason, source_system) VALUES ('sec-t-nn-crc', 'SECURITY', 'em-t-nn-dst5', 'SECURITY', 'HAS_UNDERLYING', '2026-01-01', 'test', NULL, 'null crc', 'test')`, []string{"23502"}},
			{"change_reason", `INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, actor, change_reason_code, change_reason, source_system) VALUES ('sec-t-nn-cr', 'SECURITY', 'em-t-nn-dst6', 'SECURITY', 'HAS_UNDERLYING', '2026-01-01', 'test', 'SEED_LOAD', NULL, 'test')`, []string{"23502"}},
			{"source_system", `INSERT INTO sec_edge (src_id, src_kind, dst_id, dst_kind, rel_type, valid_from, actor, change_reason_code, change_reason, source_system) VALUES ('sec-t-nn-ss', 'SECURITY', 'em-t-nn-dst7', 'SECURITY', 'HAS_UNDERLYING', '2026-01-01', 'test', 'SEED_LOAD', 'null ss', NULL)`, []string{"23502"}},
		}
		for _, tc := range cases {
			t.Run(tc.col, func(t *testing.T) {
				_, err := pool.Exec(ctx, tc.sql)
				assertSQLStateOneOf(t, err, tc.want, "sec_edge."+tc.col+" NOT NULL")
			})
		}
	})
}

func assertSQLState(t *testing.T, err error, wantCode, desc string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		if err == nil {
			t.Fatalf("%s: insert succeeded, want SQLSTATE %s", desc, wantCode)
		}
		t.Fatalf("%s: non-PG error %v, want SQLSTATE %s", desc, err, wantCode)
	}
	if pgErr.Code != wantCode {
		t.Fatalf("%s: got SQLSTATE %s (%s), want %s", desc, pgErr.Code, pgErr.Message, wantCode)
	}
}

func assertSQLStateOneOf(t *testing.T, err error, wantCodes []string, desc string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		if err == nil {
			t.Fatalf("%s: insert succeeded, want SQLSTATE %v", desc, wantCodes)
		}
		t.Fatalf("%s: non-PG error %v, want SQLSTATE %v", desc, err, wantCodes)
	}
	for _, c := range wantCodes {
		if pgErr.Code == c {
			return
		}
	}
	t.Fatalf("%s: got SQLSTATE %s (%s), want one of %v", desc, pgErr.Code, pgErr.Message, wantCodes)
}
