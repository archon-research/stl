-- VEC-786: correct the sec_edge table COMMENT to reflect that endpoint-kind legality is
-- guard-enforced (GQ-11 via sec_store_append_guard), not loader/validator-enforced.

COMMENT ON TABLE sec_edge IS '[Dimension] Directed, typed, weighted relationship store (ADR-0007 §3/§5). Append-only (full ACL revoke incl. owner — nothing FKs this table); close-and-open at processing_version 0 (valid_to is NOT NULL, ''infinity'' when open, and in the PK); retraction is a tombstone append with a zero-length window. Endpoint-kind legality vs rel_type_vocabulary is guard-enforced (GQ-11, sec_store_append_guard reads the vocabulary row for cluster_key and checks the (rel_type, src_kind, dst_kind) triple as a predicate on that same read); single-valued cardinality is a DQ check over current state, never a write trigger. Inverses and closures are derived, never stored. Plain table: governance-rate writes — block-stamped projection types (ALLOCATES) are excluded by design and would need their own hypertable store if ratified.';

INSERT INTO migrations (filename) VALUES ('20260914_120000_secstore_edge_comment_endpoint_kind_guard.sql') ON CONFLICT (filename) DO NOTHING;
