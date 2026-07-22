-- VEC-522: rename the 13 controlled-vocabulary reference tables from the `<x>_ref` suffix to a
-- `ref_<x>` prefix, so they group together in the schema and are easy to find in \dt / a client.
-- The two hierarchies carry an explicit level suffix (naming convention agreed with Peter): the
-- GICS sector hierarchy (ref_sector_l1 -> ref_industry_group_l2) and the security-classification
-- hierarchy (ref_asset_class_l1 -> ref_security_type_l2 -> ref_security_subtype_l3). The other 8 are flat.
--
-- Rename only; no schema or data change. ALTER TABLE ... RENAME preserves every dependent object by
-- OID, so nothing else is recreated:
--   * the 18 FK constraints into the reference layer keep working -- security_master (6),
--     entity_master (6), position_classification, position_entity_link, and the inter-ref hierarchy
--     (security_type->asset_class, security_subtype->security_type, industry_group->sector,
--     country->currency);
--   * the per-table BEFORE UPDATE OR DELETE `<x>_ref_immutable` triggers stay attached -- the
--     append-only guard reads TG_TABLE_NAME, so it now reports the new table name;
--   * the owner UPDATE grants that let the FK RI row-lock probe run (20260714_160000) are preserved;
--   * the security_classification and sector_classification views (20260630_130100) follow the rename.
--
-- Dependent object identifiers (FK/PK/index/trigger names) intentionally keep their historical
-- `<x>_ref` form. They follow the table by OID and are not part of what this change makes
-- discoverable; renaming them would be churn for no functional gain.

ALTER TABLE asset_class_ref            RENAME TO ref_asset_class_l1;
ALTER TABLE corporate_action_type_ref  RENAME TO ref_corporate_action_type;
ALTER TABLE counterparty_role_ref      RENAME TO ref_counterparty_role;
ALTER TABLE country_ref                RENAME TO ref_country;
ALTER TABLE credit_rating_ref          RENAME TO ref_credit_rating;
ALTER TABLE currency_ref               RENAME TO ref_currency;
ALTER TABLE deal_type_ref              RENAME TO ref_deal_type;
ALTER TABLE entity_type_ref            RENAME TO ref_entity_type;
ALTER TABLE industry_group_ref         RENAME TO ref_industry_group_l2;
ALTER TABLE origination_type_ref       RENAME TO ref_origination_type;
ALTER TABLE sector_ref                 RENAME TO ref_sector_l1;
ALTER TABLE security_subtype_ref       RENAME TO ref_security_subtype_l3;
ALTER TABLE security_type_ref          RENAME TO ref_security_type_l2;

INSERT INTO migrations (filename) VALUES ('20260722_120000_rename_reference_tables_ref_prefix.sql') ON CONFLICT (filename) DO NOTHING;
