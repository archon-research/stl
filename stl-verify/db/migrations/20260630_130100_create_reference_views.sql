-- Reference catalog views: read-only convenience over the reference tables.
-- Flattens each reference hierarchy into one browsable surface.
-- Depends on 20260630_130000_create_reference_tables.sql
--   (asset_class_ref, security_type_ref, security_subtype_ref, sector_ref, industry_group_ref).

-- security_classification: enumerates every classifiable node in the hierarchy so a security
-- can be looked up at whichever granularity it was classified to. Each security_type yields a
-- TYPE-level row (security_subtype NULL); each security_subtype additionally yields a SUBTYPE-level
-- row. So a type that has subtypes appears both at TYPE level (e.g. a vanilla CORPORATE_BOND with
-- no subtype) and once per subtype. leaf_level marks the granularity of the row (TYPE / SUBTYPE);
-- leaf_code is the code at that level (display-only, not unique - see the column comment).
DROP VIEW IF EXISTS security_classification;
CREATE VIEW security_classification AS
SELECT ac.asset_class,
       ac.category                                          AS asset_category,
       st.security_type,
       NULL::text                                           AS security_subtype,
       st.security_type                                     AS leaf_code,
       'TYPE'                                               AS leaf_level,
       concat_ws(' / ', ac.asset_class, st.security_type)   AS classification_path,
       st.description                                       AS description
FROM asset_class_ref ac
JOIN security_type_ref st USING (asset_class)
UNION ALL
SELECT ac.asset_class,
       ac.category                                                             AS asset_category,
       st.security_type,
       ss.security_subtype,
       ss.security_subtype                                                     AS leaf_code,
       'SUBTYPE'                                                               AS leaf_level,
       concat_ws(' / ', ac.asset_class, st.security_type, ss.security_subtype) AS classification_path,
       ss.description                                                          AS description
FROM asset_class_ref ac
JOIN security_type_ref st USING (asset_class)
JOIN security_subtype_ref ss USING (asset_class, security_type);

-- sector_classification: sector -> industry_group (GICS L1 -> L2).
-- industry_group is NULL for house sectors that do not subdivide (GOVERNMENT, DIGITAL_ASSETS).
CREATE OR REPLACE VIEW sector_classification AS
SELECT s.sector,
       s.gics_code  AS sector_gics_code,
       ig.industry_group,
       ig.gics_code AS industry_group_gics_code,
       concat_ws(' / ', s.sector, ig.industry_group) AS classification_path,
       coalesce(ig.description, s.description)        AS description
FROM sector_ref s
LEFT JOIN industry_group_ref ig USING (sector);

COMMENT ON VIEW security_classification IS 'Security classification catalog: every classifiable node, so a security resolves at whichever granularity it was classified to. Each security_type yields a TYPE-level row (security_subtype NULL); each security_subtype additionally yields a SUBTYPE-level row. leaf_level marks the granularity (TYPE/SUBTYPE); leaf_code is the code at that level. Read-only convenience over the reference tables. NOTE: leaf_code is display-only and NOT unique - the OTHER/UNKNOWN sentinels repeat once per asset_class; the identifying key is (asset_class, security_type, security_subtype). Do not join on leaf_code.';
COMMENT ON COLUMN security_classification.leaf_code IS 'Display-only most-specific code (subtype else type). NOT unique: OTHER/UNKNOWN repeat across asset classes. Key on (asset_class, security_type, security_subtype) instead.';
COMMENT ON VIEW sector_classification IS 'Flattened issuer-sector catalog: sector -> industry_group (industry_group NULL for house sectors that do not subdivide). Read-only convenience over the reference tables.';

INSERT INTO migrations (filename) VALUES ('20260630_130100_create_reference_views.sql') ON CONFLICT (filename) DO NOTHING;
