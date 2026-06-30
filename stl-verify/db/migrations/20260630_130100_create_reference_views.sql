-- Reference catalog views: read-only convenience over the reference tables.
-- Flattens each reference hierarchy into one browsable surface.
-- Depends on 20260630_130000_create_reference_tables.sql
--   (asset_class_ref, security_type_ref, security_subtype_ref, sector_ref, industry_group_ref).
SET search_path TO public;

-- security_classification: asset_class -> security_type -> security_subtype.
-- security_subtype is NULL where the type does not subdivide (honest, no sentinel rows);
-- the NULL is resolved by a waterfall: leaf_code = most-specific available code (subtype else type),
-- and leaf_level marks which level it resolved to (SUBTYPE / TYPE).
DROP VIEW IF EXISTS public.security_classification;
CREATE VIEW public.security_classification AS
SELECT ac.asset_class,
       ac.category AS asset_category,
       st.security_type,
       ss.security_subtype,
       coalesce(ss.security_subtype, st.security_type)                          AS leaf_code,
       CASE WHEN ss.security_subtype IS NOT NULL THEN 'SUBTYPE' ELSE 'TYPE' END AS leaf_level,
       concat_ws(' / ', ac.asset_class, st.security_type, ss.security_subtype)  AS classification_path,
       coalesce(ss.description, st.description)                                 AS description
FROM asset_class_ref ac
JOIN security_type_ref st USING (asset_class)
LEFT JOIN security_subtype_ref ss USING (asset_class, security_type);

-- sector_classification: sector -> industry_group (GICS L1 -> L2).
-- industry_group is NULL for house sectors that do not subdivide (GOVERNMENT, DIGITAL_ASSETS).
CREATE OR REPLACE VIEW public.sector_classification AS
SELECT s.sector,
       s.gics_code  AS sector_gics_code,
       ig.industry_group,
       ig.gics_code AS industry_group_gics_code,
       concat_ws(' / ', s.sector, ig.industry_group) AS classification_path,
       coalesce(ig.description, s.description)        AS description
FROM sector_ref s
LEFT JOIN industry_group_ref ig USING (sector);

COMMENT ON VIEW public.security_classification IS 'Flattened security classification catalog: asset_class -> security_type -> security_subtype. security_subtype is NULL where the type does not subdivide; leaf_code waterfalls to the most-specific available code (subtype else type) and leaf_level marks which (SUBTYPE/TYPE). Read-only convenience over the reference tables.';
COMMENT ON VIEW public.sector_classification IS 'Flattened issuer-sector catalog: sector -> industry_group (industry_group NULL for house sectors that do not subdivide). Read-only convenience over the reference tables.';

SET search_path TO public;
INSERT INTO migrations (filename) VALUES ('20260630_130100_create_reference_views.sql') ON CONFLICT (filename) DO NOTHING;
