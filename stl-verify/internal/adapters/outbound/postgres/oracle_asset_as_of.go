package postgres

// OracleAssetAsOf builds the pinned oracle_asset read: a derived table holding the version of
// each natural key (oracle_id, token_id, feed_key) that was effective at a given instant.
//
// Which oracle_asset version a reader resolved decides which prices it used, so the instant is
// the caller's parameter, never now() — a replay has to be able to supply the instant the
// original read used. effectiveAtParam is the placeholder to bind it through ("$2"), so the
// fragment drops into a query that already numbers its own parameters.
//
// Disabled versions are returned too, so a caller filtering on `enabled` can still tell
// "retired then" from "never registered".
func OracleAssetAsOf(effectiveAtParam string) string {
	return `(
		SELECT DISTINCT ON (oracle_id, token_id, feed_key) *
		FROM oracle_asset
		WHERE valid_from <= ` + effectiveAtParam + `
		ORDER BY oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC
	)`
}
