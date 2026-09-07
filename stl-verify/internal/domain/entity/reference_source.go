package entity

// ReferenceDataSource is the provenance slug shared by every row STL records
// from Sky, whichever feed reported it.
//
// One slug on purpose: downstream these are a single provenance — the API
// serves them all as source="reference" — and the table a row sits in already
// says which feed produced it. A per-feed slug would split a provenance that
// consumers are told is one thing.
const ReferenceDataSource = "skyeco:reference"
