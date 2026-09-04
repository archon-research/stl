package migrator

// SplitStatements exposes splitStatements to this package's external tests, so a test modelling a client
// that submits a migration one statement at a time uses the splitter the migrator itself uses rather than
// a second, weaker copy. The production one handles dollar-quoted bodies, single-quoted literals carrying
// ";" or "--", and block comments. A hand-rolled Split(";") tears this region's -- comments at the ";"
// inside them, and shears the WHERE off the rebuild INSERT whenever such a comment sits between its FROM
// and its WHERE -- one did, until it was shortened.
var SplitStatements = splitStatements
