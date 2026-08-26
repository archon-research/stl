package migrator

// SplitStatements exposes splitStatements to this package's external tests, so a test modelling a client
// that submits a migration one statement at a time uses the splitter the migrator itself uses rather than
// a second, weaker copy. The production one handles dollar-quoted bodies, single-quoted literals carrying
// ";" or "--", and block comments; a hand-rolled Split(";") shears the WHERE off a rebuild INSERT.
var SplitStatements = splitStatements
