package migrator

import (
	"regexp"
	"strings"
)

// stripTimescaleDBSyntax removes TimescaleDB-specific DDL from SQL so migrations
// run on vanilla PostgreSQL. Dollar-quoted bodies and comments are preserved.
func stripTimescaleDBSyntax(sql string) string {
	segments := splitDollarQuotedRegions(sql)
	var out strings.Builder
	for _, seg := range segments {
		if !seg.isCode {
			out.WriteString(seg.text)
			continue
		}
		cleaned := stripTSDBFromCode(seg.text)
		out.WriteString(cleaned)
	}
	return out.String()
}

type segment struct {
	text   string
	isCode bool // false = dollar-quoted body, do not modify
}

// splitDollarQuotedRegions splits SQL into code regions and dollar-quoted
// literal regions. Only dollar-quoted bodies are protected; single-quoted strings
// and comments within code regions are handled by the regex patterns themselves.
func splitDollarQuotedRegions(sql string) []segment {
	var segs []segment
	var cur strings.Builder

	i := 0
	for i < len(sql) {
		if sql[i] == '$' {
			if tag, ok := matchDollarTag(sql, i); ok {
				if cur.Len() > 0 {
					segs = append(segs, segment{cur.String(), true})
					cur.Reset()
				}
				end := strings.Index(sql[i+len(tag):], tag)
				if end < 0 {
					segs = append(segs, segment{sql[i:], false})
					return segs
				}
				end = i + len(tag) + end + len(tag)
				segs = append(segs, segment{sql[i:end], false})
				i = end
				continue
			}
		}
		cur.WriteByte(sql[i])
		i++
	}
	if cur.Len() > 0 {
		segs = append(segs, segment{cur.String(), true})
	}
	return segs
}

var (
	// SET [LOCAL] timescaledb.<param> = <value>;
	reSetTimescaleDB = regexp.MustCompile(`(?im)^[ \t]*SET\s+(?:LOCAL\s+)?timescaledb\.\S+\s*=\s*[^;]*;\s*\n?`)

	// ALTER TABLE [schema.]name SET ( ... );  where params may span lines.
	reAlterTableSet = regexp.MustCompile(`(?ims)ALTER\s+TABLE\s+\S+\s+SET\s*\([^)]*\)\s*;`)

	// ) WITH used to find the start of a WITH clause in CREATE TABLE.
	reWithStart = regexp.MustCompile(`(?ims)\)\s*WITH\s*\(`)

	// Function-level SET timescaledb.<param> = <value> (before AS $tag$).
	// Matches through the value (quoted or unquoted) but not a trailing AS keyword,
	// so `SET timescaledb.x = 'on' AS $fn$` keeps the ` AS` on the previous line.
	reFuncSetTimescaleDB = regexp.MustCompile(`(?m)\n[ \t]+SET\s+timescaledb\.\S+\s*=\s*(?:'[^']*'|\S+)`)
)

func stripTSDBFromCode(code string) string {
	code = stripSetStatements(code)
	code = stripAlterTableSetTimescaleDB(code)
	code = stripWithClauseTSDBParams(code)
	code = stripFunctionSetTimescaleDB(code)
	return code
}

func stripSetStatements(code string) string {
	return reSetTimescaleDB.ReplaceAllString(code, "")
}

func stripAlterTableSetTimescaleDB(code string) string {
	return reAlterTableSet.ReplaceAllStringFunc(code, func(match string) string {
		parts := reAlterTableSet.FindStringSubmatch(match)
		if parts == nil {
			return match
		}
		// Extract the param block between ( and ).
		lparen := strings.Index(match, "(")
		rparen := strings.LastIndex(match, ")")
		if lparen < 0 || rparen < 0 {
			return match
		}
		paramBlock := match[lparen+1 : rparen]
		if !containsTSDBParam(paramBlock) {
			return match
		}
		remaining := filterOutTSDBParams(paramBlock)
		if remaining == "" {
			return ""
		}
		prefix := match[:lparen+1]
		return prefix + "\n" + remaining + "\n);"
	})
}

// stripWithClauseTSDBParams handles ) WITH ( ... ) in CREATE TABLE.
// Uses a manual parser to handle `)` inside comments within the WITH clause.
func stripWithClauseTSDBParams(code string) string {
	var out strings.Builder
	i := 0

	for {
		loc := reWithStart.FindStringIndex(code[i:])
		if loc == nil {
			out.WriteString(code[i:])
			break
		}

		matchStart := i + loc[0]
		matchEnd := i + loc[1] // position right after the opening (

		// Find the balanced closing ) for the WITH clause, skipping
		// parens inside line comments and single-quoted strings.
		closeIdx := findBalancedClose(code, matchEnd)
		if closeIdx < 0 {
			out.WriteString(code[i:])
			break
		}

		paramBlock := code[matchEnd:closeIdx]
		if !containsTSDBParam(paramBlock) {
			out.WriteString(code[i : closeIdx+1])
			i = closeIdx + 1
			continue
		}

		// Write everything up to the ) that precedes WITH.
		out.WriteString(code[i:matchStart])

		remaining := filterOutTSDBParams(paramBlock)
		if remaining == "" {
			out.WriteString(")")
		} else {
			out.WriteString(") WITH (\n" + remaining + "\n)")
		}

		i = closeIdx + 1
	}

	return out.String()
}

// findBalancedClose finds the index of the closing ) that balances the opening (
// at code[start-1], skipping parens inside line comments and single-quoted strings.
func findBalancedClose(code string, start int) int {
	depth := 1
	inQuote := false
	i := start
	for i < len(code) && depth > 0 {
		switch {
		case inQuote:
			if code[i] == '\'' {
				if i+1 < len(code) && code[i+1] == '\'' {
					i += 2
					continue
				}
				inQuote = false
			}
			i++
		case code[i] == '-' && i+1 < len(code) && code[i+1] == '-':
			// Skip to end of line.
			for i < len(code) && code[i] != '\n' {
				i++
			}
		case code[i] == '\'':
			inQuote = true
			i++
		case code[i] == '(':
			depth++
			i++
		case code[i] == ')':
			depth--
			if depth == 0 {
				return i
			}
			i++
		default:
			i++
		}
	}
	return -1
}

func stripFunctionSetTimescaleDB(code string) string {
	return reFuncSetTimescaleDB.ReplaceAllString(code, "")
}

func containsTSDBParam(paramBlock string) bool {
	for _, param := range splitParams(paramBlock) {
		if isTSDBParam(param) {
			return true
		}
	}
	return false
}

func filterOutTSDBParams(paramBlock string) string {
	var kept []string
	for _, param := range splitParams(paramBlock) {
		if isTSDBParam(param) {
			continue
		}
		kept = append(kept, "    "+param)
	}
	if len(kept) == 0 {
		return ""
	}
	return strings.Join(kept, ",\n")
}

// splitParams splits a comma-separated param block (possibly multi-line) into
// individual trimmed parameter entries. Handles values containing commas inside
// single-quoted strings, and strips line comments before splitting.
func splitParams(block string) []string {
	// Strip line comments first so commas inside comments don't cause mis-splits.
	var cleaned strings.Builder
	inQuote := false
	for i := 0; i < len(block); i++ {
		switch {
		case block[i] == '\'' && !inQuote:
			inQuote = true
			cleaned.WriteByte(block[i])
		case block[i] == '\'' && inQuote:
			if i+1 < len(block) && block[i+1] == '\'' {
				cleaned.WriteString("''")
				i++
			} else {
				inQuote = false
				cleaned.WriteByte(block[i])
			}
		case block[i] == '-' && i+1 < len(block) && block[i+1] == '-' && !inQuote:
			for i < len(block) && block[i] != '\n' {
				i++
			}
			if i < len(block) {
				cleaned.WriteByte('\n')
			}
		default:
			cleaned.WriteByte(block[i])
		}
	}

	var params []string
	var cur strings.Builder
	inQuote = false
	s := cleaned.String()

	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\'' && !inQuote:
			inQuote = true
			cur.WriteByte(s[i])
		case s[i] == '\'' && inQuote:
			if i+1 < len(s) && s[i+1] == '\'' {
				cur.WriteString("''")
				i++
			} else {
				inQuote = false
				cur.WriteByte(s[i])
			}
		case s[i] == ',' && !inQuote:
			p := strings.TrimSpace(cur.String())
			if p != "" {
				params = append(params, p)
			}
			cur.Reset()
		default:
			cur.WriteByte(s[i])
		}
	}
	p := strings.TrimSpace(cur.String())
	if p != "" {
		params = append(params, p)
	}
	return params
}

func isTSDBParam(param string) bool {
	lower := strings.ToLower(param)
	return strings.HasPrefix(lower, "tsdb.") || strings.HasPrefix(lower, "timescaledb.")
}
