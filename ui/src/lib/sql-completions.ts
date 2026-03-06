import type { languages, editor, IRange } from "monaco-editor"
import type { MockTableInfo } from "./mock-data"

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

export interface SchemaInfo {
  catalogs: {
    name: string
    tables: {
      name: string
      columns: { name: string; type: string }[]
    }[]
  }[]
}

export function buildSchemaFromMockData(
  catalogTables: Record<string, Record<string, MockTableInfo>>,
): SchemaInfo {
  const catalogs = Object.entries(catalogTables).map(([catName, tables]) => ({
    name: catName,
    tables: Object.entries(tables).map(([tableName, info]) => ({
      name: tableName,
      columns: info.schema.map((col) => ({ name: col.name, type: col.type })),
    })),
  }))
  return { catalogs }
}

// ---------------------------------------------------------------------------
// DataFusion SQL keywords (comprehensive)
// ---------------------------------------------------------------------------

const SQL_KEYWORDS = [
  // DML
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
  "LIKE", "ILIKE", "SIMILAR", "TO", "BETWEEN", "EXISTS", "CASE", "WHEN",
  "THEN", "ELSE", "END", "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER",
  "OUTER", "FULL", "CROSS", "NATURAL", "USING", "GROUP", "BY", "ORDER",
  "ASC", "DESC", "NULLS", "FIRST", "LAST", "HAVING", "LIMIT", "OFFSET",
  "UNION", "ALL", "INTERSECT", "EXCEPT", "DISTINCT", "INSERT", "INTO",
  "VALUES", "UPDATE", "SET", "DELETE", "OVERWRITE",
  // DDL
  "CREATE", "TABLE", "DROP", "ALTER", "ADD", "COLUMN", "INDEX", "VIEW",
  "IF", "NOT", "EXTERNAL", "LOCATION", "STORED", "PARTITIONED", "OPTIONS",
  "TBLPROPERTIES", "REPLACE",
  // CTE / subquery
  "WITH", "RECURSIVE",
  // Window
  "OVER", "PARTITION", "WINDOW", "ROWS", "RANGE", "GROUPS", "UNBOUNDED",
  "PRECEDING", "FOLLOWING", "CURRENT", "ROW", "EXCLUDE", "TIES", "NO", "OTHERS",
  "FILTER",
  // Set operations
  "LATERAL", "UNNEST", "TABLESAMPLE",
  // Types / cast
  "CAST", "TRY_CAST", "SAFE_CAST", "BOOLEAN", "INT", "INTEGER", "BIGINT",
  "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
  "VARCHAR", "CHAR", "STRING", "TEXT", "DATE", "TIME", "TIMESTAMP",
  "INTERVAL", "BLOB", "BYTEA", "BINARY",
  // Boolean literals
  "TRUE", "FALSE",
  // Misc
  "EXPLAIN", "ANALYZE", "DESCRIBE", "SHOW", "TABLES", "COLUMNS",
  "COPY", "FORMAT", "CSV", "PARQUET", "JSON", "AVRO", "ARROW",
  "HEADER", "DELIMITER", "HAS_HEADER",
  // GROUPING
  "GROUPING", "SETS", "CUBE", "ROLLUP",
]

// ---------------------------------------------------------------------------
// DataFusion functions — comprehensive catalog
// ---------------------------------------------------------------------------

interface FnInfo {
  name: string
  signature: string
  detail: string
  category: string
}

function fn(name: string, signature: string, detail: string, category: string): FnInfo {
  return { name, signature, detail, category }
}

const DATAFUSION_FUNCTIONS: FnInfo[] = [
  // ── Aggregate ──────────────────────────────────────────────────────────
  fn("COUNT", "expr", "Count of non-null values", "aggregate"),
  fn("SUM", "expr", "Sum of values", "aggregate"),
  fn("AVG", "expr", "Average of values", "aggregate"),
  fn("MIN", "expr", "Minimum value", "aggregate"),
  fn("MAX", "expr", "Maximum value", "aggregate"),
  fn("MEDIAN", "expr", "Median value", "aggregate"),
  fn("MEAN", "expr", "Alias for AVG", "aggregate"),
  fn("ARRAY_AGG", "expr", "Collect values into an array", "aggregate"),
  fn("STRING_AGG", "expr, delimiter", "Concatenate strings with delimiter", "aggregate"),
  fn("BOOL_AND", "expr", "True if all values are true", "aggregate"),
  fn("BOOL_OR", "expr", "True if any value is true", "aggregate"),
  fn("BIT_AND", "expr", "Bitwise AND of all values", "aggregate"),
  fn("BIT_OR", "expr", "Bitwise OR of all values", "aggregate"),
  fn("BIT_XOR", "expr", "Bitwise XOR of all values", "aggregate"),
  fn("FIRST_VALUE", "expr", "First value in group", "aggregate"),
  fn("LAST_VALUE", "expr", "Last value in group", "aggregate"),
  fn("VAR", "expr", "Sample variance", "aggregate"),
  fn("VAR_POP", "expr", "Population variance", "aggregate"),
  fn("VAR_SAMP", "expr", "Sample variance", "aggregate"),
  fn("STDDEV", "expr", "Sample standard deviation", "aggregate"),
  fn("STDDEV_POP", "expr", "Population standard deviation", "aggregate"),
  fn("STDDEV_SAMP", "expr", "Sample standard deviation", "aggregate"),
  fn("COVAR", "y, x", "Sample covariance", "aggregate"),
  fn("COVAR_POP", "y, x", "Population covariance", "aggregate"),
  fn("COVAR_SAMP", "y, x", "Sample covariance", "aggregate"),
  fn("CORR", "y, x", "Correlation coefficient", "aggregate"),
  fn("PERCENTILE_CONT", "fraction", "Continuous percentile", "aggregate"),
  fn("APPROX_DISTINCT", "expr", "Approximate count distinct (HLL)", "aggregate"),
  fn("APPROX_MEDIAN", "expr", "Approximate median (t-digest)", "aggregate"),
  fn("APPROX_PERCENTILE_CONT", "expr, p", "Approximate percentile (t-digest)", "aggregate"),
  fn("APPROX_PERCENTILE_CONT_WITH_WEIGHT", "expr, weight, p", "Weighted approximate percentile", "aggregate"),
  fn("REGR_SLOPE", "y, x", "Slope of linear regression", "aggregate"),
  fn("REGR_INTERCEPT", "y, x", "Intercept of linear regression", "aggregate"),
  fn("REGR_COUNT", "y, x", "Count of non-null pairs", "aggregate"),
  fn("REGR_R2", "y, x", "R-squared of linear regression", "aggregate"),
  fn("REGR_AVGX", "y, x", "Average of independent variable", "aggregate"),
  fn("REGR_AVGY", "y, x", "Average of dependent variable", "aggregate"),
  fn("REGR_SXX", "y, x", "Sum of squares of X", "aggregate"),
  fn("REGR_SXY", "y, x", "Sum of products of X and Y", "aggregate"),
  fn("REGR_SYY", "y, x", "Sum of squares of Y", "aggregate"),
  fn("GROUPING", "expr", "Grouping set bitmask", "aggregate"),

  // ── Window ─────────────────────────────────────────────────────────────
  fn("ROW_NUMBER", "", "Sequential row number", "window"),
  fn("RANK", "", "Rank with gaps", "window"),
  fn("DENSE_RANK", "", "Rank without gaps", "window"),
  fn("PERCENT_RANK", "", "Relative rank (0..1)", "window"),
  fn("CUME_DIST", "", "Cumulative distribution", "window"),
  fn("NTILE", "n", "Divide rows into n buckets", "window"),
  fn("LAG", "expr [, offset [, default]]", "Value from previous row", "window"),
  fn("LEAD", "expr [, offset [, default]]", "Value from next row", "window"),
  fn("NTH_VALUE", "expr, n", "Nth value in window frame", "window"),

  // ── Math ───────────────────────────────────────────────────────────────
  fn("ABS", "x", "Absolute value", "math"),
  fn("ACOS", "x", "Inverse cosine", "math"),
  fn("ACOSH", "x", "Inverse hyperbolic cosine", "math"),
  fn("ASIN", "x", "Inverse sine", "math"),
  fn("ASINH", "x", "Inverse hyperbolic sine", "math"),
  fn("ATAN", "x", "Inverse tangent", "math"),
  fn("ATAN2", "y, x", "Two-argument inverse tangent", "math"),
  fn("ATANH", "x", "Inverse hyperbolic tangent", "math"),
  fn("CBRT", "x", "Cube root", "math"),
  fn("CEIL", "x", "Round up to nearest integer", "math"),
  fn("COS", "x", "Cosine", "math"),
  fn("COSH", "x", "Hyperbolic cosine", "math"),
  fn("COT", "x", "Cotangent", "math"),
  fn("DEGREES", "x", "Radians to degrees", "math"),
  fn("EXP", "x", "e raised to x", "math"),
  fn("FACTORIAL", "n", "Factorial", "math"),
  fn("FLOOR", "x", "Round down to nearest integer", "math"),
  fn("GCD", "a, b", "Greatest common divisor", "math"),
  fn("ISNAN", "x", "True if NaN", "math"),
  fn("ISZERO", "x", "True if zero", "math"),
  fn("LCM", "a, b", "Least common multiple", "math"),
  fn("LN", "x", "Natural logarithm", "math"),
  fn("LOG", "base, x", "Logarithm with base", "math"),
  fn("LOG10", "x", "Base-10 logarithm", "math"),
  fn("LOG2", "x", "Base-2 logarithm", "math"),
  fn("MOD", "x, y", "Remainder", "math"),
  fn("NANVL", "x, y", "First non-NaN value", "math"),
  fn("PI", "", "Value of pi", "math"),
  fn("POWER", "base, exp", "Raise to power", "math"),
  fn("POW", "base, exp", "Alias for POWER", "math"),
  fn("RADIANS", "x", "Degrees to radians", "math"),
  fn("RANDOM", "", "Random value 0..1", "math"),
  fn("ROUND", "x [, decimals]", "Round to decimals", "math"),
  fn("SIGNUM", "x", "Sign of number (-1, 0, 1)", "math"),
  fn("SIN", "x", "Sine", "math"),
  fn("SINH", "x", "Hyperbolic sine", "math"),
  fn("SQRT", "x", "Square root", "math"),
  fn("TAN", "x", "Tangent", "math"),
  fn("TANH", "x", "Hyperbolic tangent", "math"),
  fn("TRUNC", "x [, decimals]", "Truncate to decimals", "math"),

  // ── String ─────────────────────────────────────────────────────────────
  fn("ASCII", "str", "ASCII code of first character", "string"),
  fn("BIT_LENGTH", "str", "Length in bits", "string"),
  fn("BTRIM", "str [, chars]", "Trim characters from both ends", "string"),
  fn("CHAR_LENGTH", "str", "Character length", "string"),
  fn("CHARACTER_LENGTH", "str", "Character length", "string"),
  fn("CHR", "code", "Character from ASCII code", "string"),
  fn("CONCAT", "str1, str2, ...", "Concatenate strings", "string"),
  fn("CONCAT_WS", "sep, str1, str2, ...", "Concatenate with separator", "string"),
  fn("CONTAINS", "str, substr", "True if string contains substring", "string"),
  fn("ENDS_WITH", "str, suffix", "True if string ends with suffix", "string"),
  fn("FIND_IN_SET", "str, list", "Index in comma-separated list", "string"),
  fn("INITCAP", "str", "Capitalize first letter of each word", "string"),
  fn("INSTR", "str, substr", "Position of substring", "string"),
  fn("LEFT", "str, n", "Leftmost n characters", "string"),
  fn("LENGTH", "str", "String length", "string"),
  fn("LEVENSHTEIN", "str1, str2", "Edit distance", "string"),
  fn("LOWER", "str", "Convert to lowercase", "string"),
  fn("LPAD", "str, len [, pad]", "Left-pad to length", "string"),
  fn("LTRIM", "str [, chars]", "Trim from left", "string"),
  fn("OCTET_LENGTH", "str", "Length in bytes", "string"),
  fn("OVERLAY", "str PLACING substr FROM pos [FOR len]", "Replace substring", "string"),
  fn("POSITION", "substr IN str", "Position of substring", "string"),
  fn("REPEAT", "str, n", "Repeat string n times", "string"),
  fn("REPLACE", "str, from, to", "Replace occurrences", "string"),
  fn("REVERSE", "str", "Reverse string", "string"),
  fn("RIGHT", "str, n", "Rightmost n characters", "string"),
  fn("RPAD", "str, len [, pad]", "Right-pad to length", "string"),
  fn("RTRIM", "str [, chars]", "Trim from right", "string"),
  fn("SPLIT_PART", "str, delim, index", "Extract part by delimiter", "string"),
  fn("STARTS_WITH", "str, prefix", "True if string starts with prefix", "string"),
  fn("STRPOS", "str, substr", "Position of substring (1-based)", "string"),
  fn("SUBSTR", "str, start [, len]", "Extract substring", "string"),
  fn("SUBSTRING", "str FROM start [FOR len]", "Extract substring", "string"),
  fn("SUBSTRING_INDEX", "str, delim, count", "Substring before Nth delimiter", "string"),
  fn("TO_HEX", "n", "Convert integer to hex string", "string"),
  fn("TRANSLATE", "str, from, to", "Character-by-character translation", "string"),
  fn("TRIM", "str", "Trim whitespace", "string"),
  fn("UPPER", "str", "Convert to uppercase", "string"),
  fn("UUID", "", "Generate random UUID", "string"),

  // ── Regex ──────────────────────────────────────────────────────────────
  fn("REGEXP_COUNT", "str, pattern", "Count regex matches", "regex"),
  fn("REGEXP_LIKE", "str, pattern", "True if regex matches", "regex"),
  fn("REGEXP_MATCH", "str, pattern", "Extract regex capture groups", "regex"),
  fn("REGEXP_REPLACE", "str, pattern, replacement", "Replace regex matches", "regex"),

  // ── Date/Time ──────────────────────────────────────────────────────────
  fn("CURRENT_DATE", "", "Current date", "datetime"),
  fn("CURRENT_TIME", "", "Current time", "datetime"),
  fn("CURRENT_TIMESTAMP", "", "Current timestamp", "datetime"),
  fn("NOW", "", "Current timestamp", "datetime"),
  fn("TODAY", "", "Current date", "datetime"),
  fn("DATE_BIN", "interval, source [, origin]", "Bin timestamps into intervals", "datetime"),
  fn("DATE_FORMAT", "ts, format", "Format timestamp", "datetime"),
  fn("DATE_PART", "part, ts", "Extract date/time component", "datetime"),
  fn("DATE_TRUNC", "part, ts", "Truncate to date/time precision", "datetime"),
  fn("DATEPART", "part, ts", "Alias for DATE_PART", "datetime"),
  fn("DATETRUNC", "part, ts", "Alias for DATE_TRUNC", "datetime"),
  fn("EXTRACT", "part FROM ts", "Extract date/time component", "datetime"),
  fn("FROM_UNIXTIME", "secs", "Unix timestamp to timestamp", "datetime"),
  fn("MAKE_DATE", "year, month, day", "Create date from parts", "datetime"),
  fn("MAKE_TIME", "hour, min, sec", "Create time from parts", "datetime"),
  fn("TO_CHAR", "ts, format", "Format timestamp as string", "datetime"),
  fn("TO_DATE", "str [, format]", "Parse string to date", "datetime"),
  fn("TO_LOCAL_TIME", "ts", "Convert to local time", "datetime"),
  fn("TO_TIME", "str [, format]", "Parse string to time", "datetime"),
  fn("TO_TIMESTAMP", "str [, format]", "Parse string to timestamp", "datetime"),
  fn("TO_TIMESTAMP_MICROS", "val", "Convert to timestamp (microseconds)", "datetime"),
  fn("TO_TIMESTAMP_MILLIS", "val", "Convert to timestamp (milliseconds)", "datetime"),
  fn("TO_TIMESTAMP_NANOS", "val", "Convert to timestamp (nanoseconds)", "datetime"),
  fn("TO_TIMESTAMP_SECONDS", "val", "Convert to timestamp (seconds)", "datetime"),
  fn("TO_UNIXTIME", "ts", "Timestamp to Unix seconds", "datetime"),

  // ── Conditional ────────────────────────────────────────────────────────
  fn("COALESCE", "expr1, expr2, ...", "First non-null value", "conditional"),
  fn("GREATEST", "expr1, expr2, ...", "Maximum of values", "conditional"),
  fn("LEAST", "expr1, expr2, ...", "Minimum of values", "conditional"),
  fn("NULLIF", "expr1, expr2", "Null if values are equal", "conditional"),
  fn("NVL", "expr, default", "Default if null", "conditional"),
  fn("NVL2", "expr, not_null, is_null", "Switch on null", "conditional"),
  fn("IFNULL", "expr, default", "Alias for NVL", "conditional"),

  // ── Array / List ───────────────────────────────────────────────────────
  fn("MAKE_ARRAY", "val1, val2, ...", "Create array from values", "array"),
  fn("MAKE_LIST", "val1, val2, ...", "Alias for MAKE_ARRAY", "array"),
  fn("ARRAY_APPEND", "arr, val", "Append value to array", "array"),
  fn("ARRAY_CAT", "arr1, arr2", "Concatenate arrays", "array"),
  fn("ARRAY_CONCAT", "arr1, arr2, ...", "Concatenate arrays", "array"),
  fn("ARRAY_CONTAINS", "arr, val", "True if array contains value", "array"),
  fn("ARRAY_DISTINCT", "arr", "Remove duplicates", "array"),
  fn("ARRAY_ELEMENT", "arr, index", "Get element at index", "array"),
  fn("ARRAY_EMPTY", "arr", "True if array is empty", "array"),
  fn("ARRAY_EXCEPT", "arr1, arr2", "Elements in arr1 not in arr2", "array"),
  fn("ARRAY_HAS", "arr, val", "True if array has value", "array"),
  fn("ARRAY_HAS_ALL", "arr, vals", "True if arr has all values", "array"),
  fn("ARRAY_HAS_ANY", "arr, vals", "True if arr has any value", "array"),
  fn("ARRAY_INTERSECT", "arr1, arr2", "Common elements", "array"),
  fn("ARRAY_JOIN", "arr, delim", "Join array as string", "array"),
  fn("ARRAY_LENGTH", "arr", "Array length", "array"),
  fn("ARRAY_MAX", "arr", "Maximum array element", "array"),
  fn("ARRAY_MIN", "arr", "Minimum array element", "array"),
  fn("ARRAY_NDIMS", "arr", "Number of dimensions", "array"),
  fn("ARRAY_POP_BACK", "arr", "Remove last element", "array"),
  fn("ARRAY_POP_FRONT", "arr", "Remove first element", "array"),
  fn("ARRAY_POSITION", "arr, val", "Index of value (1-based)", "array"),
  fn("ARRAY_POSITIONS", "arr, val", "All indices of value", "array"),
  fn("ARRAY_PREPEND", "val, arr", "Prepend value to array", "array"),
  fn("ARRAY_REMOVE", "arr, val", "Remove first occurrence", "array"),
  fn("ARRAY_REMOVE_ALL", "arr, val", "Remove all occurrences", "array"),
  fn("ARRAY_REPEAT", "val, n", "Repeat value n times", "array"),
  fn("ARRAY_REPLACE", "arr, from, to", "Replace first occurrence", "array"),
  fn("ARRAY_REPLACE_ALL", "arr, from, to", "Replace all occurrences", "array"),
  fn("ARRAY_RESIZE", "arr, size [, val]", "Resize array", "array"),
  fn("ARRAY_REVERSE", "arr", "Reverse array", "array"),
  fn("ARRAY_SLICE", "arr, start, end", "Extract sub-array", "array"),
  fn("ARRAY_SORT", "arr", "Sort array", "array"),
  fn("ARRAY_TO_STRING", "arr, delim", "Join array as string", "array"),
  fn("ARRAY_UNION", "arr1, arr2", "Union of arrays", "array"),
  fn("ARRAYS_OVERLAP", "arr1, arr2", "True if arrays share elements", "array"),
  fn("CARDINALITY", "arr", "Number of elements", "array"),
  fn("FLATTEN", "arr", "Flatten nested arrays", "array"),
  fn("GENERATE_SERIES", "start, stop [, step]", "Generate series of values", "array"),
  fn("RANGE", "start, stop [, step]", "Generate range of values", "array"),
  fn("STRING_TO_ARRAY", "str, delim", "Split string into array", "array"),
  fn("UNNEST", "arr", "Expand array to rows", "array"),

  // ── Struct / Map ───────────────────────────────────────────────────────
  fn("NAMED_STRUCT", "key1, val1, ...", "Create named struct", "struct"),
  fn("ROW", "val1, val2, ...", "Create anonymous struct", "struct"),
  fn("STRUCT", "val1, val2, ...", "Create struct", "struct"),
  fn("GET_FIELD", "struct, field", "Access struct field", "struct"),
  fn("MAP", "keys, values", "Create map from arrays", "struct"),
  fn("MAP_KEYS", "map", "Extract keys from map", "struct"),
  fn("MAP_VALUES", "map", "Extract values from map", "struct"),
  fn("MAP_ENTRIES", "map", "Map as array of structs", "struct"),
  fn("MAP_EXTRACT", "map, key", "Get value by key", "struct"),
  fn("ELEMENT_AT", "map, key", "Get map element at key", "struct"),

  // ── Hash ───────────────────────────────────────────────────────────────
  fn("DIGEST", "data, algorithm", "Compute hash", "hash"),
  fn("MD5", "str", "MD5 hash as hex string", "hash"),
  fn("SHA224", "str", "SHA-224 hash", "hash"),
  fn("SHA256", "str", "SHA-256 hash", "hash"),
  fn("SHA384", "str", "SHA-384 hash", "hash"),
  fn("SHA512", "str", "SHA-512 hash", "hash"),

  // ── Encoding ───────────────────────────────────────────────────────────
  fn("ENCODE", "data, encoding", "Encode binary data", "encoding"),
  fn("DECODE", "str, encoding", "Decode to binary", "encoding"),

  // ── Arrow / Type ───────────────────────────────────────────────────────
  fn("ARROW_CAST", "expr, type_str", "Cast to Arrow type by name", "type"),
  fn("ARROW_TYPEOF", "expr", "Return Arrow type name", "type"),
  fn("ARROW_METADATA", "expr", "Return Arrow field metadata", "type"),
  fn("VERSION", "", "DataFusion version string", "type"),
]

// ---------------------------------------------------------------------------
// Statement parsing — extract table aliases and column aliases
// ---------------------------------------------------------------------------

interface AliasMap {
  /** table alias → table name, e.g. pv → page_views */
  tableAliases: Map<string, string>
  /** column alias → expression (we just store the alias name for completions) */
  columnAliases: Set<string>
  /** tables referenced in FROM/JOIN (real names) */
  referencedTables: Set<string>
}

function parseAliases(model: editor.ITextModel, _position: { lineNumber: number; column: number }): AliasMap {
  // Get all text in the model
  const lines: string[] = []
  for (let i = 1; i <= model.getLineCount(); i++) {
    lines.push(model.getLineContent(i))
  }
  const fullText = lines.join("\n")

  const tableAliases = new Map<string, string>()
  const columnAliases = new Set<string>()
  const referencedTables = new Set<string>()

  // Match table references: FROM/JOIN [catalog.]table_name [AS] alias
  // Handles: FROM page_views, FROM page_views pv, FROM page_views AS pv,
  //          FROM analytics.page_views, FROM analytics.page_views AS pv
  const tableAliasRe = /\b(?:FROM|JOIN)\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?/gi
  let m: RegExpExecArray | null
  while ((m = tableAliasRe.exec(fullText)) !== null) {
    const tableName = m[2] // the actual table name (after optional catalog.)
    const alias = m[3]
    referencedTables.add(tableName)
    if (alias && !SQL_KW_SET.has(alias.toUpperCase())) {
      tableAliases.set(alias, tableName)
    }
  }

  // Match column aliases: expr AS alias (in SELECT)
  const selectAliasRe = /\bAS\s+(\w+)/gi
  while ((m = selectAliasRe.exec(fullText)) !== null) {
    const alias = m[1]
    if (!SQL_KW_SET.has(alias.toUpperCase())) {
      columnAliases.add(alias)
    }
  }

  return { tableAliases, columnAliases, referencedTables }
}

const SQL_KW_SET = new Set(SQL_KEYWORDS)

// ---------------------------------------------------------------------------
// Context detection
// ---------------------------------------------------------------------------

type SqlContext = "select" | "from" | "where" | "group_by" | "order_by" | "having" | "on" | "general"

function detectContext(model: editor.ITextModel, position: { lineNumber: number; column: number }): SqlContext {
  // Gather all text from line 1 up to cursor position into a single string
  const parts: string[] = []
  for (let line = 1; line <= position.lineNumber; line++) {
    const text = model.getLineContent(line)
    if (line === position.lineNumber) {
      parts.push(text.substring(0, position.column - 1))
    } else {
      parts.push(text)
    }
  }
  const before = parts.join(" ").toUpperCase()

  // Find the last occurrence of each major clause keyword
  const clauses: { ctx: SqlContext; pos: number }[] = []
  const patterns: [SqlContext, RegExp][] = [
    ["select", /\bSELECT\b/g],
    ["from", /\b(?:FROM|JOIN)\b/g],
    ["where", /\bWHERE\b/g],
    ["on", /\bON\b/g],
    ["group_by", /\bGROUP\s+BY\b/g],
    ["having", /\bHAVING\b/g],
    ["order_by", /\bORDER\s+BY\b/g],
  ]

  for (const [ctx, re] of patterns) {
    let match: RegExpExecArray | null
    while ((match = re.exec(before)) !== null) {
      clauses.push({ ctx, pos: match.index })
    }
  }

  if (clauses.length === 0) return "general"

  // The context is determined by the last clause keyword before cursor
  clauses.sort((a, b) => b.pos - a.pos)
  const lastClause = clauses[0].ctx

  // For FROM/JOIN, only return "from" if cursor is still in the table-name position
  // (i.e. right after FROM/JOIN keyword, before WHERE/ON/GROUP etc.)
  if (lastClause === "from") {
    const afterFrom = before.substring(clauses[0].pos)
    // If there's a WHERE/ON/GROUP/ORDER/HAVING after FROM, we're past it
    if (/\b(?:WHERE|ON|GROUP|ORDER|HAVING|SET|LIMIT)\b/.test(afterFrom.substring(5))) {
      // Re-evaluate: find what context we're actually in
      const filtered = clauses.filter((c) => c.ctx !== "from")
      return filtered.length > 0 ? filtered[0].ctx : "general"
    }
    // Check if cursor is right after the table name or after a comma (multiple tables)
    // FROM table1, table2 — still in from context
    const tail = afterFrom.replace(/^(?:FROM|JOIN)\s+/i, "")
    if (/^[\w.,\s]*$/.test(tail)) return "from"
    return "general"
  }

  return lastClause
}

// ---------------------------------------------------------------------------
// Dot-prefix parsing
// ---------------------------------------------------------------------------

function getDotPrefix(model: editor.ITextModel, position: { lineNumber: number; column: number }): string[] {
  const line = model.getLineContent(position.lineNumber)
  const textBefore = line.substring(0, position.column - 1)
  const match = textBefore.match(/((?:\w+\.)+)\s*$/)
  if (!match) return []
  return match[1].split(".").filter(Boolean)
}

// ---------------------------------------------------------------------------
// Completion provider
// ---------------------------------------------------------------------------

export function createCompletionProvider(schema: SchemaInfo): languages.CompletionItemProvider {
  // Pre-build a flat table lookup: tableName → columns
  const tableColumns = new Map<string, { name: string; type: string }[]>()
  const tableToCatalog = new Map<string, string>()
  for (const cat of schema.catalogs) {
    for (const tbl of cat.tables) {
      tableColumns.set(tbl.name, tbl.columns)
      tableToCatalog.set(tbl.name, cat.name)
    }
  }

  // Build a lookup for function docs (used by resolveCompletionItem)
  const fnLookup = new Map<string, FnInfo>()
  for (const f of DATAFUSION_FUNCTIONS) {
    fnLookup.set(f.name, f)
  }

  return {
    triggerCharacters: [".", " "],
    // Lazily resolve documentation when user highlights a completion item
    resolveCompletionItem(item) {
      // If documentation is already set (columns, tables), return as-is
      if (item.documentation) return item

      const label = typeof item.label === "string" ? item.label : item.label.label
      // Check if it's a function
      const fnInfo = fnLookup.get(label)
      if (fnInfo) {
        item.documentation = {
          value: `### ${fnInfo.name}\n\n\`\`\`sql\n${fnInfo.name}(${fnInfo.signature})\n\`\`\`\n\n${fnInfo.detail}\n\n*Category: ${fnInfo.category}*`,
        }
      }
      return item
    },
    provideCompletionItems(model, position) {
      const word = model.getWordUntilPosition(position)
      const range: IRange = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      }

      const suggestions: languages.CompletionItem[] = []
      const seen = new Set<string>()
      const prefixParts = getDotPrefix(model, position)
      const aliases = parseAliases(model, position)

      // ── Dot completions ──────────────────────────────────────────────
      if (prefixParts.length >= 1) {
        const prefix = prefixParts[0]

        // Resolve alias → real table name
        const resolvedTable = aliases.tableAliases.get(prefix) ?? prefix

        if (prefixParts.length === 2) {
          // catalog.table. → columns
          const catName = prefixParts[0]
          const tblName = prefixParts[1]
          const catalog = schema.catalogs.find((c) => c.name === catName)
          if (catalog) {
            const table = catalog.tables.find((t) => t.name === tblName)
            if (table) {
              for (const col of table.columns) {
                suggestions.push({
                  label: col.name,
                  kind: 4, // Field
                  insertText: col.name,
                  detail: col.type,
                  documentation: { value: `**${catName}.${tblName}.${col.name}**\n\nType: \`${col.type}\``, supportHtml: true },
                  sortText: `0_${col.name}`,
                  range,
                })
              }
            }
          }
          return { suggestions }
        }

        // alias. or table. → columns
        const cols = tableColumns.get(resolvedTable)
        if (cols) {
          for (const col of cols) {
            suggestions.push({
              label: col.name,
              kind: 4, // Field
              insertText: col.name,
              detail: col.type,
              documentation: { value: `**${resolvedTable}.${col.name}**\n\nType: \`${col.type}\``, supportHtml: true },
              sortText: `0_${col.name}`,
              range,
            })
          }
          return { suggestions }
        }

        // catalog. → tables
        const catalog = schema.catalogs.find((c) => c.name === prefix)
        if (catalog) {
          for (const table of catalog.tables) {
            const colList = table.columns.map((c) => `- \`${c.name}\` ${c.type}`).join("\n")
            suggestions.push({
              label: table.name,
              kind: 1, // Class
              insertText: table.name,
              detail: `table (${table.columns.length} cols)`,
              documentation: { value: `**${catalog.name}.${table.name}**\n\nColumns:\n${colList}`, supportHtml: true },
              sortText: `0_${table.name}`,
              range,
            })
          }
          return { suggestions }
        }

        return { suggestions }
      }

      // ── Context-aware non-dot completions ────────────────────────────
      const ctx = detectContext(model, position)

      function addItem(label: string, kind: number, insertText: string, detail: string, sort: string, doc?: string) {
        if (seen.has(label)) return
        seen.add(label)
        const item: languages.CompletionItem = { label, kind, insertText, detail, sortText: sort, range }
        if (doc) item.documentation = { value: doc, supportHtml: true }
        suggestions.push(item)
      }

      function addFunction(f: FnInfo, sort: string) {
        if (seen.has(f.name)) return
        seen.add(f.name)
        const snippet = f.signature
          ? `${f.name}(\${1:${f.signature}})$0`
          : `${f.name}()$0`
        const sig = f.signature ? `${f.name}(${f.signature})` : `${f.name}()`
        suggestions.push({
          label: { label: f.name, description: f.category },
          kind: 2, // Function
          insertText: snippet,
          insertTextRules: 4, // InsertAsSnippet
          detail: sig,
          documentation: {
            value: [
              `**${f.name}**(${f.signature})`,
              "",
              f.detail,
              "",
              `_Category: ${f.category}_`,
            ].join("\n"),
          },
          sortText: sort,
          range,
        })
      }

      // Columns from referenced tables (highest priority in SELECT/WHERE/etc.)
      if (ctx === "select" || ctx === "where" || ctx === "on" || ctx === "having" || ctx === "order_by" || ctx === "group_by") {
        // Columns from tables in the query
        for (const tableName of aliases.referencedTables) {
          const cols = tableColumns.get(tableName)
          if (!cols) continue
          for (const col of cols) {
            addItem(col.name, 4, col.name, `${col.type}  — ${tableName}`, `0_${col.name}`, `**${tableName}.${col.name}**\n\nType: \`${col.type}\``)
          }
        }

        // Table alias dot suggestions
        for (const [alias, tableName] of aliases.tableAliases) {
          const cols = tableColumns.get(tableName)
          const colList = cols?.map((c) => `- \`${c.name}\` ${c.type}`).join("\n") ?? ""
          addItem(alias, 5, alias, `alias → ${tableName}`, `1_${alias}`, `**${alias}** → ${tableName}\n\nColumns:\n${colList}`)
        }

        // Column aliases (useful in ORDER BY, GROUP BY)
        if (ctx === "order_by" || ctx === "group_by") {
          for (const alias of aliases.columnAliases) {
            addItem(alias, 4, alias, "column alias", `0_${alias}`)
          }
        }
      }

      // FROM/JOIN context: prioritize tables and catalogs
      if (ctx === "from") {
        for (const catalog of schema.catalogs) {
          const tableList = catalog.tables.map((t) => `- \`${t.name}\` (${t.columns.length} columns)`).join("\n")
          addItem(catalog.name, 8, catalog.name, `catalog (${catalog.tables.length} tables)`, `0_${catalog.name}`, `**${catalog.name}**\n\nTables:\n${tableList}`)
          for (const table of catalog.tables) {
            const colList = table.columns.map((c) => `- \`${c.name}\` ${c.type}`).join("\n")
            addItem(table.name, 1, table.name, `${catalog.name} · ${table.columns.length} cols`, `1_${table.name}`, `**${catalog.name}.${table.name}**\n\nColumns:\n${colList}`)
          }
        }
        return { suggestions }
      }

      // Functions
      for (const f of DATAFUSION_FUNCTIONS) {
        addFunction(f, `3_${f.category}_${f.name}`)
      }

      // Keywords
      for (const kw of SQL_KEYWORDS) {
        addItem(kw, 13, kw, "keyword", `4_${kw}`)
      }

      // All tables and catalogs (lower priority in non-FROM context)
      for (const catalog of schema.catalogs) {
        addItem(catalog.name, 8, catalog.name, `catalog (${catalog.tables.length} tables)`, `5_${catalog.name}`)
        for (const table of catalog.tables) {
          const colList = table.columns.map((c) => `- \`${c.name}\` ${c.type}`).join("\n")
          addItem(table.name, 1, table.name, `${catalog.name}.${table.name}`, `6_${table.name}`, `**${catalog.name}.${table.name}**\n\nColumns:\n${colList}`)
        }
      }

      // If no tables referenced yet, also show all columns
      if (aliases.referencedTables.size === 0) {
        for (const cat of schema.catalogs) {
          for (const tbl of cat.tables) {
            for (const col of tbl.columns) {
              addItem(col.name, 4, col.name, `${col.type}  — ${tbl.name}`, `7_${col.name}`, `**${tbl.name}.${col.name}**\n\nType: \`${col.type}\``)
            }
          }
        }
      }

      return { suggestions }
    },
  }
}
