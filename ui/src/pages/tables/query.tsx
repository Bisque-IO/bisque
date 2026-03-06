import { useState, useCallback, useRef, useEffect } from "react"
import Editor, { type OnMount, type Monaco } from "@monaco-editor/react"
import type { editor as monacoEditor, IDisposable } from "monaco-editor"
import { ResizablePanels } from "@/components/resizable-panels"
import { useAuthStore } from "@/stores/auth"
import { catalogApi, s3Api, type CatalogEntry } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ResultsTable } from "@/components/results-table"
import { Play, Loader2, PanelLeftClose, PanelLeftOpen } from "lucide-react"
import {
  buildSchemaFromMockData,
  createCompletionProvider,
  type SchemaInfo,
} from "@/lib/sql-completions"
import { SchemaTree } from "@/components/schema-tree"
import type { MockTableInfo } from "@/lib/mock-data"

export function QueryPage() {
  const [sql, setSql] = useState(
    "SELECT * FROM page_views\nWHERE duration_ms > 100\nLIMIT 50",
  )
  const [results, setResults] = useState<Record<string, unknown>[] | null>(null)
  const [columns, setColumns] = useState<string[]>([])
  const [error, setError] = useState("")
  const [running, setRunning] = useState(false)
  const [schema, setSchema] = useState<SchemaInfo | null>(null)
  const editorRef = useRef<monacoEditor.IStandaloneCodeEditor | null>(null)
  const monacoRef = useRef<Monaco | null>(null)
  const completionDisposable = useRef<IDisposable | null>(null)
  const tenantId = useAuthStore((s) => s.tenantId)
  const [schemaCollapsed, setSchemaCollapsed] = useState(false)

  // Load schema for intellisense
  useEffect(() => {
    if (!tenantId) return

    catalogApi
      .list(tenantId)
      .then(async (entries: CatalogEntry[]) => {
        const catalogTables: Record<string, Record<string, MockTableInfo>> = {}
        for (const entry of entries) {
          try {
            const data = await s3Api.getCatalog(entry.name)
            catalogTables[entry.name] =
              (data as { tables?: Record<string, MockTableInfo> }).tables ?? {}
          } catch {
            catalogTables[entry.name] = {}
          }
        }
        setSchema(buildSchemaFromMockData(catalogTables))
      })
      .catch(() => {})
  }, [tenantId])

  const runQuery = useCallback(
    async (query: string) => {
      if (!query.trim()) return
      setRunning(true)
      setError("")
      setResults(null)
      setColumns([])

      try {
        const match = query.match(
          /SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+.*?)?(?:\s+LIMIT\s+(\d+))?$/is,
        )
        if (!match) {
          setError(
            "Could not parse query. Currently supports: SELECT ... FROM <table> [LIMIT n]",
          )
          return
        }

        const [, selectCols, tableName, limitStr] = match
        const limit = limitStr ? parseInt(limitStr) : 20

        if (!schema) {
          setError("Schema not loaded yet. Please wait...")
          return
        }

        let tableSchema: { name: string; type: string }[] | null = null
        for (const catalog of schema.catalogs) {
          const table = catalog.tables.find((t) => t.name === tableName)
          if (table) {
            tableSchema = table.columns
            break
          }
        }

        if (!tableSchema) {
          setError(`Table '${tableName}' not found in any catalog`)
          return
        }

        const isSelectStar = selectCols.trim() === "*"
        const selectedCols = isSelectStar
          ? tableSchema.map((c) => c.name)
          : selectCols.split(",").map((c) => c.trim())

        setColumns(selectedCols)

        const rows: Record<string, unknown>[] = []
        for (let i = 0; i < limit; i++) {
          const row: Record<string, unknown> = {}
          for (const col of selectedCols) {
            const colInfo = tableSchema.find((c) => c.name === col)
            row[col] = generateMockValue(col, colInfo?.type ?? "Utf8", i)
          }
          rows.push(row)
        }
        setResults(rows)
      } finally {
        setRunning(false)
      }
    },
    [schema],
  )

  // Register (or re-register) the completion provider using the real monaco instance
  const registerCompletions = useCallback(
    (monaco: Monaco, schemaInfo: SchemaInfo) => {
      completionDisposable.current?.dispose()
      completionDisposable.current = monaco.languages.registerCompletionItemProvider(
        "sql",
        createCompletionProvider(schemaInfo),
      )
    },
    [],
  )

  const handleEditorMount: OnMount = useCallback(
    (editor, monaco) => {
      editorRef.current = editor
      monacoRef.current = monaco

      // Register a custom SQL-friendly dark theme
      // vs-dark has hardcoded .sql-suffixed rules (string.sql=red, predefined.sql=magenta,
      // operator.sql=muted gray) that override generic rules. We must use .sql suffixes.
      monaco.editor.defineTheme("bisque-sql-dark", {
        base: "vs-dark",
        inherit: true,
        rules: [
          // Keywords (SELECT, FROM, WHERE): bright blue bold
          { token: "keyword.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.block.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.choice.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.try.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.catch.sql", foreground: "569cd6", fontStyle: "bold" },
          // Operators (AND, OR, NOT, IN, etc.): bright blue (same as keywords for SQL)
          { token: "operator.sql", foreground: "569cd6", fontStyle: "bold" },
          // Built-in functions (AVG, COUNT, etc.): yellow/gold
          { token: "predefined.sql", foreground: "dcdcaa" },
          // Strings: green (standard for SQL IDEs)
          { token: "string.sql", foreground: "6a9955" },
          // Numbers: light green
          { token: "number.sql", foreground: "b5cea8" },
          // Identifiers (column names, table names): light gray
          { token: "identifier.sql", foreground: "e0e0e0" },
          { token: "identifier.quote.sql", foreground: "e0e0e0" },
          // Comments: muted green italic
          { token: "comment.sql", foreground: "608b4e", fontStyle: "italic" },
          { token: "comment.quote.sql", foreground: "608b4e", fontStyle: "italic" },
          // Delimiters
          { token: "delimiter.sql", foreground: "d4d4d4" },
        ],
        colors: {},
      })
      monaco.editor.setTheme("bisque-sql-dark")

      editor.addAction({
        id: "run-query",
        label: "Run Query",
        keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
        run: () => {
          const value = editor.getValue()
          runQuery(value)
        },
      })

      if (schema) {
        registerCompletions(monaco, schema)
      }
    },
    [schema, runQuery, registerCompletions],
  )

  // Re-register completions when schema loads after editor is already mounted
  useEffect(() => {
    if (!schema || !monacoRef.current) return
    registerCompletions(monacoRef.current, schema)
  }, [schema, registerCompletions])

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      completionDisposable.current?.dispose()
    }
  }, [])

  // Insert text at cursor position in the editor
  const handleInsert = useCallback((text: string) => {
    const editor = editorRef.current
    if (!editor) return
    editor.focus()
    const selection = editor.getSelection()
    if (selection) {
      editor.executeEdits("schema-tree", [
        { range: selection, text, forceMoveMarkers: true },
      ])
    }
  }, [])

  const editorAndResults = (
    <ResizablePanels direction="vertical" defaultSplit={40} minFirst={100} minSecond={100}>
      {/* Editor panel */}
      <div className="h-full flex flex-col">
        <div className="flex items-center justify-between px-3 h-10 border-b shrink-0">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Query</span>
          <Button
            onClick={() => runQuery(editorRef.current?.getValue() ?? sql)}
            disabled={running}
            variant="secondary"
            size="sm"
            className="h-7 text-xs my-1.5"
          >
            {running ? (
              <Loader2 className="h-3 w-3 mr-1.5 animate-spin" />
            ) : (
              <Play className="h-3 w-3 mr-1.5" />
            )}
            Run
            <kbd className="ml-1.5 text-[10px] opacity-60">Ctrl+Enter</kbd>
          </Button>
        </div>
        <div className="flex-1 min-h-0">
          <Editor
            height="100%"
            defaultLanguage="sql"
            value={sql}
            onChange={(value) => setSql(value ?? "")}
            onMount={handleEditorMount}
            theme="vs-dark"
            options={{
              minimap: { enabled: false },
              fontSize: 14,
              lineNumbers: "on",
              scrollBeyondLastLine: false,
              wordWrap: "on",
              padding: { top: 8, bottom: 8 },
              suggestOnTriggerCharacters: true,
              quickSuggestions: true,
              tabSize: 2,
              automaticLayout: true,
              renderLineHighlight: "line",
              scrollbar: {
                vertical: "auto",
                horizontal: "auto",
              },
              suggest: {
                showIcons: true,
                showStatusBar: true,
                preview: true,
                showInlineDetails: true,
              },
            }}
          />
        </div>
      </div>

      {/* Results panel */}
      <div className="h-full flex flex-col">
        <div className="flex items-center gap-2 px-3 h-10 border-b shrink-0">
          <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Results</span>
          {results && (
            <Badge variant="secondary" className="text-[10px] h-5">
              {results.length} rows
            </Badge>
          )}
        </div>
        <div className="flex-1 overflow-auto min-h-0">
          {error && (
            <div className="p-3">
              <p className="text-sm text-destructive">{error}</p>
            </div>
          )}

          {results && columns.length > 0 && (
            <ResultsTable columns={columns} data={results} />
          )}

          {!error && !results && (
            <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
              Run a query to see results
            </div>
          )}
        </div>
      </div>
    </ResizablePanels>
  )

  return (
    <div className="query-page -m-6 h-[calc(100%+3rem)] flex">
      {schemaCollapsed ? (
        <>
          <div className="h-full flex flex-col border-r shrink-0">
            <button
              onClick={() => setSchemaCollapsed(false)}
              className="flex items-center justify-center h-10 w-10 border-b text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
              title="Expand schema"
            >
              <PanelLeftOpen className="h-4 w-4" />
            </button>
          </div>
          <div className="flex-1 min-w-0">{editorAndResults}</div>
        </>
      ) : (
        <ResizablePanels direction="horizontal" defaultSplit={20} minFirst={200} minSecond={400}>
          <div className="h-full flex flex-col">
            <div className="flex items-center justify-between px-3 h-10 border-b shrink-0">
              <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Schema</span>
              <div className="flex items-center gap-1">
                {schema && (
                  <Badge variant="outline" className="text-[10px] h-5">
                    {schema.catalogs.reduce((n, c) => n + c.tables.length, 0)}
                  </Badge>
                )}
                <button
                  onClick={() => setSchemaCollapsed(true)}
                  className="flex items-center justify-center h-6 w-6 rounded text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
                  title="Collapse schema"
                >
                  <PanelLeftClose className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>
            <div className="flex-1 overflow-hidden">
              <SchemaTree schema={schema} onInsert={handleInsert} />
            </div>
          </div>
          {editorAndResults}
        </ResizablePanels>
      )}
    </div>
  )
}

function generateMockValue(
  colName: string,
  colType: string,
  rowIndex: number,
): unknown {
  const now = Date.now()

  if (colType.includes("Timestamp")) {
    return new Date(now - rowIndex * 60_000).toISOString()
  }
  if (colType === "Date32") {
    return new Date(now - rowIndex * 86_400_000).toISOString().slice(0, 10)
  }
  if (colType === "Int32" || colType === "Int64") {
    if (colName.includes("duration") || colName.includes("latency"))
      return 50 + Math.floor(Math.random() * 500)
    if (colName.includes("cents") || colName.includes("revenue"))
      return 100 + Math.floor(Math.random() * 10000)
    if (colName.includes("version")) return rowIndex + 1
    if (colName.includes("status"))
      return [200, 201, 400, 404, 500][rowIndex % 5]
    if (colName === "x" || colName === "y")
      return Math.floor(Math.random() * 1920)
    return Math.floor(Math.random() * 1000)
  }
  if (colType === "Float32" || colType === "Float64") {
    return +(Math.random() * 100).toFixed(4)
  }
  if (colType === "Boolean") {
    return rowIndex % 2 === 0
  }
  if (colType.includes("List") && !colType.includes("FixedSize")) {
    return `["item_${rowIndex}_a", "item_${rowIndex}_b"]`
  }
  if (colType.includes("FixedSizeList")) {
    return "[0.123, -0.456, 0.789, ...]"
  }

  // String types
  if (colName === "user_id")
    return `usr_${String(1000 + rowIndex).padStart(6, "0")}`
  if (colName === "session_id")
    return `sess_${Math.random().toString(36).slice(2, 10)}`
  if (colName === "order_id")
    return `ord_${String(5000 + rowIndex).padStart(6, "0")}`
  if (colName === "trace_id")
    return Array.from({ length: 32 }, () =>
      Math.floor(Math.random() * 16).toString(16),
    ).join("")
  if (colName === "span_id")
    return Array.from({ length: 16 }, () =>
      Math.floor(Math.random() * 16).toString(16),
    ).join("")
  if (colName === "parent_span_id")
    return rowIndex === 0
      ? null
      : Array.from({ length: 16 }, () =>
          Math.floor(Math.random() * 16).toString(16),
        ).join("")
  if (colName === "email") return `user${rowIndex}@example.com`
  if (colName === "page_url")
    return `/products/${["electronics", "clothing", "books", "home"][rowIndex % 4]}`
  if (colName === "referrer")
    return rowIndex % 3 === 0 ? null : "https://google.com"
  if (colName === "country")
    return ["US", "GB", "DE", "JP", "BR", "IN"][rowIndex % 6]
  if (colName === "campaign")
    return rowIndex % 2 === 0
      ? null
      : ["summer-sale", "winter-promo", "launch"][rowIndex % 3]
  if (colName === "plan") return ["free", "pro", "enterprise"][rowIndex % 3]
  if (colName === "severity")
    return ["info", "info", "warn", "error", "debug"][rowIndex % 5]
  if (colName === "service_name" || colName === "operation_name")
    return ["api-gateway", "user-service", "order-service", "payment-service"][
      rowIndex % 4
    ]
  if (colName === "metric_name")
    return ["http_requests_total", "process_cpu_seconds", "memory_bytes"][
      rowIndex % 3
    ]
  if (colName === "element_id")
    return `btn-${["submit", "cancel", "next", "prev", "menu"][rowIndex % 5]}`
  if (colName === "ad_id") return `ad_${1000 + rowIndex}`
  if (colName === "placement")
    return ["header", "sidebar", "footer", "inline"][rowIndex % 4]
  if (colName === "event_name")
    return ["signup", "purchase", "add_to_cart", "page_view"][rowIndex % 4]
  if (colName === "body")
    return [
      "Request completed",
      "Connection established",
      "Segment sealed",
      "Query executed",
    ][rowIndex % 4]
  if (colName === "attributes") return `{"key_${rowIndex}": "val_${rowIndex}"}`
  if (colName === "labels")
    return `{"instance": "bisque:3200", "job": "bisque"}`

  return `value_${rowIndex}`
}
