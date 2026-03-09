import { useState, useCallback, useRef, useEffect } from "react"
import Editor, { type OnMount, type Monaco } from "@monaco-editor/react"
import type { editor as monacoEditor, IDisposable } from "monaco-editor"
import { ResizablePanels } from "@/components/resizable-panels"
import { useAuthStore } from "@/stores/auth"
import { wsClient } from "@/lib/ws"
import { useConnectionStore } from "@/stores/connection"
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
  const connState = useConnectionStore((s) => s.state)
  const [schemaCollapsed, setSchemaCollapsed] = useState(false)
  const defaultCatalogRef = useRef<string>("")

  // Load catalogs and schema for intellisense
  useEffect(() => {
    if (!tenantId || connState !== "connected") return

    const load = async () => {
      try {
        const entries = await wsClient.listCatalogs(tenantId)
        const userCatalog = entries.find((e) => e.name !== "sys")
        defaultCatalogRef.current = userCatalog?.name ?? entries[0]?.name ?? ""

        const catalogTables: Record<string, Record<string, MockTableInfo>> = {}
        await Promise.all(
          entries.map(async (entry) => {
            try {
              const data = await wsClient.getCatalog(entry.name)
              catalogTables[entry.name] =
                (data as { tables?: Record<string, MockTableInfo> }).tables ?? {}
            } catch {
              catalogTables[entry.name] = {}
            }
          }),
        )
        setSchema(buildSchemaFromMockData(catalogTables))
      } catch (err) {
        console.error("Failed to load schema:", err)
        // Set empty schema so the tree shows "No tables" instead of stuck loading
        setSchema({ catalogs: [] })
      }
    }
    load()
  }, [tenantId, connState])

  const runQuery = useCallback(
    async (query: string) => {
      if (!query.trim()) return
      setRunning(true)
      setError("")
      setResults(null)
      setColumns([])

      try {
        const catalog = defaultCatalogRef.current
        if (!catalog) {
          setError("No catalog available. Create a catalog first.")
          return
        }

        const result = await wsClient.executeSql(catalog, query, {
          onHeader: (cols) => setColumns(cols.map((c) => c.name)),
          onChunk: (rows) => setResults([...rows]),
        })
        setColumns(result.columns.map((c: { name: string }) => c.name))
        setResults(result.rows as Record<string, unknown>[])
      } catch (err) {
        setError(err instanceof Error ? err.message : "Query failed")
      } finally {
        setRunning(false)
      }
    },
    [],
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

      monaco.editor.defineTheme("bisque-sql-dark", {
        base: "vs-dark",
        inherit: true,
        rules: [
          { token: "keyword.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.block.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.choice.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.try.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "keyword.catch.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "operator.sql", foreground: "569cd6", fontStyle: "bold" },
          { token: "predefined.sql", foreground: "dcdcaa" },
          { token: "string.sql", foreground: "6a9955" },
          { token: "number.sql", foreground: "b5cea8" },
          { token: "identifier.sql", foreground: "e0e0e0" },
          { token: "identifier.quote.sql", foreground: "e0e0e0" },
          { token: "comment.sql", foreground: "608b4e", fontStyle: "italic" },
          { token: "comment.quote.sql", foreground: "608b4e", fontStyle: "italic" },
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
