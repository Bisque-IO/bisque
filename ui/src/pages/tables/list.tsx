import { useEffect, useMemo, useState } from "react"
import { useAuthStore } from "@/stores/auth"
import { wsClient } from "@/lib/ws"
import { useConnectionStore } from "@/stores/connection"
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Card, CardContent } from "@/components/ui/card"
import { Table2, ArrowUpDown } from "lucide-react"
import { Link } from "react-router"
import type { CatalogEntry } from "@/lib/api"

interface TableInfo {
  active_version: number
  sealed_version: number
}

interface TableRow_ {
  catalog: string
  engine: string
  name: string
  active_version: number
  sealed_version: number
}

export function TableListPage() {
  const tenantId = useAuthStore((s) => s.tenantId)
  const connState = useConnectionStore((s) => s.state)
  const [tables, setTables] = useState<TableRow_[]>([])
  const [loading, setLoading] = useState(true)
  const [sorting, setSorting] = useState<SortingState>([])
  const [globalFilter, setGlobalFilter] = useState("")

  useEffect(() => {
    if (!tenantId || connState !== "connected") return

    setLoading(true)
    wsClient
      .listCatalogs(tenantId)
      .then(async (entries: CatalogEntry[]) => {
        const rows: TableRow_[] = []
        for (const entry of entries) {
          try {
            const data = await wsClient.getCatalog(entry.name)
            const catalogTables = (data as { tables?: Record<string, TableInfo> }).tables ?? {}
            for (const [name, info] of Object.entries(catalogTables)) {
              rows.push({
                catalog: entry.name,
                engine: entry.engine,
                name,
                active_version: info.active_version,
                sealed_version: info.sealed_version,
              })
            }
          } catch {
            // skip catalogs that fail to load
          }
        }
        setTables(rows)
      })
      .catch((err) => console.error("Failed to load tables:", err))
      .finally(() => setLoading(false))
  }, [tenantId, connState])

  const columns = useMemo<ColumnDef<TableRow_>[]>(
    () => [
      {
        accessorKey: "name",
        header: "Table",
        cell: ({ row }) => (
          <Link
            to={`/tables/${row.original.catalog}/${row.original.name}`}
            className="font-medium text-primary hover:underline"
          >
            {row.original.name}
          </Link>
        ),
      },
      {
        accessorKey: "catalog",
        header: "Catalog",
        cell: ({ row }) => (
          <Link
            to={`/catalogs/${row.original.catalog}`}
            className="hover:underline"
          >
            {row.original.catalog}
          </Link>
        ),
      },
      {
        accessorKey: "engine",
        header: "Engine",
        cell: ({ getValue }) => (
          <Badge variant="secondary">{getValue<string>()}</Badge>
        ),
      },
      {
        accessorKey: "active_version",
        header: "Active Version",
        cell: ({ getValue }) => (
          <Badge variant="outline">v{getValue<number>()}</Badge>
        ),
      },
      {
        accessorKey: "sealed_version",
        header: "Sealed Version",
        cell: ({ getValue }) => (
          <Badge variant="secondary">v{getValue<number>()}</Badge>
        ),
      },
    ],
    [],
  )

  const table = useReactTable({
    data: tables,
    columns,
    state: { sorting, globalFilter },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  })

  if (loading) {
    return <p className="text-muted-foreground">Loading tables...</p>
  }

  if (tables.length === 0) {
    return (
      <Card>
        <CardContent className="py-12 text-center">
          <Table2 className="mx-auto h-12 w-12 text-muted-foreground/50" />
          <p className="mt-4 text-muted-foreground">No tables found across any catalog.</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4">
      <Input
        placeholder="Filter tables..."
        value={globalFilter}
        onChange={(e) => setGlobalFilter(e.target.value)}
        className="max-w-sm"
      />
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <TableHead
                    key={header.id}
                    className={header.column.getCanSort() ? "cursor-pointer select-none" : ""}
                    onClick={header.column.getToggleSortingHandler()}
                  >
                    <div className="flex items-center gap-1">
                      {flexRender(header.column.columnDef.header, header.getContext())}
                      {header.column.getCanSort() && (
                        <ArrowUpDown className="h-3 w-3 text-muted-foreground" />
                      )}
                    </div>
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={columns.length} className="h-24 text-center">
                  No results.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <p className="text-xs text-muted-foreground">
        {table.getFilteredRowModel().rows.length} table(s)
      </p>
    </div>
  )
}
