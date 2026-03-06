import { useMemo, useState, useCallback, useRef, useEffect } from "react"
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  type ColumnDef,
  type ColumnOrderState,
  type Header,
} from "@tanstack/react-table"
import { cn } from "@/lib/utils"
import { GripVertical } from "lucide-react"

interface ResultsTableProps {
  columns: string[]
  data: Record<string, unknown>[]
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL"
  if (typeof value === "object") return JSON.stringify(value)
  return String(value)
}

export function ResultsTable({ columns: colNames, data }: ResultsTableProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [columnSizing, setColumnSizing] = useState<Record<string, number>>({})

  // Calculate initial column sizes to fill container width
  useEffect(() => {
    const el = containerRef.current
    if (!el || colNames.length === 0) return
    const containerWidth = el.clientWidth
    const rowNumWidth = 50
    const available = containerWidth - rowNumWidth
    const perCol = Math.max(100, Math.floor(available / colNames.length))
    const sizing: Record<string, number> = {}
    for (const col of colNames) {
      sizing[col] = perCol
    }
    setColumnSizing(sizing)
  }, [colNames])

  const columns = useMemo<ColumnDef<Record<string, unknown>>[]>(() => {
    const rowNumCol: ColumnDef<Record<string, unknown>> = {
      id: "__row_num",
      header: "#",
      size: 50,
      minSize: 40,
      maxSize: 60,
      enableResizing: false,
      cell: ({ row }) => row.index + 1,
    }
    const dataCols: ColumnDef<Record<string, unknown>>[] = colNames.map((col) => ({
      id: col,
      accessorKey: col,
      header: col,
      size: columnSizing[col] || 150,
      minSize: 60,
      cell: ({ getValue }) => formatCellValue(getValue()),
    }))
    return [rowNumCol, ...dataCols]
  }, [colNames, columnSizing])

  const [columnOrder, setColumnOrder] = useState<ColumnOrderState>([])
  const [draggedCol, setDraggedCol] = useState<string | null>(null)
  const [dragOverCol, setDragOverCol] = useState<string | null>(null)

  // Reset column order when columns change
  useMemo(() => {
    setColumnOrder(["__row_num", ...colNames])
  }, [colNames])

  const table = useReactTable({
    data,
    columns,
    state: { columnOrder },
    onColumnOrderChange: setColumnOrder,
    columnResizeMode: "onChange",
    getCoreRowModel: getCoreRowModel(),
  })

  // Drag and drop handlers for column reordering
  const handleDragStart = useCallback((e: React.DragEvent, colId: string) => {
    if (colId === "__row_num") return
    setDraggedCol(colId)
    e.dataTransfer.effectAllowed = "move"
    e.dataTransfer.setData("text/plain", colId)
  }, [])

  const handleDragOver = useCallback((e: React.DragEvent, colId: string) => {
    if (colId === "__row_num") return
    e.preventDefault()
    e.dataTransfer.dropEffect = "move"
    setDragOverCol(colId)
  }, [])

  const handleDragLeave = useCallback(() => {
    setDragOverCol(null)
  }, [])

  const handleDrop = useCallback((e: React.DragEvent, targetColId: string) => {
    e.preventDefault()
    const sourceColId = e.dataTransfer.getData("text/plain")
    if (sourceColId === targetColId || targetColId === "__row_num") return

    setColumnOrder((prev) => {
      const order = [...prev]
      const sourceIdx = order.indexOf(sourceColId)
      const targetIdx = order.indexOf(targetColId)
      if (sourceIdx === -1 || targetIdx === -1) return prev
      order.splice(sourceIdx, 1)
      order.splice(targetIdx, 0, sourceColId)
      return order
    })
    setDraggedCol(null)
    setDragOverCol(null)
  }, [])

  const handleDragEnd = useCallback(() => {
    setDraggedCol(null)
    setDragOverCol(null)
  }, [])

  return (
    <div ref={containerRef} className="overflow-auto h-full">
      <table
        className="border-collapse text-xs"
        style={{ width: Math.max(table.getTotalSize(), containerRef.current?.clientWidth ?? 0) }}
      >
        <thead className="sticky top-0 z-10">
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id}>
              {headerGroup.headers.map((header) => (
                <HeaderCell
                  key={header.id}
                  header={header}
                  isDragged={draggedCol === header.id}
                  isDragOver={dragOverCol === header.id}
                  onDragStart={handleDragStart}
                  onDragOver={handleDragOver}
                  onDragLeave={handleDragLeave}
                  onDrop={handleDrop}
                  onDragEnd={handleDragEnd}
                />
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr
              key={row.id}
              className="border-b border-border/50 hover:bg-accent/30"
            >
              {row.getVisibleCells().map((cell) => (
                <td
                  key={cell.id}
                  className={cn(
                    "px-3 py-1.5 font-mono truncate",
                    cell.column.id === "__row_num"
                      ? "text-center text-muted-foreground"
                      : "",
                  )}
                  style={{ width: cell.column.getSize() }}
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function HeaderCell({
  header,
  isDragged,
  isDragOver,
  onDragStart,
  onDragOver,
  onDragLeave,
  onDrop,
  onDragEnd,
}: {
  header: Header<Record<string, unknown>, unknown>
  isDragged: boolean
  isDragOver: boolean
  onDragStart: (e: React.DragEvent, colId: string) => void
  onDragOver: (e: React.DragEvent, colId: string) => void
  onDragLeave: () => void
  onDrop: (e: React.DragEvent, colId: string) => void
  onDragEnd: () => void
}) {
  const isRowNum = header.id === "__row_num"
  const resizeHandler = header.getResizeHandler()
  const isResizing = header.column.getIsResizing()
  const resizeRef = useRef<HTMLDivElement>(null)

  return (
    <th
      className={cn(
        "relative bg-background border-b border-border px-3 py-2 text-left font-mono font-medium text-muted-foreground select-none",
        isRowNum ? "text-center" : "group",
        isDragged && "opacity-50",
        isDragOver && "border-l-2 border-l-primary",
      )}
      style={{ width: header.getSize() }}
      draggable={!isRowNum}
      onDragStart={(e) => onDragStart(e, header.id)}
      onDragOver={(e) => onDragOver(e, header.id)}
      onDragLeave={onDragLeave}
      onDrop={(e) => onDrop(e, header.id)}
      onDragEnd={onDragEnd}
    >
      <div className="flex items-center gap-1">
        {!isRowNum && (
          <GripVertical className="h-3 w-3 shrink-0 text-muted-foreground/30 opacity-0 group-hover:opacity-100 cursor-grab" />
        )}
        <span className="truncate">
          {flexRender(header.column.columnDef.header, header.getContext())}
        </span>
      </div>

      {/* Resize handle */}
      {header.column.getCanResize() && (
        <div
          ref={resizeRef}
          onMouseDown={resizeHandler}
          onTouchStart={resizeHandler}
          onDoubleClick={() => header.column.resetSize()}
          className={cn(
            "absolute top-0 right-0 w-1 h-full cursor-col-resize select-none touch-none",
            isResizing
              ? "bg-primary"
              : "bg-transparent hover:bg-border",
          )}
        />
      )}
    </th>
  )
}
