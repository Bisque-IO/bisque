import { useState, useCallback } from "react";
import {
  ChevronRight,
  ChevronDown,
  Database,
  Table2,
  Columns,
} from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import type { SchemaInfo } from "@/lib/sql-completions";

interface SchemaTreeProps {
  schema: SchemaInfo | null;
  onInsert?: (text: string) => void;
}

export function SchemaTree({ schema, onInsert }: SchemaTreeProps) {
  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(
    () => new Set(schema?.catalogs.map((c) => c.name) ?? []),
  );
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  const toggleCatalog = useCallback((name: string) => {
    setExpandedCatalogs((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  }, []);

  const toggleTable = useCallback((key: string) => {
    setExpandedTables((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  if (!schema) {
    return (
      <div className="p-3 text-xs text-muted-foreground">Loading schema...</div>
    );
  }

  if (schema.catalogs.length === 0) {
    return (
      <div className="p-3 text-xs text-muted-foreground">
        No catalogs found.
      </div>
    );
  }

  return (
    <ScrollArea className="h-full">
      <div className="p-2 space-y-0.5">
        {schema.catalogs.map((catalog) => {
          const catExpanded = expandedCatalogs.has(catalog.name);
          return (
            <div key={catalog.name}>
              {/* Catalog node */}
              <button
                onClick={() => toggleCatalog(catalog.name)}
                onDoubleClick={() => onInsert?.(catalog.name)}
                className={cn(
                  "flex items-center gap-1.5 w-full px-1.5 py-1 rounded text-sm hover:bg-accent text-left",
                  "group",
                )}
              >
                {catExpanded ? (
                  <ChevronDown className="h-3 w-3 shrink-0 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-3 w-3 shrink-0 text-muted-foreground" />
                )}
                <Database className="h-3.5 w-3.5 shrink-0 text-blue-400/50" />
                <span className="truncate font-medium">{catalog.name}</span>
                <span className="ml-auto text-[11px] text-muted-foreground opacity-0 group-hover:opacity-100">
                  {catalog.tables.length}
                </span>
              </button>

              {/* Tables */}
              {catExpanded && (
                <div className="ml-3">
                  {catalog.tables.map((table) => {
                    const tableKey = `${catalog.name}.${table.name}`;
                    const tableExpanded = expandedTables.has(tableKey);
                    return (
                      <div key={tableKey}>
                        {/* Table node */}
                        <button
                          onClick={() => toggleTable(tableKey)}
                          onDoubleClick={() => onInsert?.(table.name)}
                          className={cn(
                            "flex items-center gap-1.5 w-full px-1.5 py-1 rounded text-sm hover:bg-accent text-left",
                            "group",
                          )}
                        >
                          {tableExpanded ? (
                            <ChevronDown className="h-3 w-3 shrink-0 text-muted-foreground" />
                          ) : (
                            <ChevronRight className="h-3 w-3 shrink-0 text-muted-foreground" />
                          )}
                          <Table2 className="h-3.5 w-3.5 shrink-0 text-green-400/50" />
                          <span className="truncate">{table.name}</span>
                          <span className="ml-auto text-[10px] text-muted-foreground opacity-0 group-hover:opacity-100">
                            {table.columns.length}
                          </span>
                        </button>

                        {/* Columns */}
                        {tableExpanded && (
                          <div className="ml-3">
                            {table.columns.map((col) => (
                              <Tooltip key={col.name}>
                                <TooltipTrigger asChild>
                                  <button
                                    onClick={() => onInsert?.(col.name)}
                                    className="flex items-center gap-1.5 w-full px-1.5 py-0.5 rounded text-sm hover:bg-accent text-left group"
                                  >
                                    <Columns className="h-3 w-3 shrink-0 text-muted-foreground/90 ml-[18px]" />
                                    <span className="truncate text-foreground/70">
                                      {col.name}
                                    </span>
                                    <span className="ml-auto text-[10px] text-muted-foreground truncate max-w-[80px]">
                                      {simplifyType(col.type)}
                                    </span>
                                  </button>
                                </TooltipTrigger>
                                <TooltipContent side="right">
                                  <p className="font-mono">{col.type}</p>
                                </TooltipContent>
                              </Tooltip>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </ScrollArea>
  );
}

function simplifyType(type: string): string {
  if (type.startsWith("Timestamp")) return "Timestamp";
  if (type.startsWith("FixedSizeList")) return "Vector";
  if (type.startsWith("List")) return "List";
  return type;
}
