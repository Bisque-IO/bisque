import {
  useRef,
  useCallback,
  useState,
  useEffect,
  type ReactNode,
} from "react";
import { cn } from "@/lib/utils";

interface ResizablePanelsProps {
  direction: "horizontal" | "vertical";
  children: [ReactNode, ReactNode];
  defaultSplit?: number; // percentage for first panel (0-100), default 50
  minFirst?: number; // min px for first panel
  minSecond?: number; // min px for second panel
  className?: string;
}

export function ResizablePanels({
  direction,
  children,
  defaultSplit = 50,
  minFirst = 100,
  minSecond = 100,
  className,
}: ResizablePanelsProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [split, setSplit] = useState(defaultSplit);
  const dragging = useRef(false);

  const onMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      dragging.current = true;
      document.body.style.cursor =
        direction === "horizontal" ? "col-resize" : "row-resize";
      document.body.style.userSelect = "none";

      const onMouseMove = (ev: MouseEvent) => {
        if (!dragging.current || !containerRef.current) return;
        const rect = containerRef.current.getBoundingClientRect();
        const total = direction === "horizontal" ? rect.width : rect.height;
        const pos =
          direction === "horizontal"
            ? ev.clientX - rect.left
            : ev.clientY - rect.top;

        // Clamp to min sizes
        const clamped = Math.max(minFirst, Math.min(total - minSecond, pos));
        setSplit((clamped / total) * 100);
      };

      const onMouseUp = () => {
        dragging.current = false;
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
        window.removeEventListener("mousemove", onMouseMove);
        window.removeEventListener("mouseup", onMouseUp);
      };

      window.addEventListener("mousemove", onMouseMove);
      window.addEventListener("mouseup", onMouseUp);
    },
    [direction, minFirst, minSecond],
  );

  // Prevent iframe/editor from stealing mouse events while dragging
  const [isDragging, setIsDragging] = useState(false);
  useEffect(() => {
    if (!dragging.current && isDragging) setIsDragging(false);
  }, [isDragging]);

  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      setIsDragging(true);
      onMouseDown(e);
      const onUp = () => {
        setIsDragging(false);
        window.removeEventListener("mouseup", onUp);
      };
      window.addEventListener("mouseup", onUp);
    },
    [onMouseDown],
  );

  const isH = direction === "horizontal";

  return (
    <div
      ref={containerRef}
      className={cn(
        "flex overflow-hidden",
        isH ? "flex-row" : "flex-col",
        className,
      )}
      style={{ height: "100%", width: "100%" }}
    >
      {/* First panel */}
      <div
        style={{
          [isH ? "width" : "height"]: `calc(${split}% - 3px)`,
          [isH ? "minWidth" : "minHeight"]: minFirst,
          flexShrink: 0,
        }}
        className="overflow-hidden"
      >
        {children[0]}
      </div>

      {/* Drag handle */}
      <div
        onMouseDown={handleMouseDown}
        className={cn(
          "shrink-0 hover:bg-ring transition-colors relative",
          isH ? "w-1.5 cursor-col-resize" : "h-1.5 cursor-row-resize",
        )}
        style={{ zIndex: 20 }}
      />

      {/* Second panel */}
      <div
        style={{
          flex: 1,
          [isH ? "minWidth" : "minHeight"]: minSecond,
          overflow: "hidden",
          // Block pointer events on iframes/editors while dragging
          pointerEvents: isDragging ? "none" : undefined,
        }}
      >
        {children[1]}
      </div>
    </div>
  );
}
