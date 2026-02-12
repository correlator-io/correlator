"use client";

import { memo } from "react";
import { Handle, Position } from "@xyflow/react";
import { Database, AlertCircle, ArrowDown, ArrowUp } from "lucide-react";
import { cn } from "@/lib/utils";

interface VerticalDatasetNodeProps {
  data: {
    label: string;
    isCurrent?: boolean;
    direction?: "upstream" | "downstream";
    depth?: number;
  };
}

export const VerticalDatasetNode = memo(function VerticalDatasetNode({
  data,
}: VerticalDatasetNodeProps) {
  const { label, isCurrent, direction, depth } = data;

  const isUpstream = direction === "upstream";
  const isDownstream = direction === "downstream";

  return (
    <div
      className={cn(
        "px-3 py-2 rounded-lg border-2 shadow-sm min-w-[140px] max-w-[200px]",
        "bg-card text-card-foreground",
        isCurrent
          ? "border-status-failed bg-status-failed/10"
          : "border-border hover:border-muted-foreground/50"
      )}
    >
      {/* Top handle for incoming connections */}
      <Handle
        type="target"
        position={Position.Top}
        id="top"
        className="!bg-muted-foreground !w-2 !h-2"
      />

      <div className="flex items-center gap-2">
        {isCurrent ? (
          <AlertCircle className="h-4 w-4 text-status-failed flex-shrink-0" />
        ) : isUpstream ? (
          <ArrowUp className="h-4 w-4 text-muted-foreground flex-shrink-0" />
        ) : isDownstream ? (
          <ArrowDown className="h-4 w-4 text-muted-foreground flex-shrink-0" />
        ) : (
          <Database className="h-4 w-4 text-muted-foreground flex-shrink-0" />
        )}
        <div className="flex-1 min-w-0">
          <p
            className={cn(
              "text-sm font-medium truncate",
              isCurrent && "text-status-failed"
            )}
            title={label}
          >
            {label}
          </p>
          {isCurrent && (
            <p className="text-xs text-status-failed/80">Affected</p>
          )}
          {!isCurrent && depth !== undefined && (
            <p className="text-xs text-muted-foreground">
              {isUpstream ? (
                depth === 1 ? "Direct source" : `${depth} hops upstream`
              ) : (
                depth === 1 ? "Direct consumer" : `${depth} hops downstream`
              )}
            </p>
          )}
        </div>
      </div>

      {/* Bottom handle for outgoing connections */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="bottom"
        className="!bg-muted-foreground !w-2 !h-2"
      />
    </div>
  );
});
