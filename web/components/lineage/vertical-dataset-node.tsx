"use client";

import { memo } from "react";
import { Handle, Position } from "@xyflow/react";
import { Database, AlertCircle, ArrowLeft, ArrowRight } from "lucide-react";
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
      {/* Left handle for incoming connections (cross-depth) */}
      <Handle
        type="target"
        position={Position.Left}
        id="left"
        className="!bg-muted-foreground !w-2 !h-2"
      />
      {/* Top handles for same-depth connections */}
      <Handle
        type="target"
        position={Position.Top}
        id="top-target"
        className="!bg-muted-foreground !w-2 !h-2"
      />
      <Handle
        type="source"
        position={Position.Top}
        id="top-source"
        className="!bg-muted-foreground !w-2 !h-2"
      />

      <div className="flex items-center gap-2">
        {isCurrent ? (
          <AlertCircle className="h-4 w-4 text-status-failed flex-shrink-0" />
        ) : isUpstream ? (
          <ArrowLeft className="h-4 w-4 text-muted-foreground flex-shrink-0" />
        ) : isDownstream ? (
          <ArrowRight className="h-4 w-4 text-muted-foreground flex-shrink-0" />
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
                depth === 1 ? "Direct" : `${depth} hops`
              )}
            </p>
          )}
        </div>
      </div>

      {/* Right handle for outgoing connections (cross-depth) */}
      <Handle
        type="source"
        position={Position.Right}
        id="right"
        className="!bg-muted-foreground !w-2 !h-2"
      />
      {/* Bottom handles for same-depth connections */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="bottom-source"
        className="!bg-muted-foreground !w-2 !h-2"
      />
      <Handle
        type="target"
        position={Position.Bottom}
        id="bottom-target"
        className="!bg-muted-foreground !w-2 !h-2"
      />
    </div>
  );
});
