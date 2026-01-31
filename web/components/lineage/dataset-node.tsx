"use client";

import { memo } from "react";
import { Handle, Position } from "@xyflow/react";
import { Database, AlertCircle } from "lucide-react";
import { cn } from "@/lib/utils";

interface DatasetNodeProps {
  data: {
    label: string;
    isSource?: boolean;
    depth?: number;
  };
}

export const DatasetNode = memo(function DatasetNode({
  data,
}: DatasetNodeProps) {
  const { label, isSource, depth } = data;

  return (
    <div
      className={cn(
        "px-3 py-2 rounded-lg border-2 shadow-sm min-w-[120px] max-w-[200px]",
        "bg-card text-card-foreground",
        isSource
          ? "border-status-failed bg-status-failed/10"
          : "border-border hover:border-muted-foreground/50"
      )}
    >
      {/* Input handle (not shown for source) */}
      {!isSource && (
        <Handle
          type="target"
          position={Position.Left}
          className="!bg-muted-foreground !w-2 !h-2"
        />
      )}

      <div className="flex items-center gap-2">
        {isSource ? (
          <AlertCircle className="h-4 w-4 text-status-failed flex-shrink-0" />
        ) : (
          <Database className="h-4 w-4 text-muted-foreground flex-shrink-0" />
        )}
        <div className="flex-1 min-w-0">
          <p
            className={cn(
              "text-sm font-medium truncate",
              isSource && "text-status-failed"
            )}
            title={label}
          >
            {label}
          </p>
          {isSource && (
            <p className="text-xs text-muted-foreground">Affected</p>
          )}
          {!isSource && depth !== undefined && (
            <p className="text-xs text-muted-foreground">
              {depth === 1 ? "Direct" : `${depth} hops`}
            </p>
          )}
        </div>
      </div>

      {/* Output handle */}
      <Handle
        type="source"
        position={Position.Right}
        className="!bg-muted-foreground !w-2 !h-2"
      />
    </div>
  );
});
