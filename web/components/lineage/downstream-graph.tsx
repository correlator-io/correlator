"use client";

import { useMemo } from "react";
import { cn } from "@/lib/utils";
import type { DownstreamDataset } from "@/lib/types";
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  ConnectionMode,
  MarkerType,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { DatasetNode } from "./dataset-node";

interface DownstreamGraphProps {
  sourceDataset: {
    urn: string;
    name: string;
  };
  downstream: DownstreamDataset[];
  className?: string;
}

// Custom node types
const nodeTypes = {
  dataset: DatasetNode,
};

// Default edge style
const defaultEdgeOptions = {
  animated: true,
  markerEnd: {
    type: MarkerType.ArrowClosed,
    width: 16,
    height: 16,
  },
  style: {
    strokeWidth: 2,
  },
};

export function DownstreamGraph({
  sourceDataset,
  downstream,
  className,
}: DownstreamGraphProps) {
  // Build nodes and edges from downstream data
  const { nodes, edges } = useMemo(() => {
    const initialNodes: Node[] = [
      {
        id: sourceDataset.urn,
        position: { x: 0, y: 0 },
        data: { label: sourceDataset.name, isSource: true },
        type: "dataset",
      },
    ];

    const initialEdges: Edge[] = [];

    // Group downstream by depth for positioning
    const byDepth = new Map<number, DownstreamDataset[]>();
    downstream.forEach((ds) => {
      const list = byDepth.get(ds.depth) || [];
      list.push(ds);
      byDepth.set(ds.depth, list);
    });

    // Position nodes by depth
    const nodeSpacingX = 280;
    const nodeSpacingY = 100;

    byDepth.forEach((datasets, depth) => {
      datasets.forEach((ds, index) => {
        const yOffset = (index - (datasets.length - 1) / 2) * nodeSpacingY;
        initialNodes.push({
          id: ds.urn,
          position: { x: depth * nodeSpacingX, y: yOffset },
          data: { label: ds.name, depth: ds.depth },
          type: "dataset",
        });
      });
    });

    // Create edges using parentUrn for accurate connections
    downstream.forEach((ds) => {
      initialEdges.push({
        id: `${ds.parentUrn}-${ds.urn}`,
        source: ds.parentUrn,
        target: ds.urn,
      });
    });

    return { nodes: initialNodes, edges: initialEdges };
  }, [sourceDataset, downstream]);

  return (
    <div className={cn("h-[300px]", className)}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        connectionMode={ConnectionMode.Loose}
        fitView
        fitViewOptions={{ padding: 0.3 }}
        proOptions={{ hideAttribution: true }}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        panOnDrag={true}
        zoomOnScroll={true}
      >
        <Background className="!bg-background" />
        <Controls
          showInteractive={false}
          className="!bg-card !border-border !shadow-sm [&>button]:!bg-card [&>button]:!border-border [&>button]:!text-foreground [&>button:hover]:!bg-muted"
        />
      </ReactFlow>
    </div>
  );
}
