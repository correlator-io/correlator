"use client";

import { useMemo } from "react";
import { cn } from "@/lib/utils";
import type { DownstreamDataset, UpstreamDataset } from "@/lib/types";
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  ConnectionMode,
  MarkerType,
  Position,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { VerticalDatasetNode } from "./vertical-dataset-node";

interface LineageGraphProps {
  currentDataset: {
    urn: string;
    name: string;
  };
  upstream: UpstreamDataset[];
  downstream: DownstreamDataset[];
  className?: string;
}

// Custom node types
const nodeTypes = {
  dataset: VerticalDatasetNode,
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

export function LineageGraph({
  currentDataset,
  upstream,
  downstream,
  className,
}: LineageGraphProps) {
  // Build nodes and edges for vertical layout
  const { nodes, edges } = useMemo(() => {
    const initialNodes: Node[] = [];
    const initialEdges: Edge[] = [];

    // Node spacing for vertical layout
    const nodeSpacingX = 200;
    const nodeSpacingY = 120;

    // Dedupe upstream and downstream by URN
    const uniqueUpstream = [...new Map(upstream.map((u) => [u.urn, u])).values()];
    const uniqueDownstream = [...new Map(downstream.map((d) => [d.urn, d])).values()];

    // Group upstream by depth (for positioning)
    const upstreamByDepth = new Map<number, UpstreamDataset[]>();
    uniqueUpstream.forEach((u) => {
      const list = upstreamByDepth.get(u.depth) || [];
      list.push(u);
      upstreamByDepth.set(u.depth, list);
    });

    // Group downstream by depth
    const downstreamByDepth = new Map<number, DownstreamDataset[]>();
    uniqueDownstream.forEach((d) => {
      const list = downstreamByDepth.get(d.depth) || [];
      list.push(d);
      downstreamByDepth.set(d.depth, list);
    });

    // Calculate max depth for upstream positioning
    const maxUpstreamDepth = Math.max(0, ...uniqueUpstream.map((u) => u.depth));

    // Position upstream nodes (above current)
    // Higher depth = further up
    upstreamByDepth.forEach((datasets, depth) => {
      const yPos = -(maxUpstreamDepth - depth + 1) * nodeSpacingY;
      datasets.forEach((ds, index) => {
        const xOffset = (index - (datasets.length - 1) / 2) * nodeSpacingX;
        initialNodes.push({
          id: ds.urn,
          position: { x: xOffset, y: yPos },
          data: {
            label: ds.name,
            direction: "upstream" as const,
            depth: ds.depth,
          },
          type: "dataset",
        });
      });
    });

    // Position current dataset (center)
    initialNodes.push({
      id: currentDataset.urn,
      position: { x: 0, y: 0 },
      data: {
        label: currentDataset.name,
        isCurrent: true,
      },
      type: "dataset",
    });

    // Position downstream nodes (below current)
    downstreamByDepth.forEach((datasets, depth) => {
      const yPos = depth * nodeSpacingY;
      datasets.forEach((ds, index) => {
        const xOffset = (index - (datasets.length - 1) / 2) * nodeSpacingX;
        initialNodes.push({
          id: ds.urn,
          position: { x: xOffset, y: yPos },
          data: {
            label: ds.name,
            direction: "downstream" as const,
            depth: ds.depth,
          },
          type: "dataset",
        });
      });
    });

    // Create edges for upstream (use ALL entries, not deduplicated)
    // This ensures all parent→child relationships are captured
    upstream.forEach((u) => {
      const edgeId = `${u.urn}-${u.childUrn}`;
      // Avoid duplicate edges
      if (!initialEdges.some((e) => e.id === edgeId)) {
        initialEdges.push({
          id: edgeId,
          source: u.urn,
          target: u.childUrn,
          sourceHandle: "bottom",
          targetHandle: "top",
        });
      }
    });

    // Create edges for downstream (use ALL entries, not deduplicated)
    // This ensures all parent→child relationships are captured
    downstream.forEach((d) => {
      const edgeId = `${d.parentUrn}-${d.urn}`;
      // Avoid duplicate edges
      if (!initialEdges.some((e) => e.id === edgeId)) {
        initialEdges.push({
          id: edgeId,
          source: d.parentUrn,
          target: d.urn,
          sourceHandle: "bottom",
          targetHandle: "top",
        });
      }
    });

    return { nodes: initialNodes, edges: initialEdges };
  }, [currentDataset, upstream, downstream]);

  return (
    <div className={cn("h-[400px]", className)}>
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
