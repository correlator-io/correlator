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
  // Build nodes and edges for horizontal layout (left-to-right)
  const { nodes, edges } = useMemo(() => {
    const initialNodes: Node[] = [];
    const initialEdges: Edge[] = [];

    // Node spacing for horizontal layout
    const nodeSpacingX = 250;
    const nodeSpacingY = 100;

    // Dedupe upstream and downstream by URN, keeping the entry with MINIMUM depth
    // This ensures nodes are positioned at their closest distance to the current dataset
    const upstreamMap = new Map<string, UpstreamDataset>();
    upstream.forEach((u) => {
      const existing = upstreamMap.get(u.urn);
      if (!existing || u.depth < existing.depth) {
        upstreamMap.set(u.urn, u);
      }
    });
    const uniqueUpstream = [...upstreamMap.values()];

    const downstreamMap = new Map<string, DownstreamDataset>();
    downstream.forEach((d) => {
      const existing = downstreamMap.get(d.urn);
      if (!existing || d.depth < existing.depth) {
        downstreamMap.set(d.urn, d);
      }
    });
    const uniqueDownstream = [...downstreamMap.values()];

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

    // Position upstream nodes (to the left of current)
    // Higher depth = further left (depth 1 closest to current, depth 2 further away)
    upstreamByDepth.forEach((datasets, depth) => {
      const xPos = -depth * nodeSpacingX;
      datasets.forEach((ds, index) => {
        const yOffset = (index - (datasets.length - 1) / 2) * nodeSpacingY;
        initialNodes.push({
          id: ds.urn,
          position: { x: xPos, y: yOffset },
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

    // Position downstream nodes (to the right of current)
    downstreamByDepth.forEach((datasets, depth) => {
      const xPos = depth * nodeSpacingX;
      datasets.forEach((ds, index) => {
        const yOffset = (index - (datasets.length - 1) / 2) * nodeSpacingY;
        initialNodes.push({
          id: ds.urn,
          position: { x: xPos, y: yOffset },
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
          sourceHandle: "right",
          targetHandle: "left",
        });
      }
    });

    // Create edges for downstream (use ALL entries, not deduplicated)
    // This ensures all parent→child relationships are captured
    downstream.forEach((d) => {
      const edgeId = `${d.parentUrn}-${d.urn}`;
      // Avoid duplicate edges
      if (!initialEdges.some((e) => e.id === edgeId)) {
        // Check if source and target are both downstream nodes at the same depth
        // If so, use top/bottom handles for a cleaner vertical connection
        const sourceNode = downstreamMap.get(d.parentUrn);
        const targetNode = downstreamMap.get(d.urn);

        // Only use same-level logic if BOTH nodes are found in downstream map
        // (not the current/affected node which isn't in the map)
        const bothAreDownstream = sourceNode !== undefined && targetNode !== undefined;
        const sameLevelEdge = bothAreDownstream && sourceNode.depth === targetNode.depth;

        if (sameLevelEdge) {
          // Determine vertical position to choose top vs bottom handle
          const depthGroup = downstreamByDepth.get(sourceNode.depth) || [];
          const sourceIndex = depthGroup.findIndex(ds => ds.urn === d.parentUrn);
          const targetIndex = depthGroup.findIndex(ds => ds.urn === d.urn);
          const sourceAboveTarget = sourceIndex < targetIndex;

          initialEdges.push({
            id: edgeId,
            source: d.parentUrn,
            target: d.urn,
            sourceHandle: sourceAboveTarget ? "bottom-source" : "top-source",
            targetHandle: sourceAboveTarget ? "top-target" : "bottom-target",
          });
        } else {
          // Different depths - use left/right handles
          initialEdges.push({
            id: edgeId,
            source: d.parentUrn,
            target: d.urn,
            sourceHandle: "right",
            targetHandle: "left",
          });
        }
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
