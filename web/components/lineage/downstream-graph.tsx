"use client";

import { cn } from "@/lib/utils";
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  ConnectionMode,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

export interface DownstreamDataset {
  urn: string;
  name: string;
  depth: number;
}

interface DownstreamGraphProps {
  sourceDataset: {
    urn: string;
    name: string;
  };
  downstream: DownstreamDataset[];
  className?: string;
}

export function DownstreamGraph({
  sourceDataset,
  downstream,
  className,
}: DownstreamGraphProps) {
  // Build nodes and edges from downstream data
  const initialNodes: Node[] = [
    {
      id: sourceDataset.urn,
      position: { x: 0, y: 0 },
      data: { label: sourceDataset.name, isSource: true },
      type: "default",
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
  const nodeSpacingX = 250;
  const nodeSpacingY = 80;

  byDepth.forEach((datasets, depth) => {
    datasets.forEach((ds, index) => {
      const yOffset = (index - (datasets.length - 1) / 2) * nodeSpacingY;
      initialNodes.push({
        id: ds.urn,
        position: { x: depth * nodeSpacingX, y: yOffset },
        data: { label: ds.name, depth: ds.depth },
        type: "default",
      });
    });
  });

  // Create edges - connect each node to source or parent depth
  downstream.forEach((ds) => {
    if (ds.depth === 1) {
      // Connect directly to source
      initialEdges.push({
        id: `${sourceDataset.urn}-${ds.urn}`,
        source: sourceDataset.urn,
        target: ds.urn,
        animated: true,
      });
    } else {
      // Find a parent at depth - 1 (simplified: connect to first parent)
      const parents = byDepth.get(ds.depth - 1) || [];
      if (parents.length > 0) {
        initialEdges.push({
          id: `${parents[0].urn}-${ds.urn}`,
          source: parents[0].urn,
          target: ds.urn,
          animated: true,
        });
      }
    }
  });

  const [nodes] = useNodesState(initialNodes);
  const [edges] = useEdgesState(initialEdges);

  return (
    <div className={cn("h-[300px]", className)}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        connectionMode={ConnectionMode.Loose}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
