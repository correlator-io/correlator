"use client";

import { cn } from "@/lib/utils";
import type { DownstreamDataset } from "@/lib/types";
import {
  ReactFlow,
  Node,
  Edge,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  ConnectionMode,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

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

  // Create edges using parentUrn for accurate connections
  downstream.forEach((ds) => {
    initialEdges.push({
      id: `${ds.parentUrn}-${ds.urn}`,
      source: ds.parentUrn,
      target: ds.urn,
      animated: true,
    });
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
