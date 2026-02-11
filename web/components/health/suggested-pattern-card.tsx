"use client";

import { Lightbulb } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { YamlConfigBlock } from "./yaml-config-block";
import type { SuggestedPattern } from "@/lib/types";

interface SuggestedPatternCardProps {
  patterns: SuggestedPattern[];
}

/**
 * Generates YAML config string for dataset patterns.
 */
function generatePatternYaml(patterns: SuggestedPattern[]): string {
  if (patterns.length === 0) return "";

  const lines = ["dataset_patterns:"];
  for (const pattern of patterns) {
    lines.push(`  - pattern: "${pattern.pattern}"`);
    lines.push(`    canonical: "${pattern.canonical}"`);
  }
  return lines.join("\n");
}

/**
 * Calculate total orphans resolved across all patterns.
 */
function getTotalResolved(patterns: SuggestedPattern[]): number {
  // Use Set to avoid counting duplicates if multiple patterns resolve the same orphan
  const resolved = new Set<string>();
  for (const pattern of patterns) {
    for (const orphan of pattern.orphansResolved) {
      resolved.add(orphan);
    }
  }
  return resolved.size;
}

export function SuggestedPatternCard({ patterns }: SuggestedPatternCardProps) {
  if (patterns.length === 0) {
    return null;
  }

  const yaml = generatePatternYaml(patterns);
  const totalResolved = getTotalResolved(patterns);
  const patternWord = patterns.length === 1 ? "pattern" : "patterns";
  const datasetWord = totalResolved === 1 ? "dataset" : "datasets";

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Lightbulb className="h-5 w-5 text-yellow-500" aria-hidden="true" />
          Suggested Fix
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          Add {patterns.length === 1 ? "this pattern" : `these ${patterns.length} ${patternWord}`} to
          resolve {totalResolved} orphan {datasetWord}:
        </p>

        <YamlConfigBlock yaml={yaml} />

        <p className="text-xs text-muted-foreground">
          Add to your config file (<code className="font-mono bg-muted px-1 py-0.5 rounded">CORRELATOR_CONFIG_PATH</code>)
          and restart the API server for changes to take effect.
        </p>
      </CardContent>
    </Card>
  );
}
