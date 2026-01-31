import type { OrphanNamespace } from "./types";

/**
 * Generate YAML config example for fixing orphan namespaces
 */
export function generateYamlConfig(orphanNamespaces: OrphanNamespace[]): string {
  const aliases = orphanNamespaces
    .filter((ns) => ns.suggestedAlias)
    .map(
      (ns) =>
        `  # ${ns.producer} uses "${ns.namespace}"\n  - from: "${ns.namespace}"\n    to: "${ns.suggestedAlias}"`
    )
    .join("\n\n");

  return `# correlator.yaml
namespace_aliases:
${aliases || "  # No suggested aliases"}

# Add manual aliases for remaining namespaces:
${orphanNamespaces
  .filter((ns) => !ns.suggestedAlias)
  .map((ns) => `  # - from: "${ns.namespace}"\n  #   to: "your-canonical-namespace"`)
  .join("\n")}
`;
}
