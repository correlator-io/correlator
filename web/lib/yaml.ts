import type { OrphanNamespace } from "./types";

/**
 * Generate YAML config example for fixing orphan namespaces.
 *
 * The format matches .correlator.yaml which uses a map structure:
 *   namespace_aliases:
 *     orphan_namespace: "canonical_namespace"
 */
export function generateYamlConfig(orphanNamespaces: OrphanNamespace[]): string {
  const withSuggestions = orphanNamespaces.filter((ns) => ns.suggestedAlias);
  const withoutSuggestions = orphanNamespaces.filter((ns) => !ns.suggestedAlias);

  const suggestedAliases = withSuggestions
    .map((ns) => `  ${ns.namespace}: "${ns.suggestedAlias}"`)
    .join("\n");

  const manualAliases = withoutSuggestions
    .map((ns) => `  # ${ns.namespace}: "your-canonical-namespace"`)
    .join("\n");

  const aliasesSection =
    suggestedAliases || manualAliases
      ? `${suggestedAliases}${suggestedAliases && manualAliases ? "\n" : ""}${manualAliases}`
      : "  # No orphan namespaces detected";

  return `# .correlator.yaml
namespace_aliases:
${aliasesSection}
`;
}
