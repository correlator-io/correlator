import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ProducerIcon } from "@/components/icons/producer-icon";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { formatRelativeTime, formatAbsoluteTime } from "@/lib/utils";
import { ArrowRight } from "lucide-react";
import type { OrphanNamespace } from "@/lib/types";

interface OrphanNamespacesTableProps {
  orphanNamespaces: OrphanNamespace[];
}

export function OrphanNamespacesTable({ orphanNamespaces }: OrphanNamespacesTableProps) {
  return (
    <div className="rounded-lg border overflow-hidden">
      <div className="overflow-x-auto">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/50">
              <TableHead className="font-medium">Namespace</TableHead>
              <TableHead className="font-medium">Producer</TableHead>
              <TableHead className="font-medium">Events</TableHead>
              <TableHead className="font-medium">Last Seen</TableHead>
              <TableHead className="font-medium">Suggested Alias</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {orphanNamespaces.map((ns) => (
              <TableRow key={ns.namespace}>
                <TableCell>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <code className="font-mono text-sm truncate block max-w-[200px]">
                          {ns.namespace}
                        </code>
                      </TooltipTrigger>
                      <TooltipContent>
                        <code className="font-mono text-xs">{ns.namespace}</code>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                </TableCell>
                <TableCell>
                  <ProducerIcon producer={ns.producer} showLabel />
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {ns.eventCount}
                </TableCell>
                <TableCell>
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <span className="text-sm text-muted-foreground cursor-help">
                          {formatRelativeTime(ns.lastSeen)}
                        </span>
                      </TooltipTrigger>
                      <TooltipContent>
                        <p>{formatAbsoluteTime(ns.lastSeen)}</p>
                      </TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                </TableCell>
                <TableCell>
                  {ns.suggestedAlias ? (
                    <div className="flex items-center gap-2">
                      <ArrowRight className="h-4 w-4 text-muted-foreground flex-shrink-0" />
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <code className="font-mono text-sm text-status-passed truncate block max-w-[200px]">
                              {ns.suggestedAlias}
                            </code>
                          </TooltipTrigger>
                          <TooltipContent>
                            <code className="font-mono text-xs">{ns.suggestedAlias}</code>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                    </div>
                  ) : (
                    <span className="text-muted-foreground text-sm">—</span>
                  )}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
