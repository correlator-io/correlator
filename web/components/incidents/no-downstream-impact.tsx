import { Card, CardContent } from "@/components/ui/card";
import { CheckCircle2, Leaf } from "lucide-react";

interface NoDownstreamImpactProps {
  datasetName: string;
}

export function NoDownstreamImpact({ datasetName }: NoDownstreamImpactProps) {
  return (
    <Card className="border-status-passed/30 bg-status-passed/5">
      <CardContent className="py-8 text-center">
        <div className="flex justify-center mb-4">
          <div className="relative">
            <CheckCircle2 className="h-12 w-12 text-status-passed" />
            <Leaf className="h-5 w-5 text-status-passed absolute -bottom-1 -right-1" />
          </div>
        </div>
        <h3 className="font-medium text-lg">No Downstream Impact</h3>
        <p className="text-sm text-muted-foreground mt-2 max-w-md mx-auto">
          <span className="font-mono">{datasetName}</span> is a leaf node with no dependent datasets.
          Any issues here won&apos;t cascade to other tables.
        </p>
      </CardContent>
    </Card>
  );
}
