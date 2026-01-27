"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { AlertCircle, ArrowLeft, Home } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function NotFound() {
  const router = useRouter();

  return (
    <div className="flex min-h-[80vh] flex-col items-center justify-center px-4 text-center">
      <div className="rounded-full bg-muted p-4">
        <AlertCircle className="h-12 w-12 text-muted-foreground" />
      </div>

      <h1 className="mt-6 text-2xl font-semibold">Page not found</h1>

      <p className="mt-2 max-w-md text-muted-foreground">
        The page you&apos;re looking for doesn&apos;t exist or has been moved.
        If you were looking for an incident, it may have been resolved.
      </p>

      <div className="mt-8 flex gap-3">
        <Button variant="outline" onClick={() => router.back()}>
          <ArrowLeft className="mr-2 h-4 w-4" />
          Go back
        </Button>
        <Button asChild>
          <Link href="/incidents">
            <Home className="mr-2 h-4 w-4" />
            View incidents
          </Link>
        </Button>
      </div>
    </div>
  );
}
