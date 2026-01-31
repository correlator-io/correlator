"use client";

import { AlertTriangle, RefreshCw } from "lucide-react";

interface GlobalErrorProps {
  error: Error & { digest?: string };
  reset: () => void;
}

export default function GlobalError({ error, reset }: GlobalErrorProps) {
  return (
    <html lang="en">
      <body className="bg-background text-foreground">
        <div className="flex min-h-screen items-center justify-center p-6">
          <div className="max-w-md text-center">
            <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-red-100 dark:bg-red-900/20">
              <AlertTriangle className="h-8 w-8 text-red-600 dark:text-red-400" />
            </div>
            <h2 className="mb-2 text-lg font-semibold">
              Application Error
            </h2>
            <p className="mb-4 text-sm text-gray-600 dark:text-gray-400">
              A critical error occurred. Please refresh the page.
            </p>
            {error.message && (
              <p className="mb-4 rounded bg-gray-100 dark:bg-gray-800 p-2 font-mono text-xs text-gray-600 dark:text-gray-400 break-all">
                {error.message}
              </p>
            )}
            <div className="flex justify-center gap-2">
              <button
                onClick={reset}
                className="inline-flex items-center rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 px-4 py-2 text-sm font-medium hover:bg-gray-50 dark:hover:bg-gray-700"
              >
                <RefreshCw className="mr-2 h-4 w-4" />
                Try again
              </button>
              <button
                onClick={() => window.location.reload()}
                className="inline-flex items-center rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
              >
                Refresh page
              </button>
            </div>
          </div>
        </div>
      </body>
    </html>
  );
}
