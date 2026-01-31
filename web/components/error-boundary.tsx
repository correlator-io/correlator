"use client";

import { Component, type ReactNode } from "react";
import { AlertTriangle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    // Log error to console in development
    console.error("ErrorBoundary caught an error:", error, errorInfo);
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null });
  };

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <div className="flex min-h-[400px] items-center justify-center p-6">
          <div className="max-w-md text-center">
            <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-destructive/10">
              <AlertTriangle className="h-8 w-8 text-destructive" />
            </div>
            <h2 className="mb-2 text-lg font-semibold">Something went wrong</h2>
            <p className="mb-4 text-sm text-muted-foreground">
              An unexpected error occurred. Please try refreshing the page.
            </p>
            {this.state.error && (
              <p className="mb-4 rounded bg-muted p-2 font-mono text-xs text-muted-foreground">
                {this.state.error.message}
              </p>
            )}
            <div className="flex justify-center gap-2">
              <Button variant="outline" onClick={this.handleReset}>
                <RefreshCw className="mr-2 h-4 w-4" />
                Try again
              </Button>
              <Button
                variant="default"
                onClick={() => window.location.reload()}
              >
                Refresh page
              </Button>
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
