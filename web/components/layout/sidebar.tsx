"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Link as LinkIcon } from "lucide-react";
import { cn } from "@/lib/utils";
import { navigation } from "@/lib/navigation";

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden lg:fixed lg:inset-y-0 lg:z-50 lg:flex lg:w-56 lg:flex-col">
      <div className="flex grow flex-col gap-y-5 overflow-y-auto border-r border-border bg-card px-4 py-4">
        {/* Logo */}
        <Link
          href="/incidents"
          className="flex h-10 shrink-0 items-center gap-2 px-2"
        >
          <LinkIcon className="h-6 w-6 text-primary" />
          <span className="text-lg font-semibold">Correlator</span>
        </Link>

        {/* Navigation */}
        <nav className="flex flex-1 flex-col">
          <ul role="list" className="flex flex-1 flex-col gap-y-1">
            {navigation.map((item) => {
              const isActive =
                pathname === item.href ||
                (item.href !== "/" && pathname.startsWith(item.href));
              return (
                <li key={item.name}>
                  <Link
                    href={item.href}
                    aria-current={isActive ? "page" : undefined}
                    className={cn(
                      "group flex gap-x-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                      isActive
                        ? "bg-primary text-primary-foreground"
                        : "text-muted-foreground hover:bg-accent hover:text-accent-foreground"
                    )}
                  >
                    <item.icon
                      className={cn(
                        "h-5 w-5 shrink-0",
                        isActive
                          ? "text-primary-foreground"
                          : "text-muted-foreground group-hover:text-accent-foreground"
                      )}
                      aria-hidden="true"
                    />
                    {item.name}
                  </Link>
                </li>
              );
            })}
          </ul>
        </nav>

        {/* Footer */}
        <div className="border-t border-border pt-4">
          <p className="px-2 text-xs text-muted-foreground">
            We don&apos;t monitor.
            <br />
            We correlate.
          </p>
        </div>
      </div>
    </aside>
  );
}
