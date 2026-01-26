import { Sidebar } from "./sidebar";
import { Header } from "./header";

interface AppShellProps {
  title: string;
  children: React.ReactNode;
}

export function AppShell({ title, children }: AppShellProps) {
  return (
    <div className="min-h-screen">
      <Sidebar />
      <div className="lg:pl-56">
        <Header title={title} />
        <main className="p-4 lg:p-6">{children}</main>
      </div>
    </div>
  );
}
