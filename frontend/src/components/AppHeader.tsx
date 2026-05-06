import { Zap } from "lucide-react";

export function AppHeader() {
  return (
    <header className="app-header">
      <div className="brand">
        <div className="brand__icon">
          <Zap size={20} />
        </div>
        <div>
          <h1>GL AutoCoder</h1>
          <p>AI-powered GL allocation</p>
        </div>
      </div>
      <nav className="header-actions" aria-label="Application">
        <button type="button">History</button>
        <button type="button">Settings</button>
        <div className="avatar" aria-label="Signed in as TS">
          TS
        </div>
      </nav>
    </header>
  );
}
