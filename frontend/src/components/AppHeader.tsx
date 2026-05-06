import { Zap } from "lucide-react";

type AppHeaderProps = {
  activeView: "upload" | "history";
  historyCount: number;
  onShowUpload: () => void;
  onShowHistory: () => void;
};

export function AppHeader({ activeView, historyCount, onShowUpload, onShowHistory }: AppHeaderProps) {
  return (
    <header className="app-header">
      <button className="brand brand--button" type="button" onClick={onShowUpload}>
        <div className="brand__icon">
          <Zap size={20} />
        </div>
        <div>
          <h1>GL AutoCoder</h1>
          <p>AI-powered GL allocation</p>
        </div>
      </button>
      <nav className="header-actions" aria-label="Application">
        <button
          className={
            activeView === "history"
              ? "header-actions__button header-actions__button--active"
              : "header-actions__button"
          }
          type="button"
          onClick={onShowHistory}
        >
          History
          {historyCount > 0 && <span>{historyCount}</span>}
        </button>
        <button type="button">Settings</button>
        <div className="avatar" aria-label="Signed in as TS">
          TS
        </div>
      </nav>
    </header>
  );
}
