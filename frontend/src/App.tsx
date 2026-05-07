import { useCallback, useState } from "react";
import { AlertCircle } from "lucide-react";

import { codeInvoice, codeInvoices } from "./lib/api";
import { addResultsToCodingHistory, loadCodingHistory } from "./lib/history";
import type { BatchInvoiceError, CodingHistoryEntry, InvoiceCodingResult } from "./types";
import { AppHeader } from "./components/AppHeader";
import { HistoryView } from "./components/HistoryView";
import { ProcessingPanel } from "./components/ProcessingPanel";
import { ResultsView } from "./components/ResultsView";
import { UploadPanel } from "./components/UploadPanel";

type AppState = "idle" | "processing" | "complete" | "error";
type ActiveView = "upload" | "history";

export default function App() {
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [state, setState] = useState<AppState>("idle");
  const [activeView, setActiveView] = useState<ActiveView>("upload");
  const [results, setResults] = useState<InvoiceCodingResult[]>([]);
  const [codingHistory, setCodingHistory] = useState<CodingHistoryEntry[]>(() => loadCodingHistory());
  const [batchErrors, setBatchErrors] = useState<BatchInvoiceError[]>([]);
  const [error, setError] = useState<string | null>(null);

  const processFiles = useCallback(async (files: File[]) => {
    setActiveView("upload");
    setSelectedFiles(files);
    setResults([]);
    setBatchErrors([]);
    setError(null);
    setState("processing");

    try {
      let codedResults: InvoiceCodingResult[];
      if (files.length === 1) {
        const response = await codeInvoice(files[0]);
        codedResults = [response];
      } else {
        const response = await codeInvoices(files);
        codedResults = response.invoices;
        setBatchErrors(response.errors);
      }
      setResults(codedResults);
      if (codedResults.length > 0) {
        setCodingHistory((history) => addResultsToCodingHistory(history, codedResults));
      }
      setState("complete");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Invoice coding failed.");
      setState("error");
    }
  }, []);

  const reset = () => {
    setActiveView("upload");
    setSelectedFiles([]);
    setResults([]);
    setBatchErrors([]);
    setError(null);
    setState("idle");
  };

  const mainClassName =
    results.length > 0 || (activeView === "history" && codingHistory.length > 0)
      ? "main main--wide"
      : "main";

  return (
    <div className="app-shell">
      <AppHeader
        activeView={activeView}
        historyCount={codingHistory.length}
        onShowUpload={reset}
        onShowHistory={() => setActiveView("history")}
      />
      <main className={mainClassName}>
        {activeView === "history" && <HistoryView entries={codingHistory} onUpload={reset} />}
        {activeView === "upload" && (
          <>
            {state === "idle" && <UploadPanel onUpload={processFiles} />}
            {state === "processing" && (
              <ProcessingPanel
                fileName={selectedFiles[0]?.name ?? "Invoice"}
                fileCount={selectedFiles.length || 1}
              />
            )}
            {state === "complete" && results.length > 0 && (
              <ResultsView results={results} files={selectedFiles} errors={batchErrors} onReset={reset} />
            )}
            {state === "complete" && results.length === 0 && batchErrors.length > 0 && (
              <section className="error-card" aria-live="polite">
                <AlertCircle size={34} />
                <h2>Could not process invoices</h2>
                <p>All {batchErrors.length} uploads failed. Check that every file is a PDF under 10 MB.</p>
                <div className="batch-errors">
                  {batchErrors.map((item) => (
                    <p key={item.fileName ?? item.error}>
                      <strong>{item.fileName ?? "Unknown file"}:</strong> {item.error}
                    </p>
                  ))}
                </div>
                <div className="error-card__actions">
                  <button className="button button--primary" onClick={reset}>
                    Try another batch
                  </button>
                </div>
              </section>
            )}
            {state === "error" && (
              <section className="error-card" aria-live="polite">
                <AlertCircle size={34} />
                <h2>Could not process invoice</h2>
                <p>{error}</p>
                <div className="error-card__actions">
                  <button className="button button--primary" onClick={reset}>
                    Try another file
                  </button>
                </div>
              </section>
            )}
          </>
        )}
      </main>
    </div>
  );
}
