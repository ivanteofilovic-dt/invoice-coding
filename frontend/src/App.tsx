import { useCallback, useState } from "react";
import { AlertCircle } from "lucide-react";

import { fetchDemoInvoice, codeInvoice, codeInvoices } from "./lib/api";
import type { BatchInvoiceError, InvoiceCodingResult } from "./types";
import { AppHeader } from "./components/AppHeader";
import { ProcessingPanel } from "./components/ProcessingPanel";
import { ResultsView } from "./components/ResultsView";
import { UploadPanel } from "./components/UploadPanel";

type AppState = "idle" | "processing" | "complete" | "error";

export default function App() {
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [state, setState] = useState<AppState>("idle");
  const [results, setResults] = useState<InvoiceCodingResult[]>([]);
  const [batchErrors, setBatchErrors] = useState<BatchInvoiceError[]>([]);
  const [error, setError] = useState<string | null>(null);

  const processFiles = useCallback(async (files: File[]) => {
    setSelectedFiles(files);
    setResults([]);
    setBatchErrors([]);
    setError(null);
    setState("processing");

    try {
      if (files.length === 1) {
        const response = await codeInvoice(files[0]);
        setResults([response]);
      } else {
        const response = await codeInvoices(files);
        setResults(response.invoices);
        setBatchErrors(response.errors);
      }
      setState("complete");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Invoice coding failed.");
      setState("error");
    }
  }, []);

  const loadDemo = useCallback(async () => {
    setError(null);
    setBatchErrors([]);
    setState("processing");
    setSelectedFiles([]);

    try {
      const response = await fetchDemoInvoice();
      setResults([response]);
      setState("complete");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Demo invoice could not be loaded.");
      setState("error");
    }
  }, []);

  const reset = () => {
    setSelectedFiles([]);
    setResults([]);
    setBatchErrors([]);
    setError(null);
    setState("idle");
  };

  return (
    <div className="app-shell">
      <AppHeader />
      <main className={results.length > 0 ? "main main--wide" : "main"}>
        {state === "idle" && <UploadPanel onUpload={processFiles} onDemo={loadDemo} />}
        {state === "processing" && (
          <ProcessingPanel fileName={selectedFiles[0]?.name ?? "Demo invoice"} fileCount={selectedFiles.length || 1} />
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
              <button className="button button--secondary" onClick={reset}>
                Try another batch
              </button>
              <button className="button button--primary" onClick={loadDemo}>
                Load demo result
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
              <button className="button button--secondary" onClick={reset}>
                Try another file
              </button>
              <button className="button button--primary" onClick={loadDemo}>
                Load demo result
              </button>
            </div>
          </section>
        )}
      </main>
    </div>
  );
}
