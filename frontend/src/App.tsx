import { useCallback, useState } from "react";
import { AlertCircle } from "lucide-react";

import { fetchDemoInvoice, codeInvoice } from "./lib/api";
import type { InvoiceCodingResult } from "./types";
import { AppHeader } from "./components/AppHeader";
import { ProcessingPanel } from "./components/ProcessingPanel";
import { ResultsView } from "./components/ResultsView";
import { UploadPanel } from "./components/UploadPanel";

type AppState = "idle" | "processing" | "complete" | "error";

export default function App() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [state, setState] = useState<AppState>("idle");
  const [result, setResult] = useState<InvoiceCodingResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const processFile = useCallback(async (file: File) => {
    setSelectedFile(file);
    setResult(null);
    setError(null);
    setState("processing");

    try {
      const response = await codeInvoice(file);
      setResult(response);
      setState("complete");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Invoice coding failed.");
      setState("error");
    }
  }, []);

  const loadDemo = useCallback(async () => {
    setError(null);
    setState("processing");
    setSelectedFile(null);

    try {
      const response = await fetchDemoInvoice();
      setResult(response);
      setState("complete");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Demo invoice could not be loaded.");
      setState("error");
    }
  }, []);

  const reset = () => {
    setSelectedFile(null);
    setResult(null);
    setError(null);
    setState("idle");
  };

  return (
    <div className="app-shell">
      <AppHeader />
      <main className={result ? "main main--wide" : "main"}>
        {state === "idle" && <UploadPanel onUpload={processFile} onDemo={loadDemo} />}
        {state === "processing" && <ProcessingPanel fileName={selectedFile?.name ?? "Demo invoice"} />}
        {state === "complete" && result && <ResultsView result={result} file={selectedFile} onReset={reset} />}
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
