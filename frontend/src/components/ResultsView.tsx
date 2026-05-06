import { FileCheck } from "lucide-react";

import type { InvoiceCodingResult } from "../types";
import { LineItemCard } from "./LineItemCard";
import { MockPdfViewer } from "./MockPdfViewer";

type ResultsViewProps = {
  result: InvoiceCodingResult;
  file: File | null;
  onReset: () => void;
};

export function ResultsView({ result, file, onReset }: ResultsViewProps) {
  return (
    <section className="results-layout">
      <MockPdfViewer fileName={file?.name ?? result.sourceFileName ?? "invoice.pdf"} data={result} />

      <div className="results-panel">
        <div className="results-header">
          <h2>
            <FileCheck size={28} />
            Coding complete
          </h2>
          <button className="button button--secondary" type="button" onClick={onReset}>
            Upload another
          </button>
        </div>

        <div className="summary-card">
          <SummaryField label="Vendor" value={result.vendor} strong />
          <SummaryField label="Invoice number" value={result.invoiceNumber} />
          <SummaryField label="Date" value={result.date ?? "Unknown"} />
          <SummaryField label="Total" value={`${result.totalAmount} ${result.currency}`} strong />
        </div>

        <h3 className="section-title">Line Items & GL Prediction</h3>
        <div className="line-list">
          {result.lineItems.map((line, index) => (
            <LineItemCard key={line.id} line={line} index={index} currency={result.currency} />
          ))}
        </div>
      </div>
    </section>
  );
}

function SummaryField({ label, value, strong = false }: { label: string; value: string; strong?: boolean }) {
  return (
    <div className="summary-field">
      <span>{label}</span>
      <strong className={strong ? "summary-field__value summary-field__value--strong" : "summary-field__value"}>
        {value}
      </strong>
    </div>
  );
}
