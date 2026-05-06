import { AlertCircle, FileCheck } from "lucide-react";
import { useMemo, useState } from "react";

import type { BatchInvoiceError, InvoiceCodingResult } from "../types";
import { LineItemCard } from "./LineItemCard";
import { MockPdfViewer } from "./MockPdfViewer";

type ResultsViewProps = {
  results: InvoiceCodingResult[];
  files: File[];
  errors: BatchInvoiceError[];
  onReset: () => void;
};

export function ResultsView({ results, files, errors, onReset }: ResultsViewProps) {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const result = results[selectedIndex] ?? results[0];
  const file = useMemo(() => findFileForResult(result, files, selectedIndex), [files, result, selectedIndex]);
  const isBatch = results.length > 1 || errors.length > 0;

  return (
    <section className="results-layout">
      <MockPdfViewer fileName={file?.name ?? result.sourceFileName ?? "invoice.pdf"} data={result} />

      <div className="results-panel">
        <div className="results-sticky-top">
          <div className="results-header">
            <h2>
              <FileCheck size={28} />
              {isBatch ? "Batch complete" : "Coding complete"}
            </h2>
            <button className="button button--secondary" type="button" onClick={onReset}>
              Upload more
            </button>
          </div>

          {isBatch && (
            <BatchSummary
              results={results}
              errors={errors}
              selectedIndex={selectedIndex}
              onSelect={setSelectedIndex}
            />
          )}
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

function BatchSummary({
  results,
  errors,
  selectedIndex,
  onSelect
}: {
  results: InvoiceCodingResult[];
  errors: BatchInvoiceError[];
  selectedIndex: number;
  onSelect: (index: number) => void;
}) {
  return (
    <div className="batch-summary">
      <div className="batch-summary__stats">
        <span>
          <strong>{results.length}</strong> coded
        </span>
        <span>
          <strong>{errors.length}</strong> failed
        </span>
      </div>

      <div className="invoice-tabs" aria-label="Coded invoices">
        {results.map((item, index) => (
          <button
            className={index === selectedIndex ? "invoice-tab invoice-tab--active" : "invoice-tab"}
            key={`${item.sourceFileName ?? item.invoiceNumber}-${index}`}
            type="button"
            onClick={() => onSelect(index)}
          >
            <span>{item.sourceFileName ?? item.invoiceNumber}</span>
            <small>{item.vendor}</small>
          </button>
        ))}
      </div>

      {errors.length > 0 && (
        <div className="batch-errors" aria-live="polite">
          {errors.map((item) => (
            <p key={item.fileName ?? item.error}>
              <AlertCircle size={15} />
              <span>
                <strong>{item.fileName ?? "Unknown file"}:</strong> {item.error}
              </span>
            </p>
          ))}
        </div>
      )}
    </div>
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

function findFileForResult(result: InvoiceCodingResult, files: File[], fallbackIndex: number) {
  if (result.sourceFileName) {
    return files.find((file) => file.name === result.sourceFileName) ?? null;
  }
  return files[fallbackIndex] ?? null;
}
