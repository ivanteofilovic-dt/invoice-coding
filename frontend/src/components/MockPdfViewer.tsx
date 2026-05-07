import { Download, Maximize, ZoomIn, ZoomOut } from "lucide-react";

import type { InvoiceCodingResult } from "../types";

type MockPdfViewerProps = {
  fileName: string;
  data: InvoiceCodingResult;
};

export function MockPdfViewer({ fileName, data }: MockPdfViewerProps) {
  return (
    <aside className="pdf-viewer" aria-label="Invoice preview">
      <div className="pdf-toolbar">
        <span title={fileName}>{fileName}</span>
        <span>1 / 1</span>
        <div>
          <ZoomOut size={16} />
          <span>100%</span>
          <ZoomIn size={16} />
          <Maximize size={16} />
          <Download size={16} />
        </div>
      </div>

      <div className="pdf-scroll">
        <div className="invoice-page">
          <header className="invoice-page__header">
            <div>
              <h2 style={{ whiteSpace: "pre-line" }}>{data.vendor.trim() || "—"}</h2>
            </div>
            <div>
              <h3>INVOICE</h3>
              <dl>
                <dt>Invoice No:</dt>
                <dd>{data.invoiceNumber || "—"}</dd>
                <dt>Invoice Date:</dt>
                <dd>{data.date ?? "—"}</dd>
              </dl>
            </div>
          </header>

          <table className="invoice-lines">
            <thead>
              <tr>
                <th>Description</th>
                <th>Qty</th>
                <th>Unit Price</th>
                <th>Total ({data.currency})</th>
              </tr>
            </thead>
            <tbody>
              {data.lineItems.map((line) => (
                <tr key={line.id}>
                  <td>
                    <strong>{line.description}</strong>
                  </td>
                  <td>{line.quantity ?? "N/A"}</td>
                  <td>{formatAmount(line.unitPrice)}</td>
                  <td>{formatAmount(line.total)}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <footer className="invoice-total">
            <div>
              <span>Total {data.currency}</span>
              <strong>{data.totalAmount}</strong>
            </div>
          </footer>
        </div>
      </div>
    </aside>
  );
}

function formatAmount(value: number | null) {
  if (value === null) {
    return "N/A";
  }
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
}
