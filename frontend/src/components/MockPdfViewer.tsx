import { Download, Maximize, ZoomIn, ZoomOut } from "lucide-react";

import type { InvoiceCodingResult } from "../types";

type MockPdfViewerProps = {
  fileName: string;
  data: InvoiceCodingResult;
};

export function MockPdfViewer({ fileName, data }: MockPdfViewerProps) {
  const total = parseNumber(data.totalAmount);

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
              <h2>EXCLUSIVE<br />NETWORKS</h2>
              <p>Box 1234, 111 22 Stockholm, Sweden<br />Org.nr: 556123-4567<br />VAT: SE556123456701</p>
            </div>
            <div>
              <h3>INVOICE</h3>
              <dl>
                <dt>Invoice No:</dt>
                <dd>{data.invoiceNumber}</dd>
                <dt>Invoice Date:</dt>
                <dd>{data.date ?? "Unknown"}</dd>
                <dt>Due Date:</dt>
                <dd>2026-04-14</dd>
              </dl>
            </div>
          </header>

          <section className="bill-to">
            <h4>Bill to</h4>
            <p><strong>Telenor Sweden AB</strong><br />Katarinavagen 15<br />116 45 Stockholm<br />Sweden</p>
          </section>

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
              {data.lineItems.map((line, index) => (
                <tr key={line.id}>
                  <td>
                    <strong>{line.description}</strong>
                    {line.description.includes("FC-10") && <small>S/N: FWS2490812{index}</small>}
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
              <span>Subtotal</span>
              <strong>{formatAmount(total * 0.8)}</strong>
            </div>
            <div>
              <span>VAT (25%)</span>
              <strong>{formatAmount(total * 0.2)}</strong>
            </div>
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

function parseNumber(value: string) {
  return Number(value.replace(/,/g, "")) || 0;
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
