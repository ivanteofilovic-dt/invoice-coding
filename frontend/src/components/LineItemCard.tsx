import { useState } from "react";
import { AlertCircle, ChevronDown, ChevronUp, Cpu, Database, History } from "lucide-react";

import type { InvoiceLineItem } from "../types";

type LineItemCardProps = {
  line: InvoiceLineItem;
  index: number;
  currency: string;
};

const codingLabels = ["ACCOUNT", "DEPARTMENT", "PRODUCT", "IC", "PROJECT", "SYSTEM", "RESERVE"] as const;

export function LineItemCard({ line, index, currency }: LineItemCardProps) {
  const [expanded, setExpanded] = useState(index === 0);

  return (
    <article className="line-card">
      <button className="line-card__summary" type="button" onClick={() => setExpanded((current) => !current)}>
        <div>
          <div className="line-card__title">
            <span>#{index + 1}</span>
            <strong>{line.description}</strong>
          </div>
          <div className="line-card__meta">
            <span>Qty: {formatNumber(line.quantity)}</span>
            <span>Unit: {formatMoney(line.unitPrice, currency)}</span>
            <span>Total: {formatMoney(line.total, currency)}</span>
          </div>
        </div>
        <div className="line-card__confidence">
          <span>AI confidence</span>
          <strong className={confidenceClass(line.confidence)}>{formatConfidence(line.confidence)}</strong>
          {expanded ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
        </div>
      </button>

      {expanded && (
        <div className="line-card__details">
          <div className="prediction-panel">
            <div>
              <h4>
                <Database size={17} />
                Predicted general ledger coding
              </h4>
              <div className="coding-grid">
                {codingLabels.map((label) => (
                  <div className={`coding-field ${label === "ACCOUNT" || label === "DEPARTMENT" ? "coding-field--primary" : ""}`} key={label}>
                    <span>{label}</span>
                    <strong>{line.coding[label]}</strong>
                  </div>
                ))}
              </div>
            </div>

            <div className="reasoning-card">
              <h4>
                <Cpu size={17} />
                Gemini reasoning
              </h4>
              <p>{line.reasoning ?? "No reasoning summary was returned for this line."}</p>
            </div>
          </div>

          <aside className="history-panel">
            <h4>
              <History size={17} />
              BigQuery similar historical lines
            </h4>
            {line.historicalLines.length > 0 ? (
              <table>
                <thead>
                  <tr>
                    <th>Description</th>
                    <th>Account</th>
                    <th>Dept</th>
                    <th>Match</th>
                  </tr>
                </thead>
                <tbody>
                  {line.historicalLines.map((history) => (
                    <tr key={`${history.desc}-${history.account}-${history.dept}`}>
                      <td title={history.desc}>{history.desc}</td>
                      <td>{history.account}</td>
                      <td>{history.dept}</td>
                      <td>{history.similarity ?? "N/A"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p className="empty-history">
                <AlertCircle size={16} />
                Historical evidence is used by the backend pipeline but is not persisted in this response yet.
              </p>
            )}
          </aside>
        </div>
      )}
    </article>
  );
}

function confidenceClass(confidence: number | null) {
  if (confidence === null) {
    return "confidence confidence--unknown";
  }
  if (confidence >= 0.9) {
    return "confidence confidence--high";
  }
  if (confidence >= 0.7) {
    return "confidence confidence--medium";
  }
  return "confidence confidence--low";
}

function formatConfidence(confidence: number | null) {
  return confidence === null ? "N/A" : `${Math.round(confidence * 100)}%`;
}

function formatNumber(value: number | null) {
  return value === null ? "N/A" : new Intl.NumberFormat("en-US").format(value);
}

function formatMoney(value: number | null, currency: string) {
  if (value === null) {
    return "N/A";
  }
  return `${new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value)} ${currency}`;
}
