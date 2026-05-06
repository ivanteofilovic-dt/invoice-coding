import { useState } from "react";
import { AlertCircle, ChevronDown, ChevronUp, Cpu, Database, History } from "lucide-react";

import type { InvoiceLineItem } from "../types";

type LineItemCardProps = {
  line: InvoiceLineItem;
  index: number;
  currency: string;
};

const codingLabels = ["ACCOUNT", "DEPARTMENT", "PRODUCT", "IC", "PROJECT", "SYSTEM", "RESERVE"] as const;
const historyDimensions = [
  ["Account", "account"],
  ["Dept", "dept"],
  ["Product", "product"],
  ["IC", "ic"],
  ["Project", "project"],
  ["System", "system"],
  ["Reserve", "reserve"]
] as const;

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
              <div className="history-list">
                {line.historicalLines.map((history, historyIndex) => (
                  <article
                    className="history-item"
                    key={`${history.date ?? "no-date"}-${history.desc}-${history.account}-${history.dept}-${historyIndex}`}
                  >
                    <div className="history-item__header">
                      <div>
                        <strong title={history.desc}>{history.desc}</strong>
                        <span>{history.supplierName ?? "Unknown supplier"}</span>
                      </div>
                      <span className="history-item__match">{history.similarity ?? "N/A"}</span>
                    </div>

                    <div className="history-item__meta">
                      <span>{history.date ?? "No posting date"}</span>
                      <span>{formatMoney(history.amount, currency)}</span>
                      {history.hfmDescription && <span>{history.hfmDescription}</span>}
                    </div>

                    <dl className="history-dimensions">
                      {historyDimensions.map(([label, key]) => (
                        <div key={key}>
                          <dt>{label}</dt>
                          <dd>{history[key] || "N/A"}</dd>
                        </div>
                      ))}
                    </dl>
                  </article>
                ))}
              </div>
            ) : (
              <p className="empty-history">
                <AlertCircle size={16} />
                No similar historical lines were returned for this invoice line.
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
