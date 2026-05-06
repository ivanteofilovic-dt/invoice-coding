import { History, Inbox } from "lucide-react";
import { useMemo, useState } from "react";

import type { CodingHistoryEntry } from "../types";
import { ResultsView } from "./ResultsView";

type HistoryViewProps = {
  entries: CodingHistoryEntry[];
  onUpload: () => void;
};

export function HistoryView({ entries, onUpload }: HistoryViewProps) {
  const [selectedEntryId, setSelectedEntryId] = useState(entries[0]?.id ?? null);
  const selectedEntry = useMemo(
    () => entries.find((entry) => entry.id === selectedEntryId) ?? entries[0] ?? null,
    [entries, selectedEntryId]
  );

  if (!selectedEntry) {
    return (
      <section className="history-empty">
        <Inbox size={40} />
        <h2>No coding history yet</h2>
        <p>Uploaded invoices will appear here after their coding completes.</p>
        <button className="button button--primary" type="button" onClick={onUpload}>
          Upload invoices
        </button>
      </section>
    );
  }

  return (
    <section className="history-page">
      <aside className="history-sidebar" aria-label="Previous invoice codings">
        <div className="history-sidebar__header">
          <History size={20} />
          <div>
            <h2>History</h2>
            <p>{entries.length} stored coding{entries.length === 1 ? "" : "s"}</p>
          </div>
        </div>

        <div className="history-entry-list">
          {entries.map((entry) => (
            <button
              className={
                entry.id === selectedEntry.id
                  ? "history-entry history-entry--active"
                  : "history-entry"
              }
              key={entry.id}
              type="button"
              onClick={() => setSelectedEntryId(entry.id)}
            >
              <strong>{entry.result.sourceFileName ?? entry.result.invoiceNumber}</strong>
              <span>{entry.result.vendor}</span>
              <small>
                {formatDateTime(entry.codedAt)} - {entry.result.totalAmount} {entry.result.currency}
              </small>
            </button>
          ))}
        </div>
      </aside>

      <div className="history-detail">
        <ResultsView results={[selectedEntry.result]} files={[]} errors={[]} onReset={onUpload} />
      </div>
    </section>
  );
}

function formatDateTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Unknown time";
  }

  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short"
  }).format(date);
}
