import {
  BrainCircuit,
  CheckCircle2,
  FileText,
  Search,
  X,
} from "lucide-react";
import { useEffect, useState } from "react";
import type {
  AnalyzeResponse,
  CodingSuggestion,
  Extraction,
  JournalLine,
  NeighborRecord,
  SuggestionDetailResponse,
} from "../types";
import { confidenceBarClass, statusBadgeClass } from "../lib/statusStyles";

export type ReviewModalState =
  | { kind: "analyze"; data: AnalyzeResponse }
  | { kind: "stored"; data: SuggestionDetailResponse };

function primaryJournalLine(s: CodingSuggestion): JournalLine | undefined {
  return s.journal_lines[0];
}

function headerSubtitle(
  state: ReviewModalState,
  line: ReturnType<typeof primaryJournalLine> | undefined,
): string {
  if (state.kind === "analyze") {
    const e = state.data.extraction;
    const amt = e.lines[0]?.amount ?? "";
    const cur = e.currency ?? "";
    const parts = [e.supplier, e.invoice_number, amt && cur ? `${amt} ${cur}` : amt || cur].filter(
      Boolean,
    );
    return parts.join(" · ");
  }
  const d = state.data;
  const acc = line?.account ?? "";
  const cc = line?.cost_center ?? "";
  return [d.document_id, d.gcs_uri, acc && `Account ${acc}`, cc && `CC ${cc}`]
    .filter(Boolean)
    .join(" · ");
}

function lineItems(extraction: Extraction | null | undefined) {
  if (!extraction?.lines?.length) return [];
  return extraction.lines;
}

export function ReviewModal({
  state,
  onClose,
}: {
  state: ReviewModalState | null;
  onClose: () => void;
}) {
  const suggestion = state?.kind === "analyze" ? state.data.suggestion : state?.data.suggestion;
  const neighbors: NeighborRecord[] = state
    ? state.kind === "analyze"
      ? state.data.neighbors
      : state.data.neighbors
    : [];
  const extraction = state?.kind === "analyze" ? state.data.extraction : state?.data.extraction;
  const finalConf: number =
    state?.kind === "analyze"
      ? state.data.final_confidence
      : (state?.data.final_confidence ?? 0);
  const status: string =
    state?.kind === "analyze" ? state.data.status : (state?.data.status ?? "—");

  const [account, setAccount] = useState("");
  const [costCenter, setCostCenter] = useState("");
  const [product, setProduct] = useState("");
  const [ic, setIc] = useState("");
  const [project, setProject] = useState("");
  const [glSystem, setGlSystem] = useState("");
  const [reserve, setReserve] = useState("");

  useEffect(() => {
    const line = suggestion?.journal_lines?.length ? primaryJournalLine(suggestion) : undefined;
    setAccount(line?.account != null ? String(line.account) : "");
    setCostCenter(line?.cost_center != null ? String(line.cost_center) : "");
    setProduct(line?.product_code != null ? String(line.product_code) : "");
    setIc(line?.ic != null ? String(line.ic) : "");
    setProject(line?.project != null ? String(line.project) : "");
    setGlSystem(line?.gl_system != null ? String(line.gl_system) : "");
    setReserve(line?.reserve != null ? String(line.reserve) : "");
  }, [suggestion]);

  if (!state || !suggestion) return null;

  const jl = primaryJournalLine(suggestion);

  const titleId =
    state.kind === "analyze"
      ? state.data.extraction.document_id
      : state.data.document_id || state.data.suggestion_id;

  const items = lineItems(extraction ?? null);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-labelledby="review-title"
    >
      <div className="flex max-h-[90vh] w-full max-w-5xl flex-col overflow-hidden rounded-2xl bg-white shadow-2xl">
        <div className="flex items-center justify-between border-b border-slate-100 bg-slate-50 p-6">
          <div>
            <h3 id="review-title" className="text-xl font-bold text-slate-900">
              Review: {titleId}
            </h3>
            <p className="text-sm text-slate-500">{headerSubtitle(state, jl)}</p>
            <p className="mt-1 text-xs text-slate-400">
              Confidence {(finalConf * 100).toFixed(0)}% ·{" "}
              <span className={`rounded-full border px-2 py-0.5 text-[11px] font-medium ${statusBadgeClass(status)}`}>
                {status}
              </span>
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-full p-2 text-slate-500 transition-colors hover:bg-slate-200"
            aria-label="Close"
          >
            <X size={22} />
          </button>
        </div>

        <div className="flex flex-1 gap-8 overflow-y-auto p-6">
          <div className="min-w-0 flex-1 space-y-6">
            <div className="rounded-xl border border-indigo-100 bg-indigo-50 p-4">
              <div className="mb-3 flex items-center gap-2 font-semibold text-indigo-700">
                <BrainCircuit size={18} />
                AI rationale
              </div>
              <p className="text-sm leading-relaxed text-indigo-900">{suggestion.rationale || "—"}</p>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Account
                </label>
                <input
                  type="text"
                  value={account}
                  onChange={(e) => setAccount(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 p-3 font-medium outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Cost center
                </label>
                <input
                  type="text"
                  value={costCenter}
                  onChange={(e) => setCostCenter(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 p-3 font-medium outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Product
                </label>
                <input
                  type="text"
                  value={product}
                  onChange={(e) => setProduct(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 p-3 font-medium outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  IC
                </label>
                <input
                  type="text"
                  value={ic}
                  onChange={(e) => setIc(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 p-3 font-medium outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Project
                </label>
                <input
                  type="text"
                  value={project}
                  onChange={(e) => setProject(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 p-3 font-medium outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  System
                </label>
                <input
                  type="text"
                  value={glSystem}
                  onChange={(e) => setGlSystem(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 p-3 font-medium outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Reserve
                </label>
                <input
                  type="text"
                  value={reserve}
                  onChange={(e) => setReserve(e.target.value)}
                  className="w-full rounded-lg border border-slate-200 p-3 font-medium outline-none focus:ring-2 focus:ring-indigo-500"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Periodization
                </label>
                <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
                  {jl?.periodization_start || jl?.periodization_end
                    ? `${jl?.periodization_start ?? "—"} → ${jl?.periodization_end ?? "—"}`
                    : extraction?.periodization_hint || "—"}
                </div>
              </div>
              <div className="col-span-2 space-y-1">
                <label className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Model confidence
                </label>
                <div className="flex items-center gap-2 pt-2">
                  <div className="h-1.5 flex-1 overflow-hidden rounded-full bg-slate-100">
                    <div
                      className={`h-full rounded-full ${confidenceBarClass(suggestion.confidence)}`}
                      style={{ width: `${Math.min(100, suggestion.confidence * 100)}%` }}
                    />
                  </div>
                  <span className="text-sm font-medium">{(suggestion.confidence * 100).toFixed(0)}%</span>
                </div>
              </div>
            </div>

            <div>
              <h4 className="mb-3 flex items-center gap-2 font-semibold text-slate-900">
                <FileText size={18} className="text-slate-400" />
                Extracted lines
              </h4>
              {items.length ? (
                <div className="space-y-2 rounded-lg border border-slate-100 bg-slate-50 p-4 font-mono text-xs">
                  {items.map((ln) => (
                    <div key={ln.join_key} className="flex justify-between gap-4 border-b border-slate-200 pb-2 last:border-0 last:pb-0">
                      <span className="text-slate-700">{ln.description || ln.join_key}</span>
                      <span className="shrink-0 text-slate-600">
                        {ln.amount}
                        {extraction?.currency ? ` ${extraction.currency}` : ""}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-slate-500">No line extraction in this record (historical suggestion).</p>
              )}
            </div>

            {suggestion.journal_lines.length > 1 && (
              <div>
                <h4 className="mb-2 text-sm font-semibold text-slate-800">All suggested journal lines</h4>
                <ul className="space-y-2 text-sm text-slate-600">
                  {suggestion.journal_lines.map((j, i) => (
                    <li key={i} className="rounded-lg border border-slate-100 bg-slate-50 p-3 font-mono text-xs">
                      <span className="font-semibold text-slate-800">#{i + 1}</span>
                      {" "}acct={j.account ?? "—"} cc={j.cost_center ?? "—"} prod={j.product_code ?? "—"} ic={j.ic ?? "—"} proj={j.project ?? "—"} sys={j.gl_system ?? "—"} rsv={j.reserve ?? "—"}
                      {j.memo && <span className="ml-2 text-slate-500">· {j.memo}</span>}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          <div className="w-80 shrink-0 space-y-4 border-l border-slate-100 pl-8">
            <h4 className="flex items-center gap-2 font-semibold text-slate-900">
              <Search size={18} className="text-indigo-500" />
              Similar historical lines
            </h4>
            <p className="text-xs italic text-slate-500">
              Retrieval uses BigQuery <code className="rounded bg-slate-100 px-0.5">VECTOR_SEARCH</code> on{" "}
              <code className="rounded bg-slate-100 px-0.5">invoice_embeddings</code> plus GL from{" "}
              <code className="rounded bg-slate-100 px-0.5">v_invoice_gl_context</code>. Stored rows may omit distances.
            </p>
            <div className="space-y-3">
              {neighbors.length ? (
                neighbors.map((m) => {
                  const t = m.training;
                  return (
                    <div
                      key={`${m.join_key}-${m.rank}`}
                      className="cursor-default rounded-lg border border-slate-100 bg-slate-50 p-3 transition-all hover:border-indigo-200"
                    >
                      <div className="mb-1 flex items-center justify-between">
                        <span className="text-xs font-bold text-indigo-600">{m.join_key}</span>
                        {m.similarity != null && (
                          <span className="rounded bg-indigo-100 px-1 font-mono text-[10px] text-indigo-700">
                            {(m.similarity * 100).toFixed(1)}% match
                          </span>
                        )}
                      </div>
                      {t && (
                        <>
                          <div className="text-xs text-slate-600">
                            {t.supplier ?? "—"} · {t.invoice_number ?? "—"}
                          </div>
                          <div className="text-sm font-semibold text-slate-800">
                            Account: {t.account ?? "—"}
                          </div>
                          <div className="text-xs text-slate-500">{t.posting_date ?? t.invoice_date ?? ""}</div>
                          {Array.isArray(m.gl_lines_preview) && m.gl_lines_preview.length > 1 && (
                            <ul className="mt-2 space-y-1 border-t border-slate-200 pt-2 text-[11px] text-slate-600">
                              {m.gl_lines_preview.slice(1, 4).map((gl, idx) => (
                                <li key={idx} className="font-mono">
                                  {gl.account ?? "—"} / {gl.cost_center ?? "—"} / {gl.product_code ?? "—"} / ic={gl.ic ?? "—"} proj={gl.project ?? "—"} sys={gl.gl_system ?? "—"} rsv={gl.reserve ?? "—"}
                                  {gl.line_description && <span className="ml-1 text-slate-400">· {gl.line_description}</span>}
                                </li>
                              ))}
                            </ul>
                          )}
                        </>
                      )}
                      {!t && Array.isArray(m.gl_lines_preview) && m.gl_lines_preview.length > 0 && (
                        <ul className="mt-1 space-y-1 text-[11px] text-slate-600">
                          {m.gl_lines_preview.slice(0, 3).map((gl, idx) => (
                            <li key={idx} className="font-mono">
                              {gl.account ?? "—"} / {gl.cost_center ?? "—"} / {gl.product_code ?? "—"} / ic={gl.ic ?? "—"} proj={gl.project ?? "—"} sys={gl.gl_system ?? "—"} rsv={gl.reserve ?? "—"}
                              {gl.line_description && <span className="ml-1 text-slate-400">· {gl.line_description}</span>}
                            </li>
                          ))}
                        </ul>
                      )}
                      {!t && !(m.gl_lines_preview && m.gl_lines_preview.length) && (
                        <div className="text-xs text-slate-400">No GL training row for this key.</div>
                      )}
                    </div>
                  );
                })
              ) : (
                <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 p-6 text-center">
                  <p className="text-xs text-slate-400">No neighbors returned for this run.</p>
                </div>
              )}
            </div>

            <div className="space-y-3 pt-6">
              <button
                type="button"
                className="flex w-full items-center justify-center gap-2 rounded-xl bg-slate-900 py-3 font-semibold text-white transition-colors hover:bg-slate-800"
              >
                <CheckCircle2 size={18} /> Approve (UI only)
              </button>
              <p className="text-center text-[11px] text-slate-400">
                Posting to Oracle is not wired in this API yet — use this flow to review model output.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
