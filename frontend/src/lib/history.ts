import type { CodingHistoryEntry, InvoiceCodingResult } from "../types";

const STORAGE_KEY = "gl-autocoder:coding-history";
const MAX_HISTORY_ENTRIES = 50;

export function loadCodingHistory(): CodingHistoryEntry[] {
  try {
    const rawValue = window.localStorage.getItem(STORAGE_KEY);
    if (!rawValue) {
      return [];
    }

    const parsed = JSON.parse(rawValue);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed.filter(isCodingHistoryEntry);
  } catch {
    return [];
  }
}

export function addResultsToCodingHistory(
  currentHistory: CodingHistoryEntry[],
  results: InvoiceCodingResult[]
): CodingHistoryEntry[] {
  const codedAt = new Date().toISOString();
  const newEntries = results.map((result, index) => ({
    id: `${codedAt}-${index}-${result.invoiceNumber}`,
    codedAt,
    result
  }));
  const nextHistory = [...newEntries, ...currentHistory].slice(0, MAX_HISTORY_ENTRIES);
  saveCodingHistory(nextHistory);
  return nextHistory;
}

function saveCodingHistory(history: CodingHistoryEntry[]) {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(history));
  } catch {
    // The app can still show the just-coded result if browser storage is unavailable.
  }
}

function isCodingHistoryEntry(value: unknown): value is CodingHistoryEntry {
  if (!value || typeof value !== "object") {
    return false;
  }

  const entry = value as Partial<CodingHistoryEntry>;
  return (
    typeof entry.id === "string" &&
    typeof entry.codedAt === "string" &&
    Boolean(entry.result) &&
    typeof entry.result?.vendor === "string" &&
    typeof entry.result?.invoiceNumber === "string" &&
    Array.isArray(entry.result?.lineItems)
  );
}
