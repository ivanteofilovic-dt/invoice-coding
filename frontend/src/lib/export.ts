import * as XLSX from "xlsx";
import type { InvoiceCodingResult } from "../types";

type ExportRow = {
  Vendor: string;
  "Invoice Number": string;
  "Invoice Date": string;
  Currency: string;
  "Source File": string;
  "Line ID": string;
  Description: string;
  Quantity: number | string;
  "Unit Price": number | string;
  Total: number;
  ACCOUNT: string;
  DEPARTMENT: string;
  PRODUCT: string;
  IC: string;
  PROJECT: string;
  SYSTEM: string;
  RESERVE: string;
  "Confidence (%)": number | string;
  Reasoning: string;
};

export function exportResultsToExcel(results: InvoiceCodingResult[]): void {
  const rows: ExportRow[] = results.flatMap((invoice) =>
    invoice.lineItems.map((line) => ({
      Vendor: invoice.vendor,
      "Invoice Number": invoice.invoiceNumber,
      "Invoice Date": invoice.date ?? "",
      Currency: invoice.currency,
      "Source File": invoice.sourceFileName ?? "",
      "Line ID": line.id,
      Description: line.description,
      Quantity: line.quantity ?? "",
      "Unit Price": line.unitPrice ?? "",
      Total: line.total,
      ACCOUNT: line.coding.ACCOUNT,
      DEPARTMENT: line.coding.DEPARTMENT,
      PRODUCT: line.coding.PRODUCT,
      IC: line.coding.IC,
      PROJECT: line.coding.PROJECT,
      SYSTEM: line.coding.SYSTEM,
      RESERVE: line.coding.RESERVE,
      "Confidence (%)":
        line.confidence != null ? Math.round(line.confidence * 100) : "",
      Reasoning: line.reasoning ?? "",
    }))
  );

  const worksheet = XLSX.utils.json_to_sheet(rows);

  applyColumnWidths(worksheet, rows);

  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, "Coded Invoices");

  const fileName = buildFileName(results);
  XLSX.writeFile(workbook, fileName);
}

function applyColumnWidths(
  worksheet: XLSX.WorkSheet,
  rows: ExportRow[]
): void {
  const columns = Object.keys(rows[0] ?? {}) as (keyof ExportRow)[];
  worksheet["!cols"] = columns.map((col) => {
    const maxLength = rows.reduce((max, row) => {
      const cellValue = row[col];
      const length =
        cellValue != null ? String(cellValue).length : 0;
      return Math.max(max, length);
    }, col.length);
    return { wch: Math.min(maxLength + 2, 60) };
  });
}

function buildFileName(results: InvoiceCodingResult[]): string {
  if (results.length === 1) {
    const invoice = results[0];
    const base = invoice.sourceFileName
      ? invoice.sourceFileName.replace(/\.pdf$/i, "")
      : invoice.invoiceNumber;
    return `${base}_coded.xlsx`;
  }
  return `invoices_coded_${new Date().toISOString().slice(0, 10)}.xlsx`;
}
