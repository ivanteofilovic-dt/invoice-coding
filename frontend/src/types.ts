export type CodingDimensions = {
  ACCOUNT: string;
  DEPARTMENT: string;
  PRODUCT: string;
  IC: string;
  PROJECT: string;
  SYSTEM: string;
  RESERVE: string;
};

export type HistoricalLine = {
  date: string | null;
  supplierName: string | null;
  desc: string;
  hfmDescription: string | null;
  account: string;
  dept: string;
  product: string | null;
  ic: string | null;
  project: string | null;
  system: string | null;
  reserve: string | null;
  amount: number | null;
  similarity: string | null;
};

export type InvoiceLineItem = {
  id: string;
  description: string;
  quantity: number | null;
  unitPrice: number | null;
  total: number;
  coding: CodingDimensions;
  confidence: number | null;
  reasoning: string | null;
  historicalLines: HistoricalLine[];
};

export type InvoiceCodingResult = {
  vendor: string;
  invoiceNumber: string;
  date: string | null;
  totalAmount: string;
  currency: string;
  sourceFileName: string | null;
  lineItems: InvoiceLineItem[];
};

export type CodingHistoryEntry = {
  id: string;
  codedAt: string;
  result: InvoiceCodingResult;
};

export type BatchInvoiceError = {
  fileName: string | null;
  error: string;
};

export type BatchInvoiceCodingResult = {
  total: number;
  succeeded: number;
  failed: number;
  invoices: InvoiceCodingResult[];
  errors: BatchInvoiceError[];
};
