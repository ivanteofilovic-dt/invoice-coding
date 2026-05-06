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
  desc: string;
  account: string;
  dept: string;
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
