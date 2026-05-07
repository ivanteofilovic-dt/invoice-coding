import type { BatchInvoiceCodingResult, InvoiceCodingResult } from "../types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "";

type ApiErrorPayload = {
  detail?: string;
};

export async function codeInvoice(file: File): Promise<InvoiceCodingResult> {
  const formData = new FormData();
  formData.append("file", file);

  const response = await fetch(`${API_BASE_URL}/api/invoices/code`, {
    method: "POST",
    body: formData
  });

  return parseResponse<InvoiceCodingResult>(response);
}

export async function codeInvoices(files: File[]): Promise<BatchInvoiceCodingResult> {
  const formData = new FormData();
  files.forEach((file) => {
    formData.append("files", file);
  });

  const response = await fetch(`${API_BASE_URL}/api/invoices/batch/code`, {
    method: "POST",
    body: formData
  });

  return parseResponse<BatchInvoiceCodingResult>(response);
}

async function parseResponse<T>(response: Response): Promise<T> {
  if (response.ok) {
    return response.json() as Promise<T>;
  }

  let message = `Request failed with status ${response.status}`;
  try {
    const payload = (await response.json()) as ApiErrorPayload;
    if (payload.detail) {
      message = payload.detail;
    }
  } catch {
    // Keep the status-based message when the backend did not return JSON.
  }

  throw new Error(message);
}
