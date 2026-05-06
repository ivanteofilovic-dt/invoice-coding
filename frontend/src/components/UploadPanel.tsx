import { ChangeEvent, DragEvent, useState } from "react";
import { FileText, UploadCloud } from "lucide-react";

type UploadPanelProps = {
  onUpload: (files: File[]) => void;
  onDemo: () => void;
};

export function UploadPanel({ onUpload, onDemo }: UploadPanelProps) {
  const [isDragging, setIsDragging] = useState(false);

  const handleFiles = (files: FileList | null) => {
    const selectedFiles = Array.from(files ?? []);
    if (selectedFiles.length > 0) {
      onUpload(selectedFiles);
    }
  };

  const onInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    handleFiles(event.target.files);
    event.target.value = "";
  };

  const onDrop = (event: DragEvent<HTMLLabelElement>) => {
    event.preventDefault();
    setIsDragging(false);
    handleFiles(event.dataTransfer.files);
  };

  return (
    <section className="upload-page">
      <div className="hero">
        <p className="eyebrow">Invoice coding POC</p>
        <h2>Upload invoice for coding</h2>
        <p>
          Extract invoice lines, retrieve historical GL evidence from BigQuery, and produce final
          coding dimensions with Gemini.
        </p>
      </div>

      <label
        className={`upload-dropzone ${isDragging ? "upload-dropzone--active" : ""}`}
        onDragOver={(event) => {
          event.preventDefault();
          setIsDragging(true);
        }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={onDrop}
      >
        <input type="file" accept="application/pdf,.pdf" multiple onChange={onInputChange} />
        <span className="upload-dropzone__icon">
          <UploadCloud size={44} />
        </span>
        <strong>Click to upload</strong>
        <span>or drag and drop PDF invoices</span>
        <small>Maximum file size: 10 MB per PDF</small>
      </label>

      <button type="button" className="demo-button" onClick={onDemo}>
        <FileText size={18} />
        Open sample invoice result
      </button>
    </section>
  );
}
