import { CheckCircle2, Cpu, Database, FileText, Network } from "lucide-react";

type ProcessingPanelProps = {
  fileName: string;
  fileCount?: number;
};

const steps = [
  {
    title: "Gemini extraction",
    description: "Reading invoice header and line items",
    icon: FileText
  },
  {
    title: "BigQuery RAG search",
    description: "Retrieving similar historical GL lines",
    icon: Database
  },
  {
    title: "Predicting codes",
    description: "Synthesizing final GL dimensions",
    icon: Network
  }
];

export function ProcessingPanel({ fileName, fileCount = 1 }: ProcessingPanelProps) {
  const isBatch = fileCount > 1;

  return (
    <section className="processing-card" aria-live="polite">
      <div className="spinner-wrap">
        <div className="spinner" />
        <Cpu size={34} />
      </div>
      <p className="eyebrow">Processing</p>
      <h2>{isBatch ? `${fileCount} invoices` : fileName}</h2>
      {isBatch && <p className="processing-card__hint">This can take a while; files are processed in parallel.</p>}
      <div className="processing-steps">
        {steps.map((step, index) => {
          const Icon = step.icon;
          return (
            <div className="processing-step" key={step.title}>
              <span className="processing-step__icon">
                {index === 0 ? <CheckCircle2 size={20} /> : <Icon size={20} />}
              </span>
              <div>
                <strong>{step.title}</strong>
                <p>{step.description}</p>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}
