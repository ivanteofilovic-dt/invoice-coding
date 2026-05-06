import React, { useState, useEffect } from 'react';
import { 
  UploadCloud, FileText, CheckCircle2, Cpu, History, 
  ChevronDown, ChevronUp, AlertCircle, FileCheck,
  Database, Network, Zap, ZoomIn, ZoomOut, Download, Maximize
} from 'lucide-react';

// --- MOCK DATA ---
const MOCK_INVOICE_DATA = {
  vendor: "Exclusive Networks Sweden AB",
  invoiceNumber: "INSE010035371",
  date: "2026-03-15",
  totalAmount: "3,413.36",
  currency: "SEK",
  lineItems: [
    {
      id: "line-1",
      description: "FC-10-S12FP-247-02-12 Firewall appliance",
      quantity: 2,
      unitPrice: 1281.68,
      total: 2563.36,
      coding: {
        ACCOUNT: "40190",
        DEPARTMENT: "F82250",
        PRODUCT: "F800000",
        IC: "00",
        PROJECT: "000000",
        SYSTEM: "000000",
        RESERVE: "F80000000000"
      },
      confidence: 0.98,
      reasoning: "High confidence prediction based on strong historical precedent. Vendor 'Exclusive Networks Sweden AB' combined with product code 'FC-10-S12FP-247-02-12' has been coded to Account 40190 (Hardware) and Dept F82250 in 14 previous instances. The IC field defaults to 00 as no intercompany flags were detected.",
      historicalLines: [
        { date: "2026-02-10", desc: "FC-10-S12FP-247-02-12 Firewall", account: "40190", dept: "F82250", similarity: "99%" },
        { date: "2026-01-22", desc: "FC-10-F108F-247-02-12 Firewall app.", account: "40190", dept: "F82250", similarity: "92%" },
        { date: "2025-11-05", desc: "Firewall equipment hardware", account: "40190", dept: "F82250", similarity: "85%" },
      ]
    },
    {
      id: "line-2",
      description: "Installation Services & Setup - Remote",
      quantity: 1,
      unitPrice: 850.00,
      total: 850.00,
      coding: {
        ACCOUNT: "60110",
        DEPARTMENT: "F82250",
        PRODUCT: "F800000",
        IC: "00",
        PROJECT: "000000",
        SYSTEM: "000000",
        RESERVE: "F80000000000"
      },
      confidence: 0.82,
      reasoning: "Prediction based on semantic matching. 'Installation Services' for this vendor is typically mapped to Account 60110 (Consultancy IS/IT). Department remains F82250 aligned with the primary hardware purchase on this invoice.",
      historicalLines: [
        { date: "2026-01-22", desc: "Consulting - setup", account: "60110", dept: "F82250", similarity: "88%" },
        { date: "2025-09-14", desc: "Installation firewall config", account: "60110", dept: "F82250", similarity: "81%" },
      ]
    }
  ]
};

export default function App() {
  const [file, setFile] = useState(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingStep, setProcessingStep] = useState(0);
  const [results, setResults] = useState(null);

  const handleFileUpload = (e) => {
    e.preventDefault();
    setFile({ name: "INSE010035371_ExclusiveNetworks.pdf", size: "245 KB" });
    setIsProcessing(true);
    setResults(null);
    setProcessingStep(0);
  };

  useEffect(() => {
    if (isProcessing) {
      const steps = [
        { time: 1000, step: 1 },
        { time: 2500, step: 2 },
        { time: 4000, step: 3 },
        { time: 5000, step: 4 },
      ];

      steps.forEach(({ time, step }) => {
        setTimeout(() => {
          if (step === 4) {
            setIsProcessing(false);
            setResults(MOCK_INVOICE_DATA);
          } else {
            setProcessingStep(step);
          }
        }, time);
      });
    }
  }, [isProcessing]);

  const reset = () => {
    setFile(null);
    setResults(null);
    setIsProcessing(false);
  };

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900 font-sans flex flex-col">
      <header className="bg-white border-b border-slate-200 px-6 py-4 flex items-center justify-between sticky top-0 z-10 shrink-0">
        <div className="flex items-center gap-3">
          <div className="bg-indigo-600 p-2.5 rounded-xl shadow-sm">
            <Zap className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-xl font-bold tracking-tight text-slate-900 leading-tight">GL AutoCoder</h1>
            <p className="text-[10px] text-slate-500 font-bold tracking-wider uppercase">AI-Powered GL Allocation</p>
          </div>
        </div>
        <div className="flex gap-6 items-center">
          <button className="text-sm font-semibold text-slate-500 hover:text-slate-900 transition-colors">History</button>
          <button className="text-sm font-semibold text-slate-500 hover:text-slate-900 transition-colors">Settings</button>
          <div className="w-9 h-9 bg-indigo-100 rounded-full flex items-center justify-center text-indigo-700 font-bold text-sm shadow-sm">
            TS
          </div>
        </div>
      </header>

      <main className={`flex-1 ${results ? "max-w-[1600px] w-full mx-auto p-4 md:p-6" : "max-w-6xl w-full mx-auto p-6 md:p-8"}`}>
        
        {!file && !isProcessing && !results && (
          <div className="mt-20 max-w-2xl mx-auto">
            <div className="text-center mb-10">
              <h2 className="text-4xl font-extrabold mb-3 tracking-tight text-slate-900">Upload Invoice for Coding</h2>
              <p className="text-lg text-slate-500 max-w-lg mx-auto leading-relaxed">System will extract data, query historical GL entries via BigQuery, and generate final coding.</p>
            </div>
            
            <label 
              className="flex flex-col items-center justify-center w-full h-72 border-2 border-slate-300 border-dashed rounded-3xl cursor-pointer bg-white hover:bg-slate-50 hover:border-indigo-400 transition-all group shadow-sm"
            >
              <div className="flex flex-col items-center justify-center pt-5 pb-6">
                <div className="p-5 bg-indigo-50 rounded-full group-hover:bg-indigo-100 group-hover:scale-110 transition-all duration-300 mb-5">
                  <UploadCloud className="w-10 h-10 text-indigo-600" />
                </div>
                <p className="mb-2 text-base text-slate-600"><span className="font-semibold text-indigo-600">Click to upload</span> or drag and drop</p>
                <p className="text-sm text-slate-400">PDF, PNG, or JPG (MAX. 10MB)</p>
              </div>
              <input type="file" className="hidden" onChange={handleFileUpload} accept=".pdf,image/*" />
            </label>
          </div>
        )}

        {isProcessing && (
          <div className="mt-24 max-w-md mx-auto bg-white p-10 rounded-3xl shadow-lg border border-slate-100">
            <div className="flex flex-col items-center text-center">
              <div className="relative w-24 h-24 mb-8">
                <div className="absolute inset-0 border-4 border-slate-100 rounded-full"></div>
                <div className="absolute inset-0 border-4 border-indigo-600 rounded-full border-t-transparent animate-spin"></div>
                <div className="absolute inset-0 flex items-center justify-center">
                  <Cpu className="w-8 h-8 text-indigo-600" />
                </div>
              </div>
              <h3 className="text-2xl font-bold mb-8 text-slate-800">Processing Invoice</h3>
              
              <div className="w-full space-y-4">
                <ProcessingStep 
                  icon={<FileText size={18} />} 
                  title="Gemini Extraction" 
                  desc="Extracting vendor and line items"
                  active={processingStep === 1}
                  done={processingStep > 1}
                />
                <ProcessingStep 
                  icon={<Database size={18} />} 
                  title="BigQuery RAG Search" 
                  desc="Finding 20 similar historical GL lines"
                  active={processingStep === 2}
                  done={processingStep > 2}
                />
                <ProcessingStep 
                  icon={<Network size={18} />} 
                  title="Predicting Codes" 
                  desc="Synthesizing final GL dimensions"
                  active={processingStep === 3}
                  done={processingStep > 3}
                />
              </div>
            </div>
          </div>
        )}

        {results && (
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-6 h-[calc(100vh-140px)] animate-in fade-in slide-in-from-bottom-4 duration-500">
            <div className="hidden xl:block h-full border border-slate-200 rounded-2xl overflow-hidden bg-slate-200 shadow-inner">
               <MockPDFViewer file={file} data={results} />
            </div>

            <div className="h-full overflow-y-auto pr-2 pb-12 rounded-xl">
              <div className="flex items-center justify-between mb-6 sticky top-0 bg-slate-50 z-10 py-2">
                <h2 className="text-2xl font-bold flex items-center gap-2 text-slate-800">
                  <FileCheck className="text-green-600 w-7 h-7" /> 
                  Coding Complete
                </h2>
                <button 
                  onClick={reset}
                  className="px-5 py-2 bg-white border border-slate-200 text-slate-700 rounded-xl hover:bg-slate-100 hover:text-slate-900 font-semibold text-sm transition-all shadow-sm"
                >
                  Upload Another
                </button>
              </div>

              <div className="bg-white rounded-2xl shadow-sm border border-slate-200 p-6 mb-8 flex flex-wrap gap-6 items-center justify-between">
                <div>
                  <p className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-1">Vendor</p>
                  <p className="text-base font-bold text-slate-900">{results.vendor}</p>
                </div>
                <div className="w-px h-10 bg-slate-200 hidden md:block"></div>
                <div>
                  <p className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-1">Invoice Number</p>
                  <p className="text-base font-semibold text-slate-900">{results.invoiceNumber}</p>
                </div>
                <div className="w-px h-10 bg-slate-200 hidden md:block"></div>
                <div>
                  <p className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-1">Date</p>
                  <p className="text-base font-semibold text-slate-900">{results.date}</p>
                </div>
                <div className="w-px h-10 bg-slate-200 hidden md:block"></div>
                <div>
                  <p className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-1">Total</p>
                  <p className="text-xl font-black text-slate-900">{results.totalAmount} <span className="text-sm font-semibold text-slate-500">{results.currency}</span></p>
                </div>
              </div>

              <h3 className="text-lg font-bold mb-4 ml-1 text-slate-800">Line Items & GL Prediction</h3>
              <div className="space-y-4">
                {results.lineItems.map((line, index) => (
                  <LineItemCard key={line.id} line={line} index={index} />
                ))}
              </div>
            </div>
          </div>
        )}

      </main>
    </div>
  );
}

function ProcessingStep({ icon, title, desc, active, done }) {
  return (
    <div className={`flex items-start gap-4 p-4 rounded-2xl transition-colors ${active ? 'bg-indigo-50 border border-indigo-100 shadow-sm' : 'bg-transparent'}`}>
      <div className={`mt-0.5 rounded-full p-2 ${done ? 'bg-green-100 text-green-600' : active ? 'bg-indigo-600 text-white shadow-md' : 'bg-slate-100 text-slate-400'}`}>
        {done ? <CheckCircle2 size={20} /> : icon}
      </div>
      <div className="text-left">
        <h4 className={`text-sm font-bold ${done ? 'text-slate-900' : active ? 'text-indigo-900' : 'text-slate-500'}`}>{title}</h4>
        <p className={`text-xs mt-0.5 ${active ? 'text-indigo-600/80' : 'text-slate-400'}`}>{desc}</p>
      </div>
    </div>
  );
}

function LineItemCard({ line, index }) {
  const [expanded, setExpanded] = useState(index === 0);

  const getConfidenceColor = (score) => {
    if (score >= 0.9) return "bg-green-50 text-green-700 border-green-200";
    if (score >= 0.7) return "bg-yellow-50 text-yellow-700 border-yellow-200";
    return "bg-red-50 text-red-700 border-red-200";
  };

  return (
    <div className="bg-white rounded-2xl shadow-sm border border-slate-200 overflow-hidden transition-all duration-200 hover:shadow-md">
      <div 
        className="p-5 flex flex-col md:flex-row md:items-center justify-between gap-4 cursor-pointer hover:bg-slate-50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex-1">
          <div className="flex items-center gap-3 mb-1.5">
            <span className="bg-slate-100 text-slate-600 font-bold text-xs px-2.5 py-1 rounded-md">#{index + 1}</span>
            <h4 className="font-bold text-slate-900 text-base">{line.description}</h4>
          </div>
          <div className="text-sm text-slate-500 flex gap-5 ml-11">
            <span>Qty: <strong className="font-semibold text-slate-700">{line.quantity}</strong></span>
            <span>Price: <strong className="font-semibold text-slate-700">{line.unitPrice.toLocaleString()}</strong> SEK</span>
            <span className="text-indigo-600 font-semibold">Total: {line.total.toLocaleString()} SEK</span>
          </div>
        </div>

        <div className="flex items-center justify-between md:justify-end w-full md:w-auto gap-6 border-t border-slate-100 md:border-none pt-4 md:pt-0">
          <div className="flex flex-col items-start md:items-end">
             <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1.5">AI Confidence</span>
             <div className={`px-3 py-1 rounded-full text-xs font-bold border ${getConfidenceColor(line.confidence)} shadow-sm`}>
               {(line.confidence * 100).toFixed(0)}%
             </div>
          </div>
          <button className={`p-2 rounded-full transition-colors ${expanded ? 'bg-indigo-50 text-indigo-600' : 'bg-slate-50 text-slate-400 hover:bg-slate-100'}`}>
            {expanded ? <ChevronUp size={20} /> : <ChevronDown size={20} />}
          </button>
        </div>
      </div>

      {expanded && (
        <div className="border-t border-slate-100 bg-slate-50/50 p-6 flex flex-col xl:flex-row gap-8">
          <div className="flex-1 space-y-6">
            <div>
              <h5 className="text-sm font-bold text-slate-800 flex items-center gap-2 mb-4">
                <Database size={16} className="text-indigo-500" /> 
                Predicted General Ledger Coding
              </h5>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                <CodeField label="ACCOUNT" value={line.coding.ACCOUNT} isPrimary />
                <CodeField label="DEPARTMENT" value={line.coding.DEPARTMENT} isPrimary />
                <CodeField label="PRODUCT" value={line.coding.PRODUCT} />
                <CodeField label="IC" value={line.coding.IC} />
                <CodeField label="PROJECT" value={line.coding.PROJECT} />
                <CodeField label="SYSTEM" value={line.coding.SYSTEM} />
                <CodeField label="RESERVE" value={line.coding.RESERVE} />
              </div>
            </div>

            <div className="bg-white border border-indigo-100 shadow-sm rounded-xl p-5 relative overflow-hidden">
              <div className="absolute left-0 top-0 bottom-0 w-1 bg-indigo-500"></div>
              <h5 className="text-sm font-bold text-indigo-900 flex items-center gap-2 mb-2">
                <Cpu size={16} className="text-indigo-500" /> Gemini Reasoning
              </h5>
              <p className="text-sm text-slate-600 leading-relaxed">
                {line.reasoning}
              </p>
            </div>
          </div>

          <div className="flex-1 xl:max-w-[420px] bg-white border border-slate-200 rounded-xl p-5 shadow-sm">
            <h5 className="text-sm font-bold text-slate-800 flex items-center gap-2 mb-4">
              <History size={16} className="text-slate-500" /> 
              BigQuery Similar Historical Lines
            </h5>
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="text-xs text-slate-400 border-b border-slate-100">
                    <th className="pb-3 font-semibold uppercase tracking-wider">Description</th>
                    <th className="pb-3 font-semibold uppercase tracking-wider">Account</th>
                    <th className="pb-3 font-semibold uppercase tracking-wider">Dept</th>
                    <th className="pb-3 font-semibold uppercase tracking-wider text-right">Match</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-50">
                  {line.historicalLines.map((hist, i) => (
                    <tr key={i} className="hover:bg-slate-50 transition-colors group">
                      <td className="py-3 pr-2 max-w-[140px] truncate text-slate-700 font-medium text-xs group-hover:text-slate-900" title={hist.desc}>
                        {hist.desc}
                      </td>
                      <td className="py-3 pr-2"><span className="font-mono text-xs text-slate-600 bg-slate-100 rounded px-1.5 py-0.5">{hist.account}</span></td>
                      <td className="py-3 pr-2"><span className="font-mono text-xs text-slate-600 bg-slate-100 rounded px-1.5 py-0.5">{hist.dept}</span></td>
                      <td className="py-3 text-right text-green-600 font-bold text-xs">{hist.similarity}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-4 pt-3 border-t border-slate-100 flex items-start text-xs text-slate-500 gap-2">
              <AlertCircle size={14} className="shrink-0 mt-0.5 text-slate-400" />
              <span>Showing top 3 of 20 lines retrieved from BigQuery via Vector Search.</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function CodeField({ label, value, isPrimary }) {
  return (
    <div className={`p-3 rounded-xl border transition-all ${isPrimary ? 'bg-indigo-50/30 border-indigo-200 hover:border-indigo-300 shadow-sm' : 'bg-slate-50 border-slate-200 hover:border-slate-300'}`}>
      <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">{label}</div>
      <div className={`font-mono text-sm font-bold ${value.includes("00000") && label !== 'PRODUCT' ? 'text-slate-400' : 'text-slate-800'}`}>
        {value}
      </div>
    </div>
  );
}

function MockPDFViewer({ file, data }) {
  return (
    <div className="flex flex-col h-full bg-slate-300">
      <div className="bg-[#323639] text-slate-300 p-2 flex items-center justify-between text-xs font-sans shadow-md z-10">
        <div className="flex items-center gap-4 px-2">
          <span className="truncate max-w-[200px] font-medium">{file?.name || "document.pdf"}</span>
          <span className="text-slate-400">1 / 1</span>
        </div>
        <div className="flex items-center gap-4 pr-2">
          <div className="flex items-center gap-3">
            <button className="hover:text-white transition-colors p-1"><ZoomOut size={16} /></button>
            <span className="font-medium">100%</span>
            <button className="hover:text-white transition-colors p-1"><ZoomIn size={16} /></button>
          </div>
          <div className="w-px h-4 bg-slate-600"></div>
          <button className="hover:text-white transition-colors p-1"><Maximize size={16} /></button>
          <button className="hover:text-white transition-colors p-1"><Download size={16} /></button>
        </div>
      </div>
      
      <div className="flex-1 overflow-auto p-4 md:p-8 flex justify-center relative custom-scrollbar">
        <div className="bg-white w-full max-w-[210mm] min-h-[297mm] shadow-xl p-12 flex flex-col font-serif text-slate-800 mb-8 mt-2">
          <div className="flex justify-between items-start mb-12 border-b-2 border-slate-200 pb-6">
            <div>
              <h1 className="text-4xl font-black tracking-tighter text-indigo-900 mb-3">EXCLUSIVE<br/>NETWORKS</h1>
              <p className="text-xs text-slate-500 font-sans leading-relaxed">Box 1234, 111 22 Stockholm, Sweden<br/>Org.nr: 556123-4567<br/>VAT: SE556123456701</p>
            </div>
            <div className="text-right">
              <h2 className="text-3xl font-bold text-slate-300 tracking-widest mb-6">INVOICE</h2>
              <div className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm font-sans">
                <span className="text-slate-500">Invoice No:</span>
                <span className="font-bold text-slate-800">{data.invoiceNumber}</span>
                <span className="text-slate-500">Invoice Date:</span>
                <span className="font-bold text-slate-800">{data.date}</span>
                <span className="text-slate-500">Due Date:</span>
                <span className="font-bold text-slate-800">2026-04-14</span>
              </div>
            </div>
          </div>

          <div className="mb-14 font-sans text-sm">
            <h3 className="font-bold text-slate-400 mb-3 border-b border-slate-200 pb-1 w-1/2 tracking-wider">BILL TO</h3>
            <p className="font-bold text-slate-800 text-base mb-1">Telenor Sweden AB</p>
            <p className="text-slate-600 leading-relaxed">Katarinavägen 15<br/>116 45 Stockholm<br/>Sweden</p>
          </div>

          <div className="flex-1">
            <table className="w-full text-sm font-sans mb-8">
              <thead>
                <tr className="bg-slate-50 text-slate-500 border-y-2 border-slate-200">
                  <th className="py-3 px-4 text-left font-bold uppercase tracking-wider">Description</th>
                  <th className="py-3 px-4 text-right font-bold uppercase tracking-wider">Qty</th>
                  <th className="py-3 px-4 text-right font-bold uppercase tracking-wider">Unit Price</th>
                  <th className="py-3 px-4 text-right font-bold uppercase tracking-wider">Total (SEK)</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {data.lineItems.map((line, i) => (
                  <tr key={i} className="hover:bg-slate-50/50">
                    <td className="py-4 px-4">
                      <div className="font-bold text-slate-800 mb-0.5">{line.description}</div>
                      {line.description.includes('FC-10') && <div className="text-xs text-slate-400 font-mono">S/N: FWS2490812{i}</div>}
                    </td>
                    <td className="py-4 px-4 text-right text-slate-600 font-medium">{line.quantity}</td>
                    <td className="py-4 px-4 text-right text-slate-600 font-medium">{line.unitPrice.toLocaleString('en-US', {minimumFractionDigits: 2})}</td>
                    <td className="py-4 px-4 text-right font-bold text-slate-800">{line.total.toLocaleString('en-US', {minimumFractionDigits: 2})}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex justify-end font-sans mt-8">
            <div className="w-2/3 md:w-1/2">
              <div className="flex justify-between py-2.5 border-t border-slate-200 text-sm">
                <span className="text-slate-500 font-medium">Subtotal</span>
                <span className="font-bold text-slate-800">{(parseFloat(data.totalAmount.replace(',','')) * 0.8).toLocaleString('en-US', {minimumFractionDigits: 2})}</span>
              </div>
              <div className="flex justify-between py-2.5 border-t border-slate-200 text-sm">
                <span className="text-slate-500 font-medium">VAT (25%)</span>
                <span className="font-bold text-slate-800">{(parseFloat(data.totalAmount.replace(',','')) * 0.2).toLocaleString('en-US', {minimumFractionDigits: 2})}</span>
              </div>
              <div className="flex justify-between py-4 border-t-2 border-slate-800 mt-2 bg-slate-50 px-3 rounded-lg">
                <span className="font-black text-slate-800 uppercase tracking-wider">Total {data.currency}</span>
                <span className="font-black text-indigo-900 text-xl">{data.totalAmount}</span>
              </div>
            </div>
          </div>

          <div className="mt-auto pt-12 text-center text-xs text-slate-400 font-sans border-t border-slate-200">
            Payment Terms: 30 Net. Please include invoice number on your remittance.
          </div>
        </div>
      </div>
    </div>
  );
}
