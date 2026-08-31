import { useState } from "react";

const SUBMIT_URL = "http://localhost:8000/submit";

export default function App() {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("idle"); // idle | submitting | success | error
  const [message, setMessage] = useState("");
  const [dagRunId, setDagRunId] = useState(null);
  const [reviewText, setReviewText] = useState("");
  const [needsReview, setNeedsReview] = useState(false);
  const [reviewSaving, setReviewSaving] = useState(false);
  const [savedOcrText, setSavedOcrText] = useState("");
  const [showOptions, setShowOptions] = useState(false);
  const [showGptPrompt, setShowGptPrompt] = useState(false);
  const [gptPrompt, setGptPrompt] = useState("");
  const [gptLoading, setGptLoading] = useState(false);
  const [finalNotes, setFinalNotes] = useState("");
  const [showDownloadOptions, setShowDownloadOptions] = useState(false);

  async function downloadNotes(fileType) {
    const content = finalNotes || savedOcrText || reviewText || "";
    if (!content.trim()) {
      setStatus("error");
      setMessage("There is no content to download yet.");
      return;
    }

    try {
      if (!dagRunId) {
        throw new Error("No DAG run is available to generate the file.");
      }

      const response = await fetch(`http://localhost:8000/download/${dagRunId}/${fileType}`);
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.error || `Server responded with ${response.status}`);
      }

      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      const normalized = fileType === "word" ? "notes.doc" : fileType === "one" ? "notes.one" : "notes.txt";
      link.download = normalized;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);

      setStatus("success");
      setMessage(`Downloaded ${normalized}.`);
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "Download failed.");
    }
  }

  function handleFileChange(e) {
    const selected = e.target.files?.[0] ?? null;
    setFile(selected);
    setStatus("idle");
    setMessage("");
    setDagRunId(null);
    setReviewText("");
    setNeedsReview(false);
    setSavedOcrText("");
    setShowOptions(false);
    setShowGptPrompt(false);
    setGptPrompt("");
    setFinalNotes("");
    setShowDownloadOptions(false);
  }

  async function pollForResult(runId) {
    while (true) {
      try {
        const res = await fetch(`http://localhost:8000/result/${runId}`);
        if (!res.ok) {
          throw new Error(`Server responded with ${res.status}`);
        }

        const data = await res.json();

        if (data.status === "pending") {
          await new Promise((resolve) => setTimeout(resolve, 1500));
          continue;
        }

        if (data.status === "needs_review") {
          setStatus("success");
          setMessage("OCR result returned. Please review and fix the text below.");
          setReviewText(data.result.transcribed_text || "");
          setNeedsReview(true);
          return;
        }

        setStatus("success");
        setMessage("OCR finished successfully.");
        setReviewText(data.result.transcribed_text || "");
        setNeedsReview(false);
        return;
      } catch (err) {
        setStatus("error");
        setMessage(err.message || "Polling failed.");
        return;
      }
    }
  }

  async function handleSubmit() {
    if (!file) return;

    setStatus("submitting");
    setMessage("");
    setNeedsReview(false);
    setReviewText("");
    setSavedOcrText("");
    setShowOptions(false);
    setShowGptPrompt(false);
    setFinalNotes("");
    setShowDownloadOptions(false);

    const formData = new FormData();
    formData.append("image", file);

    try {
      const res = await fetch(SUBMIT_URL, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `Server responded with ${res.status}`);
      }

      const data = await res.json();
      setDagRunId(data.dag_run_id);
      setStatus("success");
      setMessage("Image submitted — waiting for OCR result...");
      await pollForResult(data.dag_run_id);
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "Submission failed.");
    }
  }

  async function saveOcrText() {
    if (!reviewText.trim()) return;

    setReviewSaving(true);

    try {
      if (dagRunId) {
        const res = await fetch(`http://localhost:8000/review/${dagRunId}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ corrected_text: reviewText }),
        });

        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          throw new Error(body.error || `Server responded with ${res.status}`);
        }
      }

      setSavedOcrText(reviewText.trim());
      setNeedsReview(false);
      setShowOptions(true);
      setShowGptPrompt(false);
      setFinalNotes("");
      setStatus("success");
      setMessage("OCR text saved. Choose how to continue.");
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "Failed to save OCR text.");
    } finally {
      setReviewSaving(false);
    }
  }

  async function handleEnhanceWithGpt() {
    if (!savedOcrText.trim() || !gptPrompt.trim()) return;

    setGptLoading(true);
    setMessage("");

    try {
      const res = await fetch("http://localhost:8000/enhance", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          notes: savedOcrText,
          prompt: gptPrompt,
        }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `Server responded with ${res.status}`);
      }

      const data = await res.json();
      setFinalNotes(data.final_notes || "");
      setShowGptPrompt(false);
      setStatus("success");
      setMessage("GPT enhanced your notes.");
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "GPT enhancement failed.");
    } finally {
      setGptLoading(false);
    }
  }

function continueToNextStep() {
  setShowOptions(false);
  setShowGptPrompt(false);
  setShowDownloadOptions(true);
  setStatus("success");
  setMessage("Choose a file type to download your notes.");
}

  return (
    <div style={{ maxWidth: 520, margin: "40px auto", fontFamily: "sans-serif" }}>
      <h1>Image Upload</h1>

      <input type="file" accept="image/*" onChange={handleFileChange} />

      {file && (
        <div style={{ marginTop: 12 }}>
          <img
            src={URL.createObjectURL(file)}
            alt="preview"
            style={{ maxWidth: "100%", maxHeight: 300 }}
          />
        </div>
      )}

      <div style={{ marginTop: 12 }}>
        <button onClick={handleSubmit} disabled={!file || status === "submitting"}>
          {status === "submitting" ? "Submitting..." : "Submit"}
        </button>
      </div>

      {needsReview && (
        <div style={{ marginTop: 20 }}>
          <label style={{ display: "block", fontWeight: 600, marginBottom: 8 }}>
            OCR result needs review
          </label>
          <textarea
            value={reviewText}
            onChange={(e) => setReviewText(e.target.value)}
            rows={8}
            style={{ width: "100%", boxSizing: "border-box" }}
          />
          <div style={{ marginTop: 10 }}>
            <button onClick={saveOcrText} disabled={reviewSaving || !reviewText.trim()}>
              {reviewSaving ? "Saving..." : "Save final OCR result"}
            </button>
          </div>
        </div>
      )}

{reviewText && !needsReview && !showOptions && !showDownloadOptions && !finalNotes && (        <div style={{ marginTop: 20 }}>
          <h3>OCR result</h3>
          <textarea
            value={reviewText}
            onChange={(e) => setReviewText(e.target.value)}
            rows={8}
            style={{ width: "100%", boxSizing: "border-box" }}
          />
          <div style={{ marginTop: 10 }}>
            <button onClick={saveOcrText} disabled={reviewSaving || !reviewText.trim()}>
              {reviewSaving ? "Saving..." : "Save final OCR result"}
            </button>
          </div>
        </div>
      )}

      {showOptions && (
        <div style={{ marginTop: 20 }}>
          <h3>What next?</h3>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <button onClick={() => setShowGptPrompt(true)}>Send to GPT</button>
            <button onClick={continueToNextStep}>Move on to next step</button>
          </div>
        </div>
      )}

      {showGptPrompt && (
        <div style={{ marginTop: 20 }}>
          <label style={{ display: "block", fontWeight: 600, marginBottom: 8 }}>
            Enter a prompt for GPT
          </label>
          <textarea
            value={gptPrompt}
            onChange={(e) => setGptPrompt(e.target.value)}
            rows={5}
            placeholder="Examples: rewrite as polished meeting notes, fix grammar, summarize into action items..."
            style={{ width: "100%", boxSizing: "border-box" }}
          />
          <div style={{ marginTop: 10 }}>
            <button onClick={handleEnhanceWithGpt} disabled={gptLoading || !gptPrompt.trim()}>
              {gptLoading ? "Enhancing..." : "Enhance my notes"}
            </button>
          </div>
        </div>
      )}

      {finalNotes && (
        <div style={{ marginTop: 20 }}>
          <h3>Final notes</h3>
          <pre style={{ whiteSpace: "pre-wrap", background: "#f5f5f5", padding: 12 }}>
            {finalNotes}
          </pre>

          <div style={{ marginTop: 16 }}>
            <h4>Download as</h4>
            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
              <button onClick={() => downloadNotes("txt")}>TXT</button>
              <button onClick={() => downloadNotes("word")}>Word</button>
              <button onClick={() => downloadNotes("one")}>OneNote</button>
            </div>
          </div>

          <div style={{ marginTop: 12 }}>
            <button onClick={continueToNextStep}>Ready for next step</button>
          </div>
        </div>
      )}{showDownloadOptions && (
  <div style={{ marginTop: 20 }}>
    <h3>Download your notes</h3>
    <p>Select the file type you want:</p>

    <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
      <button onClick={() => downloadNotes("txt")}>
        TXT
      </button>

      <button onClick={() => downloadNotes("word")}>
        Word
      </button>

      <button onClick={() => downloadNotes("one")}>
        OneNote
      </button>
    </div>
  </div>
)}


      {message && (
        <p style={{ color: status === "error" ? "red" : "green", marginTop: 16 }}>{message}</p>
      )}
    </div>
  );
}