import { useState } from "react";

// Change this to wherever your backend actually listens.
const SUBMIT_URL = "http://localhost:8000/submit";

export default function App() {
  const [file, setFile] = useState(null);
  const [noteContext, setNoteContext] = useState("");
  const [status, setStatus] = useState("idle"); // idle | submitting | success | error
  const [message, setMessage] = useState("");

  function handleFileChange(e) {
    const selected = e.target.files?.[0] ?? null;
    setFile(selected);
    setStatus("idle");
    setMessage("");
  }

  async function handleSubmit() {
    if (!file || !noteContext.trim()) return;

    setStatus("submitting");
    setMessage("");

    const formData = new FormData();
    formData.append("image", file);
    formData.append("note_context", noteContext);

    try {
      const res = await fetch(SUBMIT_URL, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error || `Server responded with ${res.status}`);
      }

      setStatus("success");
      setMessage("Image and note submitted — DAG is running.");
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "Submission failed.");
    }
  }

  return (
    <div style={{ maxWidth: 480, margin: "40px auto", fontFamily: "sans-serif" }}>
      <h1>Photo to Text</h1>

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
        <textarea
          value={noteContext}
          onChange={(e) => setNoteContext(e.target.value)}
          placeholder="Briefly describe what these notes are about..."
          rows={4}
          style={{ width: "100%" }}
        />
      </div>

      <div style={{ marginTop: 12 }}>
        <button
          onClick={handleSubmit}
          disabled={!file || !noteContext.trim() || status === "submitting"}
        >
          {status === "submitting" ? "Submitting..." : "Submit"}
        </button>
      </div>

      {message && (
        <p style={{ color: status === "error" ? "red" : "green" }}>{message}</p>
      )}
    </div>
  );
}