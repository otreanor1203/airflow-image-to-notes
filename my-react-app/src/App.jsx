import { useState } from "react";

// Change this to wherever your backend actually listens.
const UPLOAD_URL = "http://localhost:8000/upload";

export default function App() {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("idle");
  const [message, setMessage] = useState("");

  function handleFileChange(e) {
    const selected = e.target.files?.[0] ?? null;
    setFile(selected);
    setStatus("idle");
    setMessage("");
  }

  async function handleUpload() {
    if (!file) return;

    setStatus("uploading");
    setMessage("");

    const formData = new FormData();
    formData.append("image", file);

    try {
      const res = await fetch(UPLOAD_URL, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        throw new Error(`Server responded with ${res.status}`);
      }

      setStatus("success");
      setMessage("Upload succeeded.");
    } catch (err) {
      setStatus("error");
      setMessage(err.message || "Upload failed.");
    }
  }

  return (
    <div style={{ maxWidth: 480, margin: "40px auto", fontFamily: "sans-serif" }}>
      <h1>Upload Image</h1>

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
        <button onClick={handleUpload} disabled={!file || status === "uploading"}>
          {status === "uploading" ? "Uploading..." : "Upload"}
        </button>
      </div>

      {message && (
        <p style={{ color: status === "error" ? "red" : "green" }}>{message}</p>
      )}
    </div>
  );
}
