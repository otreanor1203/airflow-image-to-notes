import express from "express";
import multer from "multer";
import path from "path";
import fs from "fs";
import cors from "cors";
import dotenv from "dotenv";
import { spawnSync } from "child_process";

const app = express();
const PORT = process.env.PORT || 8000;
const OCR_REVIEW_CACHE = new Map();

dotenv.config({ path: path.resolve(process.cwd(), ".env") });
dotenv.config({ path: path.resolve(process.cwd(), "..", ".env") });

const AIRFLOW_BASE_URL = process.env.AIRFLOW_BASE_URL || "http://localhost:8080";
const AIRFLOW_DAG_ID = process.env.AIRFLOW_DAG_ID || "hackathon";
const AIRFLOW_USERNAME = process.env.AIRFLOW_USERNAME || "admin";
const AIRFLOW_PASSWORD = process.env.AIRFLOW_PASSWORD || "admin";

app.use(cors());
app.use(express.json());

const UPLOAD_DIR = path.join(process.cwd(), "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const unique = `${Date.now()}-${file.originalname}`;
    cb(null, unique);
  },
});
const upload = multer({ storage });

app.post("/submit", upload.single("image"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No image file received (expected field 'image')." });
  }

  const imagePath = req.file.path;

  try {
    const dagRun = await triggerAirflowDag(imagePath);
    const dagRunId = dagRun.dag_run_id;

    res.status(200).json({
      message: "Image submitted; DAG started.",
      dag_run_id: dagRunId,
      image_path: imagePath,
    });
  } catch (err) {
    console.error("Failed to submit image:", err);
    res.status(502).json({ error: "Submission failed.", details: err.message });
  }
});

app.get("/result/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;

  try {
    const result = await getOcrResultForDagRun(dagRunId);

    if (!result) {
      return res.status(202).json({ status: "pending" });
    }

    if (result.confidence < 90) {
      OCR_REVIEW_CACHE.set(dagRunId, result);
      return res.status(200).json({ status: "needs_review", result });
    }

    return res.status(200).json({ status: "approved", result });
  } catch (err) {
    console.error("Failed to fetch OCR result:", err);
    return res.status(500).json({ error: "Failed to fetch OCR result.", details: err.message });
  }
});

app.post("/review/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;
  const { corrected_text } = req.body || {};

  if (!corrected_text || typeof corrected_text !== "string") {
    return res.status(400).json({ error: "A valid corrected_text field is required." });
  }

  const currentResult = OCR_REVIEW_CACHE.get(dagRunId) || {};
  const updatedResult = {
    ...currentResult,
    transcribed_text: corrected_text,
    needs_human_review: false,
    reviewed: true,
    status: "reviewed",
  };

  OCR_REVIEW_CACHE.set(dagRunId, updatedResult);

  return res.status(200).json({
    status: "saved",
    dag_run_id: dagRunId,
    result: updatedResult,
  });
});

app.post("/enhance", async (req, res) => {
  const { notes, prompt } = req.body || {};

  if (!notes || typeof notes !== "string") {
    return res.status(400).json({ error: "A valid notes string is required." });
  }

  if (!prompt || typeof prompt !== "string" || !prompt.trim()) {
    return res.status(400).json({ error: "A valid prompt is required." });
  }

  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      throw new Error("OPENAI_API_KEY is not configured.");
    }

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "You are a note-enhancement assistant. Improve and polish the user's notes without inventing facts. Preserve the original meaning, fix formatting issues, and make the notes cleaner and more readable. Return only the final enhanced notes as plain text.",
          },
          {
            role: "user",
            content: `User prompt: ${prompt}\n\nOriginal notes:\n${notes}`,
          },
        ],
        temperature: 0.4,
      }),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`OpenAI request failed (${response.status}): ${text}`);
    }

    const data = await response.json();
    const finalNotes = data?.choices?.[0]?.message?.content?.trim();

    if (!finalNotes) {
      throw new Error("OpenAI returned no notes.");
    }

    return res.status(200).json({
      status: "enhanced",
      final_notes: finalNotes,
    });
  } catch (err) {
    console.error("Failed to enhance notes:", err);
    return res.status(500).json({ error: "Failed to enhance notes.", details: err.message });
  }
});

app.get("/download/:dagRunId/:fileType", async (req, res) => {
  const { dagRunId, fileType } = req.params;
  const allowed = new Set(["txt", "word", "doc", "one", "onenote"]);

  if (!allowed.has(fileType)) {
    return res.status(400).json({ error: "Unsupported file type." });
  }

  try {
    const token = await getAirflowToken();
    const dagRunResponse = await fetch(
      `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );

    if (!dagRunResponse.ok) {
      throw new Error(`Could not find DAG run ${dagRunId}`);
    }
    
    const finalText = await getFinalTextForDagRun(dagRunId);

    if (!finalText) {
      return res.status(400).json({ error: "No final text available for this DAG run." });
    }

    const exportRoot = path.resolve(process.cwd(), "..", "exports");
    fs.mkdirSync(exportRoot, { recursive: true });

    const normalizedType = fileType === "doc" ? "word" : fileType === "onenote" ? "one" : fileType;
    const fileTypeArg = normalizedType === "word" ? "word" : normalizedType === "one" ? "one" : "txt";
    const fileName = `notes.${fileTypeArg === "word" ? "doc" : fileTypeArg === "one" ? "one" : "txt"}`;
    const filePath = path.join(exportRoot, fileName);

    const pythonProcess = spawnSync("python3", [
      "-c",
      `import sys; sys.path.insert(0, '/home/owen/airflow'); from dags.tasks.export_notes import create_export_file; print(create_export_file(sys.argv[1], file_type=sys.argv[2], output_dir=sys.argv[3]))`,
      finalText,
      fileTypeArg,
      exportRoot,
    ], {
      encoding: "utf-8",
    });

    if (pythonProcess.status !== 0) {
      const stderr = pythonProcess.stderr || "";
      throw new Error(stderr || "Airflow export helper failed.");
    }

    const generatedPath = (pythonProcess.stdout || "").trim();
    if (!generatedPath || !fs.existsSync(generatedPath)) {
      throw new Error("Airflow export helper did not produce a file.");
    }

    res.download(generatedPath, fileName);
  } catch (err) {
    console.error("Download failed:", err);
    res.status(500).json({ error: "File generation failed.", details: err.message });
  }
});

// Airflow 3's API requires a JWT, not Basic Auth. We exchange
// username/password for a short-lived token first, then use that
// token as a Bearer header on the actual dagRuns call.
async function getAirflowToken() {
  const res = await fetch(`${AIRFLOW_BASE_URL}/auth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username: AIRFLOW_USERNAME,
      password: AIRFLOW_PASSWORD,
    }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Failed to get Airflow token (${res.status}): ${body}`);
  }

  const data = await res.json();
  return data.access_token;
}

async function getFinalTextForDagRun(dagRunId) {
  const token = await getAirflowToken();
  const xcomUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}/taskInstances/revise_notes/xcomEntries/return_value`;

  const xcomRes = await fetch(xcomUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!xcomRes.ok) return null;

  const xcomData = await xcomRes.json();
  const rawValue = xcomData.value ?? xcomData.data ?? xcomData;

  const parsed = typeof rawValue === "string" ? JSON.parse(rawValue) : rawValue;
  return parsed && typeof parsed === "object" ? parsed.revised_text || null : null;
}

async function triggerAirflowDag(imagePath) {
  const token = await getAirflowToken();

  const url = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({
      conf: { image_path: imagePath },
      logical_date: null,
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Airflow API responded ${response.status}: ${body}`);
  }

  return response.json();
}

async function getOcrResultForDagRun(dagRunId) {
  const token = await getAirflowToken();
  const taskUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}/taskInstances/ocr_image`;

  const taskRes = await fetch(taskUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!taskRes.ok) return null;

  const taskData = await taskRes.json();
  if (!taskData || taskData.state !== "success") return null;

  const xcomUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}/taskInstances/ocr_image/xcomEntries/return_value`;
  const xcomRes = await fetch(xcomUrl, {
    headers: { Authorization: `Bearer ${token}` },
  });

  if (!xcomRes.ok) return null;

  const xcomData = await xcomRes.json();
  const rawValue = xcomData.value ?? xcomData.data ?? xcomData;

  if (typeof rawValue === "string") {
    try {
      return JSON.parse(rawValue);
    } catch {
      return null;
    }
  }

  return rawValue && typeof rawValue === "object" ? rawValue : null;
}

app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});