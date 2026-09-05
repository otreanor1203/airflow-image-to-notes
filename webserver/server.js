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
const HITL_SUBMISSIONS = new Map();

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
app.post("/continue/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;
  const { enhance, prompt } = req.body || {};

  if (typeof enhance !== "boolean") {
    return res.status(400).json({
      error: "enhance must be true or false.",
    });
  }

  try {
    const cached = OCR_REVIEW_CACHE.get(dagRunId);

    if (!cached || !cached.transcribed_text) {
      return res.status(400).json({
        error: "No reviewed notes were found for this DAG run.",
      });
    }

    const decisionDir =
      process.env.AIRFLOW_DECISION_DIR ||
      "/home/owen/airflow/user_decisions";

    fs.mkdirSync(decisionDir, {
      recursive: true,
    });

    const decisionPath = path.join(
      decisionDir,
      `${dagRunId}.json`
    );

    const decision = {
      dag_run_id: dagRunId,
      enhance,
      prompt: prompt || "",
      notes: cached.transcribed_text,
      created_at: new Date().toISOString(),
    };

    fs.writeFileSync(
      decisionPath,
      JSON.stringify(decision, null, 2),
      "utf8"
    );

    return res.status(200).json({
      status: "choice_saved",
      dag_run_id: dagRunId,
      enhance,
    });
  } catch (err) {
    console.error(
      "Failed to save enhancement choice:",
      err
    );

    return res.status(500).json({
      error: "Failed to save enhancement choice.",
      details: err.message,
    });
  }
});

async function submitHitlResponse(dagRunId, taskId, chosenOptions, paramsInput = {}) {
  const submissionKey = `${dagRunId}:${taskId}`;
  const activeSubmission = HITL_SUBMISSIONS.get(submissionKey);
  if (activeSubmission) return activeSubmission;

  const submission = submitHitlResponseOnce(
    dagRunId,
    taskId,
    chosenOptions,
    paramsInput,
  );
  HITL_SUBMISSIONS.set(submissionKey, submission);

  try {
    await submission;
  } finally {
    HITL_SUBMISSIONS.delete(submissionKey);
  }
}

async function submitHitlResponseOnce(dagRunId, taskId, chosenOptions, paramsInput) {
  const token = await getAirflowToken();
  const taskUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}/taskInstances/${taskId}`;

  for (let attempt = 0; attempt < 60; attempt += 1) {
    const taskResponse = await fetch(taskUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!taskResponse.ok) {
      if (attempt < 59) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        continue;
      }
      throw new Error(`Could not find ${taskId} (${taskResponse.status}).`);
    }

    const task = await taskResponse.json();
    if (task.state === "success") return;
    if (!["awaiting_input", "deferred"].includes(task.state)) {
      if (attempt < 59) {
        await new Promise((resolve) => setTimeout(resolve, 500));
        continue;
      }
      throw new Error(`Timed out waiting for ${taskId} to await input (state: ${task.state}).`);
    }

    const response = await fetch(`${taskUrl}/-1/hitlDetails`, {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        ti_id: task.id,
        chosen_options: chosenOptions,
        params_input: paramsInput,
      }),
    });

    if (response.ok) return;

    const body = await response.text();
    if (response.status === 409) {
      return;
    }
    if (
      response.status === 404 &&
      body.includes("Human-in-the-loop detail does not exist") &&
      attempt < 59
    ) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      continue;
    }

    throw new Error(`Airflow rejected ${taskId} (${response.status}): ${body}`);
  }

  throw new Error(`Timed out waiting for ${taskId} to accept the HITL response.`);
}

app.post("/enhancement-choice/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;
  const { enhance, notes } = req.body || {};

  if (typeof enhance !== "boolean") {
    return res.status(400).json({ error: "enhance must be true or false." });
  }

  try {
    await submitHitlResponse(
      dagRunId,
      "choose_enhancement",
      [enhance ? "yes" : "no"],
      { notes: typeof notes === "string" ? notes.trim() : "" },
    );
    return res.status(200).json({ status: "enhancement_choice_saved", enhance });
  } catch (err) {
    console.error("Failed to submit enhancement choice:", err);
    return res.status(500).json({ error: "Failed to submit enhancement choice.", details: err.message });
  }
});

app.post("/enhancement-prompt/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;
  const { prompt, notes } = req.body || {};

  if (!prompt || typeof prompt !== "string" || !prompt.trim()) {
    return res.status(400).json({ error: "A valid prompt is required." });
  }

  try {
    await submitHitlResponse(dagRunId, "gpt_prompt", ["submit"], {
      prompt: prompt.trim(),
      notes: typeof notes === "string" ? notes.trim() : "",
    });
    return res.status(200).json({ status: "prompt_saved" });
  } catch (err) {
    console.error("Failed to submit GPT prompt:", err);
    return res.status(500).json({ error: "Failed to submit GPT prompt.", details: err.message });
  }
});

app.get("/enhancement-result/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;

  try {
    const token = await getAirflowToken();
    const taskUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}/taskInstances/gpt_enhance`;
    const taskResponse = await fetch(taskUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!taskResponse.ok) {
      return res.status(202).json({ status: "pending" });
    }

    const task = await taskResponse.json();
    if (task.state === "failed") {
      return res.status(500).json({ error: "Airflow GPT enhancement failed." });
    }
    if (task.state !== "success") {
      return res.status(202).json({ status: "pending" });
    }

    const xcomResponse = await fetch(`${taskUrl}/xcomEntries/return_value`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!xcomResponse.ok) {
      return res.status(202).json({ status: "pending" });
    }

    const xcom = await xcomResponse.json();
    const rawValue = xcom.value ?? xcom.data ?? xcom;
    const result = typeof rawValue === "string" ? JSON.parse(rawValue) : rawValue;
    return res.status(200).json({ status: "enhanced", result });
  } catch (err) {
    console.error("Failed to fetch enhancement result:", err);
    return res.status(500).json({ error: "Failed to fetch enhancement result.", details: err.message });
  }
});

app.post("/export-choice/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;
  const { file_type } = req.body || {};
  const normalizedType = file_type === "docx" ? "docx" : file_type;

  if (!["txt", "docx", "one"].includes(normalizedType)) {
    return res.status(400).json({ error: "Unsupported export format." });
  }

  try {
    await submitHitlResponse(dagRunId, "choose_export_format", [normalizedType]);
    return res.status(200).json({ status: "export_choice_saved", file_type: normalizedType });
  } catch (err) {
    console.error("Failed to submit export choice:", err);
    return res.status(500).json({ error: "Failed to submit export choice.", details: err.message });
  }
});

app.get("/export-choice-status/:dagRunId", async (req, res) => {
  const { dagRunId } = req.params;

  try {
    const token = await getAirflowToken();
    const response = await fetch(
      `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}/taskInstances/choose_export_format`,
      { headers: { Authorization: `Bearer ${token}` } },
    );

    if (!response.ok) {
      return res.status(202).json({ status: "pending" });
    }

    const task = await response.json();
    if (["awaiting_input", "deferred"].includes(task.state)) {
      return res.status(200).json({ status: "ready" });
    }
    if (["failed", "upstream_failed"].includes(task.state)) {
      return res.status(500).json({ error: "Airflow export choice task failed." });
    }

    return res.status(202).json({ status: "pending" });
  } catch (err) {
    console.error("Failed to check export choice status:", err);
    return res.status(500).json({ error: "Failed to check export choice status.", details: err.message });
  }
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

    const requestBody = JSON.stringify({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content:
            "You are a note-enhancement assistant. Follow the user's prompt carefully. Improve and polish the notes, preserve their original meaning, and fix formatting issues. If the user explicitly requests new factual content, include accurate, relevant additions and keep them clearly separated from the original notes. Return only the final enhanced notes as plain text.",
        },
        {
          role: "user",
          content: `User prompt: ${prompt.trim()}\n\nOriginal notes:\n${notes.trim()}`,
        },
      ],
      temperature: 0.4,
    });

    let response;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 120000);

      try {
        response = await fetch("https://api.openai.com/v1/chat/completions", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${apiKey}`,
          },
          body: requestBody,
          signal: controller.signal,
        });
      } catch (err) {
        if (attempt === 2) {
          throw new Error(
            err.name === "AbortError"
              ? "OpenAI request timed out after 120 seconds."
              : `Could not reach OpenAI: ${err.message}`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 1000 * (attempt + 1)));
        continue;
      } finally {
        clearTimeout(timeout);
      }

      if (response.ok || ![429, 500, 502, 503, 504].includes(response.status)) {
        break;
      }

      if (attempt < 2) {
        const retryAfter = Number(response.headers.get("retry-after"));
        const delay = Number.isFinite(retryAfter)
          ? Math.min(retryAfter * 1000, 10000)
          : 1000 * (attempt + 1);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

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
  const allowed = new Set(["txt", "word", "doc", "docx", "one", "onenote"]);

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

    const normalizedType = fileType === "doc" || fileType === "word" ? "docx" : fileType === "onenote" ? "one" : fileType;
    const fileName = `notes.${normalizedType === "docx" ? "docx" : normalizedType === "one" ? "one" : "txt"}`;
    const exportTaskUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dagRunId}/taskInstances/export_file`;
    const exportTaskResponse = await fetch(exportTaskUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (exportTaskResponse.ok) {
      const exportTask = await exportTaskResponse.json();
      if (exportTask.state === "failed" || exportTask.state === "upstream_failed") {
        return res.status(500).json({ error: "Airflow export task failed." });
      }
    }

    const exportXcomUrl = `${exportTaskUrl}/xcomEntries/return_value`;
    const exportXcomResponse = await fetch(exportXcomUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!exportXcomResponse.ok) {
      return res.status(202).json({ status: "pending" });
    }

    const exportXcom = await exportXcomResponse.json();
    const exportResult = exportXcom.value ?? exportXcom.data ?? exportXcom;
    const parsedResult = typeof exportResult === "string" ? JSON.parse(exportResult) : exportResult;
    const generatedPath = parsedResult?.output_path;

    if (!generatedPath || !fs.existsSync(generatedPath)) {
      return res.status(202).json({ status: "pending" });
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