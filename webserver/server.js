import express from "express";
import multer from "multer";
import path from "path";
import fs from "fs";
import cors from "cors";
import "dotenv/config";

const app = express();
const PORT = process.env.PORT || 8000;

// Airflow REST API config
const AIRFLOW_BASE_URL = process.env.AIRFLOW_BASE_URL || "http://localhost:8080";
const AIRFLOW_DAG_ID = process.env.AIRFLOW_DAG_ID || "hackathon";
const AIRFLOW_USERNAME = process.env.AIRFLOW_USERNAME || "admin";
const AIRFLOW_PASSWORD = process.env.AIRFLOW_PASSWORD || "admin";
// task_id of the HITLEntryOperator waiting for the message
const HITL_TASK_ID = process.env.HITL_TASK_ID || "add_user_prompt";
// The key inside `params={...}` on that operator in your DAG file —
// this MUST match exactly, e.g. Param("", type="string") registered
// under this name.
const HITL_PARAM_KEY = process.env.HITL_PARAM_KEY || "note_context";

app.use(cors());
app.use(express.json());

// --- Multer setup: saves the uploaded image to disk ---
// IMPORTANT: this "uploads" folder needs to be visible to Airflow's
// worker/scheduler too, since the DAG runs in a separate process and
// can't read Express's local filesystem otherwise.
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
  const { note_context } = req.body;
  if (!note_context) {
    return res.status(400).json({ error: "Expected a 'note_context' field in the request body." });
  }

  const imagePath = req.file.path;

  try {
    const dagRun = await triggerAirflowDag(imagePath);
    const dagRunId = dagRun.dag_run_id;

    // detect_image has to run first, so add_user_prompt won't be
    // awaiting_input the instant the run is created — poll briefly
    // until this specific run's HITL task shows up.
    const hitlDetail = await waitForHitlTask(dagRunId);
    const result = await respondToHitlTask(hitlDetail, note_context);

    res.status(200).json({
      message: "Image and note submitted; DAG resumed.",
      dag_run_id: dagRunId,
      image_path: imagePath,
      result,
    });
  } catch (err) {
    console.error("Failed to submit image + note:", err);
    res.status(502).json({ error: "Submission failed.", details: err.message });
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Polls the hitlDetails list until this exact dag_run_id's HITL task
// shows up as awaiting_input, since detect_image must finish first.
async function waitForHitlTask(dagRunId, { maxAttempts = 20, intervalMs = 500 } = {}) {
  const token = await getAirflowToken();
  const listUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/~/dagRuns/~/hitlDetails?response_received=false`;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const listRes = await fetch(listUrl, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!listRes.ok) {
      const body = await listRes.text();
      throw new Error(`Failed to list pending HITL tasks (${listRes.status}): ${body}`);
    }

    const listData = await listRes.json();
    const match = (listData.hitl_details || []).find(
      (d) =>
        d.task_instance.dag_run_id === dagRunId &&
        d.task_instance.task_id === HITL_TASK_ID &&
        d.task_instance.dag_id === AIRFLOW_DAG_ID
    );

    if (match) return match;

    await sleep(intervalMs);
  }

  throw new Error(
    `Timed out waiting for '${HITL_TASK_ID}' to reach awaiting_input for run '${dagRunId}'.`
  );
}

// Submits the response to one specific HITL detail object (as
// returned by waitForHitlTask).
async function respondToHitlTask(hitlDetail, message) {
  const token = await getAirflowToken();
  const { dag_run_id, task_id, map_index } = hitlDetail.task_instance;

  const patchUrl = `${AIRFLOW_BASE_URL}/api/v2/dags/${AIRFLOW_DAG_ID}/dagRuns/${dag_run_id}/taskInstances/${task_id}/${map_index}/hitlDetails`;

  const patchRes = await fetch(patchUrl, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({
      chosen_options: hitlDetail.defaults || ["OK"],
      params_input: { [HITL_PARAM_KEY]: message },
    }),
  });

  if (!patchRes.ok) {
    const body = await patchRes.text();
    throw new Error(`Failed to submit HITL response (${patchRes.status}): ${body}`);
  }

  return patchRes.json();
}

app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});