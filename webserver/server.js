import express from "express";
import multer from "multer";
import path from "path";
import fs from "fs";
import os from "os"
import "dotenv/config";

const app = express();
const PORT = process.env.PORT || 8000;

// Airflow REST API config
const AIRFLOW_BASE_URL = process.env.AIRFLOW_BASE_URL || "http://localhost:8080";
const AIRFLOW_DAG_ID = process.env.AIRFLOW_DAG_ID || "hackathon";
const AIRFLOW_USERNAME = process.env.AIRFLOW_USERNAME || "admin";
const AIRFLOW_PASSWORD = process.env.AIRFLOW_PASSWORD || "CrwH3emsuXpDx7AY";

// --- Multer setup: saves the uploaded image to disk ---
// IMPORTANT: this "uploads" folder needs to be visible to Airflow's
// worker/scheduler too (e.g. a shared Docker volume), since the DAG
// runs in a separate process/container and can't read Express's
// local filesystem otherwise.
const UPLOAD_DIR = path.join(os.homedir(), "airflow", "uploads");
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const unique = `${Date.now()}-${file.originalname}`;
    cb(null, unique);
  },
});
const upload = multer({ storage });

app.post("/upload", upload.single("image"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No image file received (expected field 'image')." });
  }

  const imagePath = req.file.path; // path on disk, e.g. uploads/169...-photo.jpg

  try {
    const dagRun = await triggerAirflowDag(imagePath);
    res.status(200).json({
      message: "Image received and DAG triggered.",
      dag_run_id: dagRun.dag_run_id,
      image_path: imagePath,
    });
  } catch (err) {
    console.error("Failed to trigger Airflow DAG:", err);
    res.status(502).json({ error: "Image saved, but failed to trigger Airflow DAG.", details: err.message });
  }
});

async function triggerAirflowDag(imagePath) {
  const url = `${AIRFLOW_BASE_URL}/api/v1/dags/${AIRFLOW_DAG_ID}/dagRuns`;
  const auth = Buffer.from(`${AIRFLOW_USERNAME}:${AIRFLOW_PASSWORD}`).toString("base64");

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${auth}`,
    },
    body: JSON.stringify({
      // conf is passed into the DAG run; your first task (detect_image)
      // can read it via `{{ dag_run.conf["image_path"] }}` (Jinja) or
      // `context["dag_run"].conf["image_path"]` inside a Python callable.
      conf: { image_path: imagePath },
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Airflow API responded ${response.status}: ${body}`);
  }

  return response.json();
}


app.listen(PORT, () => {
    console.log("We've now got a server!");
    console.log(`Your routes will be running on http://localhost:${PORT}`);
});
