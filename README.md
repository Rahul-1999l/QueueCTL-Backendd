# QueueCTL — Minimal Python Job Queue (SQLite + Click)

A tiny background job queue built **from scratch** with Python, SQLite, and a Click-based CLI.  
Supports retries with exponential backoff and a **Dead Letter Queue (DLQ)** for jobs that exhaust retries.

---

## ✨ Features
- Enqueue any shell command with configurable `--retries`
- Persistent storage with SQLite (no Redis/RabbitMQ needed)
- Worker with exponential backoff
- DLQ listing to inspect failed jobs
- Clean, single-file CLI entry (`python -m queuectl.cli …`)

---

## 🚀 Quick Start (Windows / CMD)

```bat
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

:: sanity check
python -m queuectl.cli hello
