# AutoDBSnap A Universal DB Backup Utility

A lightweight and reusable Python module to automate **database backups** for **PostgreSQL**, **MySQL**, and **MongoDB**, and upload them to **S3-compatible object storage** like AWS S3 or DigitalOcean Spaces. This utility is **framework-agnostic** and can be integrated easily into any **Django, Flask, FastAPI**, or plain Python project.

---

## 💡 Features

- ✅ Supports **PostgreSQL**, **MySQL**, and **MongoDB**
- ☁️ Uploads backups to **S3-compatible** object storage
- 🗑️ Automatically deletes local backup files after upload
- 🛠️ Fully configurable via `.env` or environment variables
- 🔁 Works with Celery, cron, or standalone scripts
- 📦 Easily pluggable into any Python project

---
## 🛠 Requirements

- Python 3.7+
- `pg_dump` (PostgreSQL)
- `mysqldump` (MySQL)
- `mongodump` (MongoDB)

## 📦 Dependencies

Install the required Python packages using pip:

```bash
pip install boto3
