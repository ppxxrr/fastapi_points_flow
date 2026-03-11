# FastAPI Points Flow

This project provides a FastAPI backend for exporting ICSP points flow data.

The API submits an in-process background task, fetches points flow data from ICSP, exports the result to Excel, and stores the file under `data/exports/`.

## API

- `GET /health`
- `POST /api/points-flow/tasks`
- `GET /api/points-flow/tasks/{task_id}`
- `GET /api/points-flow/downloads/{filename}`

## Task Flow

- create task -> `pending`
- background thread starts -> `running`
- service logs in to ICSP
- service fetches paged points flow data
- service exports an `.xlsx` file into `data/exports/`
- task -> `success`

The task detail does not expose or log the password.

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

`--workers 1` is required because task state is stored in process memory.
