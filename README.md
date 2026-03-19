# FastAPI Points Flow

This project provides a FastAPI backend for exporting ICSP points flow data and now includes a member information persistence module.

The API submits an in-process background task, fetches points flow data from ICSP, exports the result to Excel, and stores the file under `data/exports/`.

The member information module persists:

- `member_profile`
- `member_profile_attr`
- `member_account`
- `member_level_dict`
- `member_level_change_log`
- `member_level_period`
- `member_point_flow`
- `parking_record`

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

## Database Setup

1. Copy `.env.example` to `.env`
2. Fill `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
3. Run migrations:

```bash
alembic upgrade head
```

For local smoke testing you can also set:

```bash
DATABASE_URL=sqlite:///./data/member_module.db
```

## Member Sync Smoke Test

After the database is ready, run:

```bash
python scripts/init_member_data.py
```

Or directly use ORM metadata for a local SQLite smoke test:

```bash
python scripts/init_member_data.py --create-tables
```

To sync live member data through the existing ICSP login/session flow:

```bash
python scripts/sync_member_by_mobile.py --mobile 18719207571 --username your_icsp_user --password your_icsp_password
python scripts/sync_member_by_member_id.py --member-id 134680196345002462 --username your_icsp_user --password your_icsp_password
```

To extract `memberPhone` from historical point-flow CSV files and batch sync member information:

```bash
python scripts/sync_members_from_point_csv.py --input-dir ./data/points_csv --dry-run
python scripts/sync_members_from_point_csv.py --input-dir ./data/points_csv --limit 100 --username your_icsp_user --password your_icsp_password
python scripts/sync_members_from_point_csv.py --input-dir ./data/points_csv --output ./data/member_sync_result.csv --username your_icsp_user --password your_icsp_password
```

## CSV Import

Import historical point flow CSV files:

```bash
python scripts/import_member_point_flows_from_csv.py --dry-run
python scripts/import_member_point_flows_from_csv.py --limit-files 1 --limit-rows 500
python scripts/import_member_point_flows_from_csv.py
```

Import parking entry/exit CSV files:

```bash
python scripts/import_parking_records_from_csv.py --dry-run
python scripts/import_parking_records_from_csv.py --limit-files 1 --limit-rows 500
python scripts/import_parking_records_from_csv.py
```

## Incremental Sync

Detect missing business dates and backfill point-flow data:

```bash
python scripts/backfill_missing_point_flows.py --check-only
python scripts/backfill_missing_point_flows.py
python scripts/backfill_missing_point_flows.py --force-start-date 2026-03-01 --force-end-date 2026-03-03 --dry-run
```

Detect missing business dates and backfill parking data:

```bash
python scripts/backfill_missing_parking_records.py --check-only
python scripts/backfill_missing_parking_records.py
python scripts/backfill_missing_parking_records.py --force-start-date 2026-02-27 --force-end-date 2026-02-28 --dry-run
```

Extract new mobiles from business tables:

```bash
python scripts/extract_new_member_mobiles.py --start-date 2026-03-01 --end-date 2026-03-17 --dry-run
python scripts/extract_new_member_mobiles.py --start-date 2026-03-01 --end-date 2026-03-17 --output ./data/new_member_mobiles.csv
```

Sync only new members found in business tables:

```bash
python scripts/sync_new_members_from_business_data.py --start-date 2026-03-17 --end-date 2026-03-17 --dry-run
python scripts/sync_new_members_from_business_data.py --start-date 2026-03-17 --end-date 2026-03-17 --limit 50 --username your_icsp_user --password your_icsp_password
```

Run the daily incremental entrypoint for yesterday:

```bash
python scripts/run_daily_incremental_sync.py
python scripts/run_daily_incremental_sync.py --retry-pending-only
python scripts/run_daily_incremental_sync.py --job-date 2026-03-17 --dry-run --skip-member-sync
```

Logs are written under `logs/`:

- `logs/backfill_point_flow.log`
- `logs/backfill_parking.log`
- `logs/new_member_sync.log`
- `logs/daily_incremental_sync.log`

Parking incremental sync is currently wired to the CSV provider. A browser keepalive crawler can be added later by implementing the same window-provider interface used by `incremental_sync_service.py`.

`scripts/run_daily_incremental_sync.py` also uses a file lock under `data/scheduler/run_daily_incremental_sync.lock` so overlapping scheduler runs will exit as `skipped_locked` instead of racing on the same job date.

## Scheduler

`cron` example:

```cron
# 2:00 every day: run the full incremental sync for yesterday
0 2 * * * cd /srv/fastapi && /srv/fastapi/.venv/bin/python scripts/run_daily_incremental_sync.py >> /srv/fastapi/logs/cron_daily_incremental_sync.log 2>&1

# Every hour at :05: retry yesterday if the 2:00 run did not finish successfully
5 * * * * cd /srv/fastapi && /srv/fastapi/.venv/bin/python scripts/run_daily_incremental_sync.py --retry-pending-only >> /srv/fastapi/logs/cron_daily_incremental_retry.log 2>&1
```

`systemd` example:

```ini
# /etc/systemd/system/fastapi-daily-incremental-sync.service
[Unit]
Description=FastAPI daily incremental sync
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/srv/fastapi
ExecStart=/srv/fastapi/.venv/bin/python scripts/run_daily_incremental_sync.py
StandardOutput=append:/srv/fastapi/logs/systemd_daily_incremental_sync.log
StandardError=append:/srv/fastapi/logs/systemd_daily_incremental_sync.log
```

```ini
# /etc/systemd/system/fastapi-daily-incremental-sync.timer
[Unit]
Description=Run FastAPI daily incremental sync at 02:00

[Timer]
OnCalendar=*-*-* 02:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```ini
# /etc/systemd/system/fastapi-daily-incremental-retry.service
[Unit]
Description=FastAPI incremental retry checker
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/srv/fastapi
ExecStart=/srv/fastapi/.venv/bin/python scripts/run_daily_incremental_sync.py --retry-pending-only
StandardOutput=append:/srv/fastapi/logs/systemd_daily_incremental_retry.log
StandardError=append:/srv/fastapi/logs/systemd_daily_incremental_retry.log
```

```ini
# /etc/systemd/system/fastapi-daily-incremental-retry.timer
[Unit]
Description=Retry yesterday incremental sync every hour until success

[Timer]
OnCalendar=hourly
RandomizedDelaySec=300
Persistent=true

[Install]
WantedBy=timers.target
```

## SQLite To MySQL Migration

Create the target MySQL schema with Alembic:

```bash
python scripts/prepare_mysql_schema.py --target-url "mysql+pymysql://root:your_password@127.0.0.1:3306/fastapi_member"
```

Batch-migrate the core tables from SQLite to MySQL:

```bash
python scripts/migrate_sqlite_to_mysql.py --target-url "mysql+pymysql://root:your_password@127.0.0.1:3306/fastapi_member" --batch-size 2000
```

Migrate a subset first for rehearsal:

```bash
python scripts/migrate_sqlite_to_mysql.py --target-url "mysql+pymysql://root:your_password@127.0.0.1:3306/fastapi_member" --tables member_level_dict member_profile member_account --batch-size 1000
```

Verify counts and PK head/tail samples after migration:

```bash
python scripts/verify_mysql_migration.py --target-url "mysql+pymysql://root:your_password@127.0.0.1:3306/fastapi_member"
```

The migration order is:

- `member_level_dict`
- `member_profile`
- `member_profile_attr`
- `member_account`
- `member_level_change_log`
- `member_level_period`
- `member_point_flow`
- `parking_record`
- `sync_task_log`
- `sync_job_state`
