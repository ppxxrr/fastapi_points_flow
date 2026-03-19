# FastAPI + MariaDB(Systemd) 生产部署说明

本文档用于当前仓库在 Linux 服务器上切换到 MariaDB 生产运行。本文档默认：

- 项目部署目录为 `/srv/fastapi`
- 虚拟环境目录为 `/srv/fastapi/.venv`
- MariaDB 运行在服务器本机 `127.0.0.1:3306`
- 现有定时任务继续复用：
  - `scripts/run_daily_incremental_sync.py`
  - `scripts/run_daily_incremental_sync.py --retry-pending-only`
- 现有日志、文件锁、`sync_task_log`、`sync_job_state` 继续复用

如果你的服务器目录不是 `/srv/fastapi`，请在本文档和 `deploy/systemd/` 文件中统一替换。

## 1. 生产 `.env` 配置

生产环境建议优先使用 `MYSQL_*`，不要在生产里优先写 `DATABASE_URL`，这样可以避免密码里包含 `#` 时再做 URL 编码。

参考模板：

- [deploy/env/.env.production.mysql.example](/d:/python/menbers/codex/FastAPI/deploy/env/.env.production.mysql.example)

服务器建议放置路径：

```bash
/srv/fastapi/.env
```

关键原则：

- `ICSP_USERNAME` / `ICSP_PASSWORD` 只能填写后台高权限服务账号
- 不要把前台普通用户账号写进 `.env`
- 如果服务器上需要接反向代理，Web 进程建议仍监听 `127.0.0.1:8000`

## 2. 部署前准备

切换前先在服务器准备目录和权限：

```bash
sudo mkdir -p /srv/fastapi
sudo chown -R fastapi:fastapi /srv/fastapi
sudo -u fastapi mkdir -p /srv/fastapi/logs /srv/fastapi/data/scheduler
```

如果代码尚未拉到服务器：

```bash
sudo -u fastapi git clone <your-repo-url> /srv/fastapi
cd /srv/fastapi
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

如果代码已经在服务器，只需要更新代码并安装依赖：

```bash
cd /srv/fastapi
git pull
.venv/bin/pip install -r requirements.txt
```

## 3. 运行前预检

仓库内提供了一个轻量预检脚本：

- [scripts/deploy/preflight_check.py](/d:/python/menbers/codex/FastAPI/scripts/deploy/preflight_check.py)

建议在切换前执行：

```bash
cd /srv/fastapi
.venv/bin/python scripts/deploy/preflight_check.py --expect-mysql --require-icsp-credentials --create-dirs
```

预检会检查：

- 当前实际生效的数据库后端
- 是否误回退到了 SQLite
- MariaDB 是否可连通
- `logs/`、`data/`、`data/scheduler/` 是否存在
- 关键环境变量是否已配置

## 4. 停车数据专项核对脚本

仓库内提供了一个轻量停车数据核对脚本：

- [scripts/deploy/check_parking_data_gap.py](/d:/python/menbers/codex/FastAPI/scripts/deploy/check_parking_data_gap.py)

建议在切换前做一次基线检查，切换后再做一次对比：

```bash
cd /srv/fastapi
.venv/bin/python scripts/deploy/check_parking_data_gap.py --scan-csv-rows
```

这个脚本会输出：

- 停车 CSV 目录下匹配到的文件数
- 可识别为停车导入格式的 CSV 数
- 原始 CSV 数据行总数
- 当前 `parking_record` 表总记录数
- `parking_record` 的时间覆盖范围
- `parking_record` 中手机号/车牌/记录ID/停车流水号的空值情况
- 最近停车导入/补齐相关 `sync_task_log`

这个脚本的目的不是直接判定“谁对谁错”，而是把以下疑点明确量化：

- 停车 CSV 单文件约 14 万行，且不止一个文件
- 但当前 `parking_record` 只有几千条
- 这个数量级明显不匹配，不能默认视为“数据已经完整”

后续排查必须重点关注：

1. 停车 CSV 原始总行数
2. 实际导入成功条数
3. 幂等去重后的净新增条数
4. 是否存在按日期窗口、唯一键去重、字段解析失败、历史文件未实际执行等情况
5. 不要凭空下结论是“导入失败”或“去重正常”，必须先核对数据

## 5. 安装 FastAPI Web Service

仓库中提供了 systemd 模板：

- [deploy/systemd/fastapi-web.service](/d:/python/menbers/codex/FastAPI/deploy/systemd/fastapi-web.service)

服务器安装命令：

```bash
sudo cp deploy/systemd/fastapi-web.service /etc/systemd/system/fastapi-web.service
sudo systemctl daemon-reload
sudo systemctl enable --now fastapi-web
```

常用命令：

```bash
sudo systemctl status fastapi-web
sudo systemctl restart fastapi-web
sudo systemctl stop fastapi-web
sudo journalctl -u fastapi-web -f
```

## 6. 安装每日增量 Timer

仓库中提供：

- [deploy/systemd/fastapi-daily-incremental-sync.service](/d:/python/menbers/codex/FastAPI/deploy/systemd/fastapi-daily-incremental-sync.service)
- [deploy/systemd/fastapi-daily-incremental-sync.timer](/d:/python/menbers/codex/FastAPI/deploy/systemd/fastapi-daily-incremental-sync.timer)

安装命令：

```bash
sudo cp deploy/systemd/fastapi-daily-incremental-sync.service /etc/systemd/system/
sudo cp deploy/systemd/fastapi-daily-incremental-sync.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fastapi-daily-incremental-sync.timer
```

说明：

- 每天 `02:00` 自动执行一次
- 实际执行脚本：`scripts/run_daily_incremental_sync.py`

查看日志：

```bash
sudo systemctl status fastapi-daily-incremental-sync.timer
sudo journalctl -u fastapi-daily-incremental-sync.service -f
```

手工补跑：

```bash
sudo systemctl start fastapi-daily-incremental-sync.service
```

## 7. 安装每小时 Retry Timer

仓库中提供：

- [deploy/systemd/fastapi-daily-incremental-retry.service](/d:/python/menbers/codex/FastAPI/deploy/systemd/fastapi-daily-incremental-retry.service)
- [deploy/systemd/fastapi-daily-incremental-retry.timer](/d:/python/menbers/codex/FastAPI/deploy/systemd/fastapi-daily-incremental-retry.timer)

安装命令：

```bash
sudo cp deploy/systemd/fastapi-daily-incremental-retry.service /etc/systemd/system/
sudo cp deploy/systemd/fastapi-daily-incremental-retry.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fastapi-daily-incremental-retry.timer
```

说明：

- 每小时 `05` 分执行一次
- 实际执行脚本：`scripts/run_daily_incremental_sync.py --retry-pending-only`
- 如果昨天已成功，脚本会直接跳过
- 如果昨天失败或未成功，会继续补跑

查看日志：

```bash
sudo systemctl status fastapi-daily-incremental-retry.timer
sudo journalctl -u fastapi-daily-incremental-retry.service -f
```

手工触发 retry：

```bash
sudo systemctl start fastapi-daily-incremental-retry.service
```

## 8. 日志查看方式

当前项目保留两层日志：

### 文件日志

固定目录：

```bash
/srv/fastapi/logs
```

常见文件：

- `logs/daily_incremental_sync.log`
- `logs/backfill_point_flow.log`
- `logs/backfill_parking.log`
- `logs/new_member_sync.log`
- `logs/mysql_migration.log`

查看方式：

```bash
tail -f /srv/fastapi/logs/daily_incremental_sync.log
tail -f /srv/fastapi/logs/new_member_sync.log
tail -f /srv/fastapi/logs/backfill_parking.log
```

### 数据库日志

核心表：

- `sync_task_log`
- `sync_job_state`

推荐查询：

```sql
SELECT id, module_name, action, status, started_at, finished_at
FROM sync_task_log
ORDER BY id DESC
LIMIT 20;

SELECT job_name, job_date, status, retry_count, updated_at
FROM sync_job_state
ORDER BY updated_at DESC
LIMIT 20;
```

停车专项日志建议额外看：

```sql
SELECT id, module_name, action, status, target_value, started_at, finished_at
FROM sync_task_log
WHERE module_name IN ('parking_record_csv_import', 'parking_record_incremental', 'daily_incremental_sync')
ORDER BY id DESC
LIMIT 30;
```

## 9. 避免重叠执行

当前项目已经有文件锁：

```bash
/srv/fastapi/data/scheduler/run_daily_incremental_sync.lock
```

systemd 侧不需要再额外做复杂互斥。即使手工补跑和 timer 重叠，脚本也会因为拿不到锁而返回 `skipped_locked`。

## 10. 正式切换 SOP

### 切换前

1. 停掉当前仍指向 SQLite 的旧 Web 进程和旧定时任务
2. 备份当前 `.env`
3. 备份 `data/member_module.db`
4. 确认 MariaDB 目标库已完成迁移和对账
5. 把生产 `.env` 放到 `/srv/fastapi/.env`
6. 执行预检：

```bash
cd /srv/fastapi
.venv/bin/python scripts/deploy/preflight_check.py --expect-mysql --require-icsp-credentials --create-dirs
```

7. 执行停车专项基线检查：

```bash
cd /srv/fastapi
.venv/bin/python scripts/deploy/check_parking_data_gap.py --scan-csv-rows
```

### 切换时

```bash
cd /srv/fastapi
.venv/bin/python -m alembic upgrade head
sudo cp deploy/systemd/fastapi-web.service /etc/systemd/system/
sudo cp deploy/systemd/fastapi-daily-incremental-sync.service /etc/systemd/system/
sudo cp deploy/systemd/fastapi-daily-incremental-sync.timer /etc/systemd/system/
sudo cp deploy/systemd/fastapi-daily-incremental-retry.service /etc/systemd/system/
sudo cp deploy/systemd/fastapi-daily-incremental-retry.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fastapi-web
sudo systemctl enable --now fastapi-daily-incremental-sync.timer
sudo systemctl enable --now fastapi-daily-incremental-retry.timer
```

### 切换后验证

先看服务：

```bash
sudo systemctl status fastapi-web
sudo journalctl -u fastapi-web -n 100 --no-pager
```

再手工跑一次每日任务：

```bash
sudo systemctl start fastapi-daily-incremental-sync.service
sudo journalctl -u fastapi-daily-incremental-sync.service -n 200 --no-pager
```

然后在 MySQL 验证任务状态：

```sql
SELECT id, module_name, action, status, started_at, finished_at
FROM sync_task_log
ORDER BY id DESC
LIMIT 20;

SELECT job_name, job_date, status, retry_count, last_error, updated_at
FROM sync_job_state
ORDER BY updated_at DESC
LIMIT 20;
```

停车专项验证不要省略，必须做：

```bash
cd /srv/fastapi
.venv/bin/python scripts/deploy/check_parking_data_gap.py --scan-csv-rows
```

重点核对：

1. 停车 CSV 原始总文件数
2. 停车 CSV 原始总数据行数
3. `parking_record` 当前表记录数
4. 最近停车导入日志里的 `inserted_count / updated_count / skipped_count / failed_count`
5. 当前 `parking_record` 数量少，不能默认等于“数据已完整”
6. 如果数量级仍明显不匹配，优先进入专项排查，不要先上线后忽略

## 11. 回滚 SOP

如果切换后发现运行异常：

1. 先停掉 MySQL 版 Web 和两个 timer

```bash
sudo systemctl disable --now fastapi-daily-incremental-sync.timer
sudo systemctl disable --now fastapi-daily-incremental-retry.timer
sudo systemctl stop fastapi-web
```

2. 把 `/srv/fastapi/.env` 改回 SQLite 配置，或移除 `MYSQL_*` / `DATABASE_URL`
3. 重启 SQLite 版 Web
4. 恢复 SQLite 版旧定时任务，但前提是确认 MySQL 版定时器已经停掉
5. 回滚时不要双写，不要让 SQLite 和 MySQL 两套调度同时运行

回滚后建议先查：

- SQLite 中的 `sync_task_log`
- SQLite 中的 `sync_job_state`
- MySQL 中的 `sync_task_log`
- MySQL 中的 `sync_job_state`

确认切换窗口内到底是哪一侧执行过任务，再决定是否补跑。

## 12. 停车场数据疑点说明与后续排查方向

当前必须保留一个明确疑点：

- 停车场 CSV 单文件约 14 万行，且不止一个 CSV
- 但当前库里 `parking_record` 只有几千条
- 这个数量级明显不匹配

当前不要预设结论。需要重点排查的方向包括：

1. 历史停车 CSV 是否真的已经全部执行过导入
2. 是否只导入了测试样本或少量文件
3. 导入时是否存在大量 `failed_count`
4. 是否存在字段解析失败，但没有被及时汇总复盘
5. 是否有大量记录命中了唯一键幂等，导致净新增远小于原始行数
6. 是否有按日期窗口只扫到部分业务日期
7. 是否有 CSV 编码、表头匹配、空行判断等问题，导致文件被整批跳过
8. 是否存在其他上游业务含义，例如一条停车业务导出多次、重复导出、非最终状态重复行

建议排查顺序：

1. 先跑 `check_parking_data_gap.py --scan-csv-rows`
2. 再看 `sync_task_log` 中历史停车导入和补齐日志
3. 再按文件、日期、唯一键策略复核导入逻辑
4. 最后再判断是否需要补跑历史停车数据
