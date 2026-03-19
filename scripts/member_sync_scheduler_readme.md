# Member Sync Scheduler

这套方案只增加调度包装层，不改现有会员同步业务逻辑。

## 目标

- 统一用 UTF-8 环境启动批量任务
- 每次只跑一个批次
- 通过状态文件推进到下一批
- 结果判断不只看进程退出码

## 新增脚本

- `scripts/run_member_sync_batch.ps1`
  - Windows 任务计划程序直接调用的入口
  - 负责切换 UTF-8、设置 Python 输出编码、调用 Python 包装层
- `scripts/run_member_sync_batch.py`
  - 生成或复用手机号 manifest
  - 按 `batch_no` / `batch_size` 切出当前批次
  - 调用现有 `scripts/sync_members_from_point_csv.py`
  - 生成每批的日志、结果文件、校验报告、状态文件
- `scripts/check_member_sync_batch.py`
  - 校验一批是否真实完成
  - 综合检查结果 CSV、数据库计数增量、`sync_task_log` 增量

## 推荐目录

运行过程中会生成以下文件：

- `data/scheduler/member_sync/member_sync_manifest.csv`
- `data/scheduler/member_sync/member_sync_manifest.meta.json`
- `data/scheduler/member_sync/member_sync_state_1000.json`
- `data/scheduler/member_sync/batches_1000/batch_000001/...`

## 推荐执行方式

### 1. 先生成 manifest

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_member_sync_batch.ps1 -PrepareManifestOnly -BatchSize 1000 -InputDir .\csv
```

### 2. 跑第 1 批

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_member_sync_batch.ps1 -BatchSize 1000 -BatchNo 1 -InputDir .\csv
```

### 3. 跑第 2 批

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_member_sync_batch.ps1 -BatchSize 1000 -BatchNo 2 -InputDir .\csv
```

### 4. 自动按状态推进

不传 `-BatchNo` 时，会读取 `member_sync_state_1000.json` 的 `next_batch_no`：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_member_sync_batch.ps1 -BatchSize 1000 -InputDir .\csv
```

## UTF-8 运行环境

`run_member_sync_batch.ps1` 已经做了下面这些事：

- `chcp 65001`
- `PYTHONUTF8=1`
- `PYTHONIOENCODING=utf-8`
- PowerShell `InputEncoding / OutputEncoding` 切到 UTF-8

同时，核心脚本的输出不会再依赖当前控制台是否能正确显示，而是直接落到每批自己的日志文件。

## 成功判断标准

`scripts/check_member_sync_batch.py` 会生成每批报告，并用下面的逻辑判断：

### 批次完成

同时满足：

- 结果 CSV 已生成
- 结果 CSV 行数等于预期批次数
- `success + failed + not_found + other_status == expected_count`
- `sync_task_log` 增量大于等于本批记录数

### 批次完全成功

在“批次完成”基础上，再满足：

- `failed_count == 0`

说明：

- `member_profile / member_account` 没有增量不一定是失败，可能只是重复批次或幂等更新
- 如果核心脚本因 GBK 控制台编码退出非 0，但校验通过，包装层仍会把这批视为有效完成

## 结果文件

每一批都会生成：

- `batch_000001_result.csv`
- `batch_000001.log`
- `batch_000001_counts_before.json`
- `batch_000001_report.json`
- `batch_000001_run.json`

其中：

- `result.csv` 用来看 `success / failed / not_found`
- `log` 用来看原始 stdout/stderr
- `report.json` 用来看本批是否真实完成
- `run.json` 用来看包装层的命令、耗时、退出码

## Windows 任务计划程序

### 程序

```text
powershell.exe
```

### 参数

```text
-NoProfile -ExecutionPolicy Bypass -File "D:\python\menbers\codex\FastAPI\scripts\run_member_sync_batch.ps1" -BatchSize 1000 -InputDir "D:\python\menbers\codex\FastAPI\csv" -WorkDir "D:\python\menbers\codex\FastAPI\data\scheduler\member_sync"
```

### 起始位置

```text
D:\python\menbers\codex\FastAPI
```

### 计划任务设置建议

- 常规：
  - 选择“无论用户是否登录都要运行”
  - 勾选“使用最高权限运行”
- 触发器：
  - 每 15 分钟执行一次，持续时间“不限”
- 设置：
  - 勾选“如果任务已在运行，则不启动新实例”
  - 勾选“如果错过计划开始时间，尽快启动任务”
  - 可设置“如果运行超过 2 小时则停止”

## 5000 / 批建议

- 1000 / 批实测约 `452.734` 秒，约 `7 分 33 秒`
- 5000 / 批线性估算约 `37.7` 分钟

建议：

- 如果以稳定为第一目标：继续 `1000 / 批`，任务计划每 `15` 分钟跑一批
- 如果要提高吞吐：可切到 `5000 / 批`，但建议任务计划不要短于 `60` 分钟，且继续保留“不启动新实例”

按 `131,930` 个去重手机号估算：

- `1000 / 批`：约 `132` 批，纯跑批时间约 `16.6` 小时，按每 15 分钟调度约 `33` 小时
- `5000 / 批`：约 `27` 批，纯跑批时间约 `17` 小时，按每 60 分钟调度约 `27` 小时

如果本地机器需要长期稳定跑，不建议一开始就切到 `5000 / 批`。更稳妥的做法是：

1. 先用 `1000 / 批` 连续跑几个批次
2. 观察网络、ICSP 会话、数据库连接是否持续稳定
3. 再决定是否切到 `5000 / 批`
