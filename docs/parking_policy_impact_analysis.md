# 停车政策影响分析设计

本设计用于分析 2026-03-03 停车规则切换前后的收益差异，核心由两部分组成：

- `dim_parking_policy`
  规则维度表，保存版本边界、会员等级、基础免停时长和钻石卡全免标记。
- `fact_parking_impact_analysis`
  分析逻辑视图，逐笔停车记录计算“实际规则应收”和“另一版本模拟应收”。

## 1. 维度表

表名：`dim_parking_policy`

字段：

- `version_id`
  规则版本，当前固定为 `PARKING_RULE_V1` 和 `PARKING_RULE_V2`
- `start_date`
  版本生效开始时间
- `end_date`
  版本失效时间，开放区间用 `NULL`
- `member_level`
  归一化后的会员等级：`非会员 / 普卡 / 银卡 / 金卡 / 钻石卡`
- `base_free_hours`
  基础免停小时数
- `is_diamond_full_free`
  是否全免

当前迁移已经内置种子数据：

- V1：`2000-01-01 00:00:00` 到 `2026-03-02 23:59:59`
- V2：`2026-03-03 00:00:00` 起

## 2. 分析视图逻辑

视图名：`fact_parking_impact_analysis`

输入：

- 停车记录
- 会员当前等级
- 同日积分入账标记
- 规则维度表

逐笔输出：

- 版本命中结果
- 会员等级
- 停车总时长秒数
- 向上取整后的计费小时数
- 是否有积分加成
- 实际版本应收
- 另一版本模拟应收
- 应收差额
- 记录金额与模拟金额差额

## 3. 计费公式

- `T = CEIL(duration_seconds / 3600)`
- `F = base_free_hours + (point_earned ? 2 : 0)`
- 若钻石全免，则应收为 `0`
- 若 `F >= 1`，应收为 `LEAST(60, GREATEST(0, T - F) * 5)`
- 若 `F = 0`，应收为 `LEAST(60, 10 + GREATEST(0, T - 1) * 5)`

说明：

- 这里显式使用了向上取整，避免不足一小时被低估。
- 日封顶 60 元同时作用于实际规则和模拟规则。
- 同日积分入账按 `member_id` 优先匹配，缺失时回退到 `mobile_no`。
- 若会员有 `member_id` 但等级缺失，当前保守回退为 `普卡`；完全无会员身份则记为 `非会员`。

## 4. 可执行 SQL

MySQL 视图和 BI 对比查询见：

- [parking_policy_impact_analysis_mysql.sql](/d:/python/menbers/codex/FastAPI/docs/parking_policy_impact_analysis_mysql.sql)
