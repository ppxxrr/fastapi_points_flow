"""add sync log and point flow tables

Revision ID: 20260315_0002
Revises: 20260315_0001
Create Date: 2026-03-15 01:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260315_0002"
down_revision = "20260315_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "member_point_flow",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_key", sa.String(length=64), nullable=False, comment="幂等事件键"),
        sa.Column("flow_no", sa.String(length=64), nullable=True, comment="积分流水号"),
        sa.Column("member_id", sa.String(length=64), nullable=True, comment="会员业务ID"),
        sa.Column("member_name", sa.String(length=128), nullable=True, comment="会员姓名"),
        sa.Column("member_phone", sa.String(length=32), nullable=True, comment="会员手机号"),
        sa.Column("out_trade_no", sa.String(length=128), nullable=True, comment="外部交易号"),
        sa.Column("plaza_bu_id", sa.String(length=64), nullable=True, comment="广场BU ID"),
        sa.Column("plaza_bu_name", sa.String(length=128), nullable=True, comment="广场名称"),
        sa.Column("store_bu_id", sa.String(length=64), nullable=True, comment="商户BU ID"),
        sa.Column("store_code", sa.String(length=64), nullable=True, comment="商户编码"),
        sa.Column("store_bu_name", sa.String(length=255), nullable=True, comment="商户名称"),
        sa.Column("point_operate", sa.String(length=32), nullable=True, comment="积分方向"),
        sa.Column("change_point_amount", sa.Numeric(precision=18, scale=2), nullable=True, comment="变更积分值"),
        sa.Column("signed_change_points", sa.Numeric(precision=18, scale=2), nullable=True, comment="带符号积分值"),
        sa.Column("current_effective_amount", sa.Numeric(precision=18, scale=2), nullable=True, comment="当前有效积分余额"),
        sa.Column("consume_amount_raw", sa.Integer(), nullable=True, comment="原始消费金额（分）"),
        sa.Column("consume_amount", sa.Numeric(precision=18, scale=2), nullable=True, comment="消费金额（元）"),
        sa.Column("point_rate", sa.Numeric(precision=18, scale=4), nullable=True, comment="积分倍率"),
        sa.Column("point_ratio", sa.Numeric(precision=18, scale=4), nullable=True, comment="积分比例"),
        sa.Column("change_type_code", sa.String(length=64), nullable=True, comment="积分变更类型编码"),
        sa.Column("change_type_name", sa.String(length=128), nullable=True, comment="积分变更类型名称"),
        sa.Column("business_type_name", sa.String(length=128), nullable=True, comment="业务类型名称"),
        sa.Column("source_code", sa.String(length=64), nullable=True, comment="来源编码"),
        sa.Column("source_name", sa.String(length=128), nullable=True, comment="来源名称"),
        sa.Column("market_activity_no", sa.String(length=64), nullable=True, comment="营销活动编号"),
        sa.Column("market_activity_name", sa.String(length=255), nullable=True, comment="营销活动名称"),
        sa.Column("market_activity_type", sa.String(length=128), nullable=True, comment="营销活动类型"),
        sa.Column("create_time", sa.DateTime(), nullable=True, comment="流水创建时间"),
        sa.Column("consume_time", sa.DateTime(), nullable=True, comment="消费时间"),
        sa.Column("expire_time", sa.DateTime(), nullable=True, comment="积分失效时间"),
        sa.Column("remark", sa.Text(), nullable=True, comment="备注"),
        sa.Column("extra", sa.JSON(), nullable=True, comment="扩展字段"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="原始接口JSON"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.PrimaryKeyConstraint("id", name="pk_member_point_flow"),
        sa.UniqueConstraint("event_key", name="uq_member_point_flow_event_key"),
        comment="会员积分流水",
    )
    op.create_index("ix_member_point_flow_flow_no", "member_point_flow", ["flow_no"], unique=False)
    op.create_index(
        "ix_member_point_flow_member_id_create_time",
        "member_point_flow",
        ["member_id", "create_time"],
        unique=False,
    )
    op.create_index(
        "ix_member_point_flow_member_phone_create_time",
        "member_point_flow",
        ["member_phone", "create_time"],
        unique=False,
    )
    op.create_index("ix_member_point_flow_consume_time", "member_point_flow", ["consume_time"], unique=False)

    op.create_table(
        "sync_task_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("module_name", sa.String(length=64), nullable=False, comment="模块名称"),
        sa.Column("action", sa.String(length=64), nullable=False, comment="操作名称"),
        sa.Column("target_type", sa.String(length=32), nullable=True, comment="目标类型"),
        sa.Column("target_value", sa.String(length=255), nullable=True, comment="目标值"),
        sa.Column("triggered_by", sa.String(length=64), nullable=True, comment="触发用户"),
        sa.Column("triggered_source", sa.String(length=32), nullable=True, comment="触发来源"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running", comment="执行状态"),
        sa.Column("started_at", sa.DateTime(), nullable=False, comment="开始时间"),
        sa.Column("finished_at", sa.DateTime(), nullable=True, comment="结束时间"),
        sa.Column("request_payload", sa.JSON(), nullable=True, comment="请求参数"),
        sa.Column("result_payload", sa.JSON(), nullable=True, comment="执行结果"),
        sa.Column("error_message", sa.Text(), nullable=True, comment="错误信息"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.PrimaryKeyConstraint("id", name="pk_sync_task_log"),
        comment="同步任务日志",
    )
    op.create_index(
        "ix_sync_task_log_module_name_status_started_at",
        "sync_task_log",
        ["module_name", "status", "started_at"],
        unique=False,
    )
    op.create_index(
        "ix_sync_task_log_target_type_target_value",
        "sync_task_log",
        ["target_type", "target_value"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_sync_task_log_target_type_target_value", table_name="sync_task_log")
    op.drop_index("ix_sync_task_log_module_name_status_started_at", table_name="sync_task_log")
    op.drop_table("sync_task_log")

    op.drop_index("ix_member_point_flow_consume_time", table_name="member_point_flow")
    op.drop_index("ix_member_point_flow_member_phone_create_time", table_name="member_point_flow")
    op.drop_index("ix_member_point_flow_member_id_create_time", table_name="member_point_flow")
    op.drop_index("ix_member_point_flow_flow_no", table_name="member_point_flow")
    op.drop_table("member_point_flow")
