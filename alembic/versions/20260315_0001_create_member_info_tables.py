"""create member info tables

Revision ID: 20260315_0001
Revises:
Create Date: 2026-03-15 00:00:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260315_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "member_profile",
        sa.Column("member_id", sa.String(length=64), nullable=False, comment="会员业务ID"),
        sa.Column("member_name", sa.String(length=128), nullable=True, comment="会员姓名"),
        sa.Column("mobile_no", sa.String(length=32), nullable=True, comment="手机号"),
        sa.Column("email", sa.String(length=255), nullable=True, comment="邮箱"),
        sa.Column("avatar_url", sa.String(length=500), nullable=True, comment="头像地址"),
        sa.Column("sex", sa.String(length=16), nullable=True, comment="性别"),
        sa.Column("reg_date", sa.DateTime(), nullable=True, comment="注册时间"),
        sa.Column("birthday", sa.DateTime(), nullable=True, comment="生日"),
        sa.Column("fav_plaza_code", sa.String(length=64), nullable=True, comment="偏好商场编码"),
        sa.Column("fav_plaza_name", sa.String(length=128), nullable=True, comment="偏好商场名称"),
        sa.Column("fav_plaza_bu_id", sa.String(length=64), nullable=True, comment="偏好商场BU ID"),
        sa.Column("belong_plaza_code", sa.String(length=64), nullable=True, comment="所属商场编码"),
        sa.Column("belong_plaza_name", sa.String(length=128), nullable=True, comment="所属商场名称"),
        sa.Column("belong_plaza_bu_id", sa.String(length=64), nullable=True, comment="所属商场BU ID"),
        sa.Column("reg_plaza_code", sa.String(length=64), nullable=True, comment="注册商场编码"),
        sa.Column("reg_plaza_name", sa.String(length=128), nullable=True, comment="注册商场名称"),
        sa.Column("reg_plaza_bu_id", sa.String(length=64), nullable=True, comment="注册商场BU ID"),
        sa.Column("expanding_channel", sa.String(length=64), nullable=True, comment="拓展渠道编码"),
        sa.Column("expanding_channel_desc", sa.String(length=128), nullable=True, comment="拓展渠道描述"),
        sa.Column("card_mark", sa.String(length=128), nullable=True, comment="卡标识"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="原始接口JSON"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.PrimaryKeyConstraint("member_id", name="pk_member_profile"),
        comment="会员基础信息",
    )
    op.create_index("ix_member_profile_mobile_no", "member_profile", ["mobile_no"], unique=False)

    op.create_table(
        "member_level_dict",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("level_id", sa.String(length=64), nullable=True, comment="等级ID"),
        sa.Column("level_no", sa.String(length=64), nullable=True, comment="等级编号"),
        sa.Column("level_name", sa.String(length=128), nullable=True, comment="等级名称"),
        sa.Column("level_bit_value", sa.String(length=64), nullable=True, comment="等级位值"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="原始等级JSON"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.PrimaryKeyConstraint("id", name="pk_member_level_dict"),
        sa.UniqueConstraint("level_id", name="uq_member_level_dict_level_id"),
        sa.UniqueConstraint("level_no", name="uq_member_level_dict_level_no"),
        comment="会员等级字典",
    )
    op.create_index("ix_member_level_dict_level_name", "member_level_dict", ["level_name"], unique=False)

    op.create_table(
        "member_profile_attr",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("member_id", sa.String(length=64), nullable=False, comment="会员业务ID"),
        sa.Column("attr_code", sa.String(length=128), nullable=False, comment="属性编码"),
        sa.Column("attr_name", sa.String(length=128), nullable=False, comment="属性名称"),
        sa.Column("attr_value", sa.String(length=512), nullable=True, comment="属性值"),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="0", comment="展示顺序"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="原始属性JSON"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.ForeignKeyConstraint(["member_id"], ["member_profile.member_id"], name="fk_member_profile_attr_member_id_member_profile", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_member_profile_attr"),
        sa.UniqueConstraint("member_id", "attr_code", name="uq_member_profile_attr_member_id_attr_code"),
        comment="会员扩展属性",
    )
    op.create_index(
        "ix_member_profile_attr_member_id_attr_name",
        "member_profile_attr",
        ["member_id", "attr_name"],
        unique=False,
    )

    op.create_table(
        "member_account",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("member_id", sa.String(length=64), nullable=False, comment="会员业务ID"),
        sa.Column("current_level_dict_id", sa.Integer(), nullable=True, comment="当前等级字典ID"),
        sa.Column("level_id", sa.String(length=64), nullable=True, comment="当前等级ID"),
        sa.Column("level_name", sa.String(length=128), nullable=True, comment="当前等级名称"),
        sa.Column("level_bit_value", sa.String(length=64), nullable=True, comment="当前等级位值"),
        sa.Column("level_validity_begin", sa.DateTime(), nullable=True, comment="等级有效期开始"),
        sa.Column("level_validity_end", sa.DateTime(), nullable=True, comment="等级有效期结束"),
        sa.Column("level_long_effective_flag", sa.Boolean(), nullable=True, comment="等级永久有效标识"),
        sa.Column("staff_flag", sa.Boolean(), nullable=True, comment="员工会员标识"),
        sa.Column("level_no_down_flag", sa.Boolean(), nullable=True, comment="保级标识"),
        sa.Column("member_status", sa.String(length=64), nullable=True, comment="会员状态"),
        sa.Column("member_status_desc", sa.String(length=255), nullable=True, comment="会员状态描述"),
        sa.Column("growth_add_up", sa.Numeric(precision=18, scale=2), nullable=True, comment="累计成长值"),
        sa.Column("growth_balance", sa.Numeric(precision=18, scale=2), nullable=True, comment="成长值余额"),
        sa.Column("point_balance", sa.Numeric(precision=18, scale=2), nullable=True, comment="积分余额"),
        sa.Column("member_activate", sa.Boolean(), nullable=True, comment="激活标识"),
        sa.Column("member_abnormal", sa.Boolean(), nullable=True, comment="异常标识"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="原始接口JSON"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.ForeignKeyConstraint(["current_level_dict_id"], ["member_level_dict.id"], name="fk_member_account_current_level_dict_id_member_level_dict", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["member_id"], ["member_profile.member_id"], name="fk_member_account_member_id_member_profile", ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name="pk_member_account"),
        sa.UniqueConstraint("member_id", name="uq_member_account_member_id"),
        comment="会员当前账户状态",
    )
    op.create_index(
        "ix_member_account_member_id_level_validity_end",
        "member_account",
        ["member_id", "level_validity_end"],
        unique=False,
    )

    op.create_table(
        "member_level_change_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("event_key", sa.String(length=64), nullable=False, comment="幂等事件键"),
        sa.Column("member_id", sa.String(length=64), nullable=False, comment="会员业务ID"),
        sa.Column("level_dict_id", sa.Integer(), nullable=True, comment="变更后等级字典ID"),
        sa.Column("pre_level_no", sa.String(length=64), nullable=True, comment="变更前等级编号"),
        sa.Column("level_no", sa.String(length=64), nullable=True, comment="变更后等级编号"),
        sa.Column("pre_level_name", sa.String(length=128), nullable=True, comment="变更前等级名称"),
        sa.Column("level_name", sa.String(length=128), nullable=True, comment="变更后等级名称"),
        sa.Column("remark", sa.Text(), nullable=True, comment="备注"),
        sa.Column("update_time", sa.DateTime(), nullable=True, comment="变更时间"),
        sa.Column("opt_type", sa.String(length=64), nullable=True, comment="操作类型"),
        sa.Column("update_by", sa.String(length=64), nullable=True, comment="操作人ID"),
        sa.Column("update_name", sa.String(length=128), nullable=True, comment="操作人名称"),
        sa.Column("raw_json", sa.JSON(), nullable=True, comment="原始接口JSON"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.ForeignKeyConstraint(["level_dict_id"], ["member_level_dict.id"], name="fk_member_level_change_log_level_dict_id_member_level_dict", ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_member_level_change_log"),
        sa.UniqueConstraint("event_key", name="uq_member_level_change_log_event_key"),
        comment="会员等级变更流水",
    )
    op.create_index(
        "ix_member_level_change_log_member_id_update_time",
        "member_level_change_log",
        ["member_id", "update_time"],
        unique=False,
    )
    op.create_index("ix_member_level_change_log_update_time", "member_level_change_log", ["update_time"], unique=False)

    op.create_table(
        "member_level_period",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("member_id", sa.String(length=64), nullable=False, comment="会员业务ID"),
        sa.Column("level_dict_id", sa.Integer(), nullable=True, comment="等级字典ID"),
        sa.Column("source_change_log_id", sa.Integer(), nullable=True, comment="来源等级流水ID"),
        sa.Column("level_no", sa.String(length=64), nullable=True, comment="等级编号"),
        sa.Column("level_name", sa.String(length=128), nullable=True, comment="等级名称"),
        sa.Column("valid_from", sa.DateTime(), nullable=False, comment="生效开始时间"),
        sa.Column("valid_to", sa.DateTime(), nullable=True, comment="生效结束时间"),
        sa.Column("created_at", sa.DateTime(), nullable=False, comment="创建时间"),
        sa.Column("updated_at", sa.DateTime(), nullable=False, comment="更新时间"),
        sa.ForeignKeyConstraint(["level_dict_id"], ["member_level_dict.id"], name="fk_member_level_period_level_dict_id_member_level_dict", ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_change_log_id"], ["member_level_change_log.id"], name="fk_member_level_period_source_change_log_id", ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name="pk_member_level_period"),
        comment="会员等级历史时间片",
    )
    op.create_index(
        "ix_member_level_period_member_id_valid_from_valid_to",
        "member_level_period",
        ["member_id", "valid_from", "valid_to"],
        unique=False,
    )
    op.create_index(
        "ix_member_level_period_member_id_valid_to",
        "member_level_period",
        ["member_id", "valid_to"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_member_level_period_member_id_valid_to", table_name="member_level_period")
    op.drop_index("ix_member_level_period_member_id_valid_from_valid_to", table_name="member_level_period")
    op.drop_table("member_level_period")

    op.drop_index("ix_member_level_change_log_update_time", table_name="member_level_change_log")
    op.drop_index("ix_member_level_change_log_member_id_update_time", table_name="member_level_change_log")
    op.drop_table("member_level_change_log")

    op.drop_index("ix_member_account_member_id_level_validity_end", table_name="member_account")
    op.drop_table("member_account")

    op.drop_index("ix_member_profile_attr_member_id_attr_name", table_name="member_profile_attr")
    op.drop_table("member_profile_attr")

    op.drop_index("ix_member_level_dict_level_name", table_name="member_level_dict")
    op.drop_table("member_level_dict")

    op.drop_index("ix_member_profile_mobile_no", table_name="member_profile")
    op.drop_table("member_profile")
