from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.db.base import Base
from app.models.common import TimestampMixin


class MemberProfile(TimestampMixin, Base):
    __tablename__ = "member_profile"
    __table_args__ = (
        Index("ix_member_profile_mobile_no", "mobile_no"),
        {"comment": "会员基础信息"},
    )

    member_id: Mapped[str] = mapped_column(String(64), primary_key=True, comment="会员业务ID")
    member_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="会员姓名")
    mobile_no: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="手机号")
    email: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="邮箱")
    avatar_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="头像地址")
    sex: Mapped[str | None] = mapped_column(String(16), nullable=True, comment="性别")
    reg_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="注册时间")
    birthday: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="生日")
    fav_plaza_code: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="偏好商场编码")
    fav_plaza_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="偏好商场名称")
    fav_plaza_bu_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="偏好商场BU ID")
    belong_plaza_code: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="所属商场编码")
    belong_plaza_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="所属商场名称")
    belong_plaza_bu_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="所属商场BU ID")
    reg_plaza_code: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="注册商场编码")
    reg_plaza_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="注册商场名称")
    reg_plaza_bu_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="注册商场BU ID")
    expanding_channel: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="拓展渠道编码")
    expanding_channel_desc: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="拓展渠道描述")
    card_mark: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="卡标识")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="原始接口JSON")


class MemberProfileAttr(TimestampMixin, Base):
    __tablename__ = "member_profile_attr"
    __table_args__ = (
        UniqueConstraint("member_id", "attr_code", name="uq_member_profile_attr_member_id_attr_code"),
        Index("ix_member_profile_attr_member_id_attr_name", "member_id", "attr_name"),
        {"comment": "会员扩展属性"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    member_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("member_profile.member_id", ondelete="CASCADE"),
        nullable=False,
        comment="会员业务ID",
    )
    attr_code: Mapped[str] = mapped_column(String(128), nullable=False, comment="属性编码")
    attr_name: Mapped[str] = mapped_column(String(128), nullable=False, comment="属性名称")
    attr_value: Mapped[str | None] = mapped_column(String(512), nullable=True, comment="属性值")
    display_order: Mapped[int] = mapped_column(Integer, default=0, nullable=False, comment="展示顺序")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="原始属性JSON")


class MemberLevelDict(TimestampMixin, Base):
    __tablename__ = "member_level_dict"
    __table_args__ = (
        UniqueConstraint("level_id", name="uq_member_level_dict_level_id"),
        UniqueConstraint("level_no", name="uq_member_level_dict_level_no"),
        Index("ix_member_level_dict_level_name", "level_name"),
        {"comment": "会员等级字典"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    level_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="等级ID")
    level_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="等级编号")
    level_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="等级名称")
    level_bit_value: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="等级位值")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="原始等级JSON")


class MemberAccount(TimestampMixin, Base):
    __tablename__ = "member_account"
    __table_args__ = (
        UniqueConstraint("member_id", name="uq_member_account_member_id"),
        Index("ix_member_account_member_id_level_validity_end", "member_id", "level_validity_end"),
        {"comment": "会员当前账户状态"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    member_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("member_profile.member_id", ondelete="CASCADE"),
        nullable=False,
        comment="会员业务ID",
    )
    current_level_dict_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("member_level_dict.id", ondelete="SET NULL"),
        nullable=True,
        comment="当前等级字典ID",
    )
    level_id: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="当前等级ID")
    level_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="当前等级名称")
    level_bit_value: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="当前等级位值")
    level_validity_begin: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="等级有效期开始")
    level_validity_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="等级有效期结束")
    level_long_effective_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True, comment="等级永久有效标识")
    staff_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True, comment="员工会员标识")
    level_no_down_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True, comment="保级标识")
    member_status: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="会员状态")
    member_status_desc: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="会员状态描述")
    growth_add_up: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True, comment="累计成长值")
    growth_balance: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True, comment="成长值余额")
    point_balance: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True, comment="积分余额")
    member_activate: Mapped[bool | None] = mapped_column(Boolean, nullable=True, comment="激活标识")
    member_abnormal: Mapped[bool | None] = mapped_column(Boolean, nullable=True, comment="异常标识")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="原始接口JSON")


class MemberLevelChangeLog(TimestampMixin, Base):
    __tablename__ = "member_level_change_log"
    __table_args__ = (
        UniqueConstraint("event_key", name="uq_member_level_change_log_event_key"),
        Index("ix_member_level_change_log_member_id_update_time", "member_id", "update_time"),
        Index("ix_member_level_change_log_update_time", "update_time"),
        {"comment": "会员等级变更流水"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_key: Mapped[str] = mapped_column(String(64), nullable=False, comment="幂等事件键")
    member_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="会员业务ID")
    level_dict_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("member_level_dict.id", ondelete="SET NULL"),
        nullable=True,
        comment="变更后等级字典ID",
    )
    pre_level_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="变更前等级编号")
    level_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="变更后等级编号")
    pre_level_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="变更前等级名称")
    level_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="变更后等级名称")
    remark: Mapped[str | None] = mapped_column(Text, nullable=True, comment="备注")
    update_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="变更时间")
    opt_type: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="操作类型")
    update_by: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="操作人ID")
    update_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="操作人名称")
    raw_json: Mapped[Any | None] = mapped_column(JSON, nullable=True, comment="原始接口JSON")


class MemberLevelPeriod(TimestampMixin, Base):
    __tablename__ = "member_level_period"
    __table_args__ = (
        Index("ix_member_level_period_member_id_valid_from_valid_to", "member_id", "valid_from", "valid_to"),
        Index("ix_member_level_period_member_id_valid_to", "member_id", "valid_to"),
        {"comment": "会员等级历史时间片"},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    member_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="会员业务ID")
    level_dict_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("member_level_dict.id", ondelete="SET NULL"),
        nullable=True,
        comment="等级字典ID",
    )
    source_change_log_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("member_level_change_log.id", ondelete="SET NULL"),
        nullable=True,
        comment="来源等级流水ID",
    )
    level_no: Mapped[str | None] = mapped_column(String(64), nullable=True, comment="等级编号")
    level_name: Mapped[str | None] = mapped_column(String(128), nullable=True, comment="等级名称")
    valid_from: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="生效开始时间")
    valid_to: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, comment="生效结束时间")
