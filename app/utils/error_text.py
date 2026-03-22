from __future__ import annotations

import re
from html import unescape
from typing import Any


HTML_LIKE_RE = re.compile(r"^\s*(?:<!doctype html|<html[\s>]|<head[\s>]|<body[\s>])", re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
MAX_ERROR_TEXT_LENGTH = 240


def normalize_error_text(error: Any, *, default: str = "操作失败，请稍后重试。") -> str:
    text = _to_error_text(error)
    if not text:
        return default

    normalized = _map_known_error(text)
    if normalized:
        return normalized

    plain = _strip_html(text)
    plain = WHITESPACE_RE.sub(" ", plain).strip()
    if not plain:
        return default
    if len(plain) <= MAX_ERROR_TEXT_LENGTH:
        return plain
    return f"{plain[:MAX_ERROR_TEXT_LENGTH].rstrip()}..."


def _to_error_text(error: Any) -> str:
    if error is None:
        return ""
    if isinstance(error, BaseException):
        return str(error)
    return str(error)


def _strip_html(text: str) -> str:
    if not text:
        return ""
    value = unescape(text)
    if HTML_LIKE_RE.match(value):
        value = HTML_TAG_RE.sub(" ", value)
    return value


def _map_known_error(text: str) -> str:
    lowered = text.lower()

    if "504" in lowered and ("gateway" in lowered or "time-out" in lowered or "timeout" in lowered):
        return "服务响应超时（504），请稍后重试。"
    if "504 gateway time-out" in lowered or "504 gateway timeout" in lowered or "http_504" in lowered:
        return "服务响应超时（504），请稍后重试。"
    if "503" in lowered and ("service unavailable" in lowered or "unavailable" in lowered):
        return "服务暂时不可用（503），请稍后重试。"
    if "503 service unavailable" in lowered or "http_503" in lowered:
        return "服务暂时不可用（503），请稍后重试。"
    if "502" in lowered and "gateway" in lowered:
        return "网关异常（502），请稍后重试。"
    if "502 bad gateway" in lowered or "http_502" in lowered:
        return "网关异常（502），请稍后重试。"
    if "429" in lowered and ("too many requests" in lowered or "http_429" in lowered):
        return "请求过于频繁，请稍后再试。"
    if "timed out" in lowered or "timeout" in lowered:
        return "请求超时，请稍后重试。"
    if "connection aborted" in lowered or "connection reset" in lowered or "connection refused" in lowered:
        return "网络连接异常，请稍后重试。"
    if HTML_LIKE_RE.match(text):
        return default_gateway_message(text)
    return ""


def default_gateway_message(text: str) -> str:
    lowered = text.lower()
    if "gateway" in lowered or "nginx" in lowered:
        return "网关返回异常页面，请稍后重试。"
    return "服务返回异常页面，请稍后重试。"
