from __future__ import annotations

import hashlib
import json
import re
from typing import Any

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


def sha256_text_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_text_prefixed(value: str) -> str:
    return f"sha256:{sha256_text_hex(value)}"


def sha256_canonical_json_hex(payload: Any) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return sha256_text_hex(canonical)


def sha256_canonical_json_prefixed(payload: Any) -> str:
    return f"sha256:{sha256_canonical_json_hex(payload)}"


def normalize_sha256_hex(value: str, *, allow_prefix: bool = False) -> str:
    normalized = value.removeprefix("sha256:") if allow_prefix else value
    if not _SHA256_HEX_RE.fullmatch(normalized):
        raise ValueError("expected SHA-256 hex digest")
    return normalized
