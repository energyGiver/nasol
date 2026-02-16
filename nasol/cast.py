from __future__ import annotations

import re
from typing import Any

FREQUENT_CAST_NAMES = [
    "영수",
    "영호",
    "영식",
    "영철",
    "광수",
    "상철",
    "영숙",
    "정숙",
    "순자",
    "영자",
    "옥순",
    "현숙",
]

OCCASIONAL_CAST_NAMES = [
    "경수",
    "정희",
    "정수",
    "정식",
]

ALL_CAST_NAMES = FREQUENT_CAST_NAMES + OCCASIONAL_CAST_NAMES

# Conservative ASR typo corrections using canonical cast names.
_ASR_ALIAS_TO_CANONICAL = {
    "용수": "영수",
    "용호": "영호",
    "용식": "영식",
    "용철": "영철",
}

_SUFFIX_PATTERN = r"(?:님|씨|이|가|은|는|을|를|와|과|랑|하고|의|에게|한테)"
_BOUNDARY_PATTERN = r"(?:\s|[.,!?;:(){}\[\]\"'“”‘’…/\-]|$)"
_JOSA_SWAPS_TO_BATCHIM = {
    "가": "이",
    "는": "은",
    "를": "을",
    "와": "과",
    "랑": "이랑",
}
_JOSA_SWAPS_TO_NO_BATCHIM = {right: left for left, right in _JOSA_SWAPS_TO_BATCHIM.items()}


def _has_batchim(word: str) -> bool:
    if not word:
        return False
    code = ord(word[-1])
    if code < 0xAC00 or code > 0xD7A3:
        return False
    return (code - 0xAC00) % 28 != 0


def _normalize_alias_with_particle(text: str, wrong: str, right: str) -> str:
    wrong_batchim = _has_batchim(wrong)
    right_batchim = _has_batchim(right)
    if wrong_batchim == right_batchim:
        return text

    swap_map = _JOSA_SWAPS_TO_BATCHIM if right_batchim else _JOSA_SWAPS_TO_NO_BATCHIM
    normalized = text
    for from_josa, to_josa in swap_map.items():
        normalized = re.sub(
            fr"{wrong}{from_josa}(?={_BOUNDARY_PATTERN})",
            f"{right}{to_josa}",
            normalized,
        )
    return normalized


def normalize_cast_mentions(text: str) -> str:
    normalized = text or ""
    for wrong, right in _ASR_ALIAS_TO_CANONICAL.items():
        normalized = _normalize_alias_with_particle(normalized, wrong, right)
        normalized = re.sub(
            fr"{wrong}(?=(?:{_SUFFIX_PATTERN}|{_BOUNDARY_PATTERN}))",
            right,
            normalized,
        )
    return normalized


def normalize_transcript_segments(segments: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for segment in segments or []:
        if not isinstance(segment, dict):
            continue
        text = normalize_cast_mentions(str(segment.get("text", "")).strip())
        if not text:
            continue
        try:
            start = float(segment.get("start", 0.0) or 0.0)
        except (TypeError, ValueError):
            start = 0.0
        try:
            duration = float(segment.get("duration", 0.0) or 0.0)
        except (TypeError, ValueError):
            duration = 0.0
        normalized.append(
            {
                "start": start,
                "duration": duration,
                "text": text,
            }
        )
    return normalized


def normalize_transcript(
    text: str | None,
    segments: list[dict[str, Any]] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    normalized_text = normalize_cast_mentions((text or "").strip())
    normalized_segments = normalize_transcript_segments(segments)
    if normalized_segments:
        normalized_text = "\n".join(segment["text"] for segment in normalized_segments)
    return normalized_text, normalized_segments


def cast_reference_text() -> str:
    frequent = ", ".join(FREQUENT_CAST_NAMES)
    occasional = ", ".join(OCCASIONAL_CAST_NAMES)
    return (
        f"자주 등장: {frequent}\n"
        f"가끔 등장: {occasional}"
    )
