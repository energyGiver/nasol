from __future__ import annotations

import re
from datetime import datetime
from hashlib import sha1
from typing import Iterable

SEASON_PATTERN = re.compile(r"(?<!\d)([1-2]?\d)\s*기")

ROUND_PATTERNS = [
    re.compile(r"(?<!\d)(\d{1,3})\s*(?:회|화)\b"),
    re.compile(r"\bEP\s*\.?\s*(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"\bE\s*\.?\s*(\d{1,3})\b", re.IGNORECASE),
]

EPISODE_IN_ROUND_PATTERNS = [
    re.compile(r"(?:part|클립|장면)\s*(\d{1,3})\b", re.IGNORECASE),
    re.compile(r"#\s*(\d{1,3})\b"),
]

SPINOFF_KEYWORDS = (
    "나솔사계",
    "사랑은 계속된다",
    "지볶행",
    "지지고 볶고",
    "나는 solo 그 후",
    "나는솔로 그 후",
    "솔로민박",
)

MAIN_KEYWORDS = (
    "나는 solo",
    "나는솔로",
    "솔로나라",
)

EXCLUDE_KEYWORDS = (
    "나솔사계",
    "사랑은 계속된다",
    "지볶행",
    "지지고 볶고",
    "라이브",
    "live",
    "비하인드",
    "근황",
    "인터뷰",
    "뉴스",
    "솔로나라뉴스",
)


def clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def parse_season_numbers(text: str, min_season: int = 1, max_season: int = 29) -> list[int]:
    seasons: list[int] = []
    for match in SEASON_PATTERN.findall(text or ""):
        season = int(match)
        if min_season <= season <= max_season and season not in seasons:
            seasons.append(season)
    return seasons


def parse_first_season(text: str) -> int | None:
    seasons = parse_season_numbers(text)
    return seasons[0] if seasons else None


def parse_round_number(text: str) -> int | None:
    raw_text = text or ""
    for pattern in ROUND_PATTERNS:
        match = pattern.search(raw_text)
        if not match:
            continue
        round_number = int(match.group(1))
        if 1 <= round_number <= 999:
            return round_number
    return None


def parse_episode_in_round(text: str) -> int | None:
    raw_text = text or ""
    for pattern in EPISODE_IN_ROUND_PATTERNS:
        match = pattern.search(raw_text)
        if not match:
            continue
        episode_number = int(match.group(1))
        if 1 <= episode_number <= 999:
            return episode_number
    return None


def classify_series_type(title: str, description: str) -> str:
    if is_spinoff_content(title, description):
        return "spinoff"
    combined = f"{title} {description}".lower()
    if any(keyword in combined for keyword in MAIN_KEYWORDS):
        return "main"
    return "unknown"


def is_spinoff_content(title: str, description: str) -> bool:
    combined = f"{title} {description}".lower()
    return any(keyword in combined for keyword in SPINOFF_KEYWORDS)


def is_pure_main_content(title: str, description: str) -> bool:
    combined = f"{title} {description}".lower()
    if not any(keyword in combined for keyword in MAIN_KEYWORDS):
        return False
    if any(keyword in combined for keyword in EXCLUDE_KEYWORDS):
        return False
    return True


def normalize_title_for_key(title: str) -> str:
    cleaned = re.sub(r"\[[^\]]+\]", " ", title or "")
    cleaned = re.sub(r"\([^)]*\)", " ", cleaned)
    cleaned = re.sub(r"[^0-9A-Za-z가-힣]+", " ", cleaned)
    return clean_spaces(cleaned).lower()


def make_dedupe_key(season: int | None, episode: int | None, upload_date: str | None, title: str) -> str:
    season_part = season if season is not None else 0
    day = upload_date if upload_date else "0000-00-00"
    norm = normalize_title_for_key(title)[:48] or "untitled"
    episode_part = f":e{episode:03d}" if episode is not None else ""
    return f"s{season_part:02d}{episode_part}:d{day}:{norm}"


def normalize_text_for_hash(text: str) -> str:
    lowered = (text or "").lower()
    lowered = re.sub(r"\s+", " ", lowered)
    lowered = re.sub(r"[^0-9A-Za-z가-힣 ]+", "", lowered)
    return lowered.strip()


def transcript_hash(text: str) -> str:
    normalized = normalize_text_for_hash(text)
    return sha1(normalized.encode("utf-8")).hexdigest()


def parse_upload_date(value: str | None) -> str | None:
    if not value:
        return None

    for pattern in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def ensure_season_list(values: Iterable[int]) -> list[int]:
    seasons = sorted({int(value) for value in values if 1 <= int(value) <= 29})
    return seasons
