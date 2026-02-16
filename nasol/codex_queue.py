from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from nasol.cast import (
    ALL_CAST_NAMES,
    cast_reference_text,
    normalize_cast_mentions,
    normalize_transcript_segments,
)
from nasol.storage import NasolRepository


def _season_label(seasons: list[int]) -> str:
    if not seasons:
        return "전체"
    if len(seasons) == 1:
        return f"{seasons[0]}기"
    return f"{seasons[0]}기~{seasons[-1]}기"


def _job_kind(job: dict[str, Any]) -> str:
    kind = str(job.get("job_kind") or "analysis").strip().lower()
    return "summary" if kind == "summary" else "analysis"


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _sec_to_clock(seconds: float | int) -> str:
    sec = max(int(seconds), 0)
    hours, remains = divmod(sec, 3600)
    minutes, second = divmod(remains, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{second:02d}"
    return f"{minutes:02d}:{second:02d}"


def _video_link(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _seek_link(video_id: str, start_sec: int) -> str:
    return f"https://www.youtube.com/watch?v={video_id}&t={max(start_sec, 0)}s"


def _slugify(text: str, max_len: int = 42) -> str:
    normalized = re.sub(r"[^0-9A-Za-z가-힣]+", "-", (text or "").strip())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return (normalized or "episode")[:max_len]


def _extract_people(text: str, limit: int = 6) -> list[tuple[str, int]]:
    lowered = normalize_cast_mentions(text or "").lower()
    counts: list[tuple[str, int]] = []
    for name in ALL_CAST_NAMES:
        cnt = lowered.count(name.lower())
        if cnt > 0:
            counts.append((name, cnt))
    counts.sort(key=lambda row: (-row[1], row[0]))
    return counts[:limit]


def _extract_canonical_people(text: str) -> list[str]:
    normalized = normalize_cast_mentions(text or "")
    found = [name for name in ALL_CAST_NAMES if name in normalized]
    return found


def _parse_summary_episode_sections(result_text: str) -> list[dict[str, Any]]:
    sections = re.split(r"(?m)^##\s+EPISODE\|", result_text or "")
    if len(sections) <= 1:
        return []

    items: list[dict[str, Any]] = []
    list_keys = {"chunk_storyline", "key_incidents", "evidence_links", "highlights"}
    for raw_section in sections[1:]:
        section = raw_section.strip()
        if not section:
            continue
        lines = section.splitlines()
        if not lines:
            continue

        meta: dict[str, Any] = {}
        for token in lines[0].split("|"):
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            meta[key.strip().lower()] = value.strip()

        payload: dict[str, Any] = {
            "video_id": meta.get("video_id") or "",
            "season": _to_int(meta.get("season")),
            "round": _to_int(meta.get("round")),
            "episode": _to_int(meta.get("episode")),
            "title": "",
            "youtube_url": "",
            "key_people": "",
            "one_line": "",
            "summary": "",
            "chunk_storyline": [],
            "key_incidents": [],
            "evidence_links": [],
            "highlights": [],
        }
        current_key: str | None = None
        for line in lines[1:]:
            if line.startswith("- ") and ":" in line:
                key, value = line[2:].split(":", 1)
                current_key = key.strip().lower()
                clean = value.strip()
                if current_key in list_keys:
                    if clean:
                        payload[current_key].append(clean)
                elif current_key in payload:
                    payload[current_key] = clean
                continue

            if line.startswith("  - "):
                clean = line[4:].strip()
                if current_key in list_keys and clean:
                    payload[current_key].append(clean)
                continue

            if current_key in {"summary", "one_line"} and line.strip():
                payload[current_key] = (payload[current_key] + " " + line.strip()).strip()

        if not payload["chunk_storyline"] and payload["highlights"]:
            payload["chunk_storyline"] = payload["highlights"]
        if payload["video_id"] and not payload["youtube_url"]:
            payload["youtube_url"] = _video_link(payload["video_id"])
        items.append(payload)

    return items


def _validate_summary_result(result_text: str) -> list[str]:
    errors: list[str] = []
    summary_match = re.search(
        r"(?ms)^##\s*전체 요약\s*\n(.+?)(?:\n##\s+에피소드 요약|\Z)",
        result_text or "",
    )
    if not summary_match:
        errors.append("`## 전체 요약` 섹션이 없습니다.")
    else:
        overall = summary_match.group(1).strip()
        if len(overall) < 140:
            errors.append("`전체 요약`이 너무 짧습니다. 최소 6문장 수준으로 작성해주세요.")

    episodes = _parse_summary_episode_sections(result_text)
    if not episodes:
        errors.append("`## EPISODE|...` 섹션이 없습니다.")
        return errors

    seen_video_ids: set[str] = set()
    for item in episodes:
        video_id = item.get("video_id") or ""
        if not video_id:
            errors.append("`video_id` 누락 EPISODE가 있습니다.")
            continue
        if video_id in seen_video_ids:
            errors.append(f"`video_id={video_id}`가 중복되었습니다.")
        seen_video_ids.add(video_id)

        key_people = item.get("key_people", "")
        people = _extract_canonical_people(key_people)
        if len(people) < 2:
            errors.append(f"`video_id={video_id}` 핵심 인물은 최소 2명 이상(캐스트 기준) 필요합니다.")

        one_line = str(item.get("one_line", "")).strip()
        if len(one_line) < 35:
            errors.append(f"`video_id={video_id}` one_line이 너무 짧습니다.")

        summary = str(item.get("summary", "")).strip()
        if len(summary) < 200:
            errors.append(f"`video_id={video_id}` summary가 너무 짧습니다.")

        lowered_summary = summary.lower()
        banned_patterns = [
            "이 구간은",
            "라는 제목 그대로",
            "관계 구도가 다시 정리되는 에피소드",
        ]
        if any(pattern in lowered_summary for pattern in banned_patterns):
            errors.append(
                f"`video_id={video_id}` summary가 템플릿 문구 중심입니다. chunk 기반 사건 서사로 다시 작성해주세요."
            )

        storyline = item.get("chunk_storyline") or []
        if len(storyline) < 2:
            errors.append(f"`video_id={video_id}` chunk_storyline은 최소 2개 이상 필요합니다.")
        else:
            chunk_linked = sum(1 for row in storyline if "chunk" in str(row).lower())
            if chunk_linked == 0:
                errors.append(f"`video_id={video_id}` chunk_storyline에 chunk 단위 설명이 없습니다.")

        incidents = item.get("key_incidents") or []
        if len(incidents) < 2:
            errors.append(f"`video_id={video_id}` key_incidents는 최소 2개 이상 필요합니다.")

        evidence_links = item.get("evidence_links") or []
        valid_links = [link for link in evidence_links if str(link).startswith("http")]
        if len(valid_links) < 2:
            errors.append(f"`video_id={video_id}` evidence_links는 최소 2개 이상 필요합니다.")

    return errors


def _parse_segments(raw: Any, text_fallback: str, chunk_chars: int) -> list[dict[str, Any]]:
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                clean = normalize_transcript_segments(parsed)
                if clean:
                    return clean
        except json.JSONDecodeError:
            pass

    # Fallback: split transcript text into pseudo segments.
    chunks = []
    text = normalize_cast_mentions((text_fallback or "").strip())
    if not text:
        return chunks
    cursor = 0
    while cursor < len(text):
        snippet = text[cursor : cursor + chunk_chars]
        chunks.append({"start": 0.0, "duration": 0.0, "text": snippet})
        cursor += chunk_chars
    return chunks


def _chunk_segments(segments: list[dict[str, Any]], chunk_chars: int = 1700) -> list[dict[str, Any]]:
    if not segments:
        return []
    chunks: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    current_chars = 0

    for seg in segments:
        text = seg.get("text", "")
        if not text:
            continue
        if current and current_chars + len(text) > chunk_chars:
            chunks.append(_finalize_chunk(current))
            current = []
            current_chars = 0

        current.append(seg)
        current_chars += len(text) + 1

    if current:
        chunks.append(_finalize_chunk(current))

    return chunks


def _finalize_chunk(segments: list[dict[str, Any]]) -> dict[str, Any]:
    start = float(segments[0].get("start", 0.0) or 0.0)
    last = segments[-1]
    end = float(last.get("start", 0.0) or 0.0) + float(last.get("duration", 0.0) or 0.0)
    text = " ".join(str(seg.get("text", "")).strip() for seg in segments).strip()
    return {
        "start": start,
        "end": max(end, start),
        "text": text,
    }


def _load_job_videos(
    repo: NasolRepository,
    job: dict[str, Any],
    max_videos: int = 200,
) -> list[dict[str, Any]]:
    seasons = job.get("seasons") or []
    videos = repo.get_videos(
        seasons=seasons,
        transcript_only=True,
        main_only=True,
        limit=max_videos,
    )
    videos.sort(
        key=lambda row: (
            _to_int(row.get("season")),
            _to_int(row.get("round_number") or row.get("episode") or 9999),
            _to_int(row.get("episode_in_round") or 9999),
            row.get("upload_date") or "",
            row.get("video_id") or "",
        )
    )
    return videos


def build_context_markdown(
    repo: NasolRepository,
    job: dict[str, Any],
    max_videos: int = 80,
    chunk_chars: int = 1700,
) -> str:
    seasons = job.get("seasons") or []
    kind = _job_kind(job)
    videos = _load_job_videos(repo, job, max_videos=max_videos)
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for video in videos:
        grouped[int(video.get("season") or 0)].append(video)

    lines: list[str] = []
    title = "Codex 요약 요청" if kind == "summary" else "Codex 분석 요청"
    lines.append(f"# {title} #{job['id']}")
    lines.append("")
    lines.append(f"- 상태: {job['status']}")
    lines.append(f"- 작업종류: {kind}")
    lines.append(f"- 기수: {_season_label(seasons)}")
    lines.append(f"- 요청: {job['query']}")
    lines.append(f"- 생성시각: {job['created_at']}")
    lines.append("")
    lines.append("## 데이터 요약")
    lines.append(f"- 본편 대본 보유 영상 수: {len(videos)}")
    if kind == "summary":
        lines.append("- 요약 방식: 에피소드별 transcript chunk 단위 처리 후 에피소드 핵심 줄거리 생성")
    else:
        lines.append("- 분석 방식: 에피소드별 transcript chunk 단위로 먼저 이해한 뒤 최종 요약")
    lines.append("- 인물명 보정: 캐스트 사전을 기준으로 ASR 이름 오탈자를 우선 교정")
    lines.append("")
    lines.append("## 캐스트 레퍼런스")
    lines.append(f"```text\n{cast_reference_text()}\n```")
    lines.append("")
    lines.append("## 결과 작성 규칙")
    if kind == "summary":
        lines.append("- 에피소드마다 chunk를 순서대로 읽고, chunk별 핵심 사건을 먼저 요약한 뒤 최종 요약을 작성합니다.")
        lines.append("- 에피소드마다 핵심 인물(2명 이상), 사건(2개 이상), 근거 링크(2개 이상)를 포함합니다.")
        lines.append("- `summary`는 템플릿 문구(예: `이 구간은`, `라는 제목 그대로`)를 금지합니다.")
        lines.append("- 결과 파일은 `EPISODE|season=...|round=...|episode=...|video_id=...` 헤더 형식을 지킵니다.")
    else:
        lines.append("- 최소 8문장 이상으로 사건 서사를 작성합니다.")
        lines.append("- 사건마다 `누가/왜/어떻게/결과`를 반드시 포함합니다.")
        lines.append("- 사건마다 핵심 인물(2명 이상)과 관련 유튜브 링크를 넣습니다.")
        lines.append("- 링크는 가능하면 사건 시작 지점 timestamp 링크를 우선 사용합니다.")
    lines.append("")
    lines.append("## 에피소드 작업 단위")
    for season in sorted(grouped):
        lines.append("")
        lines.append(f"### {season}기")
        for video in grouped[season]:
            round_number = video.get("round_number") or video.get("episode")
            epi = video.get("episode_in_round")
            title = video.get("title") or ""
            video_id = video["video_id"]
            view_count = _to_int(video.get("view_count"))
            comment_count = _to_int(video.get("comment_count"))
            ratio = (comment_count / view_count * 100.0) if view_count > 0 else 0.0
            segments = _parse_segments(
                video.get("transcript_segments"),
                text_fallback=video.get("transcript_text", ""),
                chunk_chars=chunk_chars,
            )
            chunks = _chunk_segments(segments, chunk_chars=chunk_chars)
            people = _extract_people(video.get("transcript_text", ""))
            people_text = ", ".join(f"{name}({cnt})" for name, cnt in people) or "감지 없음"

            lines.append(
                f"- [{video_id}] {round_number}회차/{epi or '?'}에피소드 | {title}"
            )
            lines.append(
                f"  - 지표: 조회수 {view_count:,}, 댓글수 {comment_count:,}, 댓글비율 {ratio:.3f}%"
            )
            lines.append(f"  - 인물 힌트: {people_text}")
            lines.append(f"  - 전체 링크: {_video_link(video_id)}")
            for idx, chunk in enumerate(chunks[:2], start=1):
                start = int(chunk["start"])
                end = int(chunk["end"])
                lines.append(
                    f"  - Chunk {idx} ({_sec_to_clock(start)}~{_sec_to_clock(end)}): "
                    f"{_seek_link(video_id, start)}"
                )
                lines.append(f"    - 발췌: {chunk['text'][:320]}...")
    return "\n".join(lines)


def build_episode_packet_markdown(
    video: dict[str, Any],
    chunk_chars: int = 1700,
    job_kind: str = "analysis",
) -> str:
    video_id = video["video_id"]
    round_number = video.get("round_number") or video.get("episode")
    episode_in_round = video.get("episode_in_round")
    title = video.get("title") or ""
    view_count = _to_int(video.get("view_count"))
    comment_count = _to_int(video.get("comment_count"))
    ratio = (comment_count / view_count * 100.0) if view_count > 0 else 0.0
    people = _extract_people(video.get("transcript_text", ""))
    people_text = ", ".join(f"{name}({cnt})" for name, cnt in people) or "감지 없음"

    segments = _parse_segments(
        video.get("transcript_segments"),
        text_fallback=video.get("transcript_text", ""),
        chunk_chars=chunk_chars,
    )
    chunks = _chunk_segments(segments, chunk_chars=chunk_chars)

    lines: list[str] = []
    lines.append(f"# {video.get('season')}기-{round_number}회차-{episode_in_round or '?'}에피소드")
    lines.append("")
    lines.append(f"- video_id: `{video_id}`")
    lines.append(f"- 제목: {title}")
    lines.append(f"- 업로드일: {video.get('upload_date')}")
    lines.append(f"- 조회수: {view_count:,}")
    lines.append(f"- 댓글수: {comment_count:,}")
    lines.append(f"- 댓글비율: {ratio:.3f}%")
    lines.append(f"- 인물 힌트: {people_text}")
    lines.append("```text")
    lines.append(cast_reference_text())
    lines.append("```")
    lines.append(f"- 전체 링크: {_video_link(video_id)}")
    lines.append("")
    lines.append("## Codex 작업 지시")
    if job_kind == "summary":
        lines.append("- 아래 transcript chunk를 순서대로 읽고, 각 chunk의 핵심 사건/감정 변화를 먼저 정리합니다.")
        lines.append("- chunk 정리를 바탕으로 에피소드 전체 흐름을 연결해 상세 요약을 작성합니다.")
        lines.append("- 핵심 인물(2명 이상), 사건(2개 이상), 근거 링크(2개 이상)를 반드시 포함합니다.")
        lines.append("- 이름 표기는 캐스트 레퍼런스 기준으로 보정합니다.")
        lines.append("- 템플릿 문구(`이 구간은`, `라는 제목 그대로`)는 사용하지 않습니다.")
    else:
        lines.append("- 아래 transcript chunk를 순서대로 읽고 사건 단위로 요약합니다.")
        lines.append("- 사건마다 `누가/왜/어떻게/결과`를 명시합니다.")
        lines.append("- 사건마다 핵심 인물(2명 이상)과 근거 chunk 링크를 포함합니다.")
        lines.append("- 한 에피소드에 사건이 여러 개면 모두 분리합니다.")
    lines.append("")
    lines.append("## Transcript Chunks")
    for idx, chunk in enumerate(chunks, start=1):
        start = int(chunk["start"])
        end = int(chunk["end"])
        lines.append("")
        lines.append(
            f"### Chunk {idx} ({_sec_to_clock(start)}~{_sec_to_clock(end)}) "
            f"[바로가기]({_seek_link(video_id, start)})"
        )
        lines.append(chunk["text"] or "(텍스트 없음)")
    lines.append("")
    lines.append("## 에피소드 결과 템플릿")
    lines.append("```markdown")
    if job_kind == "summary":
        lines.append(
            f"## EPISODE|season={video.get('season')}|round={round_number or '?'}|"
            f"episode={episode_in_round or '?'}|video_id={video_id}"
        )
        lines.append(f"- title: {title}")
        lines.append(f"- youtube_url: {_video_link(video_id)}")
        lines.append("- key_people: ")
        lines.append("- one_line: ")
        lines.append("- summary: ")
        lines.append("- chunk_storyline:")
        lines.append("  - Chunk 1 | 핵심 사건/대화/감정 변화")
        lines.append("  - Chunk 2 | 핵심 사건/대화/감정 변화")
        lines.append("- key_incidents:")
        lines.append("  - 사건 1 | 관련 인물 | 사건 내용 | 갈등/반전")
        lines.append("  - 사건 2 | 관련 인물 | 사건 내용 | 다음 전개")
        lines.append("- evidence_links:")
        lines.append("  - https://www.youtube.com/watch?v=...&t=...s")
        lines.append("  - https://www.youtube.com/watch?v=...&t=...s")
    else:
        lines.append(f"### 사건 1 | {video.get('season')}기-{round_number}회차-{episode_in_round or '?'}에피소드")
        lines.append("- 핵심 인물:")
        lines.append("- 갈등/사건 요약:")
        lines.append("- 누가/왜/어떻게/결과:")
        lines.append("- 근거 구간 링크:")
        lines.append("- 영상 링크:")
    lines.append("```")
    return "\n".join(lines)


def build_packet_readme(job: dict[str, Any], videos: list[dict[str, Any]]) -> str:
    kind = _job_kind(job)
    lines: list[str] = []
    lines.append(f"# Codex Packet | Job #{job['id']}")
    lines.append("")
    lines.append(f"- 상태: {job['status']}")
    lines.append(f"- 작업종류: {kind}")
    lines.append(f"- 기수: {_season_label(job['seasons'])}")
    lines.append(f"- 요청: {job['query']}")
    lines.append(f"- 에피소드 파일 수: {len(videos)}")
    lines.append("")
    lines.append("## 작업 순서")
    if kind == "summary":
        lines.append("1. `episodes/` 아래 파일을 에피소드 단위로 읽고, chunk별 사건을 먼저 정리합니다.")
        lines.append("2. chunk 사건을 시간순으로 연결해 에피소드 전체 서사를 요약합니다.")
        lines.append("3. 에피소드별 핵심 인물, 핵심 사건, 근거 링크를 정리합니다.")
        lines.append("4. `result_template.md` 형식(EPISODE 헤더)을 그대로 유지해 작성합니다.")
    else:
        lines.append("1. `episodes/` 아래 파일을 에피소드 단위로 읽고 사건을 추출합니다.")
        lines.append("2. 사건마다 핵심 인물, 갈등 서사(누가/왜/어떻게/결과), 근거 링크를 정리합니다.")
        lines.append("3. `result_template.md` 형식으로 최종 결과를 작성합니다.")
    lines.append("")
    lines.append("## 품질 기준")
    lines.append("- 단순 지표 나열이 아니라 내용 중심으로 작성")
    if kind == "summary":
        lines.append("- 에피소드별 `chunk_storyline` 2개 이상")
        lines.append("- 에피소드별 `key_incidents` 2개 이상")
        lines.append("- 에피소드별 one-line + summary는 chunk 근거 기반으로 작성")
        lines.append("- 템플릿 문구(`이 구간은`, `라는 제목 그대로`) 금지")
        lines.append("- 에피소드별 evidence_links 2개 이상")
    else:
        lines.append("- 사건당 5문장 이상 설명")
        lines.append("- 모든 사건에 최소 1개 이상 YouTube 링크 포함")
        lines.append("- 같은 사건이라도 인물 시점이 다르면 분리 가능")
    lines.append("- 인물 표기는 아래 캐스트 레퍼런스 이름으로 통일")
    lines.append("")
    lines.append("## 캐스트 레퍼런스")
    lines.append("```text")
    lines.append(cast_reference_text())
    lines.append("```")
    lines.append("")
    lines.append("## 완료 처리")
    result_name = (
        f"/tmp/codex_summary_job_{job['id']}_result.md"
        if kind == "summary"
        else f"/tmp/codex_job_{job['id']}_result.md"
    )
    lines.append(
        f"작성한 결과를 `{result_name}`로 저장한 뒤 아래 명령 실행:"
    )
    lines.append("")
    lines.append(
        f"`python3 -m nasol.codex_queue complete --job-id {job['id']} "
        f"--result-file {result_name}`"
    )
    return "\n".join(lines)


def build_result_template(job: dict[str, Any], videos: list[dict[str, Any]]) -> str:
    kind = _job_kind(job)
    lines: list[str] = []
    lines.append(f"# 분석 결과 | Job #{job['id']}")
    lines.append("")
    lines.append("## 요청")
    lines.append(job["query"])
    lines.append("")
    if kind == "summary":
        lines.append("## 전체 요약")
        lines.append("- (여기에 선택한 기수 전체 흐름을 6문장 이상 작성)")
        lines.append("")
        lines.append("## 에피소드 요약")
        for video in videos:
            season = video.get("season")
            round_number = video.get("round_number") or video.get("episode")
            episode = video.get("episode_in_round")
            video_id = video["video_id"]
            lines.append(
                f"## EPISODE|season={season}|round={round_number or '?'}|"
                f"episode={episode or '?'}|video_id={video_id}"
            )
            lines.append(f"- title: {video.get('title')}")
            lines.append(f"- youtube_url: {_video_link(video_id)}")
            lines.append("- key_people: ")
            lines.append("- one_line: ")
            lines.append("- summary: ")
            lines.append("- chunk_storyline:")
            lines.append("  - Chunk 1 | 핵심 사건/감정 변화")
            lines.append("  - Chunk 2 | 핵심 사건/감정 변화")
            lines.append("- key_incidents:")
            lines.append("  - 사건 1 | 관련 인물 | 사건 내용 | 갈등/반전")
            lines.append("  - 사건 2 | 관련 인물 | 사건 내용 | 다음 전개")
            lines.append("- evidence_links:")
            lines.append("  - https://www.youtube.com/watch?v=...&t=...s")
            lines.append("  - https://www.youtube.com/watch?v=...&t=...s")
            lines.append("")
    else:
        lines.append("## 분석 요약")
        lines.append("- (여기에 전체 관찰 요약)")
        lines.append("")
        lines.append("## 사건별 상세")
        lines.append("### 사건 1")
        lines.append("- 기수/회차/에피소드:")
        lines.append("- 핵심 인물:")
        lines.append("- 사건 설명(누가/왜/어떻게/결과):")
        lines.append("- 관련 링크:")
        lines.append("")
        lines.append("### 사건 2")
        lines.append("- 기수/회차/에피소드:")
        lines.append("- 핵심 인물:")
        lines.append("- 사건 설명(누가/왜/어떻게/결과):")
        lines.append("- 관련 링크:")
        lines.append("")
        lines.append("## 에피소드 인덱스")
        for video in videos:
            round_number = video.get("round_number") or video.get("episode")
            episode = video.get("episode_in_round")
            lines.append(
                f"- {video.get('season')}기-{round_number}회차-{episode or '?'}에피소드 | "
                f"{video.get('title')} | {_video_link(video['video_id'])}"
            )
    return "\n".join(lines)


def cmd_list(repo: NasolRepository, status: str | None, limit: int, job_kind: str | None) -> int:
    jobs = repo.list_codex_jobs(limit=limit, status=status, job_kind=job_kind)
    if not jobs:
        print("No codex jobs found.")
        return 0
    for job in jobs:
        created = (job.get("created_at") or "")[:19]
        kind = _job_kind(job)
        print(
            f"#{job['id']:>4} | {job['status']:<9} | {kind:<8} | {_season_label(job['seasons'])} | "
            f"{created} | {job['query'][:80]}"
        )
    return 0


def cmd_context(repo: NasolRepository, job_id: int, output: str | None) -> int:
    job = repo.get_codex_job(job_id)
    if not job:
        print(f"Job #{job_id} not found.")
        return 2
    if job["status"] == "pending":
        repo.set_codex_job_running(job_id)
        job = repo.get_codex_job(job_id) or job
    content = build_context_markdown(repo, job)
    if output:
        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(content, encoding="utf-8")
        print(f"Saved: {out_path}")
        return 0
    print(content)
    return 0


def cmd_packet(
    repo: NasolRepository,
    job_id: int,
    output_dir: str,
    max_videos: int,
    chunk_chars: int,
) -> int:
    job = repo.get_codex_job(job_id)
    if not job:
        print(f"Job #{job_id} not found.")
        return 2
    if job["status"] == "pending":
        repo.set_codex_job_running(job_id)
        job = repo.get_codex_job(job_id) or job

    videos = _load_job_videos(repo, job, max_videos=max_videos)
    out_dir = Path(output_dir)
    episodes_dir = out_dir / "episodes"
    out_dir.mkdir(parents=True, exist_ok=True)
    episodes_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "README.md").write_text(build_packet_readme(job, videos), encoding="utf-8")
    (out_dir / "result_template.md").write_text(build_result_template(job, videos), encoding="utf-8")
    (out_dir / "context.md").write_text(
        build_context_markdown(repo, job, max_videos=max_videos, chunk_chars=chunk_chars),
        encoding="utf-8",
    )

    for video in videos:
        season = _to_int(video.get("season"))
        round_number = _to_int(video.get("round_number") or video.get("episode"))
        episode = _to_int(video.get("episode_in_round"))
        slug = _slugify(video.get("title", "episode"))
        filename = (
            f"s{season:02d}_r{round_number:03d}_e{episode:03d}_"
            f"{video['video_id']}_{slug}.md"
        )
        content = build_episode_packet_markdown(
            video,
            chunk_chars=chunk_chars,
            job_kind=_job_kind(job),
        )
        (episodes_dir / filename).write_text(content, encoding="utf-8")

    print(f"Saved packet: {out_dir}")
    print(f"- episodes: {len(videos)} files")
    print(f"- readme: {(out_dir / 'README.md')}")
    print(f"- template: {(out_dir / 'result_template.md')}")
    return 0


def cmd_start(repo: NasolRepository, job_id: int) -> int:
    job = repo.get_codex_job(job_id)
    if not job:
        print(f"Job #{job_id} not found.")
        return 2
    repo.set_codex_job_running(job_id)
    print(f"Running job #{job_id}")
    return 0


def cmd_complete(repo: NasolRepository, job_id: int, result_file: str) -> int:
    job = repo.get_codex_job(job_id)
    if not job:
        print(f"Job #{job_id} not found.")
        return 2
    result_path = Path(result_file)
    if not result_path.exists():
        print(f"Result file not found: {result_path}")
        return 2
    result_text = normalize_cast_mentions(result_path.read_text(encoding="utf-8"))
    kind = _job_kind(job)
    if kind == "summary":
        errors = _validate_summary_result(result_text)
        if errors:
            print("Summary result validation failed:")
            for idx, message in enumerate(errors[:30], start=1):
                print(f"{idx}. {message}")
            if len(errors) > 30:
                print(f"...and {len(errors) - 30} more")
            print("Please revise the result file with chunk-grounded narrative summaries and retry.")
            return 2
    repo.complete_codex_job(job_id, result_text)
    print(f"Completed job #{job_id}")
    return 0


def cmd_fail(repo: NasolRepository, job_id: int, message: str) -> int:
    job = repo.get_codex_job(job_id)
    if not job:
        print(f"Job #{job_id} not found.")
        return 2
    repo.fail_codex_job(job_id, message)
    print(f"Failed job #{job_id}: {message}")
    return 0


def cmd_delete(repo: NasolRepository, job_id: int, force: bool) -> int:
    job = repo.get_codex_job(job_id)
    if not job:
        print(f"Job #{job_id} not found.")
        return 2
    if job.get("status") == "running" and not force:
        print(f"Job #{job_id} is running. Use --force to delete.")
        return 2
    deleted = repo.delete_codex_job(job_id)
    if not deleted:
        print(f"Job #{job_id} delete failed.")
        return 2
    print(f"Deleted job #{job_id}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Codex analysis queue helper.")
    parser.add_argument("--db-path", default="output/nasol.db")

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--status", choices=("pending", "running", "completed", "failed"))
    list_parser.add_argument("--job-kind", choices=("analysis", "summary"))
    list_parser.add_argument("--limit", type=int, default=30)

    context_parser = subparsers.add_parser("context")
    context_parser.add_argument("--job-id", type=int, required=True)
    context_parser.add_argument("--output")

    packet_parser = subparsers.add_parser("packet")
    packet_parser.add_argument("--job-id", type=int, required=True)
    packet_parser.add_argument("--output-dir", required=True)
    packet_parser.add_argument("--max-videos", type=int, default=3000)
    packet_parser.add_argument("--chunk-chars", type=int, default=1700)

    start_parser = subparsers.add_parser("start")
    start_parser.add_argument("--job-id", type=int, required=True)

    complete_parser = subparsers.add_parser("complete")
    complete_parser.add_argument("--job-id", type=int, required=True)
    complete_parser.add_argument("--result-file", required=True)

    fail_parser = subparsers.add_parser("fail")
    fail_parser.add_argument("--job-id", type=int, required=True)
    fail_parser.add_argument("--message", required=True)

    delete_parser = subparsers.add_parser("delete")
    delete_parser.add_argument("--job-id", type=int, required=True)
    delete_parser.add_argument("--force", action="store_true")

    args = parser.parse_args()
    repo = NasolRepository(args.db_path)

    if args.command == "list":
        return cmd_list(repo, args.status, args.limit, args.job_kind)
    if args.command == "context":
        return cmd_context(repo, args.job_id, args.output)
    if args.command == "packet":
        return cmd_packet(repo, args.job_id, args.output_dir, args.max_videos, args.chunk_chars)
    if args.command == "start":
        return cmd_start(repo, args.job_id)
    if args.command == "complete":
        return cmd_complete(repo, args.job_id, args.result_file)
    if args.command == "fail":
        return cmd_fail(repo, args.job_id, args.message)
    if args.command == "delete":
        return cmd_delete(repo, args.job_id, args.force)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
