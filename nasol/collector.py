from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import yt_dlp
from youtube_transcript_api import NoTranscriptFound, TranscriptsDisabled, YouTubeTranscriptApi

from nasol.parsing import (
    classify_series_type,
    ensure_season_list,
    make_dedupe_key,
    parse_episode_number,
    parse_first_season,
    parse_upload_date,
    transcript_hash,
)
from nasol.storage import NasolRepository

LogCallback = Callable[[str], None]


@dataclass
class CollectorConfig:
    official_channel_handle: str = "@chonjang"
    official_channel_id: str = "UCIfadKo7fcwSfgARMTz7xzA"
    request_delay_seconds: float = 1.3
    transcript_delay_min: float = 2.5
    transcript_delay_max: float = 5.0
    max_search_results: int = 50
    max_retries: int = 3
    preferred_languages: tuple[str, ...] = ("ko", "ko-KR", "en", "en-US")


class NasolCollector:
    def __init__(self, repository: NasolRepository, config: CollectorConfig | None = None) -> None:
        self.repo = repository
        self.config = config or CollectorConfig()

    def collect(
        self,
        seasons: list[int],
        include_fallback_search: bool = True,
        dry_run: bool = False,
        force_transcript_refresh: bool = False,
        logger: LogCallback | None = None,
    ) -> dict[str, Any]:
        selected_seasons = ensure_season_list(seasons)
        if not selected_seasons:
            raise ValueError("수집할 기수를 선택해야 합니다.")

        job_id = self.repo.create_job(
            seasons=selected_seasons,
            include_fallback=include_fallback_search,
            dry_run=dry_run,
        )
        log = self._make_logger(job_id, logger)

        transcript_success = 0
        transcript_fail = 0
        fail_reasons: dict[str, int] = {}
        total_candidates = 0
        kept_candidates = 0

        try:
            log(f"수집 작업 시작: {selected_seasons[0]}기~{selected_seasons[-1]}기")
            official_seeds = self._discover_official(selected_seasons, log)
            official_coverage = self._count_by_season(official_seeds)

            missing_seasons = [
                season for season in selected_seasons if official_coverage.get(season, 0) == 0
            ]
            fallback_seeds: list[dict[str, Any]] = []
            if include_fallback_search and missing_seasons:
                log(f"공식 채널 누락 기수 {missing_seasons} -> 일반 검색 보완 시작")
                fallback_seeds = self._discover_fallback(missing_seasons, log)
            elif missing_seasons:
                log(
                    f"공식 채널에서 누락된 기수 {missing_seasons}가 있습니다. "
                    "필요하면 일반 검색 보완 옵션을 켜주세요."
                )

            merged_seeds = self._merge_seed_lists(official_seeds, fallback_seeds)
            total_candidates = len(merged_seeds)
            log(f"후보 영상 {total_candidates}개 상세 메타데이터 조회 시작")

            enriched = self._enrich_candidates(merged_seeds, selected_seasons, log)
            deduped = self._dedupe_candidates(enriched)
            ordered = self._sort_candidates(deduped)
            kept_candidates = len(ordered)
            log(f"중복 제거 완료: {len(enriched)} -> {kept_candidates}")

            for payload in ordered:
                self.repo.upsert_video(payload)

            log(f"Raw 데이터 저장 완료: {kept_candidates}개")

            if dry_run:
                log("Dry-run 모드: 대본 수집은 건너뜁니다.")
            else:
                for idx, video in enumerate(ordered, start=1):
                    video_id = video["video_id"]
                    if not force_transcript_refresh and self.repo.video_has_transcript(video_id):
                        if idx % 10 == 0:
                            log(f"대본 진행 {idx}/{len(ordered)} (기존 대본 유지)")
                        continue

                    transcript = self._fetch_transcript(video_id)
                    self.repo.update_transcript(video_id, transcript)

                    if transcript["transcript_status"] == "success":
                        transcript_success += 1
                    else:
                        transcript_fail += 1
                        reason = transcript["transcript_status"]
                        fail_reasons[reason] = fail_reasons.get(reason, 0) + 1

                    if idx % 5 == 0 or transcript["transcript_status"] != "success":
                        season = video.get("season")
                        short_title = (video.get("title") or "")[:36]
                        log(
                            f"대본 {idx}/{len(ordered)} | {season}기 | "
                            f"{short_title} | {transcript['transcript_status']}"
                        )

                    sleep_seconds = random.uniform(
                        self.config.transcript_delay_min,
                        self.config.transcript_delay_max,
                    )
                    time.sleep(sleep_seconds)

            self.repo.finish_job(
                job_id=job_id,
                status="completed",
                total_candidates=total_candidates,
                kept_candidates=kept_candidates,
                transcript_success=transcript_success,
                transcript_fail=transcript_fail,
            )
            log("수집 작업 완료")

        except Exception as exc:  # pylint: disable=broad-except
            self.repo.finish_job(
                job_id=job_id,
                status="failed",
                total_candidates=total_candidates,
                kept_candidates=kept_candidates,
                transcript_success=transcript_success,
                transcript_fail=transcript_fail,
            )
            self.repo.log_job(job_id, f"실패: {exc}", level="ERROR")
            raise

        return {
            "job_id": job_id,
            "seasons": selected_seasons,
            "total_candidates": total_candidates,
            "saved_videos": kept_candidates,
            "transcript_success": transcript_success,
            "transcript_fail": transcript_fail,
            "transcript_fail_reasons": fail_reasons,
            "season_summary": self.repo.get_season_summary(selected_seasons),
        }

    def _make_logger(self, job_id: str, callback: LogCallback | None) -> LogCallback:
        def log(message: str) -> None:
            self.repo.log_job(job_id, message)
            if callback:
                callback(message)

        return log

    def _discover_official(self, seasons: list[int], log: LogCallback) -> list[dict[str, Any]]:
        seeds: dict[str, dict[str, Any]] = {}

        playlist_url = f"https://www.youtube.com/{self.config.official_channel_handle}/playlists"
        playlists = self._extract_entries(playlist_url)
        matched_playlists = 0
        for playlist in playlists:
            playlist_title = (playlist.get("title") or "").strip()
            season = parse_first_season(playlist_title)
            if season not in seasons:
                continue
            matched_playlists += 1
            url = playlist.get("url")
            if not url:
                continue
            playlist_items = self._extract_entries(url)
            for entry in playlist_items:
                seed = self._seed_from_entry(
                    entry,
                    source="official_playlist",
                    forced_season=season,
                    is_official=True,
                )
                if seed:
                    seeds[seed["video_id"]] = seed

            log(f"{season}기 플레이리스트 영상 {len(playlist_items)}개 탐색")
            time.sleep(self.config.request_delay_seconds)

        if matched_playlists == 0:
            log("공식 채널 플레이리스트 기반 기수 매칭이 없어 채널 영상 목록으로 보완합니다.")

        videos_url = f"https://www.youtube.com/{self.config.official_channel_handle}/videos"
        channel_entries = self._extract_entries(videos_url)
        matched_from_channel = 0
        for entry in channel_entries:
            title = entry.get("title", "")
            description = entry.get("description", "")
            season = parse_first_season(f"{title} {description}")
            if season not in seasons:
                continue
            seed = self._seed_from_entry(
                entry,
                source="official_channel",
                forced_season=season,
                is_official=True,
            )
            if not seed:
                continue
            seeds[seed["video_id"]] = seed
            matched_from_channel += 1

        log(
            f"공식 채널 후보 수집 완료: 플레이리스트 {matched_playlists}개, "
            f"채널목록 매칭 {matched_from_channel}개"
        )
        return list(seeds.values())

    def _discover_fallback(self, seasons: list[int], log: LogCallback) -> list[dict[str, Any]]:
        seeds: dict[str, dict[str, Any]] = {}
        for season in seasons:
            query = f"나는솔로 {season}기"
            entries = self._search_entries(query, self.config.max_search_results)
            accepted = 0
            for entry in entries:
                seed = self._seed_from_entry(
                    entry,
                    source="general_search",
                    forced_season=None,
                    is_official=False,
                )
                if not seed:
                    continue
                if seed.get("season") != season:
                    continue
                if not self._is_relevant_video(seed["title"], seed.get("description", ""), season):
                    continue
                seeds[seed["video_id"]] = seed
                accepted += 1

            log(f"{season}기 일반 검색 후보 {accepted}개 확보")
            time.sleep(self.config.request_delay_seconds)

        return list(seeds.values())

    def _extract_entries(self, url: str) -> list[dict[str, Any]]:
        options = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": "in_playlist",
            "skip_download": True,
            "ignoreerrors": True,
        }
        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(url, download=False)
        except Exception:  # pylint: disable=broad-except
            return []

        if not info:
            return []

        if "entries" in info and info["entries"]:
            return [entry for entry in info["entries"] if entry]

        return [info]

    def _search_entries(self, query: str, max_results: int) -> list[dict[str, Any]]:
        search_url = f"ytsearch{max_results}:{query}"
        return self._extract_entries(search_url)

    def _seed_from_entry(
        self,
        entry: dict[str, Any],
        source: str,
        forced_season: int | None,
        is_official: bool,
    ) -> dict[str, Any] | None:
        raw_url = str(entry.get("url") or "")
        video_id = entry.get("id") or self._video_id_from_url(raw_url)
        if not video_id:
            return None

        title = (entry.get("title") or "").strip()
        description = (entry.get("description") or "").strip()
        season = forced_season or parse_first_season(f"{title} {description}")
        episode = parse_episode_number(title)

        url = raw_url or f"https://www.youtube.com/watch?v={video_id}"
        if not str(url).startswith("http"):
            url = f"https://www.youtube.com/watch?v={video_id}"

        return {
            "video_id": video_id,
            "title": title,
            "description": description[:1200],
            "url": url,
            "season": season,
            "episode": episode,
            "source": source,
            "is_official": is_official,
            "source_priority": 3 if is_official else 1,
        }

    def _enrich_candidates(
        self,
        seeds: list[dict[str, Any]],
        target_seasons: list[int],
        log: LogCallback,
    ) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        options = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "ignoreerrors": True,
        }
        with yt_dlp.YoutubeDL(options) as ydl:
            for idx, seed in enumerate(seeds, start=1):
                info = self._extract_video_detail(ydl, seed["video_id"])
                if not info:
                    continue

                title = (info.get("title") or seed.get("title") or "").strip()
                description = (info.get("description") or seed.get("description") or "").strip()
                inferred_season = seed.get("season") or parse_first_season(f"{title} {description}")
                if inferred_season not in target_seasons:
                    continue

                upload_date = parse_upload_date(info.get("upload_date")) or parse_upload_date(
                    seed.get("upload_date")
                )
                episode = seed.get("episode") or parse_episode_number(title)

                channel_id = info.get("channel_id") or seed.get("channel_id") or ""
                channel_url = info.get("channel_url") or seed.get("channel_url") or ""
                channel_name = info.get("channel") or info.get("uploader") or seed.get("channel_title")

                official = bool(
                    seed.get("is_official")
                    or channel_id == self.config.official_channel_id
                    or self.config.official_channel_handle.lower() in (channel_url or "").lower()
                )

                payload = {
                    "video_id": seed["video_id"],
                    "title": title,
                    "description": description[:4000],
                    "url": f"https://www.youtube.com/watch?v={seed['video_id']}",
                    "channel_title": channel_name or "",
                    "channel_id": channel_id,
                    "channel_url": channel_url,
                    "duration_seconds": int(info.get("duration") or 0),
                    "duration_text": info.get("duration_string") or "",
                    "upload_date": upload_date,
                    "published_ts": int(info.get("timestamp") or 0),
                    "view_count": int(info.get("view_count") or 0),
                    "like_count": int(info.get("like_count") or 0),
                    "comment_count": int(info.get("comment_count") or 0),
                    "season": inferred_season,
                    "episode": episode,
                    "series_type": classify_series_type(title, description),
                    "source": "official_channel" if official else seed.get("source", "general_search"),
                    "is_official": official,
                    "source_priority": 3 if official else 1,
                }
                payload["dedupe_key"] = make_dedupe_key(
                    season=payload["season"],
                    episode=payload.get("episode"),
                    upload_date=payload.get("upload_date"),
                    title=payload.get("title", ""),
                )
                enriched.append(payload)

                if idx % 10 == 0:
                    log(f"상세 메타데이터 {idx}/{len(seeds)} 처리")

                time.sleep(self.config.request_delay_seconds)
        return enriched

    def _extract_video_detail(self, ydl: yt_dlp.YoutubeDL, video_id: str) -> dict[str, Any] | None:
        for attempt in range(1, self.config.max_retries + 1):
            try:
                return ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            except Exception:  # pylint: disable=broad-except
                if attempt >= self.config.max_retries:
                    return None
                backoff = (2 ** (attempt - 1)) + random.random()
                time.sleep(backoff)
        return None

    def _dedupe_candidates(self, videos: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_video_id: dict[str, dict[str, Any]] = {video["video_id"]: video for video in videos}
        by_dedupe_key: dict[str, dict[str, Any]] = {}

        for video in by_video_id.values():
            key = video["dedupe_key"]
            current = by_dedupe_key.get(key)
            if not current:
                by_dedupe_key[key] = video
                continue

            if self._is_higher_priority(video, current):
                by_dedupe_key[key] = video

        return list(by_dedupe_key.values())

    def _is_higher_priority(self, incoming: dict[str, Any], existing: dict[str, Any]) -> bool:
        incoming_key = (
            1 if incoming.get("is_official") else 0,
            int(incoming.get("source_priority") or 0),
            int(incoming.get("view_count") or 0),
            int(incoming.get("comment_count") or 0),
            incoming.get("upload_date") or "",
        )
        existing_key = (
            1 if existing.get("is_official") else 0,
            int(existing.get("source_priority") or 0),
            int(existing.get("view_count") or 0),
            int(existing.get("comment_count") or 0),
            existing.get("upload_date") or "",
        )
        return incoming_key > existing_key

    def _sort_candidates(self, videos: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            videos,
            key=lambda row: (
                row.get("season") or 999,
                row.get("episode") if row.get("episode") is not None else 9999,
                row.get("upload_date") or "9999-99-99",
                row.get("video_id"),
            ),
        )

    def _is_relevant_video(self, title: str, description: str, season: int) -> bool:
        combined = f"{title} {description}".lower()
        season_text = f"{season}기"
        if season_text not in combined:
            return False
        if "나는 solo" in combined or "나는솔로" in combined or "나솔" in combined:
            return True
        return False

    def _merge_seed_lists(
        self,
        official: list[dict[str, Any]],
        fallback: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for item in fallback:
            merged[item["video_id"]] = item
        for item in official:
            merged[item["video_id"]] = item
        return list(merged.values())

    def _count_by_season(self, videos: list[dict[str, Any]]) -> dict[int, int]:
        counts: dict[int, int] = {}
        for row in videos:
            season = row.get("season")
            if not season:
                continue
            counts[season] = counts.get(season, 0) + 1
        return counts

    def _video_id_from_url(self, url: str) -> str | None:
        if not url:
            return None
        parsed = urlparse(url)
        if parsed.netloc.endswith("youtu.be"):
            return parsed.path.strip("/") or None
        query_video_id = parse_qs(parsed.query).get("v")
        if query_video_id:
            return query_video_id[0]
        return None

    def _fetch_transcript(self, video_id: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "transcript_status": "error",
            "language": "",
            "transcript_type": "",
            "transcript_text": "",
            "transcript_segments": [],
            "error_message": "",
            "transcript_hash": "",
        }
        try:
            api = YouTubeTranscriptApi()
            transcript_list = api.list(video_id)

            chosen = None
            chosen_type = ""
            for transcript in transcript_list:
                language = transcript.language_code
                is_generated = transcript.is_generated
                if language in ("ko", "ko-KR") and not is_generated:
                    chosen = transcript
                    chosen_type = "manual"
                    break
                if language in ("ko", "ko-KR") and chosen is None:
                    chosen = transcript
                    chosen_type = "auto"
                if language in self.config.preferred_languages and chosen is None:
                    chosen = transcript
                    chosen_type = "auto" if is_generated else "manual"

            if chosen is None:
                transcripts = list(transcript_list)
                if transcripts:
                    chosen = transcripts[0]
                    chosen_type = "auto" if chosen.is_generated else "manual"

            if chosen is None:
                payload["transcript_status"] = "no_transcript"
                return payload

            fetched = chosen.fetch()
            segments = [
                {
                    "start": float(getattr(segment, "start", 0.0)),
                    "duration": float(getattr(segment, "duration", 0.0)),
                    "text": str(getattr(segment, "text", "")).strip(),
                }
                for segment in fetched
                if str(getattr(segment, "text", "")).strip()
            ]
            transcript_text = "\n".join(segment["text"] for segment in segments)
            payload.update(
                {
                    "transcript_status": "success",
                    "language": chosen.language_code,
                    "transcript_type": chosen_type,
                    "transcript_text": transcript_text,
                    "transcript_segments": segments,
                    "transcript_hash": transcript_hash(transcript_text),
                    "error_message": "",
                }
            )
            return payload

        except NoTranscriptFound:
            payload["transcript_status"] = "no_transcript"
            return payload
        except TranscriptsDisabled:
            payload["transcript_status"] = "transcripts_disabled"
            return payload
        except Exception as exc:  # pylint: disable=broad-except
            payload["transcript_status"] = "error"
            payload["error_message"] = str(exc)[:180]
            return payload
