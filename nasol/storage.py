from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class NasolRepository:
    def __init__(self, db_path: str | Path = "output/nasol.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS videos (
                    video_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    channel_title TEXT,
                    channel_id TEXT,
                    channel_url TEXT,
                    description TEXT,
                    duration_seconds INTEGER DEFAULT 0,
                    duration_text TEXT,
                    upload_date TEXT,
                    published_ts INTEGER,
                    view_count INTEGER DEFAULT 0,
                    like_count INTEGER DEFAULT 0,
                    comment_count INTEGER DEFAULT 0,
                    season INTEGER,
                    episode INTEGER,
                    series_type TEXT DEFAULT 'unknown',
                    source TEXT DEFAULT 'official',
                    is_official INTEGER DEFAULT 0,
                    source_priority INTEGER DEFAULT 0,
                    dedupe_key TEXT,
                    transcript_status TEXT DEFAULT 'pending',
                    transcript_language TEXT,
                    transcript_type TEXT,
                    transcript_text TEXT,
                    transcript_segments TEXT,
                    transcript_hash TEXT,
                    transcript_updated_at TEXT,
                    error_message TEXT,
                    discovered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_videos_season ON videos(season);
                CREATE INDEX IF NOT EXISTS idx_videos_episode ON videos(episode);
                CREATE INDEX IF NOT EXISTS idx_videos_upload_date ON videos(upload_date);
                CREATE INDEX IF NOT EXISTS idx_videos_dedupe_key ON videos(dedupe_key);
                CREATE INDEX IF NOT EXISTS idx_videos_transcript_status ON videos(transcript_status);

                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    seasons_json TEXT NOT NULL,
                    include_fallback INTEGER NOT NULL,
                    dry_run INTEGER NOT NULL,
                    total_candidates INTEGER DEFAULT 0,
                    kept_candidates INTEGER DEFAULT 0,
                    transcript_success INTEGER DEFAULT 0,
                    transcript_fail INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS job_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);

                CREATE TABLE IF NOT EXISTS analysis_views (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    view_type TEXT NOT NULL,
                    query TEXT NOT NULL,
                    seasons_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS analysis_view_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    view_id INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    season INTEGER,
                    episode INTEGER,
                    score REAL NOT NULL,
                    reason TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_analysis_view_items_view_id ON analysis_view_items(view_id);
                CREATE INDEX IF NOT EXISTS idx_analysis_view_items_video_id ON analysis_view_items(video_id);

                CREATE TABLE IF NOT EXISTS analysis_chats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    query TEXT NOT NULL,
                    seasons_json TEXT NOT NULL,
                    response TEXT NOT NULL
                );
                """
            )

    def create_job(self, seasons: list[int], include_fallback: bool, dry_run: bool) -> str:
        job_id = uuid.uuid4().hex
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id, status, started_at, seasons_json, include_fallback, dry_run
                ) VALUES (?, 'running', ?, ?, ?, ?)
                """,
                (
                    job_id,
                    now,
                    json.dumps(seasons, ensure_ascii=False),
                    1 if include_fallback else 0,
                    1 if dry_run else 0,
                ),
            )
        return job_id

    def log_job(self, job_id: str, message: str, level: str = "INFO") -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO job_logs (job_id, created_at, level, message)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, utc_now(), level, message),
            )

    def finish_job(
        self,
        job_id: str,
        status: str,
        total_candidates: int,
        kept_candidates: int,
        transcript_success: int,
        transcript_fail: int,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE jobs
                SET status = ?,
                    finished_at = ?,
                    total_candidates = ?,
                    kept_candidates = ?,
                    transcript_success = ?,
                    transcript_fail = ?
                WHERE job_id = ?
                """,
                (
                    status,
                    utc_now(),
                    total_candidates,
                    kept_candidates,
                    transcript_success,
                    transcript_fail,
                    job_id,
                ),
            )

    def upsert_video(self, video: dict[str, Any]) -> None:
        now = utc_now()
        payload = {
            "video_id": video["video_id"],
            "title": video.get("title", "").strip() or "(제목 없음)",
            "url": video.get("url", f"https://www.youtube.com/watch?v={video['video_id']}"),
            "channel_title": video.get("channel_title", ""),
            "channel_id": video.get("channel_id", ""),
            "channel_url": video.get("channel_url", ""),
            "description": video.get("description", ""),
            "duration_seconds": int(video.get("duration_seconds") or 0),
            "duration_text": video.get("duration_text", ""),
            "upload_date": video.get("upload_date"),
            "published_ts": int(video.get("published_ts") or 0),
            "view_count": int(video.get("view_count") or 0),
            "like_count": int(video.get("like_count") or 0),
            "comment_count": int(video.get("comment_count") or 0),
            "season": video.get("season"),
            "episode": video.get("episode"),
            "series_type": video.get("series_type", "unknown"),
            "source": video.get("source", "official"),
            "is_official": 1 if video.get("is_official") else 0,
            "source_priority": int(video.get("source_priority") or 0),
            "dedupe_key": video.get("dedupe_key"),
            "discovered_at": now,
            "updated_at": now,
        }

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO videos (
                    video_id, title, url, channel_title, channel_id, channel_url, description,
                    duration_seconds, duration_text, upload_date, published_ts,
                    view_count, like_count, comment_count, season, episode, series_type,
                    source, is_official, source_priority, dedupe_key, discovered_at, updated_at
                )
                VALUES (
                    :video_id, :title, :url, :channel_title, :channel_id, :channel_url, :description,
                    :duration_seconds, :duration_text, :upload_date, :published_ts,
                    :view_count, :like_count, :comment_count, :season, :episode, :series_type,
                    :source, :is_official, :source_priority, :dedupe_key, :discovered_at, :updated_at
                )
                ON CONFLICT(video_id) DO UPDATE SET
                    title = excluded.title,
                    url = excluded.url,
                    channel_title = excluded.channel_title,
                    channel_id = excluded.channel_id,
                    channel_url = excluded.channel_url,
                    description = excluded.description,
                    duration_seconds = excluded.duration_seconds,
                    duration_text = excluded.duration_text,
                    upload_date = excluded.upload_date,
                    published_ts = excluded.published_ts,
                    view_count = excluded.view_count,
                    like_count = excluded.like_count,
                    comment_count = excluded.comment_count,
                    season = COALESCE(excluded.season, videos.season),
                    episode = COALESCE(excluded.episode, videos.episode),
                    series_type = excluded.series_type,
                    source = CASE
                        WHEN excluded.source_priority >= videos.source_priority THEN excluded.source
                        ELSE videos.source
                    END,
                    is_official = CASE
                        WHEN excluded.source_priority >= videos.source_priority THEN excluded.is_official
                        ELSE videos.is_official
                    END,
                    source_priority = MAX(videos.source_priority, excluded.source_priority),
                    dedupe_key = COALESCE(excluded.dedupe_key, videos.dedupe_key),
                    updated_at = excluded.updated_at
                """,
                payload,
            )

    def update_transcript(self, video_id: str, transcript: dict[str, Any]) -> None:
        segments = transcript.get("transcript_segments") or []
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE videos
                SET transcript_status = ?,
                    transcript_language = ?,
                    transcript_type = ?,
                    transcript_text = ?,
                    transcript_segments = ?,
                    transcript_hash = ?,
                    transcript_updated_at = ?,
                    error_message = ?,
                    updated_at = ?
                WHERE video_id = ?
                """,
                (
                    transcript.get("transcript_status", "error"),
                    transcript.get("language", ""),
                    transcript.get("transcript_type", ""),
                    transcript.get("transcript_text", ""),
                    json.dumps(segments, ensure_ascii=False),
                    transcript.get("transcript_hash", ""),
                    utc_now(),
                    transcript.get("error_message"),
                    utc_now(),
                    video_id,
                ),
            )

    def get_video(self, video_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM videos WHERE video_id = ?",
                (video_id,),
            ).fetchone()
        return dict(row) if row else None

    def video_has_transcript(self, video_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT transcript_status
                FROM videos
                WHERE video_id = ?
                """,
                (video_id,),
            ).fetchone()
        return bool(row and row["transcript_status"] == "success")

    def get_available_seasons(self) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT season
                FROM videos
                WHERE season IS NOT NULL
                ORDER BY season
                """
            ).fetchall()
        return [int(row["season"]) for row in rows]

    def get_videos(
        self,
        seasons: list[int] | None = None,
        transcript_only: bool | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []

        if seasons:
            placeholders = ",".join("?" for _ in seasons)
            clauses.append(f"season IN ({placeholders})")
            params.extend(seasons)

        if transcript_only is True:
            clauses.append("transcript_status = 'success'")
        elif transcript_only is False:
            clauses.append("transcript_status != 'success'")

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_sql = f"LIMIT {int(limit)}" if limit else ""
        query = f"""
            SELECT *
            FROM videos
            {where_sql}
            ORDER BY
                COALESCE(season, 999),
                COALESCE(episode, 9999),
                COALESCE(upload_date, '9999-99-99'),
                video_id
            {limit_sql}
        """
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_season_summary(self, seasons: list[int] | None = None) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if seasons:
            placeholders = ",".join("?" for _ in seasons)
            clauses.append(f"season IN ({placeholders})")
            params.extend(seasons)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"""
            SELECT
                season,
                COUNT(*) AS total_videos,
                SUM(CASE WHEN transcript_status = 'success' THEN 1 ELSE 0 END) AS transcript_success,
                ROUND(AVG(CASE WHEN view_count > 0 THEN CAST(comment_count AS REAL) / view_count ELSE 0 END), 6) AS avg_engagement
            FROM videos
            {where_sql}
            GROUP BY season
            ORDER BY season
        """
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def list_recent_jobs(self, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM jobs
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_job_logs(self, job_id: str, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT created_at, level, message
                FROM job_logs
                WHERE job_id = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (job_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def save_analysis_view(
        self,
        name: str,
        view_type: str,
        query: str,
        seasons: list[int],
        items: list[dict[str, Any]],
    ) -> int:
        created_at = utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO analysis_views (name, view_type, query, seasons_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, view_type, query, json.dumps(seasons, ensure_ascii=False), created_at),
            )
            view_id = int(cursor.lastrowid)
            conn.executemany(
                """
                INSERT INTO analysis_view_items (view_id, video_id, season, episode, score, reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        view_id,
                        item["video_id"],
                        item.get("season"),
                        item.get("episode"),
                        float(item.get("score", 0.0)),
                        item.get("reason", ""),
                    )
                    for item in items
                ],
            )
        return view_id

    def list_analysis_views(self, limit: int = 40) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, name, view_type, query, seasons_json, created_at
                FROM analysis_views
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["seasons"] = json.loads(item["seasons_json"])
            results.append(item)
        return results

    def get_analysis_view(self, view_id: int) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        with self._connect() as conn:
            view_row = conn.execute(
                """
                SELECT id, name, view_type, query, seasons_json, created_at
                FROM analysis_views
                WHERE id = ?
                """,
                (view_id,),
            ).fetchone()

            if not view_row:
                return None, []

            item_rows = conn.execute(
                """
                SELECT
                    i.video_id,
                    i.season,
                    i.episode,
                    i.score,
                    i.reason,
                    v.title,
                    v.url,
                    v.upload_date,
                    v.view_count,
                    v.comment_count
                FROM analysis_view_items AS i
                LEFT JOIN videos AS v ON i.video_id = v.video_id
                WHERE i.view_id = ?
                ORDER BY i.score DESC, i.season ASC, i.episode ASC
                """,
                (view_id,),
            ).fetchall()

        view = dict(view_row)
        view["seasons"] = json.loads(view["seasons_json"])
        items = [dict(row) for row in item_rows]
        return view, items

    def save_chat_exchange(self, query: str, seasons: list[int], response: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO analysis_chats (created_at, query, seasons_json, response)
                VALUES (?, ?, ?, ?)
                """,
                (utc_now(), query, json.dumps(seasons, ensure_ascii=False), response),
            )

    def list_chat_history(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, created_at, query, seasons_json, response
                FROM analysis_chats
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["seasons"] = json.loads(payload["seasons_json"])
            results.append(payload)
        return results
