from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any

import pandas as pd
import streamlit as st

from nasol import CollectorConfig, NasolAnalyst, NasolCollector, NasolRepository
from nasol.parsing import ensure_season_list


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+KR:wght@400;500;600;700&display=swap');

        html, body, [class*="css"]  {
            font-family: 'IBM Plex Sans KR', sans-serif;
        }

        [data-testid="stAppViewContainer"] {
            background: radial-gradient(circle at 15% 15%, #fff2d5 0%, transparent 40%),
                        radial-gradient(circle at 80% 10%, #d8e8ff 0%, transparent 35%),
                        linear-gradient(180deg, #f8fafc 0%, #f1f5f9 100%);
        }

        .title-card {
            border-radius: 16px;
            padding: 20px 22px;
            background: rgba(255, 255, 255, 0.82);
            border: 1px solid rgba(15, 23, 42, 0.08);
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
            margin-bottom: 10px;
        }

        .info-chip {
            display: inline-block;
            padding: 4px 10px;
            margin-right: 8px;
            border-radius: 999px;
            background: #0f172a;
            color: #ffffff;
            font-size: 12px;
            font-weight: 600;
        }

        .result-card {
            border-radius: 14px;
            padding: 14px 16px;
            background: rgba(255, 255, 255, 0.95);
            border: 1px solid rgba(148, 163, 184, 0.35);
            margin-bottom: 10px;
        }

        .result-title {
            font-size: 16px;
            font-weight: 700;
            margin-bottom: 4px;
            color: #0f172a;
        }

        .result-meta {
            color: #334155;
            font-size: 13px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def season_selector(prefix: str) -> list[int]:
    mode = st.radio(
        "기수 선택 방식",
        options=["단일", "범위", "다중"],
        horizontal=True,
        key=f"{prefix}_season_mode",
    )
    if mode == "단일":
        season = st.selectbox("기수", list(range(1, 30)), index=9, key=f"{prefix}_single")
        return [season]
    if mode == "범위":
        season_range = st.slider(
            "기수 범위",
            min_value=1,
            max_value=29,
            value=(10, 11),
            key=f"{prefix}_range",
        )
        return list(range(season_range[0], season_range[1] + 1))
    seasons = st.multiselect(
        "기수 다중 선택",
        options=list(range(1, 30)),
        default=[10, 11],
        key=f"{prefix}_multi",
    )
    return ensure_season_list(seasons)


def format_round(round_number: int | None) -> str:
    return f"{round_number}회차" if round_number else "회차 미확정"


def format_job_label(job: dict[str, Any]) -> str:
    started = (job.get("started_at") or "")[:19].replace("T", " ")
    status = job.get("status") or "-"
    return f"{job['job_id'][:8]} | {status} | {started}"


def format_season_label(seasons: list[int]) -> str:
    if not seasons:
        return "전체"
    sorted_seasons = sorted(seasons)
    if len(sorted_seasons) == 1:
        return f"{sorted_seasons[0]}기"
    return f"{sorted_seasons[0]}기~{sorted_seasons[-1]}기"


def build_summary_query(seasons: list[int]) -> str:
    label = format_season_label(seasons)
    return (
        f"{label} 본편 전체 에피소드 transcript를 chunk 기반으로 요약해줘. "
        "각 에피소드마다 핵심 줄거리, 핵심 인물, 핵심 장면 링크를 정리해줘."
    )


def parse_summary_result_markdown(result_text: str) -> list[dict[str, Any]]:
    def _to_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    sections = re.split(r"(?m)^##\s+EPISODE\|", result_text or "")
    if len(sections) <= 1:
        return []

    items: list[dict[str, Any]] = []
    for raw_section in sections[1:]:
        section = raw_section.strip()
        if not section:
            continue
        lines = section.splitlines()
        if not lines:
            continue

        meta_line = lines[0].strip()
        body_lines = lines[1:]
        meta: dict[str, Any] = {}
        for token in meta_line.split("|"):
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            meta[key.strip().lower()] = value.strip()

        payload: dict[str, Any] = {
            "season": _to_int(meta.get("season")),
            "round": _to_int(meta.get("round")),
            "episode": _to_int(meta.get("episode")),
            "video_id": meta.get("video_id") or "",
            "title": "",
            "youtube_url": "",
            "key_people": "",
            "one_line": "",
            "summary": "",
            "chunk_storyline": [],
            "key_incidents": [],
            "highlights": [],
            "evidence_links": [],
        }
        list_keys = {"chunk_storyline", "key_incidents", "highlights", "evidence_links"}
        current_key: str | None = None
        for line in body_lines:
            if line.startswith("- ") and ":" not in line:
                loose_value = line[2:].strip()
                if current_key in list_keys and loose_value:
                    payload[current_key].append(loose_value)
                continue

            if line.startswith("- ") and ":" in line:
                key, value = line[2:].split(":", 1)
                current_key = key.strip().lower()
                clean_value = value.strip()
                if current_key in list_keys:
                    if clean_value:
                        payload[current_key].append(clean_value)
                elif current_key in payload:
                    payload[current_key] = clean_value
                continue

            if line.startswith("  - "):
                sub_value = line[4:].strip()
                if current_key in list_keys and sub_value:
                    payload[current_key].append(sub_value)
                continue

            if current_key in {"summary", "one_line"}:
                if line.strip():
                    payload[current_key] = (payload[current_key] + " " + line.strip()).strip()

        if not payload["chunk_storyline"] and payload["highlights"]:
            payload["chunk_storyline"] = list(payload["highlights"])

        if not payload["youtube_url"] and payload["video_id"]:
            payload["youtube_url"] = f"https://www.youtube.com/watch?v={payload['video_id']}"
        if payload["season"] <= 0:
            continue
        items.append(payload)

    items.sort(key=lambda row: (row["season"], row["round"], row["episode"], row["video_id"]))
    return items


def spawn_background_collection(
    repo: NasolRepository,
    seasons: list[int],
    include_fallback: bool,
    dry_run: bool,
    force_refresh: bool,
) -> int:
    db_path = str(Path(repo.db_path).resolve())
    root_dir = str(Path(__file__).parent.resolve())
    log_dir = Path(root_dir) / "output"
    log_dir.mkdir(parents=True, exist_ok=True)
    worker_log_path = log_dir / "collector_worker.log"

    cmd = [
        sys.executable,
        "-m",
        "nasol.background_collect",
        "--db-path",
        db_path,
        "--seasons",
        ",".join(str(season) for season in seasons),
        "--include-fallback",
        "1" if include_fallback else "0",
        "--dry-run",
        "1" if dry_run else "0",
        "--force-refresh",
        "1" if force_refresh else "0",
    ]
    with worker_log_path.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(  # noqa: S603
            cmd,
            cwd=root_dir,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,
        )
    return int(process.pid)


def render_collection_tab(repo: NasolRepository, collector: NasolCollector) -> None:
    st.markdown("### 데이터 수집")
    st.caption(
        "공식 채널(@chonjang) 본편 우선 수집 후, 누락 기수만 일반 검색으로 보완합니다. "
        "지볶행/나솔사계/사랑은 계속된다 등 스핀오프는 제외합니다."
    )

    running_jobs = repo.list_recent_jobs(limit=5, status="running")
    has_running_job = bool(running_jobs)

    col_left, col_right = st.columns([2, 1], gap="large")
    with col_left:
        seasons = season_selector("collect")
        include_fallback = st.checkbox("공식 채널 누락 시 일반 검색 보완", value=True)
        dry_run = st.checkbox("Dry-run (영상 목록만 저장, 대본은 생략)", value=False)
        force_refresh = st.checkbox("기존 대본이 있어도 다시 수집", value=False)
        run_mode = st.radio(
            "실행 모드",
            options=["백그라운드(멀티프로세스)", "포그라운드(단일 프로세스)"],
            horizontal=True,
            key="collect_run_mode",
        )
    with col_right:
        status_label = "실행중 작업 있음" if has_running_job else "대기중"
        status_color = "#dc2626" if has_running_job else "#16a34a"
        st.markdown(
            f"""
            <div class="title-card">
                <span class="info-chip">중복 방지</span>
                <span class="info-chip">시간순 정렬</span>
                <span class="info-chip">무료 수집</span><br/><br/>
                <b style="color:{status_color};">{status_label}</b>
            </div>
            """,
            unsafe_allow_html=True,
        )

    run_clicked = st.button("수집 시작", use_container_width=True, type="primary")
    log_placeholder = st.empty()
    status_placeholder = st.empty()

    if run_clicked:
        if not seasons:
            st.error("최소 1개 기수를 선택해주세요.")
        elif has_running_job and run_mode == "백그라운드(멀티프로세스)":
            st.warning("이미 실행중인 백그라운드 수집 작업이 있습니다. 완료 후 다시 시작해주세요.")
        elif run_mode == "백그라운드(멀티프로세스)":
            worker_pid = spawn_background_collection(
                repo=repo,
                seasons=seasons,
                include_fallback=include_fallback,
                dry_run=dry_run,
                force_refresh=force_refresh,
            )
            st.session_state["last_worker_pid"] = worker_pid
            st.toast("백그라운드 수집 시작됨. Raw Data 탭으로 이동해 실시간 확인하세요.")
            status_placeholder.success(f"백그라운드 프로세스 시작 완료 (PID: {worker_pid})")
        else:
            logs: list[str] = []

            def append_log(message: str) -> None:
                now = datetime.now().strftime("%H:%M:%S")
                logs.append(f"[{now}] {message}")
                log_placeholder.code("\n".join(logs[-180:]), language="text")

            with st.spinner("수집 작업을 실행 중입니다..."):
                summary = collector.collect(
                    seasons=seasons,
                    include_fallback_search=include_fallback,
                    dry_run=dry_run,
                    force_transcript_refresh=force_refresh,
                    logger=append_log,
                )
            st.session_state["last_collection_summary"] = summary
            st.toast("포그라운드 수집 완료")

    summary = st.session_state.get("last_collection_summary")
    if summary and not has_running_job:
        status_placeholder.success(
            f"최근 실행 결과 | 후보 {summary['total_candidates']}개 -> 저장 {summary['saved_videos']}개"
        )

    if has_running_job:
        running_info = ", ".join(job["job_id"][:8] for job in running_jobs)
        st.info(f"실행중 수집 작업: {running_info}")

    st.markdown("### 작업 로그")
    jobs = repo.list_recent_jobs(limit=20)
    if jobs:
        job_map = {job["job_id"]: job for job in jobs}
        default_job_id = running_jobs[0]["job_id"] if running_jobs else jobs[0]["job_id"]
        selected_job_id = st.selectbox(
            "조회할 작업 선택",
            options=[job["job_id"] for job in jobs],
            index=[job["job_id"] for job in jobs].index(default_job_id),
            format_func=lambda job_id: format_job_label(job_map[job_id]),
            key="collect_log_job_id",
        )
        selected_job = job_map[selected_job_id]
        logs = repo.get_job_logs(selected_job_id, limit=500)
        if logs:
            log_text = "\n".join(
                f"[{row['created_at'][11:19]}] {row['level']}: {row['message']}"
                for row in logs
            )
            st.code(log_text, language="text")
        else:
            st.caption("아직 기록된 로그가 없습니다.")

        auto_refresh = st.checkbox(
            "실행중 작업 자동 새로고침 (3초)",
            value=False,
            key="collect_log_autorefresh",
        )
        if auto_refresh and selected_job.get("status") == "running":
            time.sleep(3)
            st.rerun()
    else:
        st.caption("아직 실행된 작업이 없습니다.")

    st.markdown("### 최근 수집 작업")
    jobs = repo.list_recent_jobs(limit=8)
    if jobs:
        job_df = pd.DataFrame(jobs)
        job_df["started_at"] = job_df["started_at"].str.slice(0, 19)
        job_df["finished_at"] = job_df["finished_at"].fillna("-").str.slice(0, 19)
        st.dataframe(
            job_df[
                [
                    "job_id",
                    "status",
                    "started_at",
                    "finished_at",
                    "total_candidates",
                    "kept_candidates",
                    "transcript_success",
                    "transcript_fail",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("아직 실행된 수집 작업이 없습니다.")


def render_raw_data_tab(repo: NasolRepository) -> None:
    st.markdown("### Raw Data 대시보드")
    available_seasons = repo.get_available_seasons()
    if not available_seasons:
        st.info("수집된 데이터가 없습니다. 먼저 수집 탭에서 작업을 실행하세요.")
        return

    default_seasons = available_seasons[-3:] if len(available_seasons) >= 3 else available_seasons
    selected_seasons = st.multiselect(
        "기수 필터",
        options=available_seasons,
        default=default_seasons,
        key="raw_filter_seasons",
    )
    transcript_filter = st.radio(
        "대본 상태",
        options=["전체", "대본 있음", "대본 없음"],
        horizontal=True,
        key="raw_transcript_filter",
    )
    main_only = st.checkbox("본편만 보기 (나솔사계/지볶행 제외)", value=True, key="raw_main_only")
    transcript_only: bool | None = None
    if transcript_filter == "대본 있음":
        transcript_only = True
    elif transcript_filter == "대본 없음":
        transcript_only = False
    auto_refresh_raw = st.checkbox(
        "수집중 자동 새로고침 (3초)",
        value=False,
        key="raw_auto_refresh",
    )

    videos = repo.get_videos(
        seasons=selected_seasons,
        transcript_only=transcript_only,
        main_only=main_only,
        limit=3000,
    )
    if not videos:
        if main_only:
            st.warning(
                "본편만 보기 조건에서 데이터가 없습니다. "
                "`본편만 보기`를 잠시 해제해서 현재 저장된 데이터 상태를 확인해보세요."
            )
        else:
            st.warning("조건에 맞는 데이터가 없습니다.")
        return

    total_count = len(videos)
    transcript_count = sum(1 for video in videos if video.get("transcript_status") == "success")
    avg_engagement = (
        sum(
            (
                (video.get("comment_count") or 0) / (video.get("view_count") or 1)
                if (video.get("view_count") or 0) > 0
                else 0
            )
            for video in videos
        )
        / total_count
    )

    m1, m2, m3 = st.columns(3)
    m1.metric("영상 수", f"{total_count:,}")
    m2.metric("대본 성공", f"{transcript_count:,}")
    m3.metric("평균 댓글비율", f"{avg_engagement * 100:.2f}%")

    table_rows = []
    for video in videos:
        table_rows.append(
            {
                "기수": video.get("season"),
                "회차": video.get("round_number") or video.get("episode"),
                "에피소드": video.get("episode_in_round"),
                "업로드일": video.get("upload_date"),
                "제목": video.get("title"),
                "채널": video.get("channel_title"),
                "조회수": video.get("view_count"),
                "댓글수": video.get("comment_count"),
                "수집경로": video.get("source"),
                "대본상태": video.get("transcript_status"),
                "_video_id": video.get("video_id"),
            }
        )
    table_df = pd.DataFrame(table_rows)
    display_df = table_df.drop(columns=["_video_id"])

    st.caption("행을 클릭하면 바로 아래 Transcript Raw Text가 열립니다.")
    table_event = st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=360,
        on_select="rerun",
        selection_mode="single-row",
        key="raw_data_table",
    )

    selected_video_id = st.session_state.get("raw_selected_video_id")
    selected_rows: list[int] = []
    try:
        selected_rows = list(table_event.selection.rows)
    except Exception:
        selected_rows = []
    if selected_rows:
        selected_video_id = table_df.iloc[selected_rows[0]]["_video_id"]
        st.session_state["raw_selected_video_id"] = selected_video_id
    elif not selected_video_id and not table_df.empty:
        selected_video_id = table_df.iloc[0]["_video_id"]
        st.session_state["raw_selected_video_id"] = selected_video_id

    selected_video = repo.get_video(selected_video_id) if selected_video_id else None
    if not selected_video:
        return

    st.markdown(
        f"""
        <div class="result-card">
            <div class="result-title">{selected_video.get('title')}</div>
            <div class="result-meta">
                {selected_video.get('season')}기 {format_round(selected_video.get('round_number') or selected_video.get('episode'))}
                / {selected_video.get('episode_in_round') or '?'}에피소드 |
                업로드 {selected_video.get('upload_date')} |
                조회수 {int(selected_video.get('view_count') or 0):,} |
                댓글수 {int(selected_video.get('comment_count') or 0):,}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    transcript_text = selected_video.get("transcript_text") or ""
    if transcript_text:
        st.text_area(
            "Transcript Raw Text",
            value=transcript_text,
            height=250,
            key=f"raw_text_{selected_video['video_id']}",
        )
    else:
        st.info("이 영상은 현재 대본이 없습니다.")

    segments_raw = selected_video.get("transcript_segments")
    if segments_raw:
        try:
            segments = json.loads(segments_raw)
        except json.JSONDecodeError:
            segments = []
        if segments:
            segment_rows = [
                {
                    "start_sec": round(float(segment.get("start", 0.0)), 2),
                    "duration_sec": round(float(segment.get("duration", 0.0)), 2),
                    "text": segment.get("text", ""),
                }
                for segment in segments
            ]
            st.dataframe(pd.DataFrame(segment_rows), use_container_width=True, height=260, hide_index=True)

    running_jobs = repo.list_recent_jobs(limit=1, status="running")
    if auto_refresh_raw and running_jobs:
        time.sleep(3)
        st.rerun()


def render_analysis_items(items: list[dict[str, Any]], title: str, key_prefix: str) -> None:
    st.markdown(f"#### {title}")
    seasons = sorted({int(item.get("season")) for item in items if item.get("season") is not None})
    if not seasons:
        st.info("표시할 결과가 없습니다.")
        return

    filter_key = f"{key_prefix}_season_filter"
    selected = st.multiselect(
        "기수 필터",
        options=seasons,
        default=seasons,
        key=filter_key,
    )
    filtered_items = [item for item in items if int(item.get("season") or 0) in selected]

    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in filtered_items:
        season = int(row.get("season") or 0)
        grouped.setdefault(season, []).append(row)

    for season in sorted(grouped):
        st.markdown(f"**{season}기**")
        for row in grouped[season]:
            round_label = format_round(row.get("episode"))
            st.markdown(
                f"""
                <div class="result-card">
                    <div class="result-title">{round_label} | {row.get("title")}</div>
                    <div class="result-meta">
                        점수 {float(row.get("score") or 0):.2f} | {row.get("reason")}<br/>
                        조회수 {int(row.get("view_count") or 0):,} / 댓글수 {int(row.get("comment_count") or 0):,}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_codex_queue_mode(repo: NasolRepository) -> None:
    left, right = st.columns([1, 2.2], gap="large")

    with left:
        st.markdown("#### Codex Jobs")
        jobs = repo.list_codex_jobs(limit=30, job_kind="analysis")
        if not jobs:
            st.caption("아직 Codex 분석 요청이 없습니다.")
        for job in jobs:
            label = f"#{job['id']} [{job['status']}] {job['query'][:16]}"
            if st.button(label, key=f"codex_job_{job['id']}", use_container_width=True):
                st.session_state["selected_codex_job_id"] = job["id"]

    with right:
        available_seasons = repo.get_available_seasons()
        selected_seasons = st.multiselect(
            "분석 대상 기수",
            options=available_seasons,
            default=available_seasons[-2:] if len(available_seasons) >= 2 else available_seasons,
            key="analysis_seasons_codex",
        )
        st.caption(
            "요청을 등록하면 Codex 큐에 저장됩니다. "
            "Codex가 처리 후 결과를 다시 이 화면에서 확인합니다."
        )

        prompt = st.chat_input("예: 10~11기 갈등 흐름을 회차별로 정리해줘")
        if prompt:
            job_id = repo.create_codex_job(prompt, selected_seasons, job_kind="analysis")
            st.session_state["selected_codex_job_id"] = job_id
            st.toast(f"Codex 분석 요청 등록 완료 (#{job_id})")
            st.rerun()

        jobs = repo.list_codex_jobs(limit=50, job_kind="analysis")
        if not jobs:
            return

        selected_job_id = st.session_state.get("selected_codex_job_id")
        valid_ids = {job["id"] for job in jobs}
        if selected_job_id is None or int(selected_job_id) not in valid_ids:
            selected_job_id = jobs[0]["id"]
            st.session_state["selected_codex_job_id"] = selected_job_id

        selected_job = repo.get_codex_job(int(selected_job_id))
        if not selected_job:
            st.warning("선택한 작업을 찾을 수 없습니다.")
            return

        st.markdown("#### 선택된 작업")
        st.markdown(
            f"""
            <div class="result-card">
                <div class="result-title">#{selected_job['id']} | {selected_job['status']}</div>
                <div class="result-meta">
                    기수: {', '.join(str(s) for s in selected_job['seasons']) or '전체'}<br/>
                    요청: {selected_job['query']}<br/>
                    생성: {(selected_job.get('created_at') or '')[:19]}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if selected_job["status"] == "completed" and selected_job.get("result_text"):
            st.markdown("#### Codex 결과")
            st.markdown(selected_job["result_text"])
        elif selected_job["status"] == "failed":
            st.error(selected_job.get("error_message") or "Codex 처리 실패")
        else:
            st.info(
                "아직 처리 전입니다. Codex에게 아래 순서로 요청하세요:\n"
                f"1) `python3 -m nasol.codex_queue packet --job-id {selected_job['id']} "
                f"--output-dir /tmp/codex_job_{selected_job['id']}`\n"
                "2) `/tmp/codex_job_<id>/episodes` 파일을 에피소드별로 읽고 사건/핵심인물/서사를 정리\n"
                f"3) 결과를 `/tmp/codex_job_{selected_job['id']}_result.md`로 작성\n"
                f"4) `python3 -m nasol.codex_queue complete --job-id {selected_job['id']} "
                f"--result-file /tmp/codex_job_{selected_job['id']}_result.md`"
            )

        auto_refresh = st.checkbox(
            "Codex 작업 자동 새로고침 (3초)",
            value=False,
            key="codex_job_autorefresh",
        )
        if auto_refresh and selected_job["status"] in {"pending", "running"}:
            time.sleep(3)
            st.rerun()


def render_summary_tab(repo: NasolRepository) -> None:
    st.markdown("### 요약 및 정리")
    available_seasons = repo.get_available_seasons()
    if not available_seasons:
        st.info("요약할 대본 데이터가 없습니다. 먼저 수집 탭에서 대본을 수집해주세요.")
        return

    left, right = st.columns([1, 2.3], gap="large")

    with left:
        st.markdown("#### Summary Jobs")
        jobs = repo.list_codex_jobs(limit=40, job_kind="summary")
        if not jobs:
            st.caption("아직 요약 요청이 없습니다.")
        for job in jobs:
            label = f"#{job['id']} [{job['status']}] {format_season_label(job['seasons'])}"
            if st.button(label, key=f"summary_job_{job['id']}", use_container_width=True):
                st.session_state["selected_summary_job_id"] = job["id"]

    with right:
        default_seasons = available_seasons[-2:] if len(available_seasons) >= 2 else available_seasons
        selected_seasons = st.multiselect(
            "요약 대상 기수",
            options=available_seasons,
            default=default_seasons,
            key="summary_target_seasons",
        )
        st.caption(
            "선택한 기수의 본편 대본 전체를 Codex 협업 큐로 요약합니다. "
            "결과는 에피소드별 핵심 줄거리 + 링크 형태로 시각화됩니다."
        )

        if st.button("요약 요청 등록", type="primary", use_container_width=True, key="create_summary_job"):
            if not selected_seasons:
                st.error("최소 1개 기수를 선택해주세요.")
            else:
                prompt = build_summary_query(selected_seasons)
                job_id = repo.create_codex_job(prompt, selected_seasons, job_kind="summary")
                st.session_state["selected_summary_job_id"] = job_id
                st.toast(f"요약 요청 등록 완료 (#{job_id})")
                st.rerun()

        jobs = repo.list_codex_jobs(limit=80, job_kind="summary")
        if not jobs:
            return

        selected_job_id = st.session_state.get("selected_summary_job_id")
        valid_ids = {job["id"] for job in jobs}
        if selected_job_id is None or int(selected_job_id) not in valid_ids:
            selected_job_id = jobs[0]["id"]
            st.session_state["selected_summary_job_id"] = selected_job_id

        selected_job = repo.get_codex_job(int(selected_job_id))
        if not selected_job:
            st.warning("선택한 요약 작업을 찾을 수 없습니다.")
            return

        st.markdown("#### 선택된 요약 작업")
        st.markdown(
            f"""
            <div class="result-card">
                <div class="result-title">#{selected_job['id']} | {selected_job['status']}</div>
                <div class="result-meta">
                    작업: 요약(summary)<br/>
                    기수: {', '.join(str(s) for s in selected_job['seasons']) or '전체'}<br/>
                    요청: {selected_job['query']}<br/>
                    생성: {(selected_job.get('created_at') or '')[:19]}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        delete_col, confirm_col = st.columns([1.1, 2.3], gap="small")
        with delete_col:
            delete_clicked = st.button(
                "선택 Summary Job 삭제",
                use_container_width=True,
                key=f"summary_delete_btn_{selected_job['id']}",
            )
        with confirm_col:
            delete_confirm = st.checkbox(
                "삭제 확인",
                value=False,
                key=f"summary_delete_confirm_{selected_job['id']}",
            )
        if delete_clicked:
            if selected_job.get("status") == "running":
                st.error("실행중 작업은 삭제할 수 없습니다.")
            elif not delete_confirm:
                st.warning("삭제 확인 체크 후 다시 눌러주세요.")
            else:
                deleted = repo.delete_codex_job(int(selected_job["id"]))
                if deleted:
                    st.toast(f"Summary Job #{selected_job['id']} 삭제 완료")
                    st.session_state["selected_summary_job_id"] = None
                    st.rerun()
                else:
                    st.error("작업 삭제에 실패했습니다.")

        if selected_job["status"] == "completed" and selected_job.get("result_text"):
            items = parse_summary_result_markdown(selected_job.get("result_text") or "")
            if not items:
                st.warning("요약 결과 파싱에 실패했습니다. 아래 원문 결과를 확인해주세요.")
                st.markdown(selected_job["result_text"])
            else:
                st.markdown("#### 에피소드 요약 시각화")
                season_options = sorted({item["season"] for item in items})
                selected_filter = st.multiselect(
                    "기수 필터",
                    options=season_options,
                    default=season_options,
                    key=f"summary_result_filter_{selected_job['id']}",
                )
                filtered = [item for item in items if item["season"] in selected_filter]
                m1, m2 = st.columns(2)
                m1.metric("요약 에피소드 수", f"{len(filtered):,}")
                m2.metric("전체 에피소드 수", f"{len(items):,}")

                for item in filtered:
                    round_label = f"{item['round']}회차" if item["round"] else "회차 미확정"
                    episode_label = f"{item['episode']}에피소드" if item["episode"] else "에피소드 미확정"
                    title = item.get("title") or "(제목 없음)"
                    key_people = item.get("key_people") or "-"
                    one_line = item.get("one_line") or "-"
                    summary = item.get("summary") or "-"
                    chunk_storyline = item.get("chunk_storyline") or []
                    key_incidents = item.get("key_incidents") or []
                    highlights = item.get("highlights") or []
                    evidence_links = item.get("evidence_links") or []
                    youtube_url = item.get("youtube_url") or ""
                    st.markdown(
                        f"""
                        <div class="result-card">
                            <div class="result-title">
                                {item['season']}기 {round_label} / {episode_label}
                            </div>
                            <div class="result-meta">
                                {title}<br/>
                                핵심 인물: {key_people}<br/>
                                한 줄 요약: {one_line}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    if youtube_url:
                        st.markdown(f"[유튜브 바로가기]({youtube_url})")
                    st.markdown(f"**요약**: {summary}")
                    if chunk_storyline:
                        st.markdown("**Chunk 흐름 요약**")
                        for point in chunk_storyline:
                            st.markdown(f"- {point}")
                    elif highlights:
                        st.markdown("**핵심 포인트**")
                        for point in highlights:
                            st.markdown(f"- {point}")
                    if key_incidents:
                        st.markdown("**핵심 사건**")
                        for incident in key_incidents:
                            st.markdown(f"- {incident}")
                    if evidence_links:
                        st.markdown("**근거 링크**")
                        for link in evidence_links:
                            if link.startswith("http"):
                                st.markdown(f"- [링크]({link})")
                            else:
                                st.markdown(f"- {link}")

                with st.expander("요약 결과 원문 보기"):
                    st.markdown(selected_job["result_text"])
        elif selected_job["status"] == "failed":
            st.error(selected_job.get("error_message") or "요약 처리 실패")
        else:
            st.info(
                "아직 처리 전입니다. Codex에게 아래 순서로 요청하세요:\n"
                f"1) `python3 -m nasol.codex_queue packet --job-id {selected_job['id']} "
                f"--output-dir /tmp/codex_summary_job_{selected_job['id']} --max-videos 3000 --chunk-chars 1200`\n"
                "2) `/tmp/codex_summary_job_<id>/episodes`를 에피소드별로 읽고 chunk별 사건을 먼저 정리\n"
                "3) chunk 정리를 이어붙여 에피소드 서사(summary)를 작성\n"
                "4) `result_template.md`의 `EPISODE|...` 형식과 `chunk_storyline/key_incidents/evidence_links`를 반드시 채움\n"
                "5) 이름 표기는 캐스트 기준(영수/영호/영식/영철/광수/상철/영숙/정숙/순자/영자/옥순/현숙/경수/정희/정수/정식)으로 보정\n"
                f"6) 결과를 `/tmp/codex_summary_job_{selected_job['id']}_result.md`로 저장\n"
                f"7) `python3 -m nasol.codex_queue complete --job-id {selected_job['id']} "
                f"--result-file /tmp/codex_summary_job_{selected_job['id']}_result.md`"
            )

        auto_refresh = st.checkbox(
            "요약 작업 자동 새로고침 (3초)",
            value=False,
            key="summary_job_autorefresh",
        )
        if auto_refresh and selected_job["status"] in {"pending", "running"}:
            time.sleep(3)
            st.rerun()


def render_analysis_tab(repo: NasolRepository, analyst: NasolAnalyst) -> None:
    st.markdown("### 분석")
    mode = st.radio(
        "분석 엔진",
        options=["빠른 규칙 분석", "Codex 협업 큐"],
        horizontal=True,
        key="analysis_engine_mode",
    )

    if mode == "Codex 협업 큐":
        render_codex_queue_mode(repo)
        return

    left, right = st.columns([1, 2.2], gap="large")

    with left:
        st.markdown("#### Saved Views")
        views = repo.list_analysis_views(limit=20)
        if not views:
            st.caption("아직 저장된 분석 View가 없습니다.")
        for view in views:
            label = f"{view['name']}"
            if st.button(label, key=f"view_{view['id']}", use_container_width=True):
                st.session_state["selected_view_id"] = view["id"]

    with right:
        available_seasons = repo.get_available_seasons()
        selected_seasons = st.multiselect(
            "분석 대상 기수",
            options=available_seasons,
            default=available_seasons[-2:] if len(available_seasons) >= 2 else available_seasons,
            key="analysis_seasons",
        )

        if "analysis_messages" not in st.session_state:
            st.session_state["analysis_messages"] = []
        if "analysis_last_items" not in st.session_state:
            st.session_state["analysis_last_items"] = []
        if "selected_view_id" not in st.session_state:
            st.session_state["selected_view_id"] = None

        for message in st.session_state["analysis_messages"]:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        prompt = st.chat_input("예: 10기~11기에 대해 빌런 에피소드만 모아줘")
        if prompt:
            st.session_state["analysis_messages"].append({"role": "user", "content": prompt})
            result = analyst.answer(prompt, selected_seasons)
            st.session_state["analysis_messages"].append(
                {"role": "assistant", "content": result["response"]}
            )
            st.session_state["analysis_last_items"] = result["items"]
            if result.get("view_id"):
                st.session_state["selected_view_id"] = result["view_id"]
            st.rerun()

        last_items = st.session_state.get("analysis_last_items") or []
        if last_items:
            render_analysis_items(last_items, "최근 분석 결과", "last_result")

        selected_view_id = st.session_state.get("selected_view_id")
        if selected_view_id:
            view, items = repo.get_analysis_view(int(selected_view_id))
            if view:
                render_analysis_items(items, f"저장된 View: {view['name']}", f"saved_view_{view['id']}")


def main() -> None:
    st.set_page_config(
        page_title="NASOL Transcript Studio",
        page_icon="📺",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    inject_styles()

    repo = NasolRepository("output/nasol.db")
    collector = NasolCollector(repo, CollectorConfig())
    analyst = NasolAnalyst(repo)

    st.markdown(
        """
        <div class="title-card">
            <h2 style="margin:0 0 6px 0;">NASOL Transcript Studio</h2>
            <p style="margin:0;color:#334155;">
                기수별 영상 수집, Raw 대본 가시화, 분석 View 저장을 한 번에 처리합니다.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    tab_collect, tab_raw, tab_summary, tab_analysis = st.tabs(["수집", "Raw Data", "요약 및 정리", "분석"])

    with tab_collect:
        render_collection_tab(repo, collector)
    with tab_raw:
        render_raw_data_tab(repo)
    with tab_summary:
        render_summary_tab(repo)
    with tab_analysis:
        render_analysis_tab(repo, analyst)


if __name__ == "__main__":
    main()
