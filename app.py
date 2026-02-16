from __future__ import annotations

import json
from datetime import datetime
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


def render_collection_tab(repo: NasolRepository, collector: NasolCollector) -> None:
    st.markdown("### 데이터 수집")
    st.caption(
        "공식 채널(@chonjang) 본편 우선 수집 후, 누락 기수만 일반 검색으로 보완합니다. "
        "지볶행/나솔사계/사랑은 계속된다 등 스핀오프는 제외합니다."
    )

    col_left, col_right = st.columns([2, 1], gap="large")
    with col_left:
        seasons = season_selector("collect")
        include_fallback = st.checkbox("공식 채널 누락 시 일반 검색 보완", value=True)
        dry_run = st.checkbox("Dry-run (영상 목록만 저장, 대본은 생략)", value=False)
        force_refresh = st.checkbox("기존 대본이 있어도 다시 수집", value=False)
    with col_right:
        st.markdown(
            """
            <div class="title-card">
                <span class="info-chip">중복 방지</span>
                <span class="info-chip">시간순 정렬</span>
                <span class="info-chip">무료 수집</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    run_clicked = st.button("수집 시작", use_container_width=True, type="primary")
    log_placeholder = st.empty()
    summary_placeholder = st.empty()

    if run_clicked:
        if not seasons:
            st.error("최소 1개 기수를 선택해주세요.")
            return

        logs: list[str] = []

        def append_log(message: str) -> None:
            now = datetime.now().strftime("%H:%M:%S")
            logs.append(f"[{now}] {message}")
            log_placeholder.code("\n".join(logs[-120:]), language="text")

        with st.spinner("수집 작업을 실행 중입니다..."):
            summary = collector.collect(
                seasons=seasons,
                include_fallback_search=include_fallback,
                dry_run=dry_run,
                force_transcript_refresh=force_refresh,
                logger=append_log,
            )

        st.session_state["last_collection_summary"] = summary
        st.toast("수집 완료: Raw Data 탭에서 결과를 확인하세요.")

    summary = st.session_state.get("last_collection_summary")
    if summary:
        summary_placeholder.success(
            (
                f"완료 | 후보 {summary['total_candidates']}개 "
                f"-> 저장 {summary['saved_videos']}개 | "
                f"대본 성공 {summary['transcript_success']}개 / 실패 {summary['transcript_fail']}개"
            )
        )

        if summary["transcript_fail_reasons"]:
            st.warning(f"대본 실패 사유: {summary['transcript_fail_reasons']}")

        if summary["season_summary"]:
            st.dataframe(
                pd.DataFrame(summary["season_summary"]).rename(
                    columns={
                        "season": "기수",
                        "total_videos": "영상 수",
                        "transcript_success": "대본 성공",
                        "avg_engagement": "평균 댓글비율",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

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
    transcript_only: bool | None = None
    if transcript_filter == "대본 있음":
        transcript_only = True
    elif transcript_filter == "대본 없음":
        transcript_only = False

    videos = repo.get_videos(
        seasons=selected_seasons,
        transcript_only=transcript_only,
        main_only=True,
        limit=3000,
    )
    if not videos:
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


def render_analysis_tab(repo: NasolRepository, analyst: NasolAnalyst) -> None:
    st.markdown("### 분석")
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

    tab_collect, tab_raw, tab_analysis = st.tabs(["수집", "Raw Data", "분석"])

    with tab_collect:
        render_collection_tab(repo, collector)
    with tab_raw:
        render_raw_data_tab(repo)
    with tab_analysis:
        render_analysis_tab(repo, analyst)


if __name__ == "__main__":
    main()
