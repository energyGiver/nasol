#!/usr/bin/env python3
"""
나는솔로 YouTube 영상 스크래퍼
- yt-dlp로 영상 검색 및 메타데이터 수집
- youtube-transcript-api로 대본(자막) 다운로드
- 조회수 상위 50개 영상 선별
"""

import os
import json
import time
import subprocess
import sys
from pathlib import Path
from datetime import datetime

from config import (
    SEARCH_QUERIES,
    TARGET_VIDEO_COUNT,
    MAX_RESULTS_PER_QUERY,
    OUTPUT_DIR,
    TRANSCRIPT_LANGUAGES,
    REQUEST_DELAY,
)


def check_dependencies():
    """필요한 패키지 확인 및 설치"""
    packages = {
        "yt_dlp": "yt-dlp",
        "youtube_transcript_api": "youtube-transcript-api",
        "pandas": "pandas",
        "tqdm": "tqdm",
    }
    missing = []
    for module, pkg in packages.items():
        try:
            __import__(module)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"[설치] 필요한 패키지 설치 중: {', '.join(missing)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
        print("[완료] 패키지 설치 완료\n")


check_dependencies()

import yt_dlp
import pandas as pd
from tqdm import tqdm
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled


def search_videos(query: str, max_results: int) -> list[dict]:
    """yt-dlp로 YouTube 영상 검색 및 메타데이터 수집"""
    results = []
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "ignoreerrors": True,
    }

    search_url = f"ytsearch{max_results}:{query}"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(search_url, download=False)
            if info and "entries" in info:
                for entry in info["entries"]:
                    if entry is None:
                        continue
                    results.append({
                        "video_id": entry.get("id", ""),
                        "title": entry.get("title", ""),
                        "url": f"https://www.youtube.com/watch?v={entry.get('id', '')}",
                        "view_count": entry.get("view_count") or 0,
                        "duration": entry.get("duration") or 0,
                        "channel": entry.get("channel") or entry.get("uploader", ""),
                        "upload_date": entry.get("upload_date", ""),
                        "description": (entry.get("description") or "")[:500],
                    })
    except Exception as e:
        print(f"  [경고] '{query}' 검색 오류: {e}")

    return results


def get_video_details(video_ids: list[str]) -> dict[str, dict]:
    """yt-dlp로 개별 영상의 정확한 조회수 등 상세 정보 수집"""
    details = {}
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
    }

    print(f"\n[2단계] 상위 후보 영상 상세 정보 수집 중... ({len(video_ids)}개)")
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        for vid_id in tqdm(video_ids, desc="  상세 정보"):
            try:
                url = f"https://www.youtube.com/watch?v={vid_id}"
                info = ydl.extract_info(url, download=False)
                if info:
                    details[vid_id] = {
                        "video_id": vid_id,
                        "title": info.get("title", ""),
                        "url": url,
                        "view_count": info.get("view_count") or 0,
                        "like_count": info.get("like_count") or 0,
                        "comment_count": info.get("comment_count") or 0,
                        "duration": info.get("duration") or 0,
                        "duration_string": info.get("duration_string", ""),
                        "channel": info.get("channel") or info.get("uploader", ""),
                        "channel_id": info.get("channel_id", ""),
                        "channel_url": info.get("channel_url", ""),
                        "upload_date": info.get("upload_date", ""),
                        "description": (info.get("description") or "")[:1000],
                        "tags": info.get("tags") or [],
                        "categories": info.get("categories") or [],
                        "thumbnail": info.get("thumbnail", ""),
                    }
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                print(f"  [경고] {vid_id} 상세 정보 오류: {e}")

    return details


def get_transcript(video_id: str) -> dict:
    """youtube-transcript-api v1.x로 자막(대본) 다운로드"""
    result = {
        "has_transcript": False,
        "language": "",
        "transcript_type": "",  # manual / auto
        "transcript_text": "",
        "transcript_segments": [],
    }

    try:
        api = YouTubeTranscriptApi()
        transcript_list = api.list(video_id)

        chosen = None
        chosen_type = ""

        # 언어별 우선순위로 자막 선택
        for tr in transcript_list:
            lang = tr.language_code
            is_auto = tr.is_generated

            if lang in ("ko", "ko-KR") and not is_auto:
                chosen = tr
                chosen_type = "manual"
                break
            if lang in ("ko", "ko-KR") and chosen is None:
                chosen = tr
                chosen_type = "auto"
            if lang in ("en", "en-US") and chosen is None:
                chosen = tr
                chosen_type = "auto" if is_auto else "manual"

        # 아무것도 없으면 첫 번째 자막 사용
        if chosen is None:
            all_tr = list(transcript_list)
            if all_tr:
                chosen = all_tr[0]
                chosen_type = "auto" if chosen.is_generated else "manual"

        if chosen:
            fetched = chosen.fetch()
            result["has_transcript"] = True
            result["language"] = chosen.language_code
            result["transcript_type"] = chosen_type
            result["transcript_segments"] = [
                {
                    "start": getattr(seg, "start", 0),
                    "duration": getattr(seg, "duration", 0),
                    "text": getattr(seg, "text", "").strip(),
                }
                for seg in fetched
            ]
            result["transcript_text"] = "\n".join(
                seg["text"] for seg in result["transcript_segments"] if seg["text"]
            )

    except NoTranscriptFound:
        result["transcript_type"] = "not_found"
    except TranscriptsDisabled:
        result["transcript_type"] = "disabled"
    except Exception as e:
        result["transcript_type"] = f"error: {str(e)[:100]}"

    return result


def save_results(videos: list[dict], output_dir: Path):
    """수집 결과를 JSON, CSV, 개별 텍스트 파일로 저장"""
    output_dir.mkdir(parents=True, exist_ok=True)
    transcripts_dir = output_dir / "transcripts"
    transcripts_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. 전체 JSON 저장
    json_path = output_dir / f"nasol_top50_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(videos, f, ensure_ascii=False, indent=2)
    print(f"  [저장] JSON: {json_path}")

    # 2. CSV 저장 (대본 제외한 메타데이터)
    rows = []
    for v in videos:
        rows.append({
            "rank": v.get("rank"),
            "video_id": v.get("video_id"),
            "title": v.get("title"),
            "url": v.get("url"),
            "view_count": v.get("view_count"),
            "like_count": v.get("like_count"),
            "comment_count": v.get("comment_count"),
            "duration_string": v.get("duration_string"),
            "channel": v.get("channel"),
            "upload_date": v.get("upload_date"),
            "has_transcript": v.get("has_transcript"),
            "transcript_language": v.get("language"),
            "transcript_type": v.get("transcript_type"),
            "transcript_length": len(v.get("transcript_text", "")),
        })
    df = pd.DataFrame(rows)
    csv_path = output_dir / f"nasol_top50_{timestamp}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  [저장] CSV: {csv_path}")

    # 3. 개별 대본 텍스트 파일 저장
    saved_transcripts = 0
    for v in videos:
        if v.get("has_transcript") and v.get("transcript_text"):
            safe_title = "".join(
                c for c in v.get("title", "")[:50] if c.isalnum() or c in " _-()[]"
            ).strip()
            filename = f"{v['rank']:02d}_{v['video_id']}_{safe_title}.txt"
            txt_path = transcripts_dir / filename
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(f"제목: {v.get('title', '')}\n")
                f.write(f"URL: {v.get('url', '')}\n")
                f.write(f"채널: {v.get('channel', '')}\n")
                f.write(f"업로드: {v.get('upload_date', '')}\n")
                f.write(f"조회수: {v.get('view_count', 0):,}\n")
                f.write(f"자막 언어: {v.get('language', '')}\n")
                f.write(f"자막 유형: {v.get('transcript_type', '')}\n")
                f.write("=" * 60 + "\n\n")
                f.write(v.get("transcript_text", ""))
            saved_transcripts += 1

    print(f"  [저장] 대본 텍스트: {saved_transcripts}개 → {transcripts_dir}/")

    return json_path, csv_path


def main():
    print("=" * 60)
    print("  나는솔로 YouTube 스크래퍼")
    print("  조회수 상위 50개 영상 + 대본 수집")
    print("=" * 60)

    # 스크립트 위치 기준 output 디렉토리
    script_dir = Path(__file__).parent
    output_dir = script_dir / OUTPUT_DIR

    # ─── 1단계: 여러 키워드로 영상 검색 ─────────────────────
    print(f"\n[1단계] YouTube 검색 중... (키워드 {len(SEARCH_QUERIES)}개)")
    all_videos: dict[str, dict] = {}

    for query in SEARCH_QUERIES:
        print(f"  검색: '{query}'")
        results = search_videos(query, MAX_RESULTS_PER_QUERY)
        for v in results:
            vid_id = v.get("video_id", "")
            if vid_id and vid_id not in all_videos:
                all_videos[vid_id] = v
        print(f"  → {len(results)}개 발견 (누적 고유 영상: {len(all_videos)}개)")
        time.sleep(REQUEST_DELAY)

    print(f"\n  총 고유 영상: {len(all_videos)}개")

    # 조회수 기준 상위 후보 추려서 상세 정보 수집 (API 호출 최소화)
    # 검색 결과의 조회수로 1차 정렬 후 상위 150개만 상세 조회
    candidates = sorted(
        all_videos.values(),
        key=lambda x: x.get("view_count", 0),
        reverse=True
    )[:min(150, len(all_videos))]

    candidate_ids = [v["video_id"] for v in candidates if v.get("video_id")]

    # ─── 2단계: 상세 정보 수집 ───────────────────────────────
    details = get_video_details(candidate_ids)

    # 조회수 기준 상위 50개 선별
    sorted_videos = sorted(
        details.values(),
        key=lambda x: x.get("view_count", 0),
        reverse=True
    )[:TARGET_VIDEO_COUNT]

    print(f"\n  ✓ 최종 상위 {len(sorted_videos)}개 선별 완료")

    # ─── 3단계: 대본(자막) 수집 ─────────────────────────────
    print(f"\n[3단계] 대본(자막) 다운로드 중...")
    final_videos = []

    for rank, video in enumerate(tqdm(sorted_videos, desc="  대본 수집"), start=1):
        vid_id = video["video_id"]
        transcript_data = get_transcript(vid_id)

        video.update(transcript_data)
        video["rank"] = rank

        status = "✓" if transcript_data["has_transcript"] else "✗"
        lang = transcript_data.get("language", transcript_data.get("transcript_type", ""))
        tqdm.write(
            f"  [{rank:2d}] {status} {video['title'][:45]:<45} "
            f"조회수:{video['view_count']:>12,}  자막:{lang}"
        )

        final_videos.append(video)
        time.sleep(REQUEST_DELAY)

    # ─── 4단계: 저장 ─────────────────────────────────────────
    print(f"\n[4단계] 결과 저장 중...")
    json_path, csv_path = save_results(final_videos, output_dir)

    # ─── 요약 ─────────────────────────────────────────────────
    has_transcript = sum(1 for v in final_videos if v.get("has_transcript"))
    print(f"""
{'=' * 60}
  완료!
  수집 영상: {len(final_videos)}개
  대본 보유: {has_transcript}개 / {len(final_videos)}개
  저장 위치: {output_dir.resolve()}/
{'=' * 60}
""")


if __name__ == "__main__":
    main()
