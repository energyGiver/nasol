#!/usr/bin/env python3
"""
기존에 수집된 영상 목록(JSON)을 불러와서 대본(자막)만 다시 수집
이미 scraper.py를 실행했다면 이 스크립트로 대본만 빠르게 보완 가능
"""

import json
import time
import glob
from pathlib import Path
from datetime import datetime

from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
REQUEST_DELAY = 1.0


def get_transcript(video_id: str) -> dict:
    result = {
        "has_transcript": False,
        "language": "",
        "transcript_type": "",
        "transcript_text": "",
        "transcript_segments": [],
    }
    try:
        api = YouTubeTranscriptApi()
        transcript_list = api.list(video_id)

        chosen = None
        chosen_type = ""

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
                s["text"] for s in result["transcript_segments"] if s["text"]
            )
    except NoTranscriptFound:
        result["transcript_type"] = "not_found"
    except TranscriptsDisabled:
        result["transcript_type"] = "disabled"
    except Exception as e:
        result["transcript_type"] = f"error: {str(e)[:100]}"
    return result


def save_transcript_txt(video: dict, transcripts_dir: Path):
    if not video.get("has_transcript") or not video.get("transcript_text"):
        return
    safe_title = "".join(
        c for c in video.get("title", "")[:50] if c.isalnum() or c in " _-()[]"
    ).strip()
    filename = f"{video['rank']:02d}_{video['video_id']}_{safe_title}.txt"
    txt_path = transcripts_dir / filename
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"제목: {video.get('title', '')}\n")
        f.write(f"URL: {video.get('url', '')}\n")
        f.write(f"채널: {video.get('channel', '')}\n")
        f.write(f"업로드: {video.get('upload_date', '')}\n")
        f.write(f"조회수: {video.get('view_count', 0):,}\n")
        f.write(f"자막 언어: {video.get('language', '')}\n")
        f.write(f"자막 유형: {video.get('transcript_type', '')}\n")
        f.write("=" * 60 + "\n\n")
        f.write(video.get("transcript_text", ""))


def main():
    # 가장 최근 JSON 파일 로드
    json_files = sorted(glob.glob(str(OUTPUT_DIR / "nasol_top50_*.json")))
    if not json_files:
        print("[오류] output/ 디렉토리에 nasol_top50_*.json 파일이 없습니다.")
        print("  먼저 scraper.py를 실행해주세요.")
        return

    latest_json = json_files[-1]
    print(f"[로드] {latest_json}")

    with open(latest_json, encoding="utf-8") as f:
        videos = json.load(f)

    print(f"  총 {len(videos)}개 영상 로드 완료\n")

    transcripts_dir = OUTPUT_DIR / "transcripts"
    transcripts_dir.mkdir(parents=True, exist_ok=True)

    print("[대본 수집 시작]")
    success = 0

    for video in tqdm(videos, desc="  자막 다운로드"):
        vid_id = video["video_id"]
        transcript_data = get_transcript(vid_id)
        video.update(transcript_data)

        status = "✓" if transcript_data["has_transcript"] else "✗"
        lang = transcript_data.get("language") or transcript_data.get("transcript_type", "")
        tqdm.write(
            f"  [{video['rank']:2d}] {status} {video['title'][:45]:<45} 자막:{lang}"
        )

        if transcript_data["has_transcript"]:
            save_transcript_txt(video, transcripts_dir)
            success += 1

        time.sleep(REQUEST_DELAY)

    # 업데이트된 JSON 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUTPUT_DIR / f"nasol_top50_{timestamp}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(videos, f, ensure_ascii=False, indent=2)

    # CSV도 갱신
    try:
        import pandas as pd
        rows = [{
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
        } for v in videos]
        df = pd.DataFrame(rows)
        csv_path = OUTPUT_DIR / f"nasol_top50_{timestamp}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"\n  [저장] CSV: {csv_path}")
    except Exception:
        pass

    print(f"""
{'=' * 60}
  완료!
  대본 수집: {success}개 / {len(videos)}개
  저장 위치: {OUTPUT_DIR.resolve()}/
{'=' * 60}
""")


if __name__ == "__main__":
    main()
