from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import datetime
from typing import Any

from nasol.parsing import parse_season_numbers
from nasol.storage import NasolRepository

VILLAIN_HINTS = ("빌런", "갈등", "논란", "싸움", "다툼", "충돌", "자극")
HOT_HINTS = ("화제", "핫", "인기", "조회수", "댓글", "역대급")

VILLAIN_KEYWORDS = (
    "빌런",
    "싸움",
    "갈등",
    "분노",
    "눈물",
    "오열",
    "논란",
    "충격",
    "언쟁",
    "분위기",
    "불편",
    "폭발",
    "격해",
)


class NasolAnalyst:
    def __init__(self, repository: NasolRepository) -> None:
        self.repo = repository

    def answer(self, query: str, selected_seasons: list[int] | None = None) -> dict[str, Any]:
        available_seasons = self.repo.get_available_seasons()
        seasons = self._resolve_seasons(query, selected_seasons or [], available_seasons)
        videos = self.repo.get_videos(seasons=seasons, transcript_only=True)

        mode = self._detect_mode(query)
        if mode == "villain":
            result = self._build_villain_result(query, seasons, videos)
        elif mode == "hot":
            result = self._build_hot_result(query, seasons, videos)
        else:
            result = self._build_general_result(query, seasons, videos)

        self.repo.save_chat_exchange(query=query, seasons=seasons, response=result["response"])
        if result["items"] and result["save_view"]:
            view_id = self.repo.save_analysis_view(
                name=result["view_name"],
                view_type=result["view_type"],
                query=query,
                seasons=seasons,
                items=result["items"],
            )
            result["view_id"] = view_id
        else:
            result["view_id"] = None

        result["seasons"] = seasons
        result["mode"] = mode
        return result

    def _resolve_seasons(
        self,
        query: str,
        selected_seasons: list[int],
        available_seasons: list[int],
    ) -> list[int]:
        parsed_seasons = self._extract_query_seasons(query)
        if parsed_seasons and selected_seasons:
            intersection = sorted(set(parsed_seasons).intersection(selected_seasons))
            if intersection:
                return intersection
        if parsed_seasons:
            return sorted(parsed_seasons)
        if selected_seasons:
            return sorted(set(selected_seasons))
        return available_seasons

    def _extract_query_seasons(self, query: str) -> list[int]:
        seasons: set[int] = set(parse_season_numbers(query))
        for start, end in re.findall(r"(\d{1,2})\s*(?:기)?\s*[~\-]\s*(\d{1,2})\s*기", query):
            start_int = int(start)
            end_int = int(end)
            low, high = sorted((start_int, end_int))
            for season in range(low, high + 1):
                if 1 <= season <= 29:
                    seasons.add(season)
        return sorted(seasons)

    def _detect_mode(self, query: str) -> str:
        lowered = query.lower()
        if any(hint in lowered for hint in VILLAIN_HINTS):
            return "villain"
        if any(hint in lowered for hint in HOT_HINTS):
            return "hot"
        return "general"

    def _build_villain_result(
        self, query: str, seasons: list[int], videos: list[dict[str, Any]]
    ) -> dict[str, Any]:
        scored: list[dict[str, Any]] = []
        for video in videos:
            transcript_text = video.get("transcript_text") or ""
            title = video.get("title") or ""
            body = f"{title}\n{transcript_text[:12000]}".lower()
            keyword_hits = sum(body.count(keyword) for keyword in VILLAIN_KEYWORDS)
            engagement = self._engagement(video)
            score = keyword_hits * 2.2 + min(engagement * 12000, 15.0)
            if score < 2.5:
                continue
            scored.append(
                {
                    "video_id": video["video_id"],
                    "season": video.get("season"),
                    "episode": video.get("episode"),
                    "title": title,
                    "url": video.get("url"),
                    "upload_date": video.get("upload_date"),
                    "view_count": int(video.get("view_count") or 0),
                    "comment_count": int(video.get("comment_count") or 0),
                    "score": round(score, 3),
                    "reason": f"갈등 키워드 {keyword_hits}회, 댓글비율 {engagement * 100:.2f}%",
                }
            )

        scored.sort(key=lambda row: row["score"], reverse=True)
        top_items = scored[:30]
        response = self._render_grouped_response(
            header=f"{self._season_label(seasons)} 빌런/갈등 에피소드 후보",
            items=top_items,
        )
        if not top_items:
            response = "조건에 맞는 빌런/갈등 에피소드를 찾지 못했습니다. 기수 범위를 넓혀서 다시 시도해주세요."

        return {
            "response": response,
            "items": top_items,
            "save_view": True,
            "view_type": "villain",
            "view_name": f"빌런 에피소드 | {self._season_label(seasons)}",
        }

    def _build_hot_result(
        self, query: str, seasons: list[int], videos: list[dict[str, Any]]
    ) -> dict[str, Any]:
        ranked: list[dict[str, Any]] = []
        for video in videos:
            views = int(video.get("view_count") or 0)
            comments = int(video.get("comment_count") or 0)
            engagement = self._engagement(video)
            score = math.log10(views + 1) * 2.0 + math.log10(comments + 1) + engagement * 10000
            ranked.append(
                {
                    "video_id": video["video_id"],
                    "season": video.get("season"),
                    "episode": video.get("episode"),
                    "title": video.get("title"),
                    "url": video.get("url"),
                    "upload_date": video.get("upload_date"),
                    "view_count": views,
                    "comment_count": comments,
                    "score": round(score, 3),
                    "reason": f"조회수 {views:,}, 댓글수 {comments:,}, 댓글비율 {engagement * 100:.2f}%",
                }
            )
        ranked.sort(key=lambda row: row["score"], reverse=True)
        top_items = ranked[:30]
        response = self._render_grouped_response(
            header=f"{self._season_label(seasons)} 화제성 상위 영상",
            items=top_items,
        )
        if not top_items:
            response = "화제성 분석 대상이 없습니다. 먼저 수집 탭에서 대본 수집을 진행해주세요."

        return {
            "response": response,
            "items": top_items,
            "save_view": True,
            "view_type": "hot",
            "view_name": f"화제성 상위 | {self._season_label(seasons)}",
        }

    def _build_general_result(
        self, query: str, seasons: list[int], videos: list[dict[str, Any]]
    ) -> dict[str, Any]:
        tokens = self._tokenize(query)
        if not tokens:
            return {
                "response": "질문 키워드를 조금 더 구체적으로 입력해주세요. 예: `10기 영숙 갈등 장면`",
                "items": [],
                "save_view": False,
                "view_type": "general",
                "view_name": "",
            }

        matches: list[dict[str, Any]] = []
        for video in videos:
            title = (video.get("title") or "").lower()
            transcript = (video.get("transcript_text") or "").lower()
            haystack = f"{title}\n{transcript[:16000]}"
            token_score = sum(haystack.count(token) for token in tokens)
            if token_score <= 0:
                continue
            engagement = self._engagement(video)
            score = token_score + engagement * 2000
            snippet = self._snippet(video.get("transcript_text") or "", tokens)
            matches.append(
                {
                    "video_id": video["video_id"],
                    "season": video.get("season"),
                    "episode": video.get("episode"),
                    "title": video.get("title"),
                    "url": video.get("url"),
                    "upload_date": video.get("upload_date"),
                    "view_count": int(video.get("view_count") or 0),
                    "comment_count": int(video.get("comment_count") or 0),
                    "score": round(score, 3),
                    "reason": f"키워드 매칭 점수 {token_score}",
                    "snippet": snippet,
                }
            )

        matches.sort(key=lambda row: row["score"], reverse=True)
        top_items = matches[:15]

        if not top_items:
            return {
                "response": "해당 키워드로 매칭되는 대본이 없었습니다. 다른 표현으로 다시 요청해 주세요.",
                "items": [],
                "save_view": False,
                "view_type": "general",
                "view_name": "",
            }

        lines = [f"{self._season_label(seasons)} 검색 결과입니다."]
        for item in top_items[:8]:
            episode_label = f"EP{item['episode']}" if item.get("episode") else "EP?"
            lines.append(
                f"- {item['season']}기 {episode_label} | {item['title']} "
                f"(매칭:{item['reason'].replace('키워드 매칭 점수 ', '')})"
            )
            if item.get("snippet"):
                lines.append(f"  ↳ {item['snippet']}")

        return {
            "response": "\n".join(lines),
            "items": top_items,
            "save_view": False,
            "view_type": "general",
            "view_name": "",
        }

    def _render_grouped_response(self, header: str, items: list[dict[str, Any]]) -> str:
        if not items:
            return f"{header}\n- 결과 없음"

        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            season = int(item.get("season") or 0)
            grouped[season].append(item)

        lines = [header]
        for season in sorted(grouped):
            lines.append(f"\n{season}기")
            for row in grouped[season][:8]:
                episode = row.get("episode")
                episode_label = f"EP{episode}" if episode else "EP?"
                lines.append(
                    f"- {episode_label} | {row.get('title')} | {row.get('reason')}"
                )
        return "\n".join(lines)

    def _engagement(self, video: dict[str, Any]) -> float:
        views = int(video.get("view_count") or 0)
        comments = int(video.get("comment_count") or 0)
        if views <= 0:
            return 0.0
        return comments / views

    def _tokenize(self, text: str) -> list[str]:
        tokens = [token.lower() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text)]
        return [token for token in tokens if token not in {"나는", "솔로", "영상"}]

    def _snippet(self, transcript: str, tokens: list[str], max_len: int = 100) -> str:
        if not transcript:
            return ""
        lowered = transcript.lower()
        position = -1
        chosen = ""
        for token in tokens:
            index = lowered.find(token)
            if index >= 0 and (position == -1 or index < position):
                position = index
                chosen = token

        if position < 0:
            return transcript[:max_len].strip()

        start = max(position - 20, 0)
        end = min(position + max_len, len(transcript))
        snippet = transcript[start:end].replace("\n", " ").strip()
        if start > 0:
            snippet = f"...{snippet}"
        if end < len(transcript):
            snippet = f"{snippet}..."
        return snippet

    def _season_label(self, seasons: list[int]) -> str:
        if not seasons:
            return "전체 기수"
        if len(seasons) == 1:
            return f"{seasons[0]}기"
        return f"{seasons[0]}기~{seasons[-1]}기"

    def default_view_name(self, mode: str, seasons: list[int]) -> str:
        now = datetime.now().strftime("%m-%d %H:%M")
        if mode == "villain":
            return f"빌런 에피소드 | {self._season_label(seasons)} | {now}"
        if mode == "hot":
            return f"화제성 상위 | {self._season_label(seasons)} | {now}"
        return f"분석 결과 | {self._season_label(seasons)} | {now}"
