# =============================================
# 나는솔로 YouTube 스크래퍼 설정
# =============================================

# 검색 키워드 목록
SEARCH_QUERIES = [
    "나는솔로",
    "나는솔로 하이라이트",
    "나는솔로 명장면",
    "나는솔로 커플",
    "나는솔로 최신",
]

# 수집할 영상 수 (최종 상위 N개)
TARGET_VIDEO_COUNT = 50

# 검색당 가져올 최대 영상 수
MAX_RESULTS_PER_QUERY = 100

# 출력 디렉토리
OUTPUT_DIR = "output"

# 자막 언어 우선순위
TRANSCRIPT_LANGUAGES = ["ko", "ko-KR", "en", "en-US"]

# 요청 딜레이 (초) - YouTube 차단 방지
REQUEST_DELAY = 1.5
