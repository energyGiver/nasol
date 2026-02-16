# NASOL Transcript Studio

나는 솔로(1기~29기) 영상의 메타데이터/대본을 무료 방식으로 수집하고,  
Raw 데이터 확인과 분석(View 저장)까지 한 번에 처리하는 Streamlit 앱입니다.

## 핵심 동작

- `@chonjang` 공식 채널을 우선 스캔
- 공식 채널에서 누락된 기수만 일반 검색으로 보완
- `video_id` 유니크 + dedupe key 기반으로 중복 제거
- 기수/회차/업로드일 기준 시간순 정렬 저장
- 대본 수집은 요청 간 지연 + 랜덤 지터 적용(무료/안정 수집)

## 실행

```bash
python3 -m pip install -r requirements.txt
streamlit run app.py
```

## UI 구성

1. `수집` 탭
- 단일/범위/다중 기수 선택
- 수집 진행 로그
- 완료 토스트 + 기수별 수집 요약

2. `Raw Data` 탭
- 기수/대본상태 필터
- 영상 메타데이터 대시보드
- 선택 영상의 transcript raw text + segment 테이블

3. `분석` 탭
- ChatGPT 스타일 질의 입력
- `빌런 에피소드`, `화제성 상위` 분석 자동 저장
- 좌측 Saved Views에서 재조회 + 기수별 필터링

## 저장 위치

- DB: `output/nasol.db`
- 기존 스크립트 결과: `output/` 하위 파일 유지

## 참고

- 현재 분석은 로컬 규칙 기반 점수화(키워드/댓글비율/조회수)입니다.
- 필요하면 이후 RAG/LLM 연동을 붙여 답변 품질을 높일 수 있습니다.
