# NASOL Transcript Studio

나는 솔로(1기~29기) 영상의 메타데이터/대본을 무료 방식으로 수집하고,  
Raw 데이터 확인과 분석(View 저장)까지 한 번에 처리하는 Streamlit 앱입니다.

## 핵심 동작

- `@chonjang` 공식 채널을 우선 스캔
- 공식 채널에서 누락된 기수만 일반 검색으로 보완
- `video_id` 유니크 + dedupe key 기반으로 중복 제거
- 기수/회차/업로드일 기준 시간순 정렬 저장
- 대본 수집은 요청 간 지연 + 랜덤 지터 적용(무료/안정 수집)
- 캐스트 이름 사전(영수/영호/영식/영철/광수/상철/영숙/정숙/순자/영자/옥순/현숙/경수/정희/정수/정식) 기반 ASR 오탈자 보정

## 실행

```bash
python3 -m pip install -r requirements.txt
streamlit run app.py
```

## UI 구성

1. `수집` 탭
- 단일/범위/다중 기수 선택
- 백그라운드(멀티프로세스) 수집 지원
- 수집 진행 로그
- 완료 토스트 + 기수별 수집 요약

2. `Raw Data` 탭
- 기수/대본상태 필터
- 영상 메타데이터 대시보드
- 선택 영상의 transcript raw text + segment 테이블

3. `요약 및 정리` 탭
- Codex 협업 큐 기반 에피소드 요약 전용 탭
- 선택한 기수의 본편/대본 보유 에피소드 전체를 chunk 단위로 요약
- 완료 후 에피소드별 핵심 줄거리/핵심 인물/근거 링크/유튜브 링크 시각화

4. `분석` 탭
- ChatGPT 스타일 질의 입력
- `빌런 에피소드`, `화제성 상위` 분석 자동 저장
- 좌측 Saved Views에서 재조회 + 기수별 필터링
- `Codex 협업 큐` 모드로 분석 요청을 큐에 쌓고, Codex가 처리한 결과를 다시 앱에서 확인 가능

## 저장 위치

- DB: `output/nasol.db`
- 백그라운드 수집 로그: `output/collector_worker.log`
- 기존 스크립트 결과: `output/` 하위 파일 유지

## 참고

- 현재 분석은 로컬 규칙 기반 점수화(키워드/댓글비율/조회수)입니다.
- 필요하면 이후 RAG/LLM 연동을 붙여 답변 품질을 높일 수 있습니다.

### Codex 협업 큐 사용법

1. 분석 탭에서 `분석 엔진 -> Codex 협업 큐` 선택
2. 요청 등록 (예: `10~11기 갈등 흐름 정리`)
3. 터미널에서 에피소드별 작업 팩 생성

```bash
python3 -m nasol.codex_queue packet --job-id <JOB_ID> --output-dir /tmp/codex_job_<JOB_ID>
```

4. Codex가 `/tmp/codex_job_<JOB_ID>/episodes` 하위 파일을 에피소드 단위로 읽고,
- 사건별 서사(누가/왜/어떻게/결과)
- 핵심 인물
- 근거 timestamp 링크
- 영상 링크
를 포함해 결과를 작성

```bash
cp /tmp/codex_job_<JOB_ID>/result_template.md /tmp/codex_job_<JOB_ID>_result.md
```

5. 결과를 완료 처리

```bash
python3 -m nasol.codex_queue complete --job-id <JOB_ID> --result-file /tmp/codex_job_<JOB_ID>_result.md
```

### 요약 및 정리 탭 (Codex 요약 전용)

1. `요약 및 정리` 탭에서 대상 기수 선택 후 `요약 요청 등록`
2. 터미널에서 패킷 생성

```bash
python3 -m nasol.codex_queue packet --job-id <JOB_ID> --output-dir /tmp/codex_summary_job_<JOB_ID> --max-videos 3000 --chunk-chars 1200
```

3. Codex가 `episodes/`를 읽고 `result_template.md`의 `EPISODE|...` 포맷을 유지해 작성
   - `chunk_storyline`(최소 2개): chunk별 핵심 사건/감정 변화
   - `key_incidents`(최소 2개): 사건/인물/갈등 포인트
   - `evidence_links`(최소 2개): timestamp YouTube 링크
   - `summary`는 템플릿 문구(예: `이 구간은`, `라는 제목 그대로`) 금지
   - 캐릭터명은 캐스트 사전 기준으로 보정
4. 완료 처리

```bash
python3 -m nasol.codex_queue complete --job-id <JOB_ID> --result-file /tmp/codex_summary_job_<JOB_ID>_result.md
```

5. 마음에 안 드는 요약 작업 삭제

```bash
python3 -m nasol.codex_queue delete --job-id <JOB_ID>
```
