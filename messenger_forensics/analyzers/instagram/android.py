"""
instagram/android.py - Instagram Android 분석기

DB 경로  : /data/data/com.instagram.android/databases/direct.db
           (평문 SQLite, 별도 복호화 불필요)

스키마 및 분석 로직은 instagram/__init__.py 참조.

messages 테이블 주요 칼럼:
  _id             행 고유 ID
  user_id         계정 소유자 ID
  server_item_id  서버 메시지 ID
  thread_id       대화방 ID
  timestamp       전송 시각 (Unix Microseconds, INTEGER)
  text            앱 표시 최종 메시지
  message         메타데이터 BLOB (UTF-8 JSON)
                    - edit_count    : 수정 횟수
                    - edit_history[]: 수정 이전 원본 배열
                        .body       : 수정 전 내용
                        .timestamp  : Unix Milliseconds
                    - replied_to_message: 답장 원본 정보
                        .text       : 원본 내용 (수정 시 자동 갱신)

타임스탬프 단위:
  messages.timestamp / message.timestamp_in_micro → Unix Microseconds (/1,000,000)
  edit_history[*].timestamp                       → Unix Milliseconds (/1,000)
"""

from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult
from analyzers.instagram import analyze_db, is_instagram_db


def _find_db_files(path: Path) -> list[Path]:
    """Instagram Android DB 후보 파일 목록 반환."""
    if path.is_file():
        return [path]
    results: list[Path] = []
    # Instagram 표준 DB 파일명 우선 탐색
    for f in path.rglob("direct.db"):
        results.append(f)
    # 보조: 일반 SQLite 확장자 (중복 제외)
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        for f in path.rglob(ext):
            if f not in results:
                results.append(f)
    return sorted(results)


class InstagramAndroidAnalyzer(BaseAnalyzer):
    MESSENGER = "Instagram"
    PLATFORM  = "Android"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        db_files = [f for f in _find_db_files(path) if is_instagram_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"messages 테이블을 포함한 Instagram DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /data/data/com.instagram.android/databases/direct.db"
            )
            return result

        total_msgs = total_modified = 0
        for db_path in db_files:
            m, e = analyze_db(db_path, result)
            total_msgs     += m
            total_modified += e

        result.summary["분석 DB 수"]    = str(len(db_files))
        result.summary["전체 메시지"]    = str(total_msgs)
        result.summary["수정된 메시지"]  = str(total_modified)
        result.summary["원본 복구 방법"] = "message(JSON) → edit_history[].body"
        result.summary["수정 횟수 확인"] = "message(JSON) → edit_count"
        result.summary["전송 시각 단위"] = "Unix Microseconds (messages.timestamp)"
        result.summary["수정 시각 단위"] = "Unix Milliseconds (edit_history[*].timestamp)"
        result.summary["답장 원본 갱신"] = "수정 시 replied_to_message.text 자동 업데이트"
        return result
