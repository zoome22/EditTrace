"""
facebook/android.py - Facebook Messenger Android 분석기

DB 경로  : /data/data/com.facebook.orca/databases/msys_database_[Account_ID].db
           (평문 SQLite, 별도 복호화 불필요)

DB 스키마 및 분석 로직은 facebook/__init__.py 참조.
Android / iOS 공통 스키마이므로 핵심 로직은 공유합니다.
"""

from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult
from analyzers.facebook import analyze_db, is_facebook_db


def _find_db_files(path: Path) -> list[Path]:
    """Android FB Messenger DB 후보 파일 목록 반환."""
    if path.is_file():
        return [path]
    results: list[Path] = []
    # 파일명 패턴: msys_database_[Account_ID].db
    for f in path.rglob("msys_database_*.db"):
        results.append(f)
    # 패턴 미탐지 시 일반 SQLite 파일도 후보에 포함
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        for f in path.rglob(ext):
            if f not in results:
                results.append(f)
    return sorted(results)


class FacebookAndroidAnalyzer(BaseAnalyzer):
    MESSENGER = "Facebook Messenger"
    PLATFORM  = "Android"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        db_files = [f for f in _find_db_files(path) if is_facebook_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"client_messages 테이블을 포함한 Facebook Messenger DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /data/data/com.facebook.orca/databases/msys_database_[Account_ID].db"
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
        result.summary["원본 복구 가능"] = "가능 (client_edit_message_history)"
        result.summary["수정 횟수 확인"] = "가능 (edit_count)"
        return result

