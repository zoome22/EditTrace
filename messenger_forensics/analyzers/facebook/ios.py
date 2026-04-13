"""
facebook/ios.py - Facebook Messenger iOS 분석기

DB 경로  : /private/var/mobile/Containers/Shared/AppGroup/[UUID]/Library
            /Application Support/lightspeed-userDatabases/[Account_ID].db
           (FFS 수집 기준, 논리 백업으로는 수집 불가)
           (평문 SQLite, 별도 복호화 불필요)

DB 스키마 및 분석 로직은 Android와 완전히 동일합니다.
공통 로직은 facebook/__init__.py 에서 공유합니다.
"""

from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult
from analyzers.facebook import analyze_db, is_facebook_db


def _find_db_files(path: Path) -> list[Path]:
    """iOS FB Messenger DB 후보 파일 목록 반환."""
    if path.is_file():
        return [path]
    results: list[Path] = []
    # iOS FFS 기준 파일명 패턴: [Account_ID].db (숫자로 구성된 파일명)
    # lightspeed-userDatabases 디렉터리 우선 탐색
    for f in path.rglob("*.db"):
        # lightspeed-userDatabases 경로 하위이거나, 숫자로만 된 파일명이면 우선 후보
        if "lightspeed" in str(f) or f.stem.isdigit():
            results.append(f)
    # 나머지 SQLite 파일도 후보에 포함 (중복 제외)
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        for f in path.rglob(ext):
            if f not in results:
                results.append(f)
    return sorted(results)


class FacebookIOSAnalyzer(BaseAnalyzer):
    MESSENGER = "Facebook Messenger"
    PLATFORM  = "iOS"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        db_files = [f for f in _find_db_files(path) if is_facebook_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"client_messages 테이블을 포함한 Facebook Messenger DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /private/var/mobile/Containers/Shared/AppGroup/[UUID]"
                "/Library/Application Support/lightspeed-userDatabases/[Account_ID].db\n"
                "  ※ 논리 백업으로는 수집 불가 — FFS(전체 파일시스템) 수집 필요"
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
        result.summary["수집 방법"]      = "FFS 수집 필요 (논리 백업 불가)"
        return result

