"""
whatsapp/ios.py - WhatsApp iOS 분석기

DB 경로  : /private/var/mobile/Containers/Shared/AppGroup/[UUID]/ChatStorage.sqlite
           (FFS 수집 필요, 논리 백업 불가 / 평문 SQLite, 복호화 불필요)

핵심 테이블:
  ZWAMESSAGE (메시지)
    Z_PK           행 고유 ID
    ZMESSAGEDATE   전송 시각 (Apple Absolute Time, Cocoa epoch 초)
    ZTEXT          메시지 본문 (수정 시 최종 수정본으로 덮어씌워짐)
    ZISFROMME      발신 방향 (1=내가 보냄, 0=받은 메시지)
    ZCHATSESSION   → ZWACHATSESSION.Z_PK (대화방)
    ZFLAGS         메시지 플래그 (수정 여부 식별 불가 — 별도 메타데이터 없음)

  ZWACHATSESSION (대화방)
    Z_PK           행 고유 ID
    ZCONTACTJID    상대방 JID

수정 이력:
  Android와 달리 iOS WhatsApp은 수정 메타데이터 테이블이 존재하지 않음.
  ZTEXT가 최종 수정본으로 덮어씌워지며 원본 복원 불가.
  수정 여부를 식별할 수 있는 칼럼도 없음 → 수정 메시지 별도 식별 불가.

타임스탬프:
  ZMESSAGEDATE = Apple Absolute Time (Cocoa epoch, 2001-01-01 기준, 초)
  Unix 변환: 값 + 978307200
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult

_COCOA_EPOCH_OFFSET = 978_307_200
_DB_NAME = "ChatStorage.sqlite"


def _cocoa_to_str(val) -> str:
    if val is None:
        return ""
    try:
        return (
            datetime.fromtimestamp(float(val) + _COCOA_EPOCH_OFFSET, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception:
        return str(val)


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _is_whatsapp_ios_db(db_path: Path) -> bool:
    try:
        conn = sqlite3.connect(str(db_path))
        ok = _has_table(conn, "ZWAMESSAGE")
        conn.close()
        return ok
    except Exception:
        return False


def _find_db_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    results: list[Path] = []
    for f in sorted(path.rglob(_DB_NAME)):
        results.append(f)
    for ext in ("*.sqlite", "*.db"):
        for f in sorted(path.rglob(ext)):
            if f not in results:
                results.append(f)
    return results


def _analyze_ios_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
        return 0, 0

    if not _has_table(conn, "ZWAMESSAGE"):
        conn.close()
        result.add_error(f"ZWAMESSAGE 테이블 없음 [{db_path.name}]")
        return 0, 0

    has_session = _has_table(conn, "ZWACHATSESSION")

    session_join = (
        "LEFT JOIN ZWACHATSESSION s ON m.ZCHATSESSION = s.Z_PK"
        if has_session else ""
    )
    session_col = (
        "s.ZCONTACTJID AS chat_jid"
        if has_session else
        "NULL AS chat_jid"
    )

    try:
        rows = conn.execute(f"""
            SELECT
                m.Z_PK, m.ZMESSAGEDATE, m.ZTEXT,
                m.ZISFROMME, m.ZCHATSESSION,
                {session_col}
            FROM ZWAMESSAGE m
            {session_join}
            WHERE m.ZTEXT IS NOT NULL
            ORDER BY m.ZMESSAGEDATE
        """).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"메시지 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    conn.close()

    all_rows = []
    for msg in rows:
        all_rows.append([
            str(msg["Z_PK"]),
            str(msg["chat_jid"] or msg["ZCHATSESSION"] or ""),
            "→ 발신" if msg["ZISFROMME"] else "← 수신",
            _cocoa_to_str(msg["ZMESSAGEDATE"]),
            str(msg["ZTEXT"] or ""),
            db_path.name,
        ])

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 ID (Z_PK)",
            "대화방 (ZCONTACTJID)",
            "방향",
            "전송 시각 (ZMESSAGEDATE)",
            "메시지 본문 (ZTEXT)",
            "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=set(),
        sub_rows={},
    )

    return len(all_rows), 0   # 수정 식별 불가 → 수정 메시지 수 0


class WhatsAppIOSAnalyzer(BaseAnalyzer):
    """
    WhatsApp iOS 분석기.

    수집 경로 (FFS):
      /private/var/mobile/Containers/Shared/AppGroup/[UUID]/ChatStorage.sqlite

    논리 백업으로는 수집 불가. 복호화 불필요 (평문 SQLite).

    ※ Android와 달리 수정 메타데이터 테이블 없음
       → 수정 여부 식별 불가, 원본 복원 불가
       → ZTEXT는 최종 수정본으로 덮어씌워진 상태
    """

    MESSENGER = "WhatsApp"
    PLATFORM  = "iOS"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        candidates = _find_db_files(path)
        db_files   = [f for f in candidates if _is_whatsapp_ios_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"ZWAMESSAGE 테이블을 포함한 WhatsApp iOS DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /private/var/mobile/Containers/Shared/AppGroup/"
                "[UUID]/ChatStorage.sqlite\n"
                "  ※ iOS WhatsApp DB는 논리 백업으로 수집 불가 (FFS 필요)"
            )
            return result

        total_msgs = 0
        for db_path in db_files:
            m, _ = _analyze_ios_db(db_path, result)
            total_msgs += m

        result.summary["분석 DB 수"]    = str(len(db_files))
        result.summary["전체 메시지"]    = str(total_msgs)
        result.summary["수정 식별"]      = "불가 — 수정 메타데이터 테이블 없음 (Android와 상이)"
        result.summary["원본 복원"]      = "불가 — ZTEXT 덮어쓰기 방식"
        result.summary["전송 시각"]      = "ZWAMESSAGE.ZMESSAGEDATE (Apple Absolute Time, 초)"
        result.summary["수집 방법"]      = "FFS (논리 백업 불가)"

        return result
