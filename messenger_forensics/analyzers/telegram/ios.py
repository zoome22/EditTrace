"""
telegram/ios.py - Telegram iOS 분석기

DB 경로  : /private/var/mobile/Containers/Shared/AppGroup/[UUID]/
             Telegram-data/account-[Account_ID]/postbox/db/db_sqlite
           (FFS 수집 필요, 논리 백업 불가 / 평문 SQLite)
           실제 파일명은 db_sqlite이나 .db 확장자로 저장된 경우도 있음

핵심 테이블: t7 (key-value 쌍)

  key (BLOB, 20바이트):
    [0:8]   peer_id / account_id (Big-endian int64)
    [8:12]  padding (0x00000000)
    [12:16] 전송 시각 (Big-endian uint32, Unix seconds)
    [16:20] 메시지 시퀀스 번호 (Big-endian uint32)

  value (BLOB, 가변 길이):
    [0:28]  메시지 메타데이터 (발신자 ID, 플래그 등)
    [28:32] 텍스트 길이 (Little-endian uint32)
    [32:32+len] 메시지 본문 (UTF-8)
    [이후]  message_attributes 영역 (가변)

  수정 메시지 식별:
    value 내 마커 b'\\x01\\x64\\x00' 존재 여부로 판단
    마커 직후 4바이트 = edit_date (Little-endian uint32, Unix seconds)

  미수정 메시지:
    마커 b'\\x01\\x64\\x00' 없음

타임스탬프:
  key[12:16] Big-endian uint32 = 전송 시각 (Unix seconds)
  value 내 edit_date = Little-endian uint32 (Unix seconds)
"""

import struct
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 상수 ─────────────────────────────────────────────────────────────────────

_DB_NAMES = ("db_sqlite", "Telegram_ios.db")

# edit_date 마커: 이 3바이트 직후 4바이트가 edit_date (LE uint32)
_EDIT_MARKER = bytes.fromhex("016400")

# 유효 Unix timestamp 범위 (2010 ~ 2100년)
_TS_MIN = 1262304000
_TS_MAX = 4102444800


# ── 유틸 ─────────────────────────────────────────────────────────────────────

def _unix_to_str(val) -> str:
    """Unix seconds → 'YYYY-MM-DD HH:MM:SS' (로컬 시간)."""
    if not val:
        return ""
    try:
        return (
            datetime.fromtimestamp(int(val), tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception:
        return str(val)


# ── key / value 파서 ─────────────────────────────────────────────────────────

def _parse_key(key: bytes) -> dict | None:
    """
    t7.key (20바이트) 파싱.
    반환: {peer_id, timestamp, seq} 또는 None
    """
    if len(key) < 20:
        return None
    peer_id   = struct.unpack_from(">q", key, 0)[0]
    timestamp = struct.unpack_from(">I", key, 12)[0]
    seq       = struct.unpack_from(">I", key, 16)[0]

    if not (_TS_MIN < timestamp < _TS_MAX):
        return None

    return {"peer_id": peer_id, "timestamp": timestamp, "seq": seq}


def _parse_value(value: bytes) -> dict:
    """
    t7.value BLOB 파싱.
    반환: {text, edit_date, is_edited}
    """
    result = {"text": "", "edit_date": None, "is_edited": False}

    if not value or len(value) < 36:
        return result

    # 텍스트: value[28:32] LE uint32 = 길이, value[32:32+len] = UTF-8 본문
    try:
        text_len = struct.unpack_from("<I", value, 28)[0]
        if 0 < text_len < len(value) - 32:
            result["text"] = value[32:32 + text_len].decode("utf-8", errors="replace")
    except Exception:
        pass

    # edit_date: 마커 b'\x01\x64\x00' 직후 4바이트 (LE uint32)
    idx = value.find(_EDIT_MARKER)
    if idx >= 0:
        pos = idx + len(_EDIT_MARKER)
        if pos + 4 <= len(value):
            ed = struct.unpack_from("<I", value, pos)[0]
            if _TS_MIN < ed < _TS_MAX:
                result["edit_date"] = ed
                result["is_edited"] = True

    return result


# ── DB 유틸 ──────────────────────────────────────────────────────────────────

def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _is_telegram_ios_db(db_path: Path) -> bool:
    """t7 테이블이 존재하는 Telegram iOS DB인지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        ok = _has_table(conn, "t7")
        conn.close()
        return ok
    except Exception:
        return False


def _find_db_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    results: list[Path] = []
    for name in _DB_NAMES:
        for f in sorted(path.rglob(name)):
            results.append(f)
    for ext in ("*.db", "*.sqlite"):
        for f in sorted(path.rglob(ext)):
            if f not in results:
                results.append(f)
    return results


# ── 핵심 분석 ─────────────────────────────────────────────────────────────────

def _analyze_ios_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 db_sqlite 분석.

    Returns:
        (total_msgs, total_modified)
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
        return 0, 0

    if not _has_table(conn, "t7"):
        conn.close()
        result.add_error(f"t7 테이블 없음 [{db_path.name}]")
        return 0, 0

    try:
        rows = conn.execute("SELECT key, value FROM t7").fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"t7 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    conn.close()

    all_rows:   list[list[str]] = []
    hl_indices: set[int]        = set()
    sub_rows:   dict[int, list] = {}
    total_msgs     = 0
    total_modified = 0

    parsed_msgs = []
    for row in rows:
        key_b = bytes(row["key"])
        val_b = bytes(row["value"])

        kp = _parse_key(key_b)
        if kp is None:
            continue

        vp = _parse_value(val_b)
        if not vp["text"]:
            continue

        parsed_msgs.append((kp, vp))

    # 시퀀스 번호(seq) 기준 정렬
    parsed_msgs.sort(key=lambda x: (x[0]["timestamp"], x[0]["seq"]))

    for kp, vp in parsed_msgs:
        total_msgs += 1
        is_edited = vp["is_edited"]
        if is_edited:
            total_modified += 1

        send_ts = _unix_to_str(kp["timestamp"])
        edit_ts = _unix_to_str(vp["edit_date"]) if vp["edit_date"] else ""
        text    = vp["text"]

        row_index = len(all_rows)
        all_rows.append([
            str(kp["seq"]),
            str(kp["peer_id"]),
            send_ts,
            edit_ts,
            "O" if is_edited else "",
            text,
            db_path.name,
        ])

        if is_edited:
            hl_indices.add(row_index)

        # ── 서브행 ───────────────────────────────────────────────────────────
        if not is_edited:
            continue

        sub_rows[row_index] = [
            [
                "  ↳ 원본",
                "", send_ts, "", "",
                "원본 저장 없음 — Telegram iOS는 최종 수정본만 보존",
                "",
            ],
            [
                "  ↳ 최종 수정본",
                "", "", edit_ts, "",
                text,
                "",
            ],
        ]

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 번호 (seq)",
            "Peer ID",
            "전송 시각",
            "수정 시각",
            "수정 여부",
            "메시지 본문",
            "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=hl_indices,
        sub_rows=sub_rows,
    )

    return total_msgs, total_modified


# ── 분석기 클래스 ──────────────────────────────────────────────────────────────

class TelegramIOSAnalyzer(BaseAnalyzer):
    """
    Telegram iOS 수정 메시지 분석기.

    수집 경로 (FFS):
      /private/var/mobile/Containers/Shared/AppGroup/[UUID]/
        Telegram-data/account-[Account_ID]/postbox/db/db_sqlite

    논리 백업으로는 수집 불가. 평문 SQLite, 복호화 불필요.
    Postbox 커스텀 바이너리 BLOB 파싱 필요.

    분석 핵심:
      t7 테이블 key-value 파싱
        key[12:16] BE uint32 → 전송 시각 (Unix seconds)
        value[28:32] LE uint32 → 텍스트 길이
        value[32:32+len] → 메시지 본문 (UTF-8)
        value 내 마커(0x016400) 존재 → 수정된 메시지
          마커+3B LE uint32 → edit_date (Unix seconds)

      ※ 원본 메시지 복원 불가 (최종 수정본만 저장)
      ※ 수정 횟수 식별 불가 (마지막 수정 시각만 기록)
    """

    MESSENGER = "Telegram"
    PLATFORM  = "iOS"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        candidates = _find_db_files(path)
        db_files   = [f for f in candidates if _is_telegram_ios_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"t7 테이블을 포함한 Telegram iOS DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /private/var/mobile/Containers/Shared/AppGroup/"
                "[UUID]/Telegram-data/account-[Account_ID]/postbox/db/db_sqlite\n"
                "  ※ iOS Telegram DB는 논리 백업으로 수집 불가 (FFS 필요)"
            )
            return result

        total_msgs = total_modified = 0
        for db_path in db_files:
            m, e = _analyze_ios_db(db_path, result)
            total_msgs     += m
            total_modified += e

        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정된 메시지"]    = str(total_modified)
        result.summary["수정 식별 방법"]   = "value 내 마커(0x016400) + 4B edit_date(LE)"
        result.summary["수정 시각"]        = "edit_date (Unix seconds, LE)"
        result.summary["전송 시각"]        = "t7.key[12:16] Big-endian uint32"
        result.summary["원본 복원"]        = "불가 — 최종 수정본만 저장"
        result.summary["수집 방법"]        = "FFS (논리 백업 불가)"

        return result
