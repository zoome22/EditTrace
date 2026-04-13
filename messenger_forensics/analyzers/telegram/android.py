"""
telegram/android.py - Telegram Android 분석기

DB 경로  : /data/data/org.Telegram.messenger/files/cache4.db
           (평문 SQLite, 복호화 불필요)

핵심 테이블:
  messages_v2 (메시지)
    mid        서버 메시지 ID
    uid        대화 상대방/채팅방 ID
    date       전송 시각 (Unix seconds)
    data       TL 직렬화 BLOB (메시지 본문 및 메타데이터)
    out        발신 방향 (1=내가 보냄, 0=받은 메시지)

  users (사용자 정보)
    uid        사용자 ID
    name       표시 이름

TL BLOB 구조 (messages_v2.data):
  constructor(4B LE) → 메시지 타입 분기

  일반 메시지 (constructor 0x9cb490e9):
    [0]  constructor(4B)
    [4]  id(4B)
    [8]  extra(4B)         = 0
    [12] flags(4B)         ← 수정 비트 포함
    [16] from_id(12B)      if bit8(0x0100) set
    [..]  peer_id(12B)
    [..]  reply_to(가변)   if bit3(0x0008) set
    [..]  date(4B)         = date 칼럼 값
    [..]  message(TLString)
    [..]  edit_date(4B)    if bit15(0x8000) set

  서비스 메시지 (constructor 0x7a800e0a):
    [0]  constructor(4B)
    [4]  flags(4B)
    [8]  id(4B)
    [..] peer_id(12B), date(4B), action...

  flags 비트 필드:
    bit1  (0x0002) out       내가 보낸 메시지
    bit3  (0x0008) reply_to  답장 (가변 크기 구조체)
    bit8  (0x0100) from_id   발신 Peer 추가 (12B)
    bit9  (0x0200) media     미디어 포함
    bit15 (0x8000) edit_date 수정됨 → date 이후 TLString 다음에 edit_date(4B) 존재

TLString 인코딩:
  len < 254: [len(1B)][data][padding to 4B alignment]
  len = 0xFE: [0xFE][len(3B LE)][data][padding to 4B alignment]

타임스탬프:
  date / edit_date = Unix seconds
"""

import struct
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 상수 ─────────────────────────────────────────────────────────────────────

_DB_NAME = "cache4.db"

# flags 비트 마스크
_FLAG_OUT       = 0x00000002   # bit1
_FLAG_REPLY_TO  = 0x00000008   # bit3
_FLAG_FROM_ID   = 0x00000100   # bit8
_FLAG_MEDIA     = 0x00000200   # bit9
_FLAG_EDIT_DATE = 0x00008000   # bit15: 수정됨

# constructor → flags 필드 offset (BLOB 내 절대 위치)
# 모든 알려진 constructor에서 flags는 cid(4B) 직후인 offset 4에 위치
# (cid 4B + flags 4B + serverMsgId 4B + ...)
_CONSTRUCTOR_FLAGS_OFFSET: dict[int, int] = {
    0x9cb490e9: 4,   # 일반 메시지 (flags@4, serverMsgId@8)
    0x7a800e0a: 4,   # 서비스/채널 메시지 (flags@4, id@8)
}

# 텍스트 본문이 없는 서비스/미디어 메시지 constructor
# date 직후 필드가 TLString(텍스트)이 아니라 action 구조체
_NO_TEXT_CONSTRUCTORS: set[int] = {
    0x7a800e0a,   # messageService (시스템 이벤트)
}


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


def _read_tl_string(data: bytes, pos: int) -> tuple[str, int]:
    """
    TLString 디코딩.
    반환: (텍스트, 다음 pos)
    """
    if pos >= len(data):
        return "", pos
    length = data[pos]
    if length == 0xFE:
        if pos + 4 > len(data):
            return "", pos
        length = struct.unpack_from("<I", data, pos)[0] >> 8
        pos += 4
        pad_base = length
    else:
        pos += 1
        pad_base = length + 1

    if pos + length > len(data):
        length = len(data) - pos

    text = data[pos:pos + length].decode("utf-8", errors="replace")
    pos += length
    pad = (4 - (pad_base % 4)) % 4
    pos += pad
    return text, pos


# ── TL BLOB 파서 ──────────────────────────────────────────────────────────────

def _get_flags(data: bytes) -> tuple[int, int]:
    """
    constructor 기반으로 flags 값과 그 offset을 반환.

    알려진 constructor는 고정 offset으로 바로 읽고,
    미지 constructor는 4~20 범위에서 유효 후보를 탐색.
    반환: (flags, flags_offset)  offset=-1이면 탐색 실패
    """
    if len(data) < 8:
        return 0, -1

    cid = struct.unpack_from("<I", data, 0)[0]

    if cid in _CONSTRUCTOR_FLAGS_OFFSET:
        offset = _CONSTRUCTOR_FLAGS_OFFSET[cid]
        if offset + 4 <= len(data):
            flags = struct.unpack_from("<I", data, offset)[0]
            return flags, offset
        return 0, -1

    # 미지 constructor: 소규모 범위 탐색 (0 제외, 0x0001FFFF 이하)
    for j in range(4, min(24, len(data) - 3), 4):
        v = struct.unpack_from("<I", data, j)[0]
        if 0 < v <= 0x0001FFFF:
            return v, j

    return 0, -1


def _parse_blob(data: bytes, date_val: int) -> dict | None:
    """
    messages_v2.data BLOB 파싱.

    전략:
      1. constructor로 flags 위치 결정 → flags 추출
      2. date 칼럼 값을 anchor로 BLOB 내 date 위치 탐색
      3. date 직후 TLString으로 메시지 본문 읽기
      4. bit15(edit_date) set → TLString 직후 edit_date(4B) 읽기

    반환: {flags, text, edit_date, is_edited, is_out} 또는 None
    """
    if not data or len(data) < 12:
        return None

    cid = struct.unpack_from("<I", data, 0)[0]

    # 서비스 메시지: 텍스트 없음 → 본문 파싱 스킵
    if cid in _NO_TEXT_CONSTRUCTORS:
        return None

    flags, _ = _get_flags(data)

    # date anchor 탐색
    date_bytes = struct.pack("<i", date_val)
    date_pos = -1
    for i in range(4, len(data) - 3):
        if data[i:i + 4] == date_bytes:
            date_pos = i
            break
    if date_pos < 0:
        return None

    # date 직후 TLString (메시지 본문)
    text, pos_after = _read_tl_string(data, date_pos + 4)

    # edit_date (bit15 set 시 TLString 직후)
    edit_date = None
    if flags & _FLAG_EDIT_DATE:
        if pos_after + 4 <= len(data):
            raw = struct.unpack_from("<i", data, pos_after)[0]
            # 유효 Unix timestamp 범위 검증 (2000~2100년)
            if 946684800 < raw < 4102444800:
                edit_date = raw

    return {
        "flags":     flags,
        "text":      text,
        "edit_date": edit_date,
        "is_edited": bool(flags & _FLAG_EDIT_DATE),
        "is_out":    bool(flags & _FLAG_OUT),
    }


# ── DB 유틸 ──────────────────────────────────────────────────────────────────

def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _is_telegram_android_db(db_path: Path) -> bool:
    try:
        conn = sqlite3.connect(str(db_path))
        ok = _has_table(conn, "messages_v2")
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
    for ext in ("*.db", "*.sqlite"):
        for f in sorted(path.rglob(ext)):
            if f not in results:
                results.append(f)
    return results


# ── 핵심 분석 ─────────────────────────────────────────────────────────────────

def _build_uid_name_map(conn: sqlite3.Connection) -> dict[int, str]:
    """users 테이블에서 uid → name 매핑 구성."""
    name_map: dict[int, str] = {}
    if not _has_table(conn, "users"):
        return name_map
    try:
        for row in conn.execute("SELECT uid, name FROM users").fetchall():
            if row[0] and row[1]:
                name_map[row[0]] = row[1]
    except Exception:
        pass
    return name_map


def _analyze_telegram_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 cache4.db 분석.

    Returns:
        (total_msgs, total_modified)
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
        return 0, 0

    if not _has_table(conn, "messages_v2"):
        conn.close()
        result.add_error(f"messages_v2 테이블 없음 [{db_path.name}]")
        return 0, 0

    uid_name_map = _build_uid_name_map(conn)

    try:
        rows = conn.execute(
            "SELECT mid, uid, date, data, out "
            "FROM messages_v2 ORDER BY date"
        ).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"messages_v2 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    conn.close()

    all_rows:   list[list[str]] = []
    hl_indices: set[int]        = set()
    sub_rows:   dict[int, list] = {}
    total_msgs     = 0
    total_modified = 0

    for msg in rows:
        data_blob = bytes(msg["data"]) if msg["data"] else b""
        parsed    = _parse_blob(data_blob, msg["date"])

        text = parsed["text"] if parsed else ""
        if not text:
            continue   # 텍스트 없는 메시지(미디어·서비스) 제외

        total_msgs += 1

        flags     = parsed["flags"] if parsed else 0
        is_edited = parsed["is_edited"] if parsed else False
        is_out    = bool(msg["out"]) or (parsed["is_out"] if parsed else False)
        edit_date = parsed["edit_date"] if parsed else None

        if is_edited:
            total_modified += 1

        peer_name = uid_name_map.get(msg["uid"], str(msg["uid"]))
        send_ts   = _unix_to_str(msg["date"])
        edit_ts   = _unix_to_str(edit_date) if edit_date else ""

        row_index = len(all_rows)
        all_rows.append([
            str(msg["mid"]),
            peer_name,
            "→ 발신" if is_out else "← 수신",
            send_ts,
            edit_ts,
            "O" if is_edited else "",
            text,
            db_path.name,
        ])

        if is_edited:
            hl_indices.add(row_index)

        # ── 서브행: 수정 상세 ─────────────────────────────────────────────────
        if not is_edited:
            continue

        children: list[list[str]] = []

        children.append([
            "  ↳ 원본",
            "", "", send_ts, "", "",
            "원본 저장 없음 — Telegram은 최종 수정본만 보존",
            "",
        ])
        children.append([
            "  ↳ 최종 수정본",
            "", "", "", edit_ts, "",
            text,
            "",
        ])
        sub_rows[row_index] = children

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 ID (mid)",
            "대화 상대 (uid/name)",
            "방향",
            "전송 시각 (date)",
            "수정 시각 (edit_date)",
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

class TelegramAndroidAnalyzer(BaseAnalyzer):
    """
    Telegram Android 수정 메시지 분석기.

    수집 경로:
      /data/data/org.Telegram.messenger/files/cache4.db

    평문 SQLite. TL 직렬화 BLOB 파싱 필요.

    분석 핵심:
      messages_v2.data (TL BLOB) 파싱
        constructor(4B) → flags offset 결정
        flags bit15(0x8000) = edit_date 필드 존재 → 수정된 메시지
        date 칼럼 anchor → TLString 디코딩 → edit_date 추출

      수정 식별: flags & 0x8000 (bit15 set)
      수정 시각: edit_date (Unix seconds, TLString 직후)
      수정 본문: 최종 수정본으로 덮어씌워짐 (원본 복원 불가)

      ※ 수정 횟수 저장 없음 — 최종 수정 여부만 식별 가능
      ※ 인용 답장 후 수정 시 인용 내용도 수정본으로 반영됨
    """

    MESSENGER = "Telegram"
    PLATFORM  = "Android"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        candidates = _find_db_files(path)
        db_files   = [f for f in candidates if _is_telegram_android_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"messages_v2 테이블을 포함한 Telegram Android DB를 "
                f"찾지 못했습니다: {path}\n"
                "  예상 경로: /data/data/org.Telegram.messenger/files/cache4.db"
            )
            return result

        total_msgs = total_modified = 0
        for db_path in db_files:
            m, e = _analyze_telegram_db(db_path, result)
            total_msgs     += m
            total_modified += e

        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정된 메시지"]    = str(total_modified)
        result.summary["수정 식별 방법"]   = "TL flags bit15 (0x8000) = edit_date 필드 존재"
        result.summary["수정 시각"]        = "BLOB 내 edit_date (Unix seconds)"
        result.summary["원본 복원"]        = "불가 — 최종 수정본만 저장"
        result.summary["파싱 방식"]        = "constructor → flags offset / date anchor → TLString"
        result.summary["수집 방법"]        = "논리/물리 백업"

        return result
