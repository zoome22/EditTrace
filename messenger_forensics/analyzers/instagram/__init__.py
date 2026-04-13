"""
instagram/__init__.py - Instagram 공통 분석 로직

DB 경로  : /data/data/com.instagram.android/databases/direct.db
           (평문 SQLite, 별도 복호화 불필요)

실제 DB 스키마 (messages 테이블):
  _id             행 고유 ID
  user_id         계정 소유자 ID
  server_item_id  서버 메시지 ID
  client_item_id  클라이언트 메시지 ID
  thread_id       대화방 ID
  recipient_ids   수신자 ID
  timestamp       전송 시각 (INTEGER, Unix Microseconds) ← 최상위 칼럼
  message_type    메시지 유형 (text 등)
  text            앱 표시 최종 메시지 (단순 텍스트)
  message         메타데이터 (BLOB/bytes → UTF-8 JSON 디코딩 필요)

message JSON 주요 필드:
  text                  최종 메시지 내용
  timestamp             전송 시각 문자열 (Unix Microseconds)
  timestamp_in_micro    전송 시각 정수  (Unix Microseconds)
  edit_count            수정 횟수
  edit_history[]        수정 이전 원본 배열 (시간순)
    .body               수정 전 메시지 내용
    .timestamp          해당 시점 시각 (Unix Milliseconds)
  replied_to_message    답장 원본 정보 (dict, 없으면 키 없음)
    .text               원본 메시지 내용 (원본 수정 시 수정본으로 자동 갱신)
    .item_id            원본 메시지 server_item_id
    .timestamp_in_micro 원본 전송 시각 (Unix Microseconds)

타임스탬프 단위 주의:
  messages.timestamp / message.timestamp_in_micro → Unix Microseconds (/1,000,000)
  edit_history[*].timestamp                       → Unix Milliseconds (/1,000)
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import AnalysisResult


# ── 타임스탬프 변환 유틸 ──────────────────────────────────────────────────────

def _us_to_str(us) -> str:
    """Unix Microseconds → 'YYYY-MM-DD HH:MM:SS' (로컬 시간). None·0 → ''."""
    if not us:
        return ""
    try:
        return datetime.fromtimestamp(
            int(us) / 1_000_000, tz=timezone.utc
        ).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(us)


def _ms_to_str(ms) -> str:
    """Unix Milliseconds → 'YYYY-MM-DD HH:MM:SS' (로컬 시간). None·0 → ''."""
    if not ms:
        return ""
    try:
        return datetime.fromtimestamp(
            int(ms) / 1_000, tz=timezone.utc
        ).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ms)


# ── DB 유틸 ──────────────────────────────────────────────────────────────────

def has_table(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        return any(
            r[1] == col
            for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
        )
    except Exception:
        return False


def is_instagram_db(db_path: Path) -> bool:
    """messages 테이블 + message 칼럼이 존재하는 Instagram direct.db 인지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        ok = has_table(conn, "messages") and col_exists(conn, "messages", "message")
        conn.close()
        return ok
    except Exception:
        return False


# ── JSON 파싱 헬퍼 ────────────────────────────────────────────────────────────

def _parse_message_json(raw) -> dict:
    """
    message 칼럼 파싱.
    실제 DB에서 BLOB(bytes) 형태로 저장되므로 UTF-8 디코딩 후 JSON 파싱.
    """
    if not raw:
        return {}
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


# ── 핵심 분석 ─────────────────────────────────────────────────────────────────

def analyze_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 direct.db 파일을 분석하여 result 에 테이블을 추가합니다.

    Returns:
        (total_msgs, total_modified) 집계 값
    """
    all_rows:   list[list[str]] = []
    hl_indices: set[int]        = set()
    sub_rows:   dict[int, list] = {}
    total_msgs     = 0
    total_modified = 0

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
        return 0, 0

    if not has_table(conn, "messages"):
        conn.close()
        result.add_error(
            f"messages 테이블이 존재하지 않습니다 [{db_path.name}]\n"
            "  Instagram direct.db 파일이 맞는지 확인하세요."
        )
        return 0, 0

    # ── 가용 칼럼 탐지 ────────────────────────────────────────────────────────
    has_col = lambda c: col_exists(conn, "messages", c)
    has_server_id  = has_col("server_item_id")
    has_user       = has_col("user_id")
    has_thread     = has_col("thread_id")
    has_timestamp  = has_col("timestamp")     # 최상위 INTEGER 칼럼
    has_text       = has_col("text")
    has_message    = has_col("message")

    select_cols = ["_id"]
    if has_server_id: select_cols.append("server_item_id")
    if has_user:      select_cols.append("user_id")
    if has_thread:    select_cols.append("thread_id")
    if has_timestamp: select_cols.append("timestamp")
    if has_text:      select_cols.append("text")
    if has_message:   select_cols.append("message")

    try:
        raw_msgs = conn.execute(
            f"SELECT {', '.join(select_cols)} FROM messages ORDER BY rowid"
        ).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"messages 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    conn.close()

    # ── 메시지별 분석 ─────────────────────────────────────────────────────────
    for msg in raw_msgs:
        total_msgs += 1

        row_id     = str(msg["_id"] or "")
        server_id  = str(msg["server_item_id"] if has_server_id else "") or ""
        user_id    = str(msg["user_id"]        if has_user      else "") or ""
        thread_id  = str(msg["thread_id"]      if has_thread    else "") or ""

        # 전송 시각: 최상위 timestamp 칼럼 (Unix Microseconds)
        sent_ts = _us_to_str(msg["timestamp"] if has_timestamp else None)

        # message 칼럼 JSON 파싱 (BLOB → bytes → UTF-8 → JSON)
        meta = _parse_message_json(msg["message"] if has_message else None)

        # ─ 수정 횟수
        try:
            edit_count = int(meta.get("edit_count") or 0)
        except (ValueError, TypeError):
            edit_count = 0

        # ─ 최종 표시 메시지: message.text 우선, 없으면 messages.text
        final_text = str(meta.get("text") or "")
        if not final_text and has_text:
            final_text = str(msg["text"] or "")

        # ─ edit_history: 수정 이전 원본 배열 (시간순)
        #   각 항목: { "body": str, "timestamp": Unix Milliseconds }
        edit_history: list[dict] = []
        raw_history = meta.get("edit_history")
        if isinstance(raw_history, list):
            for item in raw_history:
                if isinstance(item, dict):
                    edit_history.append(item)

        # ─ 답장(replied_to_message) 정보
        #   원본 수정 시 이 dict 의 text 가 수정본으로 자동 갱신됨
        reply_info = meta.get("replied_to_message")
        is_reply   = isinstance(reply_info, dict) and bool(reply_info)

        # ─ 마지막 수정 시각: edit_history 마지막 항목의 timestamp (Unix ms)
        last_edit_ts = ""
        if edit_history:
            last_edit_ts = _ms_to_str(edit_history[-1].get("timestamp"))

        has_edit = edit_count > 0
        if has_edit:
            total_modified += 1

        # ── 본행 구성 ─────────────────────────────────────────────────────────
        row_index = len(all_rows)
        reply_marker = " [답장]" if is_reply else ""
        all_rows.append([
            server_id or row_id,
            user_id,
            thread_id,
            sent_ts,
            last_edit_ts,
            str(edit_count) if has_edit else "",
            final_text + reply_marker,
            db_path.name,
        ])

        if has_edit:
            hl_indices.add(row_index)

        # ── 서브 행: 수정 이력 ────────────────────────────────────────────────
        # edit_history 구조:
        #   [0]   → 최초 원본 내용  + 전송 시각 (Unix ms)
        #   [1]   → 수정 1회 직전   + 수정 시각 (Unix ms)
        #   [N-1] → 수정 N-1회 직전 + 수정 시각 (Unix ms)
        #   final_text → 현재 최종본 (edit_count == N)
        #
        # 답장 원본(replied_to_message.text)은 원본 메시지 수정 시 자동 갱신.
        # 해당 내용을 서브행 마지막에 별도 표시.
        children: list[list[str]] = []

        if has_edit and edit_history:
            for i, hist in enumerate(edit_history):
                body   = str(hist.get("body") or "")
                ts_str = _ms_to_str(hist.get("timestamp"))

                if i == 0:
                    label    = "  ↳ 원본"
                    sent_col = ts_str   # 원본의 timestamp ≈ 전송 시각 (ms)
                    edit_col = ""
                else:
                    label    = f"  ↳ 수정 {i}"
                    sent_col = ""
                    edit_col = ts_str

                children.append([
                    label, "", "",
                    sent_col,
                    edit_col,
                    "", body, "",
                ])

            # 최종본
            children.append([
                f"  ↳ 수정 {edit_count} (최종)",
                "", "", "", "", "",
                final_text, "",
            ])

        elif has_edit:
            children.append([
                "  ↳ 수정 이력", "", "", "", "", "",
                f"수정 {edit_count}회 확인 — edit_history 없음 (동기화 미완료 또는 삭제)",
                "",
            ])

        # 답장이면 원본 메시지 내용도 서브행에 표시
        if is_reply:
            reply_text = str(reply_info.get("text") or "")
            reply_ts   = _us_to_str(reply_info.get("timestamp_in_micro"))
            children.append([
                "  ↳ 답장 원본", "", "",
                reply_ts, "", "",
                reply_text, "",
            ])

        if children:
            sub_rows[row_index] = children

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 ID (server_item_id)", "발신자 ID (user_id)", "대화방 (thread_id)",
            "전송 시각", "마지막 수정 시각", "수정 횟수",
            "앱 내 표시 메시지 (최종본)", "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=hl_indices,
        sub_rows=sub_rows,
    )

    return total_msgs, total_modified
