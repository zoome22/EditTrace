"""
facebook/__init__.py - Facebook Messenger 공통 분석 로직

Android / iOS 모두 동일한 DB 스키마를 사용하므로
탐색·파싱·테이블 구성 로직을 이곳에서 공유합니다.

공통 스키마:
  client_messages
    pk                  메시지 고유 ID
    text                앱에 표시되는 최종(수정본) 메시지 내용
    edit_count          수정 횟수 (0 = 미수정)
    sender_contact_pk   발신자 ID
    authoritative_ts_ms 메시지 전송 시각 (Unix Milliseconds)
    thread_pk           대화방 식별자

  client_edit_message_history
    pk                         히스토리 레코드 고유 ID
    original_message_pk        원본 메시지의 pk (client_messages.pk 와 JOIN)
    message_content            수정 전 메시지 내용
    server_adjusted_edit_ts_ms 수정 시각 (Unix Milliseconds, 서버 기준)
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import AnalysisResult


# ── 유틸 ──────────────────────────────────────────────────────────────────────

def ms_to_str(ms) -> str:
    """Unix Milliseconds → 'YYYY-MM-DD HH:MM:SS' (로컬 시간). 0·None → ''."""
    if not ms:
        return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).astimezone().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except Exception:
        return str(ms)


def col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    """테이블에 특정 컬럼이 존재하는지 확인."""
    try:
        return any(r[1] == col for r in conn.execute(f"PRAGMA table_info({table})").fetchall())
    except Exception:
        return False


def has_table(conn: sqlite3.Connection, name: str) -> bool:
    """해당 이름의 테이블이 존재하는지 확인."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def is_facebook_db(db_path: Path) -> bool:
    """client_messages 테이블이 존재하는 FB Messenger DB인지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        found = has_table(conn, "client_messages")
        conn.close()
        return found
    except Exception:
        return False


# ── 핵심 분석 ─────────────────────────────────────────────────────────────────

def analyze_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 DB 파일을 분석하여 result 에 행을 추가합니다.

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

    # ── client_messages 컬럼 유연 처리 ───────────────────────────────────────
    has_edit_count   = col_exists(conn, "client_messages", "edit_count")
    has_sender_id    = col_exists(conn, "client_messages", "sender_contact_pk")
    has_timestamp_ms = col_exists(conn, "client_messages", "authoritative_ts_ms")
    has_thread_key   = col_exists(conn, "client_messages", "thread_pk")

    select_cols = ["pk", "text"]
    if has_edit_count:   select_cols.append("edit_count")
    if has_sender_id:    select_cols.append("sender_contact_pk")
    if has_timestamp_ms: select_cols.append("authoritative_ts_ms")
    if has_thread_key:   select_cols.append("thread_pk")

    try:
        raw_msgs = conn.execute(
            f"SELECT {', '.join(select_cols)} FROM client_messages ORDER BY rowid"
        ).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"client_messages 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    # ── client_edit_message_history 로드 ────────────────────────────────────
    # { original_message_pk → [ {message_content, server_adjusted_edit_ts_ms}, … ] }
    history_map: dict[str, list[dict]] = {}

    if has_table(conn, "client_edit_message_history"):
        has_edit_ts = col_exists(conn, "client_edit_message_history", "server_adjusted_edit_ts_ms")
        hist_cols = ["pk", "original_message_pk", "message_content"]
        if has_edit_ts:
            hist_cols.append("server_adjusted_edit_ts_ms")
        try:
            for hr in conn.execute(
                f"SELECT {', '.join(hist_cols)} FROM client_edit_message_history ORDER BY rowid"
            ).fetchall():
                key = str(hr["original_message_pk"] or "")
                history_map.setdefault(key, []).append(dict(hr))
        except sqlite3.Error as e:
            result.add_error(f"client_edit_message_history 쿼리 실패 [{db_path.name}]: {e}")

    conn.close()

    # ── 메시지별 분석 ─────────────────────────────────────────────────────────
    for msg in raw_msgs:
        total_msgs += 1

        msg_pk     = str(msg["pk"] or "")
        text       = str(msg["text"] or "")
        edit_count = int(msg["edit_count"]) if has_edit_count and msg["edit_count"] is not None else 0
        sender_id  = str(msg["sender_contact_pk"] or "") if has_sender_id else ""
        sent_ts    = ms_to_str(msg["authoritative_ts_ms"] if has_timestamp_ms else None)
        thread_key = str(msg["thread_pk"] or "") if has_thread_key else ""
        has_edit   = edit_count > 0

        histories = sorted(
            history_map.get(msg_pk, []),
            key=lambda h: h.get("server_adjusted_edit_ts_ms") or 0
        )

        if has_edit:
            total_modified += 1

        last_edit_ts = ms_to_str(histories[-1].get("server_adjusted_edit_ts_ms") or "") if histories else ""

        row_index = len(all_rows)
        all_rows.append([
            msg_pk,
            sender_id,
            thread_key,
            sent_ts,
            last_edit_ts,
            str(edit_count) if has_edit else "",
            text,
            db_path.name,
        ])

        if has_edit:
            hl_indices.add(row_index)

        # ── 서브 행: 수정 이력 ────────────────────────────────────────────────
        # 실제 DB 구조:
        #   history 레코드의 message_content = 해당 시각에 저장된 버전의 내용
        #   history 레코드의 server_adjusted_edit_ts_ms = 해당 버전의 시각
        #   histories[0] → 원본 (전송 시각)
        #   histories[1] → 수정 1본 (수정 1 시각)
        #   histories[N-1] → 수정 N본 = 최종본 (edit_count == N-1 이므로 text와 동일)
        #   따라서 text를 별도로 추가하면 중복 → histories만으로 모든 버전 표시
        if has_edit and histories:
            children: list[list[str]] = []

            for i, hr in enumerate(histories):
                content  = str(hr.get("message_content") or "")
                ts_val   = hr.get("server_adjusted_edit_ts_ms") or ""
                ts_str   = ms_to_str(ts_val)

                if i == 0:
                    # 원본: 전송 시각 칸에 표시, 수정 시각 칸은 공백
                    label    = "  ↳ 원본"
                    sent_col = ts_str
                    edit_col = ""
                else:
                    # 수정 i본: 수정 시각 칸에 표시, 전송 시각 칸은 공백
                    label    = f"  ↳ 수정 {i}"
                    sent_col = ""
                    edit_col = ts_str

                children.append([
                    label, "", "",
                    sent_col,  # [3] 전송 시각
                    edit_col,  # [4] 수정 시각
                    "",
                    content,
                    "",
                ])

            sub_rows[row_index] = children

        elif has_edit:
            # edit_count > 0이지만 히스토리 레코드 없는 예외 케이스
            sub_rows[row_index] = [[
                "  ↳ 수정 이력", "", "", "", "", "",
                f"수정 {edit_count}회 확인 — 히스토리 레코드 없음 (삭제되었거나 동기화 미완료)",
                "",
            ]]

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 PK", "발신자 ID (sender_contact_pk)", "대화방 (thread_pk)",
            "전송 시각", "마지막 수정 시각", "수정 횟수",
            "앱 내 표시 메시지 (최종본)", "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=hl_indices,
        sub_rows=sub_rows,
    )

    return total_msgs, total_modified
