"""
whatsapp/android.py - WhatsApp Android 분석기

DB 경로  : /data/data/com.whatsapp/database/msgstore.db
           (평문 SQLite, 복호화 불필요)

핵심 테이블 및 연계 구조:
  message (메시지 본문 및 메타데이터)
    _id              행 고유 ID (다른 테이블의 외래키)
    chat_row_id    → chat._id (대화방)
    from_me          발신 방향 (1=내가 보냄, 0=받은 메시지)
    key_id           메시지 키 (수정 시 변경됨)
    sender_jid_row_id → jid._id (발신자, 그룹 메시지에서 사용)
    status           메시지 상태 (5=수정됨, 6=전달 완료 등)
    timestamp        전송/최종수정 시각 (Unix Milliseconds)
    text_data        메시지 본문 (수정 시 최종 수정본으로 덮어씌워짐)
    message_type     메시지 유형 (0=텍스트 등)

  message_edit_info (수정 메타데이터)
    message_row_id → message._id (외래키)
    original_key_id  원본 메시지의 key_id (수정 전 key)
    edited_timestamp 최종 수정 시각 (Unix Milliseconds)
    sender_timestamp 발신자 기준 수정 시각 (Unix Milliseconds)

  message_add_on (수정 이벤트 참조)
    parent_message_row_id → message._id
    key_id               수정 시점의 원본 key_id
    timestamp            수정 시각 (Unix Milliseconds)
    message_add_on_type  이벤트 유형 (74=수정 이벤트)

  chat (대화방 정보)
    _id            행 고유 ID
    jid_row_id   → jid._id
    subject        그룹 채팅방 이름

  jid (연락처/대화방 식별자)
    _id            행 고유 ID
    raw_string     JID 문자열 (예: 821012345678@s.whatsapp.net)

수정 이력 추적 로직:
  1. message LEFT JOIN message_edit_info ON _id = message_row_id
  2. status=5 OR edited_timestamp IS NOT NULL → 수정된 메시지
  3. 원본 메시지 미저장 → 수정 전 본문 복원 불가
  4. message_edit_info.original_key_id → 수정 전 key_id 식별 가능
  5. message_add_on (type=74) → 수정 이벤트 참조 기록
  6. 다회 수정 시 최종 상태만 저장됨

타임스탬프:
  timestamp / edited_timestamp = Unix Milliseconds (/1000 → Unix 초)
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 상수 ─────────────────────────────────────────────────────────────────────

_DB_NAME     = "msgstore.db"
_STATUS_EDIT = 5          # message.status 값: 수정된 메시지
_ADDON_EDIT  = 74         # message_add_on.message_add_on_type: 수정 이벤트


# ── 타임스탬프 유틸 ────────────────────────────────────────────────────────────

def _ms_to_str(val) -> str:
    """Unix Milliseconds → 'YYYY-MM-DD HH:MM:SS' (로컬 시간)."""
    if val is None:
        return ""
    try:
        return (
            datetime.fromtimestamp(int(val) / 1000, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M:%S")
        )
    except Exception:
        return str(val)


# ── DB 유틸 ──────────────────────────────────────────────────────────────────

def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _is_whatsapp_android_db(db_path: Path) -> bool:
    """message + message_edit_info 테이블이 존재하는 WhatsApp DB인지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        ok = (
            _has_table(conn, "message")
            and _has_table(conn, "message_edit_info")
        )
        conn.close()
        return ok
    except Exception:
        return False


def _find_db_files(path: Path) -> list[Path]:
    """msgstore.db 후보 파일 목록 반환."""
    if path.is_file():
        return [path]
    results: list[Path] = []
    for f in sorted(path.rglob(_DB_NAME)):
        results.append(f)
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        for f in sorted(path.rglob(ext)):
            if f not in results:
                results.append(f)
    return results


# ── 핵심 분석 ─────────────────────────────────────────────────────────────────

def _analyze_whatsapp_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 msgstore.db 분석.

    수정 메시지 식별:
      message.status = 5  OR  message_edit_info.message_row_id IS NOT NULL

    Returns:
        (total_msgs, total_modified)
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
        return 0, 0

    if not _has_table(conn, "message"):
        conn.close()
        result.add_error(f"message 테이블 없음 [{db_path.name}]")
        return 0, 0

    has_edit_info = _has_table(conn, "message_edit_info")
    has_add_on    = _has_table(conn, "message_add_on")
    has_chat      = _has_table(conn, "chat")
    has_jid       = _has_table(conn, "jid")

    # ── ① 메인 메시지 JOIN ─────────────────────────────────────────────────────
    # message LEFT JOIN message_edit_info → 수정 메타데이터 병합
    # message LEFT JOIN chat + jid → 대화방명 / 발신자 JID
    try:
        edit_join = (
            "LEFT JOIN message_edit_info ei ON m._id = ei.message_row_id"
            if has_edit_info else ""
        )
        chat_join = (
            "LEFT JOIN chat c ON m.chat_row_id = c._id "
            "LEFT JOIN jid cj ON c.jid_row_id = cj._id"
            if has_chat and has_jid else ""
        )
        sender_join = (
            "LEFT JOIN jid sj ON m.sender_jid_row_id = sj._id"
            if has_jid else ""
        )

        ei_cols = (
            "ei.original_key_id, ei.edited_timestamp, ei.sender_timestamp,"
            if has_edit_info else
            "NULL AS original_key_id, NULL AS edited_timestamp, NULL AS sender_timestamp,"
        )
        chat_cols = (
            "cj.raw_string AS chat_jid, c.subject AS chat_subject,"
            if has_chat and has_jid else
            "NULL AS chat_jid, NULL AS chat_subject,"
        )
        sender_col = (
            "sj.raw_string AS sender_jid"
            if has_jid else
            "NULL AS sender_jid"
        )

        main_rows = conn.execute(f"""
            SELECT
                m._id, m.chat_row_id, m.from_me, m.key_id,
                m.status, m.timestamp, m.text_data, m.message_type,
                {ei_cols}
                {chat_cols}
                {sender_col}
            FROM message m
            {edit_join}
            {chat_join}
            {sender_join}
            WHERE m.message_type IN (0, 7)   -- 0=텍스트, 7=시스템/그룹 이름 포함
               OR ei.message_row_id IS NOT NULL
            ORDER BY m.timestamp
        """).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"메시지 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    # ── ② message_add_on 수정 이벤트 맵 ───────────────────────────────────────
    # {message._id: [{"key_id": str, "timestamp": int}, ...]}
    addon_map: dict[int, list[dict]] = {}
    if has_add_on:
        try:
            addon_rows = conn.execute(f"""
                SELECT parent_message_row_id, key_id, timestamp
                FROM message_add_on
                WHERE message_add_on_type = {_ADDON_EDIT}
                ORDER BY timestamp
            """).fetchall()
            for ar in addon_rows:
                mid = ar["parent_message_row_id"]
                if mid not in addon_map:
                    addon_map[mid] = []
                addon_map[mid].append({
                    "key_id":    ar["key_id"],
                    "timestamp": ar["timestamp"],
                })
        except sqlite3.Error:
            pass

    conn.close()

    # ── 결과 테이블 구성 ──────────────────────────────────────────────────────
    all_rows:   list[list[str]] = []
    hl_indices: set[int]        = set()
    sub_rows:   dict[int, list] = {}
    total_msgs     = 0
    total_modified = 0

    for msg in main_rows:
        msg_id       = msg["_id"]
        is_edited    = (msg["status"] == _STATUS_EDIT
                        or msg["edited_timestamp"] is not None)

        # text_data가 없고 수정도 아닌 경우 → 미디어/시스템 메시지 등, 포함
        body      = str(msg["text_data"] or "")
        send_ts   = _ms_to_str(msg["timestamp"])
        edit_ts   = _ms_to_str(msg["edited_timestamp"]) if is_edited else ""
        from_me   = "→ 발신" if msg["from_me"] else "← 수신"

        # 대화방 식별자: subject(그룹명) 우선, 없으면 JID
        chat_subject = msg["chat_subject"] or ""
        chat_jid     = msg["chat_jid"]     or ""
        chat_label   = chat_subject if chat_subject else chat_jid

        # 발신자: 그룹 메시지 수신 시 sender_jid 사용, 1:1은 chat_jid
        sender_jid = msg["sender_jid"] or ""

        orig_key_id = msg["original_key_id"] or ""
        curr_key_id = msg["key_id"]           or ""

        if not body and not is_edited:
            # 비텍스트/시스템 메시지는 포함하지 않음
            continue

        total_msgs += 1
        if is_edited:
            total_modified += 1

        row_index = len(all_rows)
        all_rows.append([
            str(msg_id),
            chat_label,
            from_me,
            sender_jid if sender_jid else ("나" if msg["from_me"] else chat_jid),
            send_ts,
            edit_ts,
            "O" if is_edited else "",
            body,
            db_path.name,
        ])

        if is_edited:
            hl_indices.add(row_index)

        # ── 서브행: 수정 상세 ─────────────────────────────────────────────────
        if not is_edited:
            continue

        children: list[list[str]] = []

        # 원본 미저장
        children.append([
            "  ↳ 원본",
            "", "", "",
            send_ts, "", "",
            "원본 저장 없음 — WhatsApp Android는 수정 전 본문을 보존하지 않음",
            "",
        ])

        # 원본 key_id (수정 전 메시지 식별자)
        if orig_key_id:
            children.append([
                "  ↳ 원본 key_id",
                "", "", "", "", "", "",
                orig_key_id,
                "",
            ])

        # message_add_on 수정 이벤트
        for ao in addon_map.get(msg_id, []):
            children.append([
                "  ↳ 수정 이벤트 (message_add_on)",
                "", "", "",
                "", _ms_to_str(ao["timestamp"]), "",
                f"key_id: {ao['key_id']}",
                "",
            ])

        # 최종 수정본
        children.append([
            "  ↳ 최종 수정본",
            "", "", "",
            "", edit_ts, "",
            body,
            "",
        ])

        sub_rows[row_index] = children

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 ID (_id)",
            "대화방",
            "방향",
            "발신자 (JID)",
            "전송 시각 (timestamp)",
            "수정 시각 (edited_timestamp)",
            "수정 여부 (status=5)",
            "메시지 본문 (text_data)",
            "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=hl_indices,
        sub_rows=sub_rows,
    )

    return total_msgs, total_modified


# ── 분석기 클래스 ──────────────────────────────────────────────────────────────

class WhatsAppAndroidAnalyzer(BaseAnalyzer):
    """
    WhatsApp Android 수정 메시지 분석기.

    수집 경로:
      /data/data/com.whatsapp/database/msgstore.db

    평문 SQLite, 복호화 불필요.

    분석 핵심:
      message ─(LEFT JOIN)→ message_edit_info
        text_data        : 메시지 본문 (최종 수정본으로 덮어쓰임)
        status=5         : 수정된 메시지 식별자
        timestamp        : 전송 시각 (최초 전송 시각, Unix Milliseconds)
        edited_timestamp : 최종 수정 시각 (Unix Milliseconds)
        original_key_id  : 수정 전 원본 key_id

      message_add_on (type=74):
        → 수정 이벤트 참조 기록 (수정 시점 key_id / timestamp)

      ※ 원본 메시지 미저장 → 수정 전 본문 복원 불가
      ※ 다회 수정 시 최종 상태만 저장됨
    """

    MESSENGER = "WhatsApp"
    PLATFORM  = "Android"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        candidates = _find_db_files(path)
        db_files   = [f for f in candidates if _is_whatsapp_android_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"message / message_edit_info 테이블을 포함한 "
                f"WhatsApp Android DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /data/data/com.whatsapp/database/msgstore.db"
            )
            return result

        total_msgs = total_modified = 0
        for db_path in db_files:
            m, e = _analyze_whatsapp_db(db_path, result)
            total_msgs     += m
            total_modified += e

        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정된 메시지"]    = str(total_modified)
        result.summary["수정 식별 방법"]   = "message.status=5 / message_edit_info 참조"
        result.summary["최종 수정 본문"]   = "message.text_data (덮어쓰기)"
        result.summary["원본 복원"]        = "불가 (수정 전 본문 미저장)"
        result.summary["원본 key_id"]      = "message_edit_info.original_key_id"
        result.summary["수정 시각"]        = "message_edit_info.edited_timestamp (Unix ms)"
        result.summary["수정 이벤트 참조"] = "message_add_on.message_add_on_type=74"
        result.summary["수집 방법"]        = "논리/물리 백업"

        return result
