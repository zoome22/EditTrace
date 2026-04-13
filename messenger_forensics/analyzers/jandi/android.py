"""
jandi/android.py - Jandi Android 분석기

DB 경로  : /data/data/com.tosslab.jandi.app/databases/jandi-v2.db
           (평문 SQLite, 복호화 불필요)

핵심 테이블 및 연계 구조:
  message_text (메시지 메타데이터)
    id           서버 메시지 ID (PK 역할)
    content_id → message_text_content._id (외래키)
    isEdited     수정 여부 (0=원본, 1=수정됨)
    createTime   원본 전송 시각 (Unix Milliseconds)
    updateTime   최종 수정 시각 (Unix Milliseconds)
    writerId     발신자 ID
    feedbackId   채널(피드백) ID
    teamId       팀 ID
    contentType  메시지 유형 (text 등)
    status       메시지 상태 (created 등)

  message_text_content (메시지 본문)
    _id          PK (← message_text.content_id 가 참조)
    body         메시지 본문 (평문)
    textMessage_id  → message_text.id (역참조)

  message_shared (인용 답장 메타데이터)
    id           인용 원본 메시지 ID (수정된 메시지와 동일 id)
    content_id → message_shared_content._id (외래키)
    isEdited     인용 시점 수정 여부
    messageId    답장한 메시지 ID
    writerId     원본 메시지 발신자 ID

  message_shared_content (인용 시점 본문 스냅샷)
    _id               PK
    body              답장 시점 인용 본문 (수정 중간본 보조 증거)
    sharedMessage_id  답장한 메시지 ID

수정 이력 추적 로직:
  1. message_text JOIN message_text_content ON content_id = _id
  2. isEdited=1 → 수정된 메시지 (최종 수정본 본문만 존재)
  3. 원본 메시지 저장 없음 → 원본 복원 불가
  4. message_shared / message_shared_content
     → 답장 인용 시점의 메시지 본문 스냅샷으로 중간본 보조 복원 가능
  5. 2회 이상 수정 시 횟수·이력 추적 불가

타임스탬프:
  createTime / updateTime = Unix Milliseconds (/1000 → Unix 초)
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 상수 ─────────────────────────────────────────────────────────────────────

_DB_NAME = "jandi-v2.db"


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


def _is_jandi_android_db(db_path: Path) -> bool:
    """message_text + message_text_content 테이블이 모두 존재하는지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        ok = (
            _has_table(conn, "message_text")
            and _has_table(conn, "message_text_content")
        )
        conn.close()
        return ok
    except Exception:
        return False


def _find_db_files(path: Path) -> list[Path]:
    """jandi-v2.db 후보 파일 목록 반환."""
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

def _analyze_android_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 jandi-v2.db 분석.

    연계:
      message_text.content_id = message_text_content._id
      message_shared.id       = 수정된 message_text.id (동일 메시지 ID)
      message_shared.content_id = message_shared_content._id

    Returns:
        (total_msgs, total_modified)
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
        return 0, 0

    if not _has_table(conn, "message_text"):
        conn.close()
        result.add_error(f"message_text 테이블 없음 [{db_path.name}]")
        return 0, 0

    has_shared = (
        _has_table(conn, "message_shared")
        and _has_table(conn, "message_shared_content")
    )

    # ── ① 메인 메시지 JOIN ─────────────────────────────────────────────────────
    # message_text.content_id → message_text_content._id
    # contentType='text' 인 실제 메시지만 대상 (시스템 이벤트 제외 가능하나 포함)
    try:
        main_rows = conn.execute("""
            SELECT
                t.id          AS msg_id,
                t.isEdited    AS is_edited,
                t.createTime  AS create_time,
                t.updateTime  AS update_time,
                t.writerId    AS writer_id,
                t.feedbackId  AS feedback_id,
                t.teamId      AS team_id,
                t.contentType AS content_type,
                t.status      AS status,
                c.body        AS body
            FROM message_text t
            JOIN message_text_content c ON t.content_id = c._id
            ORDER BY t.createTime
        """).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"메시지 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    # ── ② 인용 답장 스냅샷 맵 구성 ────────────────────────────────────────────
    # message_shared.id = 원본(수정된) 메시지 ID
    # message_shared.content_id = message_shared_content._id
    # message_shared_content.body = 답장 시점의 인용 본문 (수정 중간본 보조)
    # message_shared.messageId = 답장한 메시지 ID
    # {수정된_msg_id: [{quoted_body, reply_msg_id, reply_create_time}, ...]}
    quoted_map: dict[int, list[dict]] = {}
    if has_shared:
        try:
            quoted_rows = conn.execute("""
                SELECT
                    s.id           AS shared_msg_id,
                    s.messageId    AS reply_msg_id,
                    sc.body        AS quoted_body,
                    tr.createTime  AS reply_create_time
                FROM message_shared s
                JOIN message_shared_content sc ON s.content_id = sc._id
                LEFT JOIN message_text tr ON s.messageId = tr.id
                ORDER BY s.rowid
            """).fetchall()
            for qr in quoted_rows:
                zid = qr["shared_msg_id"]
                if zid not in quoted_map:
                    quoted_map[zid] = []
                quoted_map[zid].append({
                    "quoted_body":       str(qr["quoted_body"] or ""),
                    "reply_msg_id":      qr["reply_msg_id"],
                    "reply_create_time": qr["reply_create_time"],
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
        total_msgs += 1
        msg_id    = msg["msg_id"]
        is_edited = bool(msg["is_edited"])

        if is_edited:
            total_modified += 1

        body        = str(msg["body"] or "")
        create_ts   = _ms_to_str(msg["create_time"])
        update_ts   = _ms_to_str(msg["update_time"]) if is_edited else ""
        writer_id   = str(msg["writer_id"] or "")
        feedback_id = (
            str(msg["feedback_id"])
            if msg["feedback_id"] is not None and msg["feedback_id"] != -1
            else ""
        )

        row_index = len(all_rows)
        all_rows.append([
            str(msg_id),
            writer_id,
            feedback_id,
            create_ts,
            update_ts,
            "O" if is_edited else "",
            body,
            db_path.name,
        ])

        if is_edited:
            hl_indices.add(row_index)

        # ── 서브행: 수정 이력 ─────────────────────────────────────────────────
        if not is_edited:
            continue

        children: list[list[str]] = []

        # 원본 메시지 미저장 → 원본 복원 불가
        children.append([
            "  ↳ 원본",
            "", "",
            create_ts, "", "",
            "원본 저장 없음 — Android Jandi는 수정 전 본문을 보존하지 않음",
            "",
        ])

        # 인용 답장 스냅샷 — 수정 중간본 보조 증거
        for qi, qitem in enumerate(quoted_map.get(msg_id, []), start=1):
            reply_ts = _ms_to_str(qitem["reply_create_time"])
            children.append([
                f"  ↳ 인용 스냅샷 {qi} (답장 시점 본문)",
                "", "",
                "", reply_ts, "",
                qitem["quoted_body"],
                f"답장 메시지 ID: {qitem['reply_msg_id']}",
            ])

        # 최종 수정본
        children.append([
            "  ↳ 최종 수정본",
            "", "",
            "", update_ts, "",
            body, "",
        ])

        sub_rows[row_index] = children

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 ID (id)",
            "발신자 ID (writerId)",
            "채널 ID (feedbackId)",
            "전송 시각 (createTime)",
            "수정 시각 (updateTime)",
            "수정 여부 (isEdited)",
            "메시지 본문 (body)",
            "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=hl_indices,
        sub_rows=sub_rows,
    )

    return total_msgs, total_modified


# ── 분석기 클래스 ──────────────────────────────────────────────────────────────

class JandiAndroidAnalyzer(BaseAnalyzer):
    """
    Jandi Android 수정 메시지 분석기.

    수집 경로:
      /data/data/com.tosslab.jandi.app/databases/jandi-v2.db

    평문 SQLite, 복호화 불필요.

    분석 핵심:
      message_text ─(content_id = _id)→ message_text_content
        body       : 메시지 본문 (최종 수정본 또는 원본)
        isEdited=1 : 수정된 메시지
        createTime : 원본 전송 시각 (Unix Milliseconds)
        updateTime : 최종 수정 시각 (Unix Milliseconds)

      인용 스냅샷 (수정 중간본 보조):
        message_shared ─(content_id = _id)→ message_shared_content
        → 답장 인용 시점의 메시지 본문 스냅샷

      ※ 원본 메시지 미저장 → 수정 전 본문 복원 불가
      ※ 2회 이상 수정 시 중간 수정 횟수/이력 추적 불가
         (인용 답장 스냅샷으로 중간본 부분 복원 가능)
    """

    MESSENGER = "Jandi"
    PLATFORM  = "Android"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        candidates = _find_db_files(path)
        db_files   = [f for f in candidates if _is_jandi_android_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"message_text / message_text_content 테이블을 포함한 "
                f"Jandi Android DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /data/data/com.tosslab.jandi.app/databases/jandi-v2.db"
            )
            return result

        total_msgs = total_modified = 0
        for db_path in db_files:
            m, e = _analyze_android_db(db_path, result)
            total_msgs     += m
            total_modified += e

        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정된 메시지"]    = str(total_modified)
        result.summary["수정 여부 칼럼"]   = "message_text.isEdited (0/1)"
        result.summary["최종 수정 본문"]   = "message_text_content.body (isEdited=1)"
        result.summary["원본 복원"]        = "불가 (Android Jandi 수정 전 본문 미저장)"
        result.summary["전송 시각"]        = "message_text.createTime (Unix Milliseconds)"
        result.summary["수정 시각"]        = "message_text.updateTime (Unix Milliseconds)"
        result.summary["중간본 보조 증거"] = "message_shared_content.body → 답장 인용 시점 스냅샷"
        result.summary["수집 방법"]        = "논리/물리 백업"

        return result
