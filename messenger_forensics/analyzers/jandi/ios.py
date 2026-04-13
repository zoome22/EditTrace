"""
jandi/ios.py - Jandi iOS 분석기

DB 경로  : /AppDomain-com.jandi.toss/Library/Application Support/JandiCoreData.sqlite
           (논리 백업으로 수집 가능 / 평문 SQLite, 복호화 불필요)

핵심 테이블 및 연계 구조:
  ZCONTENTMO (메시지 본문)
    Z_PK             행 고유 ID
    ZTEXTMESSAGE   → ZRESMESSAGES_ORIGINALMESSAGEMO.Z_PK (외래키)
    ZBODY2           메시지 본문 (평문)
    ZSHAREDMESSAGEOF → ZSHAREDMESSAGEMO.Z_PK (답장 인용 시)

  ZRESMESSAGES_ORIGINALMESSAGEMO (메시지 메타데이터)
    Z_PK           행 고유 ID
    ZID            서버 메시지 ID
    ZISEDITED      수정 여부 (0=원본, 1=수정됨)
    ZCREATEDAT     원본 전송 시각 (Apple Absolute Time, 초)
    ZUPDATEDAT     최종 수정 시각 (Apple Absolute Time, 초)
    ZWRITERID      발신자 ID
    ZFEEDBACKID    채널(피드백) ID

  ZSHAREDMESSAGEMO (답장 인용 스냅샷 — 수정 이력 보조 증거)
    ZID            원본 메시지 서버 ID (수정된 메시지의 ZID와 동일)
    ZCONTENT     → ZCONTENTMO.Z_PK (답장 시점 인용 본문)
    ZTEXTMESSAGE → ZRESMESSAGES_ORIGINALMESSAGEMO.Z_PK (답장한 메시지 메타)
    ZCREATEDAT     원본 메시지 전송 시각

수정 이력 추적 로직:
  1. ZCONTENTMO.ZTEXTMESSAGE → ZRESMESSAGES_ORIGINALMESSAGEMO.Z_PK JOIN
  2. ZISEDITED=1 레코드 = 수정된 메시지 (최종 수정본 본문)
  3. 동일 ZID에 ZISEDITED=0 레코드 별도 존재 → 원본 전송 내용 스냅샷
  4. 동일 ZID에 복수 레코드 존재 가능 (앱 캐싱) → ZID+ZISEDITED 기준 dedup
  5. ZSHAREDMESSAGEMO → 답장 인용 시점의 수정 중간본 스냅샷 (보조 증거)
  6. 2회 이상 수정 시 횟수·이력 추적 불가
     (단, 인용 답장 스냅샷으로 중간본 부분 복원 가능)

타임스탬프:
  ZCREATEDAT / ZUPDATEDAT = Apple Absolute Time (Cocoa epoch, 2001-01-01, 초)
  Unix 변환: 값 + 978307200
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 상수 ─────────────────────────────────────────────────────────────────────

_COCOA_EPOCH_OFFSET = 978_307_200   # 2001-01-01 − 1970-01-01 (초)
_DB_NAME = "JandiCoreData.sqlite"


# ── 타임스탬프 유틸 ────────────────────────────────────────────────────────────

def _cocoa_to_str(val) -> str:
    """Apple Absolute Time (Cocoa epoch, 초) → 'YYYY-MM-DD HH:MM:SS' (로컬 시간)."""
    if val is None:
        return ""
    try:
        unix_ts = float(val) + _COCOA_EPOCH_OFFSET
        return (
            datetime.fromtimestamp(unix_ts, tz=timezone.utc)
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


def _is_jandi_db(db_path: Path) -> bool:
    """ZCONTENTMO + ZRESMESSAGES_ORIGINALMESSAGEMO 테이블이 모두 존재하는지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        ok = (
            _has_table(conn, "ZCONTENTMO")
            and _has_table(conn, "ZRESMESSAGES_ORIGINALMESSAGEMO")
        )
        conn.close()
        return ok
    except Exception:
        return False


def _find_db_files(path: Path) -> list[Path]:
    """JandiCoreData.sqlite 후보 파일 목록 반환."""
    if path.is_file():
        return [path]
    results: list[Path] = []
    for f in sorted(path.rglob(_DB_NAME)):
        results.append(f)
    for ext in ("*.sqlite", "*.db", "*.sqlite3"):
        for f in sorted(path.rglob(ext)):
            if f not in results:
                results.append(f)
    return results


# ── 핵심 분석 ─────────────────────────────────────────────────────────────────

def _analyze_jandi_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 JandiCoreData.sqlite 분석.

    dedup 전략:
      동일 ZID+ZISEDITED 조합에 복수 ZRESMESSAGES_ORIGINALMESSAGEMO 행이
      존재할 수 있음(앱 캐싱 아티팩트) → Z_PK DESC 기준 1개만 선택.
      ZISEDITED=1(수정본)과 ZISEDITED=0(원본 스냅샷)은 별도 행으로 유지.

    Returns:
        (total_msgs, total_modified)
    """
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
        return 0, 0

    if not _has_table(conn, "ZCONTENTMO"):
        conn.close()
        result.add_error(f"ZCONTENTMO 테이블 없음 [{db_path.name}]")
        return 0, 0

    has_shared = _has_table(conn, "ZSHAREDMESSAGEMO")

    # ── ① 메인 메시지 JOIN + dedup ──────────────────────────────────────────
    # ZID+ZISEDITED 기준 중복 제거 (Z_PK DESC = 최신 레코드 우선)
    # Date 구분자 및 ZID≤1 더미 행 제외
    try:
        main_rows = conn.execute("""
            WITH ranked AS (
                SELECT
                    c.Z_PK          AS content_pk,
                    c.ZBODY2        AS body,
                    m.Z_PK          AS orig_pk,
                    m.ZID           AS msg_id,
                    m.ZISEDITED     AS is_edited,
                    m.ZCREATEDAT    AS created_at,
                    m.ZUPDATEDAT    AS updated_at,
                    m.ZWRITERID     AS writer_id,
                    m.ZFEEDBACKID   AS feedback_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.ZID, m.ZISEDITED
                        ORDER BY m.Z_PK DESC
                    ) AS rn
                FROM ZCONTENTMO c
                JOIN ZRESMESSAGES_ORIGINALMESSAGEMO m ON c.ZTEXTMESSAGE = m.Z_PK
                WHERE m.ZCONTENTTYPE != 'Date'
                  AND m.ZID > 1
            )
            SELECT * FROM ranked
            WHERE rn = 1
            ORDER BY created_at, msg_id, is_edited DESC
        """).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"메시지 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    # ── ② 원본 스냅샷 맵 구성 ─────────────────────────────────────────────────
    # {msg_id(ZID): (body, created_at)} — ZISEDITED=0 행
    snap_map: dict[int, tuple[str, float | None]] = {}
    for row in main_rows:
        if not row["is_edited"]:
            snap_map[row["msg_id"]] = (
                str(row["body"] or ""),
                row["created_at"],
            )

    # ── ③ 답장 인용 스냅샷 맵 구성 ────────────────────────────────────────────
    # {수정된_msg_id: [{quoted_body, reply_msg_id, reply_created_at}, ...]}
    quoted_map: dict[int, list[dict]] = {}
    if has_shared:
        try:
            quoted_rows = conn.execute("""
                SELECT
                    s.ZID               AS shared_msg_id,
                    c_quot.ZBODY2       AS quoted_body,
                    m_reply.ZID         AS reply_msg_id,
                    m_reply.ZCREATEDAT  AS reply_created_at
                FROM ZSHAREDMESSAGEMO s
                LEFT JOIN ZCONTENTMO c_quot ON s.ZCONTENT = c_quot.Z_PK
                LEFT JOIN ZRESMESSAGES_ORIGINALMESSAGEMO m_reply
                       ON s.ZTEXTMESSAGE = m_reply.Z_PK
                ORDER BY s.Z_PK
            """).fetchall()
            for qr in quoted_rows:
                zid = qr["shared_msg_id"]
                if zid not in quoted_map:
                    quoted_map[zid] = []
                quoted_map[zid].append({
                    "quoted_body":      str(qr["quoted_body"] or ""),
                    "reply_msg_id":     qr["reply_msg_id"],
                    "reply_created_at": qr["reply_created_at"],
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

    # 이미 본행으로 출력한 ZID 추적 (수정본 우선 → 원본 스냅샷 별도 출력 방지)
    seen_zids: set[int] = set()

    for msg in main_rows:
        msg_id    = msg["msg_id"]
        is_edited = bool(msg["is_edited"])

        # ZISEDITED=0 행은 수정본(ZISEDITED=1)이 있으면 서브행에서만 표시
        if not is_edited and msg_id in seen_zids:
            continue

        seen_zids.add(msg_id)
        total_msgs += 1
        if is_edited:
            total_modified += 1

        body        = str(msg["body"] or "")
        created_ts  = _cocoa_to_str(msg["created_at"])
        updated_ts  = _cocoa_to_str(msg["updated_at"]) if is_edited else ""
        writer_id   = str(msg["writer_id"] or "")
        feedback_id = (
            str(msg["feedback_id"])
            if msg["feedback_id"] and msg["feedback_id"] != -1
            else ""
        )

        row_index = len(all_rows)
        all_rows.append([
            str(msg_id),
            writer_id,
            feedback_id,
            created_ts,
            updated_ts,
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

        # 원본 스냅샷 (ZISEDITED=0, 동일 ZID)
        orig_body, orig_ts_raw = snap_map.get(msg_id, ("", None))
        orig_ts = _cocoa_to_str(orig_ts_raw)

        children.append([
            "  ↳ 원본 (최초 전송)",
            "", "",
            orig_ts if orig_ts else created_ts,
            "", "",
            orig_body if orig_body else "원본 스냅샷 없음",
            "",
        ])

        # 인용 답장 스냅샷 — 수정 중간본 보조 증거
        # 동일 수정 메시지를 여러 사람이 인용했을 경우 복수 등장 가능
        for qi, qitem in enumerate(quoted_map.get(msg_id, []), start=1):
            reply_ts = _cocoa_to_str(qitem["reply_created_at"])
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
            "", updated_ts, "",
            body, "",
        ])

        sub_rows[row_index] = children

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 ID (ZID)",
            "발신자 ID (ZWRITERID)",
            "채널 ID (ZFEEDBACKID)",
            "전송 시각 (ZCREATEDAT)",
            "수정 시각 (ZUPDATEDAT)",
            "수정 여부 (ZISEDITED)",
            "메시지 본문 (ZBODY2)",
            "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=hl_indices,
        sub_rows=sub_rows,
    )

    return total_msgs, total_modified


# ── 분석기 클래스 ──────────────────────────────────────────────────────────────

class JandiIOSAnalyzer(BaseAnalyzer):
    """
    Jandi iOS 수정 메시지 분석기.

    수집 경로 (논리 백업):
      /AppDomain-com.jandi.toss/Library/Application Support/JandiCoreData.sqlite

    논리 백업으로 수집 가능. 복호화 불필요 (평문 SQLite).

    분석 핵심:
      ZCONTENTMO ─(ZTEXTMESSAGE)→ ZRESMESSAGES_ORIGINALMESSAGEMO
        ZBODY2       : 메시지 본문
        ZISEDITED=1  : 수정된 메시지 (최종 수정본)
        ZCREATEDAT   : 원본 전송 시각 (Apple Absolute Time)
        ZUPDATEDAT   : 최종 수정 시각 (Apple Absolute Time)

      원본 스냅샷:
        동일 ZID에 ZISEDITED=0 레코드 별도 존재 → 원본 전송 내용

      인용 스냅샷 (수정 중간본 보조):
        ZSHAREDMESSAGEMO → 답장 인용 시점의 메시지 본문 스냅샷

      ※ 2회 이상 수정 시 중간 수정 횟수/이력 추적 불가
         (인용 답장 스냅샷으로 중간본 부분 복원 가능)
    """

    MESSENGER = "Jandi"
    PLATFORM  = "iOS"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        candidates = _find_db_files(path)
        db_files   = [f for f in candidates if _is_jandi_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"ZCONTENTMO / ZRESMESSAGES_ORIGINALMESSAGEMO 테이블을 포함한 "
                f"Jandi DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /AppDomain-com.jandi.toss/Library/"
                "Application Support/JandiCoreData.sqlite\n"
                "  ※ iOS Jandi DB는 논리 백업으로 수집 가능"
            )
            return result

        total_msgs = total_modified = 0
        for db_path in db_files:
            m, e = _analyze_jandi_db(db_path, result)
            total_msgs     += m
            total_modified += e

        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정된 메시지"]    = str(total_modified)
        result.summary["수정 여부 칼럼"]   = "ZRESMESSAGES_ORIGINALMESSAGEMO.ZISEDITED (0/1)"
        result.summary["원본 본문"]        = "ZCONTENTMO.ZBODY2 (ZISEDITED=0, 동일 ZID)"
        result.summary["최종 수정 본문"]   = "ZCONTENTMO.ZBODY2 (ZISEDITED=1)"
        result.summary["전송 시각"]        = "ZRESMESSAGES_ORIGINALMESSAGEMO.ZCREATEDAT (Cocoa epoch)"
        result.summary["수정 시각"]        = "ZRESMESSAGES_ORIGINALMESSAGEMO.ZUPDATEDAT (Cocoa epoch)"
        result.summary["중간본 보조 증거"] = "ZSHAREDMESSAGEMO → 답장 인용 시 스냅샷 저장"
        result.summary["수집 방법"]        = "논리 백업 가능"

        return result
