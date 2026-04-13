"""
instagram/ios.py - Instagram iOS 분석기

DB 경로  : /private/var/mobile/Containers/Data/Application/[UUID]/
             Library/Application Support/DirectSQLiteDatabase/[Account_ID].db
           (FFS 수집 필요, 논리 백업 불가 / 평문 SQLite, 별도 복호화 불필요)

messages 테이블 주요 칼럼:
  message_id   서버 메시지 ID (TEXT)
  thread_id    대화방 ID (TEXT)
  archive      BPlist(NSKeyedArchiver) 직렬화 BLOB
  class_name   Obj-C 클래스명 (예: IGDirectPublishedMessage)
  row_id       행 순번 (INTEGER)

archive(BPlist/NSKeyedArchiver) 객체 구조:
  IGDirectPublishedMessage (root, $objects[1])
    ├─ IGDirectPublishedMessageMetadata*metadata
    │    └─ NSDate*serverTimestamp   → 전송 시각 (NS.time, Cocoa epoch 초)
    │    └─ NSString*senderPk        → 발신자 ID
    ├─ IGDirectPublishedMessageContent*content
    │    ├─ NSString*string          → 앱 표시 최종 메시지
    │    └─ NSUInteger editCount     → 수정 횟수
    └─ NSArray<IGDirectMessageEditHistory*>*editHistory
         각 항목 (IGDirectMessageEditHistory):
           ├─ body      : NSString  — 수정 전 메시지 내용
           └─ timestamp : NSDate    — 해당 원본/직전 메시지 시각 (NS.time, Cocoa epoch 초)

타임스탬프:
  NSDate.NS.time = Apple Absolute Time (Cocoa epoch, 2001-01-01 기준, 초)
  Unix 변환: NS.time + 978307200
"""

import plistlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 상수 ─────────────────────────────────────────────────────────────────────

_COCOA_EPOCH_OFFSET = 978_307_200   # 2001-01-01 − 1970-01-01 (초)

# root 객체에서 각 하위 객체를 가리키는 실제 키 이름
_KEY_METADATA  = "IGDirectPublishedMessageMetadata*metadata"
_KEY_CONTENT   = "IGDirectPublishedMessageContent*content"
_KEY_EDIT_HIST = "NSArray<IGDirectMessageEditHistory *>*editHistory"

# metadata 내부 키
_KEY_SERVER_TS  = "NSDate*serverTimestamp"
_KEY_SENDER_PK  = "NSString*senderPk"

# content 내부 키
_KEY_STRING     = "NSString*string"
_KEY_EDIT_COUNT = "NSUIntegereditCount"

# editHistory 항목 내부 키
_KEY_BODY      = "body"
_KEY_TIMESTAMP = "timestamp"


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


# ── NSKeyedArchiver BPlist 파서 ───────────────────────────────────────────────

class _NSKeyedUnarchiver:
    """
    plistlib 기반 경량 NSKeyedArchiver 역직렬화기.

    구조:
      $archiver : "NSKeyedArchiver"
      $top      : {"root": UID(N)}
      $objects  : [
          "$null",   # 0 — null sentinel
          { ... },   # 1 — root 객체
          ...
      ]
    UID.data 값으로 $objects 배열을 인덱싱하여 역참조.
    """

    def __init__(self, data: bytes) -> None:
        try:
            self._pl = plistlib.loads(data)
        except Exception as e:
            raise ValueError(f"BPlist 파싱 실패: {e}") from e
        if not isinstance(self._pl, dict):
            raise ValueError("최상위 객체가 dict가 아님")
        if self._pl.get("$archiver") != "NSKeyedArchiver":
            raise ValueError(f"NSKeyedArchiver가 아님: {self._pl.get('$archiver')!r}")
        self._objects: list = self._pl.get("$objects", [])
        self._cache: dict[int, Any] = {}

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _resolve(self, obj: Any) -> Any:
        """UID 레퍼런스를 재귀 역참조하여 실제 값 반환."""
        if isinstance(obj, plistlib.UID):
            idx = obj.data
            if idx == 0:          # $null sentinel
                return None
            if idx in self._cache:
                return self._cache[idx]
            raw = self._objects[idx]
            self._cache[idx] = None   # 순환 참조 방지
            result = self._resolve(raw)
            self._cache[idx] = result
            return result
        if isinstance(obj, dict):
            return self._decode_dict(obj)
        if isinstance(obj, list):
            return [self._resolve(item) for item in obj]
        return obj

    def _classname(self, cls_ref) -> str:
        """$class UID → 최상위 클래스명."""
        if cls_ref is None:
            return ""
        try:
            obj = self._resolve(cls_ref)
            if isinstance(obj, dict):
                names = obj.get("$classes", [])
                return str(names[0]) if names else str(obj.get("$classname", ""))
        except Exception:
            pass
        return ""

    def _decode_dict(self, obj: dict) -> Any:
        """$class 기반 클래스별 디코딩."""
        cls = self._classname(obj.get("$class"))

        if cls in ("NSString", "NSMutableString", "__NSCFString",
                   "NSTaggedPointerString"):
            return obj.get("NS.string", "")

        if cls == "NSDate":
            return {"__type__": "NSDate", "NS.time": obj.get("NS.time")}

        if cls in ("NSNumber", "__NSCFNumber", "__NSCFBoolean"):
            for k in ("NS.intval", "NS.uintval", "NS.longval",
                      "NS.longlongval", "NS.doubleval", "NS.floatval"):
                if k in obj:
                    return obj[k]
            return None

        if cls in ("NSArray", "NSMutableArray", "__NSArrayI",
                   "__NSArrayM", "__NSFrozenArrayM"):
            return [self._resolve(v) for v in obj.get("NS.objects", [])]

        if cls in ("NSDictionary", "NSMutableDictionary",
                   "__NSDictionaryI", "__NSDictionaryM"):
            keys   = obj.get("NS.keys",   [])
            values = obj.get("NS.objects", [])
            return {str(self._resolve(k)): self._resolve(v)
                    for k, v in zip(keys, values)}

        if cls in ("NSData", "NSMutableData", "__NSCFData"):
            return obj.get("NS.data", b"")

        # 클래스 불명 — 키 기반 Best-effort
        result = {}
        for k, v in obj.items():
            if k.startswith("$"):
                continue
            result[k] = self._resolve(v)
        return result if result else obj

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def resolve_uid(self, uid: plistlib.UID) -> Any:
        return self._resolve(uid)

    def root_object(self) -> dict:
        """$top.root → $objects[N] 원시 dict (resolve 전) 반환."""
        root_uid = self._pl["$top"]["root"]
        return self._objects[root_uid.data]

    def get(self, obj_dict: dict, key: str) -> Any:
        """
        obj_dict[key] 가 UID이면 역참조한 실제 값을 반환.
        없으면 None.
        """
        val = obj_dict.get(key)
        if val is None:
            return None
        return self._resolve(val)


# ── archive 칼럼 파싱 ────────────────────────────────────────────────────────

def _ensure_bytes(raw) -> bytes | None:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    if isinstance(raw, memoryview):
        return bytes(raw)
    if isinstance(raw, str):
        try:
            return bytes.fromhex(raw)
        except ValueError:
            return raw.encode("utf-8", errors="replace")
    return None


def _parse_archive(raw) -> dict:
    """
    archive BLOB(BPlist/NSKeyedArchiver) 파싱.

    반환 dict:
      string          : str   최종 메시지 (IGDirectPublishedMessageContent.string)
      edit_count      : int   수정 횟수   (IGDirectPublishedMessageContent.editCount)
      sender_pk       : str   발신자 ID   (IGDirectPublishedMessageMetadata.senderPk)
      sent_ts_str     : str   전송 시각   (IGDirectPublishedMessageMetadata.serverTimestamp)
      edit_history    : list  수정 이력
          각 항목: {"body": str, "timestamp_str": str}
    """
    result = {
        "string":       "",
        "edit_count":   0,
        "sender_pk":    "",
        "sent_ts_str":  "",
        "edit_history": [],
    }

    data = _ensure_bytes(raw)
    if not data or not data.startswith(b"bplist"):
        return result

    try:
        ua = _NSKeyedUnarchiver(data)
        root = ua.root_object()   # $objects[1] — IGDirectPublishedMessage
    except Exception:
        return result

    if not isinstance(root, dict):
        return result

    # ── 1. metadata → 전송 시각 / 발신자 ID ──────────────────────────────────
    metadata = ua.get(root, _KEY_METADATA)
    if isinstance(metadata, dict):
        ts_obj = metadata.get(_KEY_SERVER_TS)
        if isinstance(ts_obj, dict) and ts_obj.get("__type__") == "NSDate":
            result["sent_ts_str"] = _cocoa_to_str(ts_obj.get("NS.time"))

        sender = metadata.get(_KEY_SENDER_PK)
        if sender and isinstance(sender, str):
            result["sender_pk"] = sender

    # ── 2. content → 최종 메시지 / 수정 횟수 ─────────────────────────────────
    content = ua.get(root, _KEY_CONTENT)
    if isinstance(content, dict):
        final_str = content.get(_KEY_STRING)
        if isinstance(final_str, str):
            result["string"] = final_str

        edit_count_raw = content.get(_KEY_EDIT_COUNT)
        try:
            result["edit_count"] = int(edit_count_raw or 0)
        except (ValueError, TypeError):
            result["edit_count"] = 0

    # ── 3. editHistory → 수정 이력 배열 ──────────────────────────────────────
    # IGDirectMessageEditHistory 각 항목:
    #   body      : NSString  — 해당 시점 메시지 내용 (수정 직전 원본)
    #   timestamp : NSDate    — 해당 시점 시각 (Cocoa epoch 초)
    edit_hist_arr = ua.get(root, _KEY_EDIT_HIST)
    if isinstance(edit_hist_arr, list):
        for item in edit_hist_arr:
            if not isinstance(item, dict):
                continue
            body    = item.get(_KEY_BODY, "")
            body    = str(body) if body is not None else ""
            ts_obj  = item.get(_KEY_TIMESTAMP)
            ts_str  = ""
            if isinstance(ts_obj, dict) and ts_obj.get("__type__") == "NSDate":
                ts_str = _cocoa_to_str(ts_obj.get("NS.time"))
            result["edit_history"].append({"body": body, "timestamp_str": ts_str})

    return result


# ── DB 유틸 ──────────────────────────────────────────────────────────────────

def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        return any(r[1] == col
                   for r in conn.execute(f"PRAGMA table_info({table})").fetchall())
    except Exception:
        return False


def _is_instagram_ios_db(db_path: Path) -> bool:
    """messages 테이블 + archive 칼럼이 존재하는 Instagram iOS DB인지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        ok = (_has_table(conn, "messages")
              and _col_exists(conn, "messages", "archive"))
        conn.close()
        return ok
    except Exception:
        return False


def _find_db_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    results: list[Path] = []
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        for f in sorted(path.rglob(ext)):
            if f not in results:
                results.append(f)
    return results


# ── 핵심 분석 ─────────────────────────────────────────────────────────────────

def _analyze_ios_db(db_path: Path, result: AnalysisResult) -> tuple[int, int]:
    """
    단일 [Account_ID].db 파일을 분석하여 result 에 테이블 추가.

    Returns:
        (total_msgs, total_modified)
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

    if not _has_table(conn, "messages"):
        conn.close()
        result.add_error(
            f"messages 테이블이 존재하지 않습니다 [{db_path.name}]\n"
            "  Instagram [Account_ID].db 파일이 맞는지 확인하세요."
        )
        return 0, 0

    # 가용 칼럼 탐지
    hc          = lambda c: _col_exists(conn, "messages", c)
    has_msg_id  = hc("message_id")
    has_thread  = hc("thread_id")
    has_archive = hc("archive")
    has_cls     = hc("class_name")
    has_row_id  = hc("row_id")

    sel = []
    if has_msg_id:  sel.append("message_id")
    if has_thread:  sel.append("thread_id")
    if has_archive: sel.append("archive")
    if has_cls:     sel.append("class_name")
    if has_row_id:  sel.append("row_id")
    if not sel:
        sel = ["*"]

    # row_id 칼럼 기준 정렬 (없으면 rowid)
    order = "row_id" if has_row_id else "rowid"

    try:
        raw_msgs = conn.execute(
            f"SELECT {', '.join(sel)} FROM messages ORDER BY {order}"
        ).fetchall()
    except sqlite3.Error as e:
        conn.close()
        result.add_error(f"messages 쿼리 실패 [{db_path.name}]: {e}")
        return 0, 0

    conn.close()

    # ── 메시지별 분석 ─────────────────────────────────────────────────────────
    for msg in raw_msgs:
        total_msgs += 1

        msg_id    = str(msg["message_id"] if has_msg_id else "")  or ""
        thread_id = str(msg["thread_id"]  if has_thread else "")  or ""

        archive_raw = msg["archive"] if has_archive else None
        parsed = _parse_archive(archive_raw)

        final_text = parsed["string"]
        sent_ts    = parsed["sent_ts_str"]
        sender_pk  = parsed["sender_pk"]
        edit_count = parsed["edit_count"]
        edit_hist  = parsed["edit_history"]
        has_edit   = edit_count > 0

        if has_edit:
            total_modified += 1

        # 마지막 수정 시각: editHistory 마지막 항목의 timestamp_str
        last_edit_ts = edit_hist[-1]["timestamp_str"] if edit_hist else ""

        # ── 본행 ─────────────────────────────────────────────────────────────
        row_index = len(all_rows)
        all_rows.append([
            msg_id,
            sender_pk,
            thread_id,
            sent_ts,
            last_edit_ts,
            str(edit_count) if has_edit else "",
            final_text,
            db_path.name,
        ])

        if has_edit:
            hl_indices.add(row_index)

        # ── 서브행: 수정 이력 ─────────────────────────────────────────────────
        # IGDirectMessageEditHistory 구조:
        #   [0]   → 최초 원본 내용  + 원본 전송 시각
        #   [1]   → 수정 1회 직전 내용 + 수정 시각
        #   ...
        #   [N-1] → 수정 N-1회 직전 내용 + 수정 시각
        #   final_text → 수정 N회 최종본 (현재 앱 표시 내용)
        children: list[list[str]] = []

        if has_edit and edit_hist:
            for i, hist in enumerate(edit_hist):
                body   = hist.get("body", "")
                ts_str = hist.get("timestamp_str", "")

                if i == 0:
                    label    = "  ↳ 원본 (최초 전송)"
                    sent_col = ts_str
                    edit_col = ""
                else:
                    label    = f"  ↳ 수정 {i}회 직전"
                    sent_col = ""
                    edit_col = ts_str

                children.append([
                    label, "", "",
                    sent_col, edit_col,
                    "", body, "",
                ])

            children.append([
                f"  ↳ 수정 {edit_count}회 (최종본)",
                "", "", "", "", "",
                final_text, "",
            ])

        elif has_edit:
            children.append([
                "  ↳ 수정 이력",
                "", "", "", "", "",
                f"수정 {edit_count}회 확인 — 이력 없음 (동기화 미완료 또는 삭제됨)",
                "",
            ])

        if children:
            sub_rows[row_index] = children

    result.add_table(
        title=f"전체 메시지 ({db_path.name})",
        columns=[
            "메시지 ID (message_id)",
            "발신자 ID (senderPk)",
            "대화방 (thread_id)",
            "전송 시각",
            "마지막 수정 시각",
            "수정 횟수 (editCount)",
            "앱 내 표시 메시지 (최종본)",
            "DB 파일",
        ],
        rows=all_rows,
        highlight_rows=hl_indices,
        sub_rows=sub_rows,
    )

    return total_msgs, total_modified


# ── 분석기 클래스 ──────────────────────────────────────────────────────────────

class InstagramIOSAnalyzer(BaseAnalyzer):
    """
    Instagram iOS 수정 메시지 분석기.

    수집 경로 (FFS):
      /private/var/mobile/Containers/Data/Application/[UUID]/
        Library/Application Support/DirectSQLiteDatabase/[Account_ID].db

    논리 백업으로는 수집 불가. 복호화 불필요 (평문 SQLite).

    분석 핵심:
      messages.archive 칼럼 (BPlist/NSKeyedArchiver) 파싱
        IGDirectPublishedMessage (root)
          ├─ IGDirectPublishedMessageMetadata.serverTimestamp (NSDate)
          │    → 전송 시각 (Apple Absolute Time, Cocoa epoch)
          ├─ IGDirectPublishedMessageContent.string (NSString)
          │    → 앱 표시 최종 메시지
          ├─ IGDirectPublishedMessageContent.editCount (NSUInteger)
          │    → 수정 횟수
          └─ NSArray<IGDirectMessageEditHistory>.body / .timestamp
               → 수정 전 원본 내용 / 각 수정 시각
    """

    MESSENGER = "Instagram"
    PLATFORM  = "iOS"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        candidates = _find_db_files(path)
        db_files   = [f for f in candidates if _is_instagram_ios_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"messages 테이블 및 archive 칼럼을 포함한 Instagram iOS DB를 "
                f"찾지 못했습니다: {path}\n"
                "  예상 경로: /private/var/mobile/Containers/Data/Application/"
                "[UUID]/Library/Application Support/DirectSQLiteDatabase/"
                "[Account_ID].db\n"
                "  ※ iOS Instagram DB는 논리 백업으로 수집 불가 (FFS 필요)"
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
        result.summary["메시지 저장 형식"] = "BPlist (NSKeyedArchiver)"
        result.summary["최종 메시지"]      = "archive → IGDirectPublishedMessageContent.NSString*string"
        result.summary["원본 메시지"]      = "archive → IGDirectMessageEditHistory[0].body"
        result.summary["수정 횟수"]        = "archive → IGDirectPublishedMessageContent.NSUIntegereditCount"
        result.summary["시각 형식"]        = "Apple Absolute Time (NS.time + 978307200 = Unix 초)"
        result.summary["수집 방법"]        = "FFS (논리 백업 불가)"

        return result
