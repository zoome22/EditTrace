"""
discord/ios.py - Discord iOS 분석기

DB 경로  : /var/private/mobile/Containers/[UUID]/Library/Caches/kv-storage
             /@account.[user_id]/a
테이블   : messages0  (Android와 동일한 스키마)
주요 컬럼:
  a          메시지 타입 문자열 (예: .messages)
  b          NULL
  c          채널 ID – Discord Snowflake ID를 hex 형태로 저장
  d          메시지 ID – Discord Snowflake ID를 hex 형태로 저장
  e          상태 플래그 / 메시지 속성
  f          보조 상태 플래그 / 예약 필드
  data       메시지 전체 내용 및 메타데이터 JSON
             (작성자, 내용, 타임스탬프, 참조 메시지 등)
  generation 메시지 ID와 동일한 Snowflake ID

수정 이력 메커니즘 (iOS):
  - 수정 이력은 별도로 저장되지 않고 원본 레코드를 덮어씀
    → 동일 message_id에 대해 단일 레코드만 존재
  - data 칼럼 JSON 내 edited_timestamp 가 NULL  → 원본(미수정) 메시지
  - data 칼럼 JSON 내 edited_timestamp 가 NOT NULL → 수정된 메시지
    (마지막 수정 시각 1개만 기록됨)
  - 원본 메시지 내용 복구 불가 (덮어쓰기로 소실)
  - 수정 횟수를 알 수 있는 데이터 없음 → 수정 여부(O/X)만 판별 가능
  - 원본 전송 시각(timestamp)은 수정 후에도 보존됨
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 유틸 ──────────────────────────────────────────────────────────────────────

def _parse_iso(val) -> str:
    """ISO 8601 타임스탬프 → 'YYYY-MM-DD HH:MM:SS' (로컬 시간)."""
    if not val:
        return ""
    try:
        s = str(val).replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(val)


def _decode_data(raw) -> dict | None:
    """
    data 컬럼 파싱.
    Android와 동일한 DB 구조이므로 앞쪽 non-JSON 바이트가 있을 경우 제거 후 파싱.
    일반 텍스트 JSON이면 그대로 파싱.
    """
    if raw is None:
        return None
    # bytes 타입인 경우 (BLOB) → 첫 '{' 위치부터 파싱
    if isinstance(raw, (bytes, bytearray)):
        start = raw.find(b"{")
        if start < 0:
            return None
        try:
            return json.loads(raw[start:])
        except (json.JSONDecodeError, ValueError):
            return None
    # str / memoryview 등
    try:
        text = bytes(raw).decode("utf-8", errors="replace") if not isinstance(raw, str) else raw
        start = text.find("{")
        if start < 0:
            return None
        return json.loads(text[start:])
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _find_db_files(path: Path) -> list[Path]:
    """경로 내 Discord DB 후보 파일 목록 반환."""
    if path.is_file():
        return [path]
    results: list[Path] = []
    # iOS FFS 기준 파일명은 확장자 없는 'a'
    for candidate in path.rglob("a"):
        if candidate.is_file():
            results.append(candidate)
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        for f in path.rglob(ext):
            if f not in results:
                results.append(f)
    return sorted(results)


def _get_messages_table(conn: sqlite3.Connection) -> str | None:
    """messages0 또는 message0 테이블명 자동 탐지 (Android와 동일한 구조)."""
    row = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name IN ('messages0', 'message0')"
    ).fetchone()
    return row[0] if row else None


def _is_discord_ios_db(db_path: Path) -> bool:
    """messages0(또는 message0) 테이블이 존재하는 SQLite 파일인지 확인."""
    try:
        conn = sqlite3.connect(str(db_path))
        found = _get_messages_table(conn) is not None
        conn.close()
        return found
    except Exception:
        return False


# ── 분석기 ────────────────────────────────────────────────────────────────────

class DiscordIOSAnalyzer(BaseAnalyzer):
    MESSENGER = "Discord"
    PLATFORM  = "iOS"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        db_files = [f for f in _find_db_files(path) if _is_discord_ios_db(f)]

        if not db_files:
            result.success = False
            result.add_error(
                f"messages0(또는 message0) 테이블을 포함한 Discord DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /var/private/mobile/Containers/[UUID]"
                "/Library/Caches/kv-storage/@account.[user_id]/a"
            )
            return result

        all_rows:   list[list[str]] = []
        hl_indices: set[int]        = set()
        sub_rows:   dict[int, list] = {}

        total_msgs     = 0
        total_modified = 0

        for db_path in db_files:
            try:
                conn = sqlite3.connect(str(db_path))
                conn.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
                continue

            tbl = _get_messages_table(conn)
            try:
                # c: channel_id(hex), d: message_id(hex), data: JSON
                raw_rows = conn.execute(
                    f"SELECT a, c, d, e, data, generation FROM {tbl} ORDER BY rowid"
                ).fetchall()
            except sqlite3.OperationalError:
                # 컬럼 구성이 다를 경우 최소 컬럼만 조회
                try:
                    raw_rows = conn.execute(
                        f"SELECT d, data FROM {tbl} ORDER BY rowid"
                    ).fetchall()
                except sqlite3.Error as e:
                    conn.close()
                    result.add_error(f"{tbl} 쿼리 실패 [{db_path.name}]: {e}")
                    continue
            conn.close()

            if not raw_rows:
                continue

            for row in raw_rows:
                # data 컬럼 파싱
                raw_data = row["data"] if "data" in row.keys() else None
                obj = _decode_data(raw_data)
                if not obj:
                    continue

                # iOS DB 구조: Android와 달리 data JSON이 message 래퍼 없이
                # 바로 메시지 필드를 담고 있거나, "message" 키로 감싸진 형태 모두 지원
                msg = obj.get("message") or obj

                total_msgs += 1

                # ── 기본 필드 추출 ─────────────────────────────────────
                # 메시지 ID: JSON > d 컬럼(hex Snowflake)
                msg_id = str(
                    msg.get("id")
                    or (row["d"] if "d" in row.keys() else "")
                    or "unknown"
                )

                # 채널 ID: JSON > c 컬럼(hex Snowflake)
                channel_id = str(
                    msg.get("channel_id")
                    or (row["c"] if "c" in row.keys() else "")
                    or ""
                )

                # 작성자
                author_obj = msg.get("author") or {}
                author = str(
                    author_obj.get("username")
                    or author_obj.get("global_name")
                    or msg.get("author_id")
                    or ""
                )

                # 원본 전송 시각 (수정 후에도 보존됨)
                orig_ts = _parse_iso(msg.get("timestamp") or "")

                # 수정 시각 (마지막 수정 1개만 기록, 미수정 시 NULL)
                edited_ts_raw = msg.get("edited_timestamp") or ""
                edited_ts     = _parse_iso(edited_ts_raw)
                has_edit      = bool(edited_ts_raw)

                # 현재 앱에 표시되는 메시지 내용 (원본이 덮어쓰인 최종본)
                content = str(msg.get("content") or "")

                if has_edit:
                    total_modified += 1

                # ── 부모 행 ────────────────────────────────────────────
                row_index = len(all_rows)
                all_rows.append([
                    msg_id,
                    author,
                    channel_id,
                    orig_ts,       # 원본 전송 시각 (수정 후에도 불변)
                    edited_ts,     # 마지막 수정 시각 (미수정이면 공백)
                    # 수정 횟수: iOS는 횟수 확인 불가 → 수정 여부만 표시
                    "확인 불가" if has_edit else "",
                    content,       # 현재 앱 표시 메시지 (원본 복구 불가)
                    db_path.name,
                ])

                if has_edit:
                    hl_indices.add(row_index)

                    # ── 서브 행: iOS 제한 사항 안내 ──────────────────
                    # 덮어쓰기로 원본·중간 수정본 모두 소실
                    sub_rows[row_index] = [[
                        "  ↳ 수정 확인",
                        "", "",
                        orig_ts,   # 원본 전송 시각
                        edited_ts, # 마지막 수정 시각
                        "",
                        "원본 복구 불가 / 수정 횟수 확인 불가 (덮어쓰기 방식)",
                        "",
                    ]]

        # ── 요약 ──────────────────────────────────────────────────────────────
        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정된 메시지"]    = str(total_modified)
        result.summary["원본 복구 가능"]   = "불가 (덮어쓰기)"
        result.summary["수정 횟수 확인"]   = "불가 (기록 없음)"

        result.add_table(
            title="전체 메시지",
            columns=[
                "메시지 ID", "작성자", "채널 ID",
                "원본 전송 시각", "마지막 수정 시각", "수정 횟수",
                "앱 내 표시 메시지 (최종본)", "DB 파일",
            ],
            rows=all_rows,
            highlight_rows=hl_indices,
            sub_rows=sub_rows,
        )

        # iOS 포렌식 한계 고지
        result.add_error(
            "[iOS 한계] 수정 시 원본 레코드를 덮어씀 → "
            "원본 메시지 내용 및 수정 횟수 복구 불가. "
            "edited_timestamp 를 통해 수정 여부와 마지막 수정 시각만 확인 가능."
        )
        return result
