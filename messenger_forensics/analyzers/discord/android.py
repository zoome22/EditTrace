"""
discord/android.py - Discord Android 분석기

DB 경로 : /data/data/com.discord/files/kv-storage/@account.[user_id]/a
테이블  : messages0
컬럼    : d (\\x07 prefix + message_id), data (\\x08 prefix + JSON)

JSON 구조:
  data["message"]["id"]               → 메시지 ID
  data["message"]["content"]          → 메시지 내용
  data["message"]["timestamp"]        → 원본 전송 시각 (ISO 8601)
  data["message"]["edited_timestamp"] → 수정 시각 (null이면 원본)
  data["message"]["author"]           → 작성자 정보

수정 이력 메커니즘 (Android):
  - 동일 message_id에 대해 레코드를 누적 저장
  - edited_timestamp = null  → 원본 행
  - edited_timestamp != null → 수정 행 (수정할 때마다 새 레코드 추가)
  - 수정 횟수 = (동일 ID 행 수) - 1
  - 원본 메시지 내용 복구 가능
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult


# ── 유틸 ──────────────────────────────────────────────────────────────────────

def _parse_iso(val) -> str:
    """ISO 8601 → 'YYYY-MM-DD HH:MM:SS' (로컬 시간)."""
    if not val:
        return ""
    try:
        s = str(val).replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(val)


def _decode_data(raw) -> dict | None:
    """data 컬럼: 앞쪽 non-JSON 바이트 제거 후 파싱."""
    if raw is None:
        return None
    try:
        b = bytes(raw)
        start = b.find(b"{")
        return json.loads(b[start:]) if start >= 0 else None
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _find_db_files(path: Path) -> list:
    if path.is_file():
        return [path]
    results = []
    for candidate in path.rglob("a"):
        if candidate.is_file():
            results.append(candidate)
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        for f in path.rglob(ext):
            if f not in results:
                results.append(f)
    return sorted(results)


def _get_messages_table(conn: sqlite3.Connection) -> str | None:
    """messages0 또는 message0 테이블명 자동 탐지."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('messages0','message0')"
    ).fetchone()
    return row[0] if row else None


def _is_discord_db(db_path: Path) -> bool:
    try:
        conn = sqlite3.connect(str(db_path))
        found = _get_messages_table(conn) is not None
        conn.close()
        return found
    except Exception:
        return False


# ── 분석기 ────────────────────────────────────────────────────────────────────

class DiscordAndroidAnalyzer(BaseAnalyzer):
    MESSENGER = "Discord"
    PLATFORM  = "Android"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()

        db_files = [f for f in _find_db_files(path) if _is_discord_db(f)]
        if not db_files:
            result.success = False
            result.add_error(
                f"messages0(또는 message0) 테이블을 포함한 Discord DB를 찾지 못했습니다: {path}\n"
                "  예상 경로: /data/data/com.discord/files/kv-storage/@account.[user_id]/a"
            )
            return result

        all_rows:   list[list[str]]      = []
        hl_indices: set[int]             = set()
        sub_rows:   dict[int, list]      = {}
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
                raw_rows = conn.execute(
                    f"SELECT d, data FROM {tbl} ORDER BY rowid"
                ).fetchall()
            except sqlite3.Error as e:
                conn.close()
                result.add_error(f"{tbl} 쿼리 실패 [{db_path.name}]: {e}")
                continue
            conn.close()

            # ── message_id별 행 그룹화 ────────────────────────────────────
            msg_groups: dict[str, list[dict]] = {}
            for row in raw_rows:
                obj = _decode_data(row["data"])
                if not obj:
                    continue
                msg = obj.get("message") or {}
                if not msg:
                    continue
                msg_id = str(msg.get("id") or obj.get("id") or "unknown")
                msg_groups.setdefault(msg_id, []).append(msg)

            # ── 그룹별 분석 ───────────────────────────────────────────────
            for msg_id, versions in msg_groups.items():
                # timestamp 오름차순 정렬 → 원본이 첫 번째
                versions.sort(key=lambda m: m.get("timestamp") or "")

                total_msgs += 1

                edited_versions = [v for v in versions if v.get("edited_timestamp")]
                has_edit = bool(edited_versions)
                if has_edit:
                    total_modified += 1

                # 원본: edited_timestamp = null인 첫 번째 행
                original = next(
                    (v for v in versions if not v.get("edited_timestamp")),
                    versions[0],
                )
                orig_ts = _parse_iso(original.get("timestamp"))

                # 작성자
                author_obj = original.get("author") or {}
                author = str(
                    author_obj.get("username") or
                    author_obj.get("global_name") or ""
                )

                # 최종 표시 메시지 (마지막 버전)
                final       = versions[-1]
                final_msg   = str(final.get("content") or "")
                # 전송/수정 시각: 수정 있으면 최종 수정 시각, 없으면 전송 시각
                last_edited = _parse_iso(final.get("edited_timestamp") or "")
                display_ts  = last_edited if has_edit else orig_ts
                edit_count  = len(versions) - 1  # 행 수 - 1

                channel_id = str(original.get("channel_id") or "")

                # ── 부모 행 ─────────────────────────────────────────────
                row_index = len(all_rows)
                all_rows.append([
                    msg_id,
                    author,
                    channel_id,
                    display_ts,
                    str(edit_count) if has_edit else "",
                    final_msg,
                    db_path.name,
                ])
                if has_edit:
                    hl_indices.add(row_index)

                # ── 서브 행 (수정 이력) ──────────────────────────────────
                if has_edit:
                    children: list[list[str]] = []

                    # ↳ 원본
                    children.append([
                        "  ↳ 원본", "", "",
                        orig_ts,              # 전송/수정 시각
                        "",                   # 수정 횟수
                        str(original.get("content") or ""),
                        "",
                    ])

                    # ↳ 수정 1, 2, 3 ...
                    edited_sorted = sorted(
                        edited_versions,
                        key=lambda v: v.get("edited_timestamp") or "",
                    )
                    for i, ev in enumerate(edited_sorted, 1):
                        children.append([
                            f"  ↳ 수정 {i}", "", "",
                            _parse_iso(ev.get("edited_timestamp") or ""),
                            "",
                            str(ev.get("content") or ""),
                            "",
                        ])

                    sub_rows[row_index] = children

        # ── 요약 ──────────────────────────────────────────────────────────
        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정 이력 메시지"] = str(total_modified)

        result.add_table(
            title="전체 메시지",
            columns=[
                "메시지 ID", "작성자", "채널 ID",
                "전송/수정 시각", "수정 횟수",
                "앱 내 표시 메시지 / 최종 수정 메시지",
                "DB 파일",
            ],
            rows=all_rows,
            highlight_rows=hl_indices,
            sub_rows=sub_rows,
        )
        return result
