"""
kakao/ios.py - KakaoTalk iOS 분석기

타임스탬프 스펙:
  - sentAt: Apple NSDate (Cocoa epoch, 2001-01-01 기준, 초 단위)
  - type=0 행: message 복호화 → logId (수정 대상 메시지 ID)
               sentAt = 해당 수정이 일어난 시각 (Cocoa epoch)
  - type=0 행이 수정될 때마다 생성 → 각 수정의 정확한 시각 추적 가능
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult
from analyzers.kakao.ios_core import ios_decrypter

# 2001-01-01 00:00:00 UTC - 1970-01-01 00:00:00 UTC = 978307200초
_COCOA_EPOCH_OFFSET = 978307200


def _ts_cocoa(val) -> str:
    """Apple NSDate (Cocoa epoch, 초) → YYYY-MM-DD HH:MM:SS 문자열."""
    if val is None:
        return ""
    try:
        unix_ts = float(val) + _COCOA_EPOCH_OFFSET
        return datetime.fromtimestamp(unix_ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(val)


def _find_db_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    db_files = []
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        db_files.extend(path.rglob(ext))
    return sorted(db_files)


class KakaoIOSAnalyzer(BaseAnalyzer):
    MESSENGER = "KakaoTalk"
    PLATFORM  = "iOS"

    # sentAt 컬럼명 후보 (우선순위 순)
    _SENTAT_CANDIDATES = ["sentAt", "sendAt", "send_at", "sent_at",
                          "createdAt", "created_at", "writtenDate",
                          "timestamp", "time", "date"]

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult()
        decrypter = ios_decrypter()

        db_files = _find_db_files(path)
        if not db_files:
            result.success = False
            result.add_error(f"분석할 .db 파일을 찾지 못했습니다: {path}")
            return result

        all_rows   = []
        hl_indices: set[int] = set()
        sub_rows: dict[int, list] = {}

        total_msgs     = 0
        total_modified = 0

        for db_path in db_files:
            try:
                conn = sqlite3.connect(str(db_path))
                conn.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
                continue

            cur = conn.cursor()
            try:
                cur.execute("SELECT * FROM Message")
            except sqlite3.Error as e:
                conn.close()
                result.add_error(f"Message 쿼리 실패 [{db_path.name}]: {e}")
                continue

            rows = cur.fetchall()
            conn.close()

            if not rows:
                continue

            # 실제 sentAt 컬럼명 확정
            actual_cols = list(rows[0].keys())
            sentat_col = next((c for c in self._SENTAT_CANDIDATES if c in actual_cols), None)

            # ── 1단계: type=0 / feedType=25 행 → {serverLogId: 최종수정시각} ──
            modify_time_map: dict[int, tuple[float, str]] = {}  # {logId: (raw, ts)}

            for row in rows:
                if row["type"] != 0:
                    continue

                user_id_t0 = int(row["userId"]) if row["userId"] is not None else 0
                msg_t0     = str(row["message"]) if row["message"] is not None else ""
                raw_sentat = row[sentat_col] if sentat_col else None

                try:
                    decrypted = decrypter.decrypt(user_id_t0, msg_t0)
                    stripped  = decrypted.strip()
                    if stripped.startswith("{"):
                        parsed    = json.loads(stripped)
                        feed_type = parsed.get("feedType")
                        log_id    = int(parsed["logId"])
                    else:
                        feed_type = None
                        log_id    = int(stripped)

                    if feed_type != 25:
                        continue

                    raw_f  = float(raw_sentat) if raw_sentat is not None else 0.0
                    ts_str = _ts_cocoa(raw_sentat)
                    # 같은 logId가 여러 개면 sentAt 가장 큰 값(최신) 유지
                    if log_id not in modify_time_map or raw_f > modify_time_map[log_id][0]:
                        modify_time_map[log_id] = (raw_f, ts_str)
                except Exception:
                    pass

                    dec_repr = repr(decrypted) if "decrypted" in dir() else "?"


            # ── 2단계: 일반 메시지 처리 ─────────────────────────────────────
            for row in rows:
                if row["type"] == 0:
                    continue

                total_msgs   += 1
                row_id       = row["id"]
                server_log_id = int(row["serverLogId"]) if row["serverLogId"] is not None else 0
                user_id      = int(row["userId"]) if row["userId"] is not None else 0
                message = str(row["message"]) if row["message"] is not None else ""
                extra   = row["extraInfo"] or ""

                # 원본 전송 시각 (Cocoa epoch)
                send_time = _ts_cocoa(row[sentat_col] if sentat_col else None)

                # 앱 내 표시 메시지
                try:
                    displayed_msg = decrypter.decrypt(user_id, message)
                except Exception as e:
                    displayed_msg = f"(오류: {e})"

                # 수정 이력 파싱 (extraInfo.modifyHistory)
                modify_entries = []  # [(idx, mod_msg)]
                if "modifyHistory" in extra:
                    try:
                        extra_obj = json.loads(extra)
                        for idx, entry in enumerate(extra_obj.get("modifyHistory", []), 1):
                            if "message" not in entry:
                                continue
                            raw_hist = entry["message"].replace("\r", "").replace("\n", "")
                            try:
                                mod_msg = decrypter.decrypt(user_id, raw_hist)
                            except Exception:
                                mod_msg = "(복호화 실패)"
                            modify_entries.append((idx, mod_msg))
                    except (json.JSONDecodeError, TypeError):
                        pass

                has_modify = bool(modify_entries)
                if has_modify:
                    total_modified += 1

                # ── 부모 행 ─────────────────────────────────────────────────
                row_index = len(all_rows)
                all_rows.append([
                    str(row_id),
                    str(user_id),
                    send_time,
                    displayed_msg,
                    str(len(modify_entries)) if has_modify else "",
                    db_path.name,
                ])
                if has_modify:
                    hl_indices.add(row_index)

                # ── 서브 행 ──────────────────────────────────────────────────
                # 타임스탬프:
                #   ↳ 원본        → send_time (sentAt, Cocoa epoch)
                #   ↳ 수정 1      → modify_time_map[logId][0] (type=0 행 첫 번째)
                #   ↳ 수정 2      → modify_time_map[logId][1]
                #   ↳ 수정 N (최종본) → modify_time_map[logId][N-1]
                if has_modify:
                    last_ts = modify_time_map[server_log_id][1] if server_log_id in modify_time_map else ""
                    total   = len(modify_entries)
                    children = []

                    for idx, mod_msg in modify_entries:
                        if idx == 1:
                            label   = "  ↳ 원본"
                            ts_cell = send_time
                        else:
                            label   = f"  ↳ 수정 {idx - 1}"
                            ts_cell = ""
                        children.append([label, "", ts_cell, mod_msg, "", ""])

                    children.append([f"  ↳ 수정 {total}", "", last_ts, displayed_msg, "", ""])
                    sub_rows[row_index] = children

        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정 이력 메시지"] = str(total_modified)

        result.add_table(
            title="전체 메시지",
            columns=["ID", "UserID", "전송/수정 시각", "앱 내 표시 메시지 / 최종 수정 메시지", "수정 횟수", "DB파일"],
            rows=all_rows,
            highlight_rows=hl_indices,
            sub_rows=sub_rows,
        )

        return result
