"""
kakao/android.py - KakaoTalk Android 분석기

타임스탬프 스펙:
  - sendAt  : 원본 메시지 전송 시각 (Unix ms) → ↳ 원본 행에 표시
  - v.c     : 마지막 수정 시각 (Unix ms) → ↳ 수정 N (마지막) 행에만 표시
  - 중간 수정들의 시각은 추적 불가 → 빈칸
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from analyzers.base import BaseAnalyzer, AnalysisResult
from analyzers.kakao.android_core import android_decrypter


def _ts(val, fallback_year: int = None) -> str:
    """Unix ms/s 또는 날짜 문자열 -> 'YYYY-MM-DD HH:MM:SS'.

    일부 카카오 버전은 v.c를 'MM-DD HH:MM:SS' 문자열로 저장.
    fallback_year(원본 전송 시각의 연도)를 넘기면 연도를 보완한다.
    """
    import re as _re
    if val is None:
        return ""
    # ① Unix ms / s 숫자
    try:
        ts = int(float(val))
        if ts > 1_000_000_000_000:
            ts //= 1000
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        pass
    # ② 완전한 날짜 문자열
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                    "%Y/%m/%d %H:%M:%S", "%Y.%m.%d %H:%M:%S"):
            try:
                return datetime.strptime(val, fmt).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        # ③ 'MM-DD HH:MM:SS' → fallback_year 또는 현재 연도로 보완
        m = _re.fullmatch(r"(\d{2}-\d{2} \d{2}:\d{2}:\d{2})", val.strip())
        if m:
            year = fallback_year or datetime.now().year
            try:
                return datetime.strptime(
                    f"{year}-{m.group(1)}", "%Y-%m-%d %H:%M:%S"
                ).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
    return str(val)


def _find_db_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    db_files = []
    for ext in ("*.db", "*.sqlite", "*.sqlite3"):
        db_files.extend(path.rglob(ext))
    return sorted(db_files)


def _guess_user_id(row: sqlite3.Row):
    for col in ["userId", "userid", "user_id", "uid", "ownerId", "owner_id"]:
        try:
            val = row[col]
            if val is not None:
                return int(val)
        except (IndexError, KeyError, ValueError):
            continue
    return None


def _guess_sendat_col(row: sqlite3.Row):
    """원본 전송 시각 컬럼 탐색."""
    for col in ["sendAt", "send_at", "created_at", "createdAt",
                "sent_at", "sentAt", "timestamp", "time", "date", "written_date"]:
        try:
            _ = row[col]
            return col
        except (IndexError, KeyError):
            continue
    return None


class KakaoAndroidAnalyzer(BaseAnalyzer):
    MESSENGER = "KakaoTalk"
    PLATFORM  = "Android"

    def analyze(self, path: Path, fallback_user_id: int | None = None, **kwargs) -> AnalysisResult:
        result = AnalysisResult()
        decrypter = android_decrypter()

        db_files = _find_db_files(path)
        if not db_files:
            result.success = False
            result.add_error(f"분석할 .db 파일을 찾지 못했습니다: {path}")
            return result

        all_rows   = []
        hl_indices: set[int] = set()
        sub_rows: dict[int, list] = {}

        total_msgs = 0
        total_modified = 0
        skip_count = 0

        for db_path in db_files:
            try:
                conn = sqlite3.connect(str(db_path))
                conn.row_factory = sqlite3.Row
            except sqlite3.Error as e:
                result.add_error(f"DB 열기 실패 [{db_path.name}]: {e}")
                continue

            cur = conn.cursor()
            try:
                cur.execute("SELECT rowid AS id, * FROM chat_logs")
            except sqlite3.Error as e:
                conn.close()
                result.add_error(f"chat_logs 쿼리 실패 [{db_path.name}]: {e}")
                continue

            rows = cur.fetchall()
            conn.close()
            if not rows:
                continue
            sendat_col = _guess_sendat_col(rows[0]) if rows else None

            for row in rows:
                total_msgs += 1
                row_id = row["id"]

                user_id = _guess_user_id(row)
                if user_id is None:
                    if fallback_user_id is not None:
                        user_id = fallback_user_id
                    else:
                        skip_count += 1
                        continue

                # 원본 전송 시각
                send_time = _ts(row[sendat_col]) if sendat_col else ""

                # 앱 내 표시 메시지 복호화
                raw_msg = str(row["message"]) if row["message"] is not None else ""
                try:
                    dec = decrypter.decrypt_try_all(user_id, raw_msg)
                    displayed_msg = dec[1] if dec else "(복호화 실패)"
                except Exception as e:
                    displayed_msg = f"(오류: {e})"

                # 수정 이력 파싱
                modify_entries = []  # [(idx, mod_msg)]  — 시각은 별도 처리
                last_modify_time = ""  # v.c = 마지막 수정 시각

                v_raw = row["v"] if "v" in row.keys() else None
                if v_raw:
                    try:
                        v_obj = json.loads(v_raw)

                        # v.c = 마지막 수정 시각 (Unix ms)
                        # send_time에서 연도 추출해 fallback으로 사용
                        _fy = None
                        if send_time and len(send_time) >= 4:
                            try:
                                _fy = int(send_time[:4])
                            except ValueError:
                                _fy = datetime.now().year
                        last_modify_time = _ts(v_obj.get("c"), fallback_year=_fy)

                        if "modifyLog" in v_obj:
                            modify_list = json.loads(v_obj["modifyLog"])
                            for idx, entry in enumerate(modify_list, 1):
                                raw_hist = entry.get("message", "").replace("\r", "").replace("\n", "")
                                try:
                                    dec_h = decrypter.decrypt_try_all(user_id, raw_hist)
                                    mod_msg = dec_h[1] if dec_h else "(복호화 실패)"
                                except Exception:
                                    mod_msg = "(복호화 실패)"
                                modify_entries.append((idx, mod_msg))
                    except (json.JSONDecodeError, TypeError, KeyError):
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
                # 타임스탬프 규칙:
                #   ↳ 원본    → send_time  (sendAt: 최초 전송 시각)
                #   ↳ 수정 1..N-1 → ""    (중간 수정 시각 추적 불가)
                #   ↳ 수정 N  (마지막) → last_modify_time  (v.c)
                #   ↳ 수정 N+1 (최종본) → last_modify_time (동일)
                if has_modify:
                    total = len(modify_entries)
                    children = []
                    for idx, mod_msg in modify_entries:
                        if idx == 1:
                            label    = "  ↳ 원본"
                            ts_cell  = send_time
                        elif idx == total:
                            label    = f"  ↳ 수정 {idx - 1}"
                            ts_cell  = last_modify_time
                        else:
                            label    = f"  ↳ 수정 {idx - 1}"
                            ts_cell  = ""          # 중간 수정 시각 미기록
                        children.append([label, "", ts_cell, mod_msg, "", ""])

                    # 최종본 행 (앱 표시 메시지)
                    children.append([
                        f"  ↳ 수정 {total}",
                        "",
                        last_modify_time,
                        displayed_msg,
                        "",
                        "",
                    ])
                    sub_rows[row_index] = children

        # ── 요약 ──────────────────────────────────────────────────────────
        result.summary["분석 DB 수"]      = str(len(db_files))
        result.summary["전체 메시지"]      = str(total_msgs)
        result.summary["수정 이력 메시지"] = str(total_modified)
        if skip_count:
            result.summary["user_id 미확인"] = str(skip_count)

        result.add_table(
            title="전체 메시지",
            columns=["ID", "UserID", "전송/수정 시각", "앱 내 표시 메시지 / 최종 수정 메시지", "수정 횟수", "DB파일"],
            rows=all_rows,
            highlight_rows=hl_indices,
            sub_rows=sub_rows,
        )

        if skip_count:
            result.add_error(
                f"{skip_count}개 행은 user_id를 찾지 못해 건너뛰었습니다. "
                "GUI의 Fallback User ID 필드에 입력하세요."
            )

        return result
