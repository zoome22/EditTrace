#!/usr/bin/env python3
"""
debug_ios.py - iOS KakaoTalk DB 타임스탬프 디버그 스크립트

사용법:
    python debug_ios.py <DB경로>

출력:
    1. Message 테이블 컬럼 목록
    2. type=0 행 상세 (raw sentAt, 복호화 결과)
    3. 수정 이력 있는 메시지의 서브행 타임스탬프 매핑 결과
"""

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

_COCOA_EPOCH_OFFSET = 978307200


def _ts_cocoa(val):
    if val is None:
        return "None"
    try:
        unix_ts = float(val) + _COCOA_EPOCH_OFFSET
        return datetime.fromtimestamp(unix_ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        return f"ERROR({val}): {e}"


def main():
    if len(sys.argv) < 2:
        print("Usage: python debug_ios.py <DB경로>")
        sys.exit(1)

    db_path = sys.argv[1]
    print(f"\n{'='*60}")
    print(f"DB: {db_path}")
    print(f"{'='*60}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ── 1. 컬럼 목록 ──────────────────────────────────────────────
    cur.execute("SELECT * FROM Message LIMIT 1")
    sample = cur.fetchone()
    if not sample:
        print("Message 테이블이 비어 있습니다.")
        return

    cols = list(sample.keys())
    print(f"\n[1] Message 테이블 컬럼 ({len(cols)}개):")
    print("    " + ", ".join(cols))

    # sentAt 컬럼 탐색
    candidates = ["sentAt", "sendAt", "send_at", "sent_at",
                  "createdAt", "created_at", "writtenDate", "timestamp"]
    sentat_col = next((c for c in candidates if c in cols), None)
    print(f"\n    → 사용할 시각 컬럼: {sentat_col!r}")

    # ── 2. type=0 행 분석 ─────────────────────────────────────────
    cur.execute("SELECT * FROM Message WHERE type = 0")
    t0_rows = cur.fetchall()
    print(f"\n[2] type=0 행 ({len(t0_rows)}개):")

    if not t0_rows:
        print("    type=0 행 없음")
    else:
        try:
            from analyzers.kakao.ios_core import ios_decrypter
            decrypter = ios_decrypter()
            can_decrypt = True
        except Exception as e:
            print(f"    [복호화 불가: {e}]")
            can_decrypt = False

        for i, row in enumerate(t0_rows[:20]):  # 최대 20행
            uid      = int(row["userId"]) if row["userId"] else 0
            msg_raw  = str(row["message"]) if row["message"] else ""
            raw_ts   = row[sentat_col] if sentat_col else None
            ts_str   = _ts_cocoa(raw_ts)

            decrypted = "(복호화 스킵)"
            log_id    = None
            if can_decrypt:
                try:
                    decrypted = decrypter.decrypt(uid, msg_raw)
                    log_id    = int(decrypted.strip())
                except Exception as ex:
                    decrypted = f"복호화실패: {ex}"

            print(f"    [{i+1:03d}] id={row['id']}  userId={uid}")
            print(f"           raw sentAt = {raw_ts!r}")
            print(f"           변환 시각  = {ts_str}")
            print(f"           복호화     = {decrypted!r}  → logId={log_id}")
            print()

    # ── 3. modifyHistory 있는 메시지 + 매핑 결과 ─────────────────
    cur.execute("SELECT * FROM Message WHERE extraInfo LIKE '%modifyHistory%'")
    mod_rows = cur.fetchall()
    print(f"\n[3] modifyHistory 있는 메시지 ({len(mod_rows)}개):")

    # type=0 맵 재구성
    cur.execute("SELECT * FROM Message WHERE type = 0")
    t0_all = cur.fetchall()
    modify_time_map: dict[int, list[tuple[float, str]]] = {}
    if can_decrypt:
        for row in t0_all:
            uid     = int(row["userId"]) if row["userId"] else 0
            msg_raw = str(row["message"]) if row["message"] else ""
            raw_ts  = row[sentat_col] if sentat_col else None
            try:
                decrypted = decrypter.decrypt(uid, msg_raw)
                log_id    = int(decrypted.strip())
                raw_f     = float(raw_ts) if raw_ts is not None else 0.0
                ts_str    = _ts_cocoa(raw_ts)
                if log_id not in modify_time_map:
                    modify_time_map[log_id] = []
                modify_time_map[log_id].append((raw_f, ts_str))
            except Exception:
                pass
        for k in modify_time_map:
            modify_time_map[k].sort(key=lambda x: x[0])

    for row in mod_rows[:10]:  # 최대 10개
        row_id   = row["id"]
        uid      = int(row["userId"]) if row["userId"] else 0
        raw_ts   = row[sentat_col] if sentat_col else None
        send_ts  = _ts_cocoa(raw_ts)
        extra    = row["extraInfo"] or ""

        print(f"\n    메시지 id={row_id}  userId={uid}")
        print(f"      전송 시각: {send_ts}  (raw={raw_ts})")

        mod_times = [(ts, s) for ts, s in modify_time_map.get(row_id, [])]
        print(f"      modify_time_map 매핑: {len(mod_times)}개")
        for j, (_, s) in enumerate(mod_times):
            print(f"        수정 {j+1}: {s}")

        try:
            extra_obj = json.loads(extra)
            history   = extra_obj.get("modifyHistory", [])
            print(f"      modifyHistory 항목: {len(history)}개")
            for j, entry in enumerate(history):
                print(f"        [{j+1}] message={entry.get('message','')[:30]!r}")
        except Exception as e:
            print(f"      extraInfo 파싱 실패: {e}")

    conn.close()
    print(f"\n{'='*60}\n디버그 완료\n{'='*60}\n")


if __name__ == "__main__":
    main()
