# Messenger Forensics Analyzer

메신저 데이터베이스 포렌식 분석 GUI 툴 (Python / tkinter)

---

## 실행 방법

```bash
cd messenger_forensics
python main.py
```

> Python 3.10+ 권장. 외부 라이브러리 없이 표준 라이브러리(tkinter)만 사용합니다.

---

## 프로젝트 구조

```
messenger_forensics/
├── main.py                        # 진입점
├── README.md
│
├── ui/
│   ├── __init__.py
│   └── app.py                     # 메인 GUI (tkinter)
│
└── analyzers/
    ├── __init__.py
    ├── base.py                    # BaseAnalyzer 추상 클래스 + AnalysisResult
    ├── registry.py                # 분석기 등록 테이블 & run_analysis()
    │
    ├── kakao/
    │   ├── android.py             # KakaoAndroidAnalyzer  ← 구현 대상
    │   └── ios.py                 # KakaoIOSAnalyzer      ← 구현 대상
    ├── discord/
    │   ├── android.py
    │   └── ios.py
    ├── telegram/
    │   ├── android.py
    │   └── ios.py
    ├── facebook/
    │   ├── android.py
    │   └── ios.py
    ├── wechat/
    │   ├── android.py
    │   └── ios.py
    ├── instagram/
    │   ├── android.py
    │   └── ios.py
    └── jandi/
        ├── android.py
        └── ios.py
```

---

## 새 분석기 추가 방법

### 1. 분석기 클래스 구현

`analyzers/kakao/android.py` 를 예시로, `analyze()` 를 구현합니다.

```python
from pathlib import Path
from analyzers.base import BaseAnalyzer, AnalysisResult
import sqlite3

class KakaoAndroidAnalyzer(BaseAnalyzer):
    MESSENGER = "KakaoTalk"
    PLATFORM  = "Android"

    def analyze(self, path: Path) -> AnalysisResult:
        result = AnalysisResult()

        # 1) 요약 정보 추가
        result.summary["총 메시지 수"] = "1,234"
        result.summary["채팅방 수"]    = "12"
        result.summary["기간"]        = "2023-01-01 ~ 2024-03-15"

        # 2) 상세 테이블 추가
        result.add_table(
            title="채팅방 목록",
            columns=["채팅방", "참여자 수", "메시지 수", "첫 메시지", "마지막 메시지"],
            rows=[
                ["친구들",  "5", "320", "2023-01-01", "2024-03-15"],
                ["업무방",  "8", "914", "2023-02-10", "2024-04-01"],
            ],
        )
        return result
```

### 2. registry.py 확인

`analyzers/registry.py` 의 `REGISTRY` 딕셔너리에 이미 등록되어 있습니다.  
클래스명을 바꾸는 경우에만 import와 REGISTRY를 수정하세요.

---

## AnalysisResult 필드 요약

| 필드       | 타입             | 설명                              |
|------------|------------------|-----------------------------------|
| `summary`  | `dict[str, str]` | 상단 요약 카드에 표시 (키: 값)     |
| `tables`   | `list[dict]`     | 탭별 테이블 (`add_table()` 사용)  |
| `errors`   | `list[str]`      | 경고/에러 메시지 (로그창에 표시)  |
| `success`  | `bool`           | 분석 성공 여부                    |
| `raw_data` | `Any`            | 원본 데이터 (필요 시 자유 활용)   |
