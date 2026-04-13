"""
analyzers 패키지
각 메신저별 iOS/Android 분석 모듈을 포함합니다.

구조:
    analyzers/
    ├── base.py              # 공통 BaseAnalyzer 추상 클래스
    ├── registry.py          # 분석기 등록 레지스트리
    ├── kakao/
    │   ├── android.py       # 카카오톡 Android 분석기
    │   └── ios.py           # 카카오톡 iOS 분석기
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
"""
