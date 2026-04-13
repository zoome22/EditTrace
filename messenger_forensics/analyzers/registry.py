"""
registry.py - 분석기 레지스트리

새 분석기를 추가할 때 이 파일에 등록합니다.
분석기 클래스를 import하고 REGISTRY에 추가하면 GUI에 자동으로 반영됩니다.
"""

from pathlib import Path
from typing import Type

from .base import BaseAnalyzer, AnalysisResult

# ──────────────────────────────────────────────────────────────────────────────
# 분석기 Import 목록 (Lazy import — 개별 분석기의 의존성 오류가
# 다른 분석기 전체를 막지 않도록 각 항목을 독립적으로 로드)
# ──────────────────────────────────────────────────────────────────────────────

def _lazy_import(module_path: str, class_name: str):
    """
    분석기 클래스를 지연 로드하여 반환.
    import 실패 시 None 을 반환하고 오류 메시지를 출력.
    """
    try:
        import importlib
        mod = importlib.import_module(module_path, package=__package__)
        return getattr(mod, class_name)
    except Exception as e:
        print(f"[registry] {module_path}.{class_name} 로드 실패: {e}")
        return None


# ──────────────────────────────────────────────────────────────────────────────
# REGISTRY: (메신저명, 플랫폼) → (모듈 경로, 클래스명)
# ──────────────────────────────────────────────────────────────────────────────

_REGISTRY_MAP: dict[tuple[str, str], tuple[str, str]] = {
    ("KakaoTalk",           "Android"): (".kakao.android",    "KakaoAndroidAnalyzer"),
    ("KakaoTalk",           "iOS"):     (".kakao.ios",         "KakaoIOSAnalyzer"),
    ("Discord",             "Android"): (".discord.android",  "DiscordAndroidAnalyzer"),
    ("Discord",             "iOS"):     (".discord.ios",       "DiscordIOSAnalyzer"),
    ("Telegram",            "Android"): (".telegram.android", "TelegramAndroidAnalyzer"),
    ("Telegram",            "iOS"):     (".telegram.ios",      "TelegramIOSAnalyzer"),
    ("Facebook Messenger",  "Android"): (".facebook.android", "FacebookAndroidAnalyzer"),
    ("Facebook Messenger",  "iOS"):     (".facebook.ios",      "FacebookIOSAnalyzer"),
    ("WeChat",              "Android"): (".wechat.android",   "WeChatAndroidAnalyzer"),
    ("WeChat",              "iOS"):     (".wechat.ios",        "WeChatIOSAnalyzer"),
    ("Instagram",           "Android"): (".instagram.android","InstagramAndroidAnalyzer"),
    ("Instagram",           "iOS"):     (".instagram.ios",     "InstagramIOSAnalyzer"),
    ("Jandi",               "Android"): (".jandi.android",    "JandiAndroidAnalyzer"),
    ("Jandi",               "iOS"):     (".jandi.ios",         "JandiIOSAnalyzer"),
    ("WhatsApp",            "Android"): (".whatsapp.android", "WhatsAppAndroidAnalyzer"),
    ("WhatsApp",            "iOS"):     (".whatsapp.ios",     "WhatsAppIOSAnalyzer"),
}

# 하위 호환성을 위해 REGISTRY 딕셔너리 유지 (lazy load)
REGISTRY: dict[tuple[str, str], Type[BaseAnalyzer]] = {}
for _key, (_mod, _cls) in _REGISTRY_MAP.items():
    _klass = _lazy_import(_mod, _cls)
    if _klass is not None:
        REGISTRY[_key] = _klass


def get_analyzer(messenger: str, platform: str) -> BaseAnalyzer:
    """
    메신저명과 플랫폼에 해당하는 분석기 인스턴스를 반환합니다.
    등록되지 않은 조합은 미구현 안내 Stub 을 반환합니다.
    """
    key = (messenger, platform)
    if key in REGISTRY:
        return REGISTRY[key]()

    # 미구현 조합 → Stub 반환 (KeyError 대신)
    _messenger = messenger
    _platform  = platform

    class _StubAnalyzer(BaseAnalyzer):
        MESSENGER = _messenger
        PLATFORM  = _platform

        def analyze(self, path, **kwargs):
            r = AnalysisResult(success=False)
            r.add_error(f"[미구현] {_messenger} {_platform} 분석기는 아직 개발 중입니다.")
            r.summary["상태"]   = "미구현 (Stub)"
            r.summary["메신저"] = _messenger
            r.summary["플랫폼"] = _platform
            return r

    return _StubAnalyzer()


def run_analysis(path: Path, messenger: str, platform: str, **kwargs):
    """
    경로, 메신저, 플랫폼을 받아 분석을 실행하고 결과를 반환합니다.
    GUI의 분석 버튼에서 이 함수를 호출합니다.

    kwargs:
        fallback_user_id (int | None): Android에서 DB에 userId가 없을 때 사용할 폴백 값
    """
    analyzer = get_analyzer(messenger, platform)
    return analyzer.analyze(path, **kwargs)
