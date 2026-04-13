"""
wechat/ios.py - WeChat Ios 분석기

TODO: 이 파일에 실제 분석 로직을 구현하세요.
"""

from pathlib import Path
from analyzers.base import BaseAnalyzer, AnalysisResult


class WeChatIOSAnalyzer(BaseAnalyzer):
    MESSENGER = "WeChat"
    PLATFORM  = "Ios"

    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        result = AnalysisResult(success=False)
        result.add_error(
            "[미구현] WeChat Ios 분석기는 아직 개발 중입니다."
        )
        result.summary["상태"]   = "미구현 (Stub)"
        result.summary["메신저"] = self.MESSENGER
        result.summary["플랫폼"] = self.PLATFORM
        result.summary["경로"]   = str(path)
        result.add_table(
            title="분석 결과 예시 구조",
            columns=["항목", "값", "비고"],
            rows=[
                ["총 메시지 수", "—", "분석기 구현 후 표시됩니다"],
                ["채팅방 수",    "—", "분석기 구현 후 표시됩니다"],
                ["사용자 수",    "—", "분석기 구현 후 표시됩니다"],
            ],
        )
        return result
