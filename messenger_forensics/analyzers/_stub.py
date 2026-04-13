"""
stub_template.py
모든 미구현 분석기 스텁의 공통 로직
"""

from pathlib import Path
from analyzers.base import BaseAnalyzer, AnalysisResult


class StubAnalyzer(BaseAnalyzer):
    """아직 구현되지 않은 분석기의 공통 스텁."""

    MESSENGER: str = ""
    PLATFORM: str = ""

    def analyze(self, path: Path) -> AnalysisResult:
        result = AnalysisResult(success=False)
        result.add_error(
            f"[미구현] {self.MESSENGER} {self.PLATFORM} 분석기는 아직 개발 중입니다."
        )
        result.summary["상태"] = "미구현 (Stub)"
        result.summary["메신저"] = self.MESSENGER
        result.summary["플랫폼"] = self.PLATFORM
        result.summary["경로"] = str(path)
        result.add_table(
            title="분석 결과 (예시 구조)",
            columns=["항목", "값", "비고"],
            rows=[
                ["총 메시지 수", "—", "분석기 구현 후 표시됩니다"],
                ["채팅방 수",   "—", "분석기 구현 후 표시됩니다"],
                ["사용자 수",   "—", "분석기 구현 후 표시됩니다"],
            ],
        )
        return result
