"""
base.py - 모든 분석기의 공통 추상 기반 클래스

새 분석 모듈을 추가하려면:
    1. BaseAnalyzer를 상속받는 클래스를 생성합니다.
    2. analyze() 메서드를 구현합니다.
    3. analyzers/registry.py에 등록합니다.

AnalysisResult 구조:
    - summary: dict         전체 요약 정보 (표의 상단 요약 행에 표시)
    - tables: list[dict]    각 테이블 {title, columns, rows} 형태
    - raw_data: any         원본 데이터 (선택사항, 추가 분석용)
    - errors: list[str]     분석 중 발생한 경고/에러 메시지
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class AnalysisResult:
    """분석 결과를 담는 데이터 클래스."""

    # 요약 정보: {"항목": "값"} 형태
    summary: dict[str, str] = field(default_factory=dict)

    # 테이블 목록: [{"title": str, "columns": [str], "rows": [[str]]}]
    tables: list[dict] = field(default_factory=list)

    # 원본 데이터 (필요 시 활용)
    raw_data: Any = None

    # 경고 및 에러 메시지
    errors: list[str] = field(default_factory=list)

    # 분석 성공 여부
    success: bool = True

    def add_table(
        self,
        title: str,
        columns: list[str],
        rows: list[list[str]],
        highlight_rows: set[int] | None = None,
        sub_rows: dict | None = None,
    ) -> None:
        """
        결과 테이블을 추가합니다.

        Args:
            highlight_rows: 강조 표시할 행 인덱스 집합 (0-based).
            sub_rows: 서브행 dict {부모행_인덱스: [[col, col, ...], ...]}
                      부모 행 아래에 들여쓰기된 자식 행으로 표시됩니다.
        """
        self.tables.append({
            "title": title,
            "columns": columns,
            "rows": rows,
            "highlight_rows": highlight_rows or set(),
            "sub_rows": sub_rows or {},
        })

    def add_error(self, message: str) -> None:
        """에러/경고 메시지를 추가합니다."""
        self.errors.append(message)


class BaseAnalyzer(ABC):
    """
    모든 메신저 분석기의 추상 기반 클래스.

    하위 클래스 구현 예시:
        class KakaoAndroidAnalyzer(BaseAnalyzer):
            MESSENGER = "KakaoTalk"
            PLATFORM  = "Android"

            def analyze(self, path: Path) -> AnalysisResult:
                result = AnalysisResult()
                # ... 실제 분석 로직 ...
                result.summary["총 메시지 수"] = "1,234"
                result.add_table(
                    title="채팅방 목록",
                    columns=["채팅방", "참여자 수", "메시지 수", "첫 메시지", "마지막 메시지"],
                    rows=[["친구들", "5", "320", "2023-01-01", "2024-03-15"]],
                )
                return result
    """

    # 하위 클래스에서 반드시 정의해야 하는 클래스 변수
    MESSENGER: str = ""   # 예: "KakaoTalk"
    PLATFORM: str = ""    # "Android" 또는 "iOS"

    @abstractmethod
    def analyze(self, path: Path, **kwargs) -> AnalysisResult:
        """
        주어진 경로의 데이터베이스/폴더를 분석합니다.

        Args:
            path: 분석할 파일 또는 폴더 경로
            **kwargs: 분석기별 추가 옵션
                - fallback_user_id (int | None): Android KakaoTalk 전용

        Returns:
            AnalysisResult: 분석 결과
        """
        raise NotImplementedError

    def validate_path(self, path: Path) -> list[str]:
        """
        경로 유효성을 검사합니다. (선택적으로 오버라이드 가능)

        Returns:
            에러 메시지 목록 (빈 리스트 = 유효)
        """
        errors = []
        if not path.exists():
            errors.append(f"경로가 존재하지 않습니다: {path}")
        return errors

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} [{self.PLATFORM}]>"
