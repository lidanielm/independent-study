"""
Agentic AI system for financial data analysis.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .base_agent import BaseAgent as BaseAgent
    from .research_agent import ResearchAgent as ResearchAgent


def __getattr__(name: str) -> Any:
    if name == "BaseAgent":
        from .base_agent import BaseAgent
        return BaseAgent
    if name == "ResearchAgent":
        from .research_agent import ResearchAgent
        return ResearchAgent
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ['BaseAgent', 'ResearchAgent']

