# spark_event_analyzer/__init__.py

from .parser import parse_event_log
from .analyzer import analyze_events, BottleneckAnalyzer
from .root_cause_engine import determine_root_cause
from .report_generator import generate_report
from .failure_detector import FailureDetector
from .visualizer import generate_visualizations
from .stage_analyzer import StageJobAnalyzer  # NEW
from .root_cause_explorer import RootCauseExplorer  # NEW

__version__ = "2.1.0"
__all__ = [
    'parse_event_log',
    'analyze_events',
    'BottleneckAnalyzer',
    'determine_root_cause',
    'generate_report',
    'FailureDetector',
    'generate_visualizations',
    'StageJobAnalyzer',  # NEW
    'RootCauseExplorer'  # NEW
]