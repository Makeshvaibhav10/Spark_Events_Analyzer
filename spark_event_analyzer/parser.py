"""
Standardized Spark Event Log Parser

Parses Spark event logs using the new unified schema.
Supports JSON, NDJSON, local files, and S3 paths with automatic decompression.
"""

import json
import logging
from typing import Iterator, Dict, Any, Optional
from smart_open import open
from tqdm import tqdm

from spark_event_analyzer.models import (
    SparkEvent,
    SparkListenerApplicationStart,
    SparkListenerApplicationEnd,
    SparkListenerJobStart,
    SparkListenerJobEnd,
    SparkListenerStageSubmitted,
    SparkListenerStageCompleted,
    SparkListenerTaskStart,
    SparkListenerTaskEnd,
    SparkListenerExecutorAdded,
    SparkListenerExecutorRemoved,
    SparkListenerBlockManagerAdded,
    SparkListenerBlockManagerRemoved,
)

EVENT_TYPE_MAP = {
    "SparkListenerApplicationStart": SparkListenerApplicationStart,
    "SparkListenerApplicationEnd": SparkListenerApplicationEnd,
    "SparkListenerJobStart": SparkListenerJobStart,
    "SparkListenerJobEnd": SparkListenerJobEnd,
    "SparkListenerStageSubmitted": SparkListenerStageSubmitted,
    "SparkListenerStageCompleted": SparkListenerStageCompleted,
    "SparkListenerTaskStart": SparkListenerTaskStart,
    "SparkListenerTaskEnd": SparkListenerTaskEnd,
    "SparkListenerExecutorAdded": SparkListenerExecutorAdded,
    "SparkListenerExecutorRemoved": SparkListenerExecutorRemoved,
    "SparkListenerBlockManagerAdded": SparkListenerBlockManagerAdded,
    "SparkListenerBlockManagerRemoved": SparkListenerBlockManagerRemoved,
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkEventParser:
    """
    Unified parser for Spark event logs.
    
    Handles both JSON arrays and NDJSON formats.
    Automatically detects and decompresses gzipped files.
    Supports local and S3 file paths.
    """
    
    def __init__(self, strict_mode: bool = False):
        """
        Initialize the parser. 
        
        Args:
            strict_mode: If True, raises exceptions on parsing errors.
                        If False, logs warnings and continues.
        """
        self.strict_mode = strict_mode
        self.stats = {
            'total_events': 0,
            'parsed_events': 0,
            'failed_events': 0,
            'event_type_counts': {}
        }
    
    def parse(self, file_path: str, show_progress: bool = True) -> Iterator[SparkEvent]:
        """
        Parse a Spark event log file.
        
        Args:
            file_path: Path to the event log (local or S3)
            show_progress: Whether to show progress bar
            
        Yields:
            Parsed SparkEvent objects
            
        Example:
            >>> parser = SparkEventParser()
            >>> for event in parser.parse('s3://bucket/spark-events.json.gz'):
            ...     print(event.Event)
        """
        logger.info(f"Starting to parse: {file_path}")
        
        try:
            with open(file_path, 'rt', encoding='utf-8') as f:
                # Detect format by reading first character
                first_char = f.read(1)
                f.seek(0)
                
                if first_char == '[':
                    yield from self._parse_json_array(f, show_progress)
                else:
                    yield from self._parse_ndjson(f, show_progress)
                    
        except Exception as e:
            logger.error(f"Failed to open file {file_path}: {e}")
            if self.strict_mode:
                raise
        
        logger.info(f"Parsing complete: {self.stats}")
    
    def _parse_json_array(self, file_handle, show_progress: bool) -> Iterator[SparkEvent]:
        """Parse JSON array format."""
        try:
            data = json.load(file_handle)
            
            iterator = tqdm(data, desc="Parsing events") if show_progress else data
            
            for event_dict in iterator:
                event = self._parse_event_dict(event_dict)
                if event:
                    self.stats['parsed_events'] += 1
                    yield event
                    
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            if self.strict_mode:
                raise
    
    def _parse_ndjson(self, file_handle, show_progress: bool) -> Iterator[SparkEvent]:
        """Parse NDJSON (newline-delimited JSON) format."""
        lines = file_handle.readlines() if show_progress else file_handle
        iterator = tqdm(lines, desc="Parsing events") if show_progress else lines
        
        for line in iterator:
            line = line.strip()
            if not line:
                continue
                
            try:
                event_dict = json.loads(line)
                event = self._parse_event_dict(event_dict)
                if event:
                    self.stats['parsed_events'] += 1
                    yield event
                    
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping malformed line: {line[:100]}... Error: {e}")
                self.stats['failed_events'] += 1
                if self.strict_mode:
                    raise
    
    def _parse_event_dict(self, event_dict: Dict[str, Any]) -> Optional[SparkEvent]:
        """
        Parse a single event dictionary into a Pydantic model.
        
        Args:
            event_dict: Raw event dictionary
            
        Returns:
            Parsed SparkEvent or None if parsing fails
        """
        self.stats['total_events'] += 1
        
        event_type = event_dict.get('Event')
        if not event_type:
            logger.warning(f"Event missing 'Event' field: {event_dict}")
            self.stats['failed_events'] += 1
            return None
        
        # Update event type statistics
        self.stats['event_type_counts'][event_type] = \
            self.stats['event_type_counts'].get(event_type, 0) + 1
        
        # Get appropriate model class
        model_class = EVENT_TYPE_MAP.get(event_type, SparkEvent)
        
        try:
            # Normalize event dict for compatibility
            normalized_dict = self._normalize_event_dict(event_dict)
            
            # Parse using Pydantic
            return model_class.model_validate(normalized_dict)
            
        except Exception as e:
            logger.warning(f"Failed to parse {event_type}: {e}")
            self.stats['failed_events'] += 1
            
            if self.strict_mode:
                raise
            
            # Fallback to base SparkEvent
            try:
                return SparkEvent.model_validate(event_dict)
            except:
                return None
    
    def _normalize_event_dict(self, event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize event dictionary for cross-version compatibility.
        
        Handles field name variations between Spark versions.
        """
        normalized = event_dict.copy()
        
        # Handle timestamp variations
        if 'Time' in normalized and 'Timestamp' not in normalized:
            normalized['Timestamp'] = normalized['Time']
        
        # Handle task metrics structure variations
        if 'Task Metrics' in normalized and isinstance(normalized['Task Metrics'], dict):
            task_metrics = normalized['Task Metrics']
            
            # Ensure nested metrics exist
            if 'Shuffle Read Metrics' not in task_metrics:
                task_metrics['Shuffle Read Metrics'] = {}
            if 'Shuffle Write Metrics' not in task_metrics:
                task_metrics['Shuffle Write Metrics'] = {}
            if 'Input Metrics' not in task_metrics:
                task_metrics['Input Metrics'] = {}
            if 'Output Metrics' not in task_metrics:
                task_metrics['Output Metrics'] = {}
        
        # Handle executor metrics structure
        if 'Task Executor Metrics' in normalized and isinstance(normalized['Task Executor Metrics'], dict):
            exec_metrics = normalized['Task Executor Metrics']
            
            # Ensure nested metrics exist
            if 'Shuffle Write Metrics' not in exec_metrics:
                exec_metrics['Shuffle Write Metrics'] = {}
            if 'Shuffle Read Metrics' not in exec_metrics:
                exec_metrics['Shuffle Read Metrics'] = {}
            if 'Input Metrics' not in exec_metrics:
                exec_metrics['Input Metrics'] = {}
            if 'Output Metrics' not in exec_metrics:
                exec_metrics['Output Metrics'] = {}
        
        return normalized
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get parsing statistics."""
        return self.stats.copy()
    
    def reset_statistics(self):
        """Reset parsing statistics."""
        self.stats = {
            'total_events': 0,
            'parsed_events': 0,
            'failed_events': 0,
            'event_type_counts': {}
        }


# ============================================================================ 
# CONVENIENCE FUNCTION (backward compatibility)
# ============================================================================ 

def parse_event_log(file_path: str, strict_mode: bool = False, 
                   show_progress: bool = True) -> Iterator[SparkEvent]:
    """
    Parse a Spark event log file.
    
    Args:
        file_path: Path to the event log (local or S3)
        strict_mode: If True, raises exceptions on errors
        show_progress: Whether to show progress bar
        
    Yields:
        Parsed SparkEvent objects
        
    Example:
        >>> for event in parse_event_log('spark-events.json'):
        ...     print(f"Event: {event.Event}")
    """
    parser = SparkEventParser(strict_mode=strict_mode)
    yield from parser.parse(file_path, show_progress=show_progress)


# ============================================================================ 
# CLI INTERFACE
# ============================================================================ 

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Parse and validate Spark event logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse local file
  python parser.py --input spark-events.json --output parsed.json
  
  # Parse S3 file
  python parser.py --input s3://bucket/spark-events.gz --validate-only
  
  # Strict mode (fail on first error)
  python parser.py --input events.json --strict
        """
    )
    
    parser.add_argument('--input', required=True, help='Input event log path')
    parser.add_argument('--output', help='Output JSON path (optional)')
    parser.add_argument('--strict', action='store_true', help='Enable strict mode')
    parser.add_argument('--validate-only', action='store_true', 
                       help='Only validate, do not save output')
    parser.add_argument('--no-progress', action='store_true',
                       help='Disable progress bar')
    
    args = parser.parse_args()
    
    # Parse events
    event_parser = SparkEventParser(strict_mode=args.strict)
    events = []
    
    for event in event_parser.parse(args.input, show_progress=not args.no_progress):
        events.append(event.model_dump(by_alias=True))
    
    # Display statistics
    stats = event_parser.get_statistics()
    print("\n" + "="*80)
    print("PARSING STATISTICS")
    print("="*80)
    print(f"Total events processed: {stats['total_events']}")
    print(f"Successfully parsed:    {stats['parsed_events']}")
    print(f"Failed to parse:        {stats['failed_events']}")
    print(f"Success rate:           {stats['parsed_events']/stats['total_events']*100:.1f}%")
    
    print("\nEvent Type Distribution:")
    print("-"*80)
    for event_type, count in sorted(stats['event_type_counts'].items(), 
                                    key=lambda x: x[1], reverse=True):
        print(f"  {event_type:50s}: {count:6d}")
    print("="*80)
    
    # Save output if requested
    if args.output and not args.validate_only:
        with open(args.output, 'w') as f:
            json.dump(events, f, indent=2)
        print(f"\n✓ Saved {len(events)} events to {args.output}")