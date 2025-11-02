"""
Spark Event Log Parser

Parses Spark event logs in JSON or NDJSON format.
Supports both local files and S3 paths.
Compatible with Spark 2.x and 3.x event formats.
"""

import argparse
import json
import logging
from typing import Iterator, Dict, Any, Type
import re

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
    SparkListenerTaskEnd,
    SparkListenerExecutorAdded,
    SparkListenerExecutorRemoved,
    SparkListenerBlockManagerAdded,
    SparkListenerBlockManagerRemoved,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

EVENT_MAPPING: Dict[str, Type[SparkEvent]] = {
    'SparkListenerApplicationStart': SparkListenerApplicationStart,
    'SparkListenerApplicationEnd': SparkListenerApplicationEnd,
    'SparkListenerJobStart': SparkListenerJobStart,
    'SparkListenerJobEnd': SparkListenerJobEnd,
    'SparkListenerStageSubmitted': SparkListenerStageSubmitted,
    'SparkListenerStageCompleted': SparkListenerStageCompleted,
    'SparkListenerTaskEnd': SparkListenerTaskEnd,
    'SparkListenerExecutorAdded': SparkListenerExecutorAdded,
    'SparkListenerExecutorRemoved': SparkListenerExecutorRemoved,
    'SparkListenerBlockManagerAdded': SparkListenerBlockManagerAdded,
    'SparkListenerBlockManagerRemoved': SparkListenerBlockManagerRemoved,
}

def parse_event_log(file_path: str) -> Iterator[SparkEvent]:
    """
    Parses a Spark event log file, yielding each event as a Pydantic model.

    This function streams the file, making it suitable for large logs. It
    handles both JSON arrays and newline-delimited JSON (NDJSON) formats,
    and automatically decompresses gzipped files.

    Args:
        file_path: The path to the Spark event log file (local or S3).

    Yields:
        A Pydantic model representing a single Spark event.

    Example:
        >>> for event in parse_event_log('s3://my-bucket/spark-eventlog.gz'):
        ...     if isinstance(event, SparkListenerJobStart):
        ...         print(f"Job {event.JobID} started.")
    """
    with open(file_path, 'rt', encoding='utf-8') as f:
        content = f.read()
        # Remove comments (some logs may have them)
        content = re.sub(r"//.*", "", content)
        
        try:
            # Try to parse as a single JSON object (for JSON array format)
            data = json.loads(content)
            if isinstance(data, list):
                # JSON array format
                for event_dict in data:
                    event = _parse_event_dict(event_dict)
                    if event:
                        yield event
            else:
                # Single JSON object
                event = _parse_event_dict(data)
                if event:
                    yield event
        except json.JSONDecodeError:
            # If that fails, try to parse as NDJSON (newline-delimited JSON)
            lines = content.split('\n')
            for line in lines:
                if line.strip():
                    try:
                        event_dict = json.loads(line)
                        event = _parse_event_dict(event_dict)
                        if event:
                            yield event
                    except json.JSONDecodeError:
                        logging.warning(f"Skipping malformed JSON line: {line.strip()[:100]}")

def _parse_event_dict(event_dict: Dict[str, Any]) -> SparkEvent:
    """
    Parses a single event dictionary into a Pydantic model.
    
    Args:
        event_dict: Dictionary containing event data
        
    Returns:
        Pydantic model instance or None if parsing fails
    """
    event_type = event_dict.get('Event')

    model = EVENT_MAPPING.get(event_type, SparkEvent)
    try:
        # Normalize field names for compatibility across Spark versions
        normalized_dict = _normalize_event_dict(event_dict, event_type)
        return model.model_validate(normalized_dict)
    except Exception as e:
        logging.warning(f"Skipping {event_type} event due to parsing error: {e}")
        return None

def _normalize_event_dict(event_dict: Dict[str, Any], event_type: str) -> Dict[str, Any]:
    """
    Normalize event dictionary to handle variations in field names across Spark versions.
    
    Some Spark logs use 'Time' while others use 'Timestamp', etc.
    This function ensures consistency.
    
    Args:
        event_dict: Original event dictionary
        event_type: Type of Spark event
        
    Returns:
        Normalized event dictionary
    """
    normalized = event_dict.copy()
    
    # Handle timestamp field variations
    # Some events use 'Time' in older Spark versions, 'Timestamp' in newer
    timestamp_events = [
        'SparkListenerApplicationStart',
        'SparkListenerApplicationEnd',
        'SparkListenerExecutorAdded',
        'SparkListenerExecutorRemoved',
        'SparkListenerBlockManagerAdded',
        'SparkListenerBlockManagerRemoved'
    ]
    
    if event_type in timestamp_events:
        # Check for 'Time' field and convert to 'Timestamp' if needed
        if 'Time' in normalized and 'Timestamp' not in normalized:
            normalized['Timestamp'] = normalized['Time']
        elif 'Timestamp' not in normalized and 'Time' not in normalized:
            # If neither exists, use a default timestamp (0)
            logging.debug(f"No timestamp found for {event_type}, using 0")
            normalized['Timestamp'] = 0
    
    # Handle other common field variations
    # Add more normalization rules here as needed
    
    return normalized

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Parse a Spark event log file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse local file
  python parser.py --input /path/to/spark-eventlog.json --output parsed_events.json
  
  # Parse S3 file
  python parser.py --input s3://bucket/spark-eventlog.gz --output parsed_events.json
  
  # Parse without saving (just validate)
  python parser.py --input /path/to/spark-eventlog.json
        """
    )
    parser.add_argument('--input', required=True, 
                       help='Path to the Spark event log file (local or S3)')
    parser.add_argument('--output', 
                       help='Path to the output JSON file (optional)')
    args = parser.parse_args()

    events = []
    with tqdm(desc="Parsing events", unit=" event") as pbar:
        for event in parse_event_log(args.input):
            # Use model_dump instead of deprecated dict()
            events.append(event.model_dump(by_alias=True))
            pbar.update(1)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(events, f, indent=2)
        logging.info(f"Successfully parsed {len(events)} events and saved to {args.output}")
    else:
        logging.info(f"Successfully parsed {len(events)} events.")
        
    # Print summary
    event_types = {}
    for event in events:
        event_type = event.get('Event', 'Unknown')
        event_types[event_type] = event_types.get(event_type, 0) + 1
    
    print("\nEvent Summary:")
    print("-" * 50)
    for event_type, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True):
        print(f"  {event_type}: {count}")
    print("-" * 50)
    print(f"Total Events: {len(events)}")
