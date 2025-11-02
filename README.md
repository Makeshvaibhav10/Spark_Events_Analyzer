
# Spark Event Analyzer

A high-performance, explainable analytics engine that consumes parsed Spark event log data and produces in-depth performance diagnostics, failure analysis, and root-cause reasoning for long-running Spark applications.

## Features

- **Metrics Aggregation**: Computes application-level and stage-level statistics for CPU, memory, I/O, shuffle, and GC.
- **Performance Bottleneck Detection**: Dynamically detects performance inefficiencies such as GC overhead, memory pressure, data skew, and more.
- **Failure Analysis**: Identifies and classifies job, stage, and task failures.
- **Root Cause Reasoning**: Infers the most probable root cause of performance issues and provides recommendations for optimization.
- **JSON Report**: Generates a comprehensive JSON report of the analysis.

## Architecture

The analyzer is composed of the following modules:

- **`parser.py`**: A streaming parser for Apache Spark event logs.
- **`metrics_engine.py`**: A metrics computation module that uses pandas for efficient metric summarization.
- **`analyzer.py`**: The main analysis engine that consumes parsed data and produces performance diagnostics.
- **`root_cause_engine.py`**: A dynamic reasoning engine that infers the root cause of performance issues.
- **`report_generator.py`**: A report generator that produces a comprehensive JSON report of the analysis.

## How to Use

1. **Parse a Spark event log:**

   ```bash
   python -m spark_event_analyzer.parser --input /path/to/event_log --output /path/to/parsed_log.json
   ```

2. **Analyze the parsed log:**

   ```python
   import json
   from spark_event_analyzer.analyzer import analyze_events
   from spark_event_analyzer.root_cause_engine import determine_root_cause
   from spark_event_analyzer.report_generator import generate_report

   with open('/path/to/parsed_log.json', 'r') as f:
       events = json.load(f)

   analysis = analyze_events(events)
   root_causes = determine_root_cause(analysis)
   generate_report(analysis, root_causes, '/path/to/report')
   ```

## Output

The analyzer generates a JSON report with the following structure:

```json
{
    "summary": {
        "total_jobs": 0,
        "total_stages": 0,
        "total_tasks": 0,
        "total_runtime": "0h 0m 0s"
    },
    "performance_summary": {},
    "bottlenecks": [],
    "failures": [],
    "root_cause_analysis": [],
    "recommendations": []
}
```
