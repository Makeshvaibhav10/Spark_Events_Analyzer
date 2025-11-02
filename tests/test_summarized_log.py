import unittest
import json
import os
from spark_event_analyzer.analyzer import analyze_events
from spark_event_analyzer.root_cause_engine import determine_root_cause
from spark_event_analyzer.report_generator import generate_report

def parse_and_analyze_summarized_log(file_path: str):
    """
    Parses and analyzes a summarized Spark log file.

    Args:
        file_path: The path to the summarized Spark log file.
    """
    with open(file_path, 'r') as f:
        summarized_log = json.load(f)

    # Create a mock list of events from the summarized log
    events = []
    for stage in summarized_log.get('stages', []):
        for _ in range(stage.get('numTasks', 0)):
            events.append({
                'Event': 'SparkListenerTaskEnd',
                'Stage ID': stage.get('stageId'),
                'Task Info': {
                    'Finish Time': stage.get('taskMetrics', {}).get('avgTaskDurationMs', 0),
                    'Launch Time': 0,
                    'Executor ID': 'mock_executor'
                },
                'Task Metrics': {
                    'Executor Run Time': stage.get('executorRunTime', 0) / stage.get('numTasks', 1),
                    'Executor CPU Time': stage.get('executorRunTime', 0) / stage.get('numTasks', 1) * stage.get('taskMetrics', {}).get('avgCpuUtilization', 0),
                    'JVM GC Time': stage.get('gcTime', 0) / stage.get('numTasks', 1),
                    'Memory Bytes Spilled': stage.get('memoryBytesSpilled', 0) / stage.get('numTasks', 1),
                    'Input Metrics': {
                        'Bytes Read': 0
                    },
                    'Output Metrics': {
                        'Bytes Written': 0
                    },
                    'Shuffle Read Metrics': {
                        'Total Bytes Read': 0
                    },
                    'Shuffle Write Metrics': {
                        'Bytes Written': 0
                    },
                    'Fetch Wait Time': 0
                }
            })

    # Analyze the mock events
    analysis = analyze_events(events)
    root_causes = determine_root_cause(analysis)
    
    # Generate the report
    output_dir = '/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, 'analyzer_report_3')
    generate_report(analysis, root_causes, output_path)

    # Print the report
    with open(f"{output_path}.json", 'r') as f:
        print(f.read())

class TestSummarizedLog(unittest.TestCase):

    def test_summarized_log(self):
        parse_and_analyze_summarized_log('/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/sample_logs/sample_log_3.json')

if __name__ == '__main__':
    unittest.main()
