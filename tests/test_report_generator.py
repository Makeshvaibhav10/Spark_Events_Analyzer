
import unittest
import json
import os
from spark_event_analyzer.analyzer import analyze_events
from spark_event_analyzer.root_cause_engine import determine_root_cause
from spark_event_analyzer.report_generator import generate_report

class TestReportGenerator(unittest.TestCase):

    def test_generate_report(self):
        # Create the output directory if it doesn't exist
        output_dir = '/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open('/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/sample_logs/sample_log.json', 'r') as f:
            events = json.load(f)
        
        analysis = analyze_events(events)
        root_causes = determine_root_cause(analysis)
        output_path = os.path.join(output_dir, 'analyzer_report')
        
        generate_report(analysis, root_causes, output_path)
        
        self.assertTrue(os.path.exists(f"{output_path}.json"))

if __name__ == '__main__':
    unittest.main()
