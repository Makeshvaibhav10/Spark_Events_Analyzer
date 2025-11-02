
import unittest
import json
from spark_event_analyzer.analyzer import analyze_events

class TestAnalyzer(unittest.TestCase):

    def test_analyze_events(self):
        with open('/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/sample_logs/sample_log.json', 'r') as f:
            events = json.load(f)
        
        analysis = analyze_events(events)
        
        self.assertIn('performance_summary', analysis)
        self.assertIn('bottlenecks', analysis)

if __name__ == '__main__':
    unittest.main()
