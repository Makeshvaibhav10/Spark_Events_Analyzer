
import unittest
import json
from spark_event_analyzer.analyzer import analyze_events
from spark_event_analyzer.root_cause_engine import determine_root_cause

class TestRootCauseEngine(unittest.TestCase):

    def test_determine_root_cause(self):
        with open('/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/sample_logs/sample_log.json', 'r') as f:
            events = json.load(f)
        
        analysis = analyze_events(events)
        root_causes = determine_root_cause(analysis)
        
        self.assertIsInstance(root_causes, list)
        if root_causes:
            self.assertIn('root_cause', root_causes[0])
            self.assertIn('confidence', root_causes[0])
            self.assertIn('evidence', root_causes[0])
            self.assertIn('recommendation', root_causes[0])

if __name__ == '__main__':
    unittest.main()
