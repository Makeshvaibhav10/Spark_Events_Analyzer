
import unittest
import json
from spark_event_analyzer.metrics_engine import compute_metrics

class TestMetricsEngine(unittest.TestCase):

    def test_compute_metrics(self):
        with open('/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/sample_logs/sample_log.json', 'r') as f:
            events = json.load(f)
        
        metrics = compute_metrics(events)
        
        self.assertIn('task_metrics', metrics)
        self.assertIn('mean_task_duration', metrics['task_metrics'])
        self.assertIn('std_task_duration', metrics['task_metrics'])
        self.assertIn('task_skew_ratio', metrics['task_metrics'])
        
        self.assertIn('executor_metrics', metrics)
        self.assertIn('executor_task_counts', metrics['executor_metrics'])
        self.assertIn('executor_imbalance_ratio', metrics['executor_metrics'])
        
        self.assertIn('cpu_metrics', metrics)
        self.assertIn('cpu_efficiency', metrics['cpu_metrics'])
        
        self.assertIn('memory_metrics', metrics)
        self.assertIn('gc_overhead_ratio', metrics['memory_metrics'])
        self.assertIn('memoryBytesSpilled', metrics['memory_metrics'])
        
        self.assertIn('io_metrics', metrics)
        self.assertIn('inputBytes', metrics['io_metrics'])
        self.assertIn('outputBytes', metrics['io_metrics'])
        
        self.assertIn('shuffle_metrics', metrics)
        self.assertIn('shuffleReadBytes', metrics['shuffle_metrics'])
        self.assertIn('shuffleWriteBytes', metrics['shuffle_metrics'])
        self.assertIn('shuffleTime', metrics['shuffle_metrics'])
        self.assertIn('shuffle_ratio', metrics['shuffle_metrics'])

if __name__ == '__main__':
    unittest.main()
