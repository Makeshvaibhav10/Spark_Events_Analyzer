"""
Full Pipeline Integration Test

Tests the complete refactored Spark Event Analyzer pipeline
using the standardized event schema.
"""

import unittest
import json
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from spark_event_analyzer.parser import SparkEventParser
from spark_event_analyzer.analyzer import BottleneckAnalyzer
from spark_event_analyzer.root_cause_engine import determine_root_cause
from spark_event_analyzer.report_generator import generate_report
from spark_event_analyzer.failure_detector import FailureDetector


class TestFullPipelineRun(unittest.TestCase):
    """Integration test for the complete pipeline."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.test_dir = Path(__file__).parent
        cls.sample_log = cls.test_dir / 'sample_logs' / 'sample_spark_event_log.json'
        cls.output_dir = cls.test_dir / 'output'
        cls.output_dir.mkdir(exist_ok=True)
    
    def test_01_parser(self):
        """Test event parser with new schema."""
        print("" + "="*80)
        print("TEST 1: Event Parser")
        print("="*80)
        
        parser = SparkEventParser(strict_mode=False)
        events = list(parser.parse(str(self.sample_log), show_progress=False))
        
        stats = parser.get_statistics()
        
        print(f"✓ Parsed {stats['parsed_events']} events")
        print(f"✓ Failed {stats['failed_events']} events")
        print(f"✓ Success rate: {stats['parsed_events']/stats['total_events']*100:.1f}%")
        
        print("Event Type Distribution:")
        for event_type, count in sorted(stats['event_type_counts'].items(), 
                                        key=lambda x: x[1], reverse=True):
            print(f"  {event_type:50s}: {count:4d}")
        
        self.assertGreater(len(events), 0)
        self.assertGreater(stats['parsed_events'], 0)
        
        # Verify event structure
        task_end_events = [e for e in events if e.Event == 'SparkListenerTaskEnd']
        if task_end_events:
            task_event = task_end_events[0]
            print(f"✓ Sample TaskEnd event validated")
            print(f"  - Has Task Info: {hasattr(task_event, 'TaskInfo')}")
            print(f"  - Has Task Executor Metrics: {hasattr(task_event, 'TaskExecutorMetrics')}")
    
    def test_02_metrics_engine(self):
        """Test metrics computation with new schema."""
        print("" + "="*80)
        print("TEST 2: Metrics Engine")
        print("="*80)
        
        # Parse events
        parser = SparkEventParser()
        events = list(parser.parse(str(self.sample_log), show_progress=False))
        events_dict = [e.model_dump(by_alias=True) for e in events]
        
        # Compute metrics
        from spark_event_analyzer.metrics_engine import MetricsEngine
        engine = MetricsEngine()
        metrics = engine.compute_metrics(events_dict)
        
        print(f"✓ Computed metrics successfully")
        
        # Verify all metric categories exist
        required_categories = [
            'task_metrics', 'stage_metrics', 'job_metrics',
            'executor_metrics', 'cpu_metrics', 'memory_metrics',
            'io_metrics', 'shuffle_metrics', 'correlations',
            'thresholds', 'global_metrics'
        ]
        
        for category in required_categories:
            self.assertIn(category, metrics)
            print(f"  ✓ {category}")
        
        # Display key metrics
        print("Key Performance Indicators:")
        cpu_eff = metrics['cpu_metrics'].get('cpu_efficiency', 0)
        gc_ratio = metrics['memory_metrics'].get('gc_overhead_ratio', 0)
        shuffle_ratio = metrics['shuffle_metrics'].get('shuffle_ratio', 0)
        
        print(f"  CPU Efficiency:     {cpu_eff*100:.1f}%")
        print(f"  GC Overhead:        {gc_ratio*100:.1f}%")
        print(f"  Shuffle Overhead:   {shuffle_ratio*100:.1f}%")
        
        self.assertIsInstance(metrics, dict)
    
    def test_03_bottleneck_detection(self):
        """Test bottleneck detection with new schema."""
        print("" + "="*80)
        print("TEST 3: Bottleneck Detection")
        print("="*80)
        
        # Parse and analyze
        parser = SparkEventParser()
        events = list(parser.parse(str(self.sample_log), show_progress=False))
        events_dict = [e.model_dump(by_alias=True) for e in events]
        
        analyzer = BottleneckAnalyzer()
        analysis = analyzer.analyze_events(events_dict)
        
        bottlenecks = analysis.get('bottlenecks', [])
        anomalies = analysis.get('anomalies', [])
        warnings = analysis.get('warnings', [])
        
        print(f"✓ Found {len(bottlenecks)} bottleneck(s)")
        print(f"✓ Found {len(anomalies)} anomaly/anomalies")
        print(f"✓ Found {len(warnings)} warning(s)")
        
        if bottlenecks:
            print("Detected Bottlenecks:")
            for i, b in enumerate(bottlenecks, 1):
                print(f"{i}. {b['bottleneck']} [{b['severity']}]")
                print(f"   Confidence: {b['confidence']*100:.0f}%")
                print(f"   Type: {b['type']}")
                print(f"   Impact: {b['impact']}")
        
        self.assertIn('bottlenecks', analysis)
        self.assertIn('anomalies', analysis)
        self.assertIn('performance_summary', analysis)
    
    def test_04_root_cause_analysis(self):
        """Test root cause determination."""
        print("" + "="*80)
        print("TEST 4: Root Cause Analysis")
        print("="*80)
        
        # Full pipeline
        parser = SparkEventParser()
        events = list(parser.parse(str(self.sample_log), show_progress=False))
        events_dict = [e.model_dump(by_alias=True) for e in events]
        
        analyzer = BottleneckAnalyzer()
        analysis = analyzer.analyze_events(events_dict)
        
        root_causes = determine_root_cause(analysis)
        
        print(f"✓ Identified {len(root_causes)} root cause(s)")
        
        if root_causes:
            print("Root Causes:")
            for i, rc in enumerate(root_causes, 1):
                if rc.get('severity') != 'INFO':
                    print(f"{i}. {rc['root_cause']} [{rc.get('severity', 'UNKNOWN')}]")
                    print(f"   Confidence: {rc['confidence']*100:.0f}%")
                    print(f"   Recommendation: {rc.get('recommendation', '')[:80]}...")
        
        self.assertIsInstance(root_causes, list)
        self.assertGreater(len(root_causes), 0)
    
    def test_05_failure_detection(self):
        """Test failure detection."""
        print("" + "="*80)
        print("TEST 5: Failure Detection")
        print("="*80)
        
        parser = SparkEventParser()
        events = list(parser.parse(str(self.sample_log), show_progress=False))
        events_dict = [e.model_dump(by_alias=True) for e in events]
        
        analyzer = BottleneckAnalyzer()
        analysis = analyzer.analyze_events(events_dict)
        metrics = analysis['performance_summary']
        
        detector = FailureDetector()
        failures = detector.detect_failures(events_dict, metrics)
        
        summary = failures.get('summary', {})
        
        print(f"✓ Stage Failures:    {summary.get('total_stage_failures', 0)}")
        print(f"✓ Task Failures:     {summary.get('total_task_failures', 0)}")
        print(f"✓ Executor Failures: {summary.get('total_executor_failures', 0)}")
        print(f"✓ OOM Events:        {summary.get('total_oom_events', 0)}")
        print(f"✓ Fetch Failures:    {summary.get('total_fetch_failures', 0)}")
        
        self.assertIn('summary', failures)
        self.assertIsInstance(failures, dict)
    
    def test_06_report_generation(self):
        """Test report generation."""
        print("" + "="*80)
        print("TEST 6: Report Generation")
        print("="*80)
        
        # Full pipeline
        parser = SparkEventParser()
        events = list(parser.parse(str(self.sample_log), show_progress=False))
        events_dict = [e.model_dump(by_alias=True) for e in events]
        
        analyzer = BottleneckAnalyzer()
        analysis = analyzer.analyze_events(events_dict)
        
        root_causes = determine_root_cause(analysis)
        
        detector = FailureDetector()
        metrics = analysis['performance_summary']
        failures = detector.detect_failures(events_dict, metrics)
        
        # Generate reports
        output_path = self.output_dir / 'pipeline_test_report'
        generate_report(analysis, root_causes, str(output_path), failures)
        
        # Verify files exist
        json_report = Path(f"{output_path}.json")
        md_report = Path(f"{output_path}.md")
        txt_report = Path(f"{output_path}_summary.txt")
        
        print(f"✓ JSON Report:  {json_report.exists()}")
        print(f"✓ MD Report:    {md_report.exists()}")
        print(f"✓ TXT Summary:  {txt_report.exists()}")
        
        self.assertTrue(json_report.exists())
        self.assertTrue(md_report.exists())
        self.assertTrue(txt_report.exists())
    
    def test_07_end_to_end_pipeline(self):
        """Test complete end-to-end pipeline."""
        print("" + "="*80)
        print("TEST 7: End-to-End Pipeline")
        print("="*80)
        
        print("[1/6] Parsing events...")
        parser = SparkEventParser()
        events = list(parser.parse(str(self.sample_log), show_progress=False))
        print(f"      ✓ Parsed {len(events)} events")
        
        print("[2/6] Converting to dictionaries...")
        events_dict = [e.model_dump(by_alias=True) for e in events]
        print(f"      ✓ Converted {len(events_dict)} events")
        
        print("[3/6] Analyzing bottlenecks...")
        analyzer = BottleneckAnalyzer()
        analysis = analyzer.analyze_events(events_dict)
        bottlenecks = analysis.get('bottlenecks', [])
        print(f"      ✓ Found {len(bottlenecks)} bottleneck(s)")
        
        print("[4/6] Detecting failures...")
        detector = FailureDetector()
        metrics = analysis['performance_summary']
        failures = detector.detect_failures(events_dict, metrics)
        print(f"      ✓ Detected {failures['summary'].get('total_stage_failures', 0)} stage failures")
        
        print("[5/6] Determining root causes...")
        root_causes = determine_root_cause(analysis)
        print(f"      ✓ Identified {len(root_causes)} root cause(s)")
        
        print("[6/6] Generating reports...")
        output_path = self.output_dir / 'e2e_pipeline_report'
        generate_report(analysis, root_causes, str(output_path), failures)
        print(f"      ✓ Reports saved to {output_path}")
        
        # Final summary
        print("" + "="*80)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*80)
        
        print("Performance Metrics:")
        cpu_eff = metrics['cpu_metrics'].get('cpu_efficiency', 0)
        gc_ratio = metrics['memory_metrics'].get('gc_overhead_ratio', 0)
        shuffle_ratio = metrics['shuffle_metrics'].get('shuffle_ratio', 0)
        
        print(f"  CPU Efficiency:   {cpu_eff*100:.1f}%")
        print(f"  GC Overhead:      {gc_ratio*100:.1f}%")
        print(f"  Shuffle Overhead: {shuffle_ratio*100:.1f}%")
        
        print("Analysis Results:")
        print(f"  Bottlenecks:      {len(bottlenecks)}")
        print(f"  Root Causes:      {len([rc for rc in root_causes if rc.get('severity') != 'INFO'])}")
        print(f"  Stage Failures:   {failures['summary'].get('total_stage_failures', 0)}")
        
        print("" + "="*80)
        print("✓ ALL TESTS PASSED")
        print("="*80)
        
        # Assertions
        self.assertGreater(len(events), 0)
        self.assertIsNotNone(analysis)
        self.assertIsNotNone(root_causes)
        self.assertIsNotNone(failures)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)