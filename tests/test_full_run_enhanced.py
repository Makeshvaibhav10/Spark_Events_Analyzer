import unittest
import json
import os
import sys
sys.path.insert(0, '.')
from typing import Dict
from spark_event_analyzer.parser import parse_event_log
from spark_event_analyzer.analyzer import analyze_events
from spark_event_analyzer.root_cause_engine import determine_root_cause
from spark_event_analyzer.report_generator import generate_report
from spark_event_analyzer.failure_detector import FailureDetector
from spark_event_analyzer.stage_analyzer import StageJobAnalyzer
from spark_event_analyzer.root_cause_explorer import RootCauseExplorer

class TestFullRunEnhanced(unittest.TestCase):
    """Enhanced testing with stage/job analysis and root cause chains."""

    def test_full_run_with_stage_analysis(self):
        """Test the full Phase 2+ analysis pipeline with stage/job analysis."""
        # Create the output directory if it doesn't exist
        output_dir = '/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/output'
        os.makedirs(output_dir, exist_ok=True)

        log_file = '/Users/makeshvaibhav/Desktop/spark-event-analyzer/spark-event-file.json'
        
        print("\n" + "="*80)
        print("SPARK EVENT ANALYZER - ENHANCED PHASE 2+ FULL RUN TEST")
        print("="*80)
        
        # Step 1: Parse events
        print("\n[1/7] Parsing event log...")
        events = list(parse_event_log(log_file))
        print(f"      ✓ Parsed {len(events)} events")
        
        # Convert Pydantic models to dictionaries
        print("\n[2/7] Converting events to dictionaries...")
        events_dict = [event.model_dump(by_alias=True) for event in events]
        print(f"      ✓ Converted {len(events_dict)} events")

        # Step 2: Analyze events with correlation
        print("\n[3/7] Analyzing events with correlation detection...")
        analysis = analyze_events(events_dict)
        
        bottlenecks = analysis.get('bottlenecks', [])
        anomalies = analysis.get('anomalies', [])
        warnings = analysis.get('warnings', [])
        
        print(f"      ✓ Found {len(bottlenecks)} bottleneck(s)")
        print(f"      ✓ Found {len(anomalies)} anomaly/anomalies")
        print(f"      ✓ Found {len(warnings)} warning(s)")
        
        # Step 3: Detect failures
        print("\n[4/7] Detecting failures and anomalies...")
        failure_detector = FailureDetector()
        metrics = analysis.get('performance_summary', {})
        failures = failure_detector.detect_failures(events_dict, metrics)
        
        failure_summary = failures.get('summary', {})
        print(f"      ✓ Stage Failures: {failure_summary.get('total_stage_failures', 0)}")
        print(f"      ✓ Task Failures: {failure_summary.get('total_task_failures', 0)}")
        print(f"      ✓ OOM Events: {failure_summary.get('total_oom_events', 0)}")
        
        # Step 4: Analyze stages and jobs (NEW)
        print("\n[5/7] Analyzing stages and jobs...")
        stage_analyzer = StageJobAnalyzer()
        stage_analysis = stage_analyzer.analyze_stages_and_jobs(events_dict, metrics)
        
        stage_summary = stage_analysis.get('stage_summary', {})
        job_summary = stage_analysis.get('job_summary', {})
        
        print(f"      ✓ Total Stages: {stage_summary.get('total_stages', 0)}")
        print(f"      ✓ Failed Stages: {stage_summary.get('failed_stages', 0)}")
        print(f"      ✓ Stages with Issues: {stage_summary.get('stages_with_issues', 0)}")
        print(f"      ✓ Total Jobs: {job_summary.get('total_jobs', 0)}")
        print(f"      ✓ Failed Jobs: {job_summary.get('failed_jobs', 0)}")
        
        # Step 5: Build root cause chains (NEW)
        print("\n[6/7] Building root cause chains...")
        explorer = RootCauseExplorer()
        cause_chains = explorer.explore_all_failures(stage_analysis, failures, metrics)
        
        print(f"      ✓ Generated {len(cause_chains)} cause chain(s)")
        
        # Step 6: Determine root causes
        print("\n[7/7] Determining root causes...")
        root_causes = determine_root_cause(analysis)
        print(f"      ✓ Identified {len(root_causes)} root cause(s)")
        
        # Generate comprehensive report
        output_path = os.path.join(output_dir, 'analyzer_report_enhanced')
        generate_report(analysis, root_causes, output_path, failures)
        
        # Generate stage/job analysis report (NEW)
        self._generate_stage_job_report(stage_analysis, 
                                       os.path.join(output_dir, 'stage_job_analysis'))
        
        # Generate root cause chain report (NEW)
        chain_report = explorer.generate_interactive_report(cause_chains)
        with open(os.path.join(output_dir, 'root_cause_chains.txt'), 'w') as f:
            f.write(chain_report)
        
        # Print results
        print("\n" + "="*80)
        print("STAGE/JOB ANALYSIS RESULTS")
        print("="*80)
        
        # Print problematic transformations
        problematic = stage_analysis.get('problematic_transformations', [])
        if problematic:
            print("\n[PROBLEMATIC TRANSFORMATIONS]")
            for i, trans in enumerate(problematic[:5], 1):  # Top 5
                print(f"\n{i}. Stage {trans['stage_id']}: {trans['stage_name']}")
                print(f"   Transformation: {trans['transformation_type']} ({trans['operation']})")
                print(f"   Severity: {trans['severity']}")
                print(f"   Issues:")
                for issue in trans['issues'][:3]:  # Show top 3 issues
                    print(f"     - {issue['description']}")
        
        # Print performance hotspots
        hotspots = stage_analysis.get('performance_hotspots', [])
        if hotspots:
            print("\n[PERFORMANCE HOTSPOTS - Top 5 Slowest Stages]")
            for i, hotspot in enumerate(hotspots[:5], 1):
                print(f"\n{i}. Stage {hotspot['stage_id']}: {hotspot['stage_name']}")
                print(f"   Duration: {hotspot['duration_formatted']}")
                print(f"   Tasks: {hotspot['task_count']}")
                print(f"   Transformation: {hotspot['transformation']}")
                if hotspot['issues']:
                    print(f"   Issues: {len(hotspot['issues'])}")
        
        # Print root cause chains
        if cause_chains:
            print("\n" + "="*80)
            print("ROOT CAUSE CHAINS")
            print("="*80)
            print(chain_report)
        
        print("\n" + "="*80)
        print("REPORT FILES GENERATED")
        print("="*80)
        print(f"✓ Main Report (JSON): {output_path}.json")
        print(f"✓ Main Report (MD): {output_path}.md")
        print(f"✓ Summary (TXT): {output_path}_summary.txt")
        print(f"✓ Stage/Job Analysis: {output_dir}/stage_job_analysis.json")
        print(f"✓ Root Cause Chains: {output_dir}/root_cause_chains.txt")
        print("="*80)
        
        # Assertions
        self.assertIsNotNone(analysis)
        self.assertIsNotNone(stage_analysis)
        self.assertIsNotNone(failures)
        self.assertIsInstance(cause_chains, list)
        self.assertTrue(os.path.exists(f"{output_path}.json"))
        self.assertTrue(os.path.exists(f"{output_dir}/stage_job_analysis.json"))
        self.assertTrue(os.path.exists(f"{output_dir}/root_cause_chains.txt"))
        
        print(f"\n✓ All enhanced tests completed successfully!")
    
    def _generate_stage_job_report(self, stage_analysis: Dict, output_path: str):
        """Generate stage/job analysis JSON report."""
        import json
        
        with open(f"{output_path}.json", 'w') as f:
            json.dump(stage_analysis, f, indent=2)
        
        # Also generate markdown version
        with open(f"{output_path}.md", 'w') as f:
            f.write("# Stage and Job Analysis Report\n\n")
            
            # Stage summary
            stage_summary = stage_analysis.get('stage_summary', {})
            f.write("## Stage Summary\n\n")
            f.write(f"- **Total Stages:** {stage_summary.get('total_stages', 0)}\n")
            f.write(f"- **Failed Stages:** {stage_summary.get('failed_stages', 0)}\n")
            f.write(f"- **Stages with Issues:** {stage_summary.get('stages_with_issues', 0)}\n")
            f.write(f"- **Total Duration:** {stage_summary.get('total_duration_formatted', 'N/A')}\n\n")
            
            # Job summary
            job_summary = stage_analysis.get('job_summary', {})
            f.write("## Job Summary\n\n")
            f.write(f"- **Total Jobs:** {job_summary.get('total_jobs', 0)}\n")
            f.write(f"- **Failed Jobs:** {job_summary.get('failed_jobs', 0)}\n")
            f.write(f"- **Successful Jobs:** {job_summary.get('successful_jobs', 0)}\n\n")
            
            # Problematic transformations
            problematic = stage_analysis.get('problematic_transformations', [])
            if problematic:
                f.write("## Problematic Transformations\n\n")
                for trans in problematic:
                    f.write(f"### Stage {trans['stage_id']}: {trans['stage_name']}\n\n")
                    f.write(f"- **Transformation:** {trans['transformation_type']} ({trans['operation']})\n")
                    f.write(f"- **Severity:** {trans['severity']}\n")
                    f.write(f"- **Issues:**\n")
                    for issue in trans['issues']:
                        f.write(f"  - {issue['description']}\n")
                    f.write("\n")
            
            # Performance hotspots
            hotspots = stage_analysis.get('performance_hotspots', [])
            if hotspots:
                f.write("## Performance Hotspots\n\n")
                f.write("| Rank | Stage ID | Duration | Tasks | Transformation | Issues |\n")
                f.write("|------|----------|----------|-------|----------------|--------|\n")
                for i, hotspot in enumerate(hotspots[:10], 1):
                    f.write(f"| {i} | {hotspot['stage_id']} | {hotspot['duration_formatted']} | ")
                    f.write(f"{hotspot['task_count']} | {hotspot['transformation']} | ")
                    f.write(f"{len(hotspot['issues'])} |\n")
                f.write("\n")
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes into human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"

if __name__ == '__main__':
    unittest.main(verbosity=2)
