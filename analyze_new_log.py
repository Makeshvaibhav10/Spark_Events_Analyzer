import json
import os
from spark_event_analyzer.parser import parse_event_log
from spark_event_analyzer.analyzer import analyze_events
from spark_event_analyzer.root_cause_engine import determine_root_cause
from spark_event_analyzer.report_generator import generate_report
from spark_event_analyzer.failure_detector import FailureDetector

def analyze_new_log():
    """Analyzes the new sample log file."""
    output_dir = '/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/output'
    os.makedirs(output_dir, exist_ok=True)

    log_file = '/Users/makeshvaibhav/Desktop/spark-event-analyzer/tests/sample_logs/sample_log_new.json'
    
    print("\n" + "="*80)
    print("SPARK EVENT ANALYZER - NEW LOG ANALYSIS")
    print("="*80)
    
    # Step 1: Parse events
    print("\n[1/5] Parsing event log...")
    events = list(parse_event_log(log_file))
    print(f"      ✓ Parsed {len(events)} events")
    
    # Convert Pydantic models to dictionaries
    print("\n[2/5] Converting events to dictionaries...")
    events_dict = [event.model_dump(by_alias=True) for event in events]
    print(f"      ✓ Converted {len(events_dict)} events")

    # Step 2: Analyze events with correlation
    print("\n[3/5] Analyzing events with correlation detection...")
    analysis = analyze_events(events_dict)
    
    bottlenecks = analysis.get('bottlenecks', [])
    anomalies = analysis.get('anomalies', [])
    warnings = analysis.get('warnings', [])
    
    print(f"      ✓ Found {len(bottlenecks)} bottleneck(s)")
    print(f"      ✓ Found {len(anomalies)} anomaly/anomalies")
    print(f"      ✓ Found {len(warnings)} warning(s)")
    
    # Display bottlenecks with confidence
    if bottlenecks:
        print("\n      Detected Bottlenecks:")
        for b in bottlenecks:
            print(f"        - {b['bottleneck']} [{b['severity']}] - "
                  f"Confidence: {b['confidence']*100:.0f}% - "
                  f"Type: {b.get('type', 'UNKNOWN')}")
    
    # Step 3: Detect failures
    print("\n[4/5] Detecting failures and anomalies...")
    failure_detector = FailureDetector()
    metrics = analysis.get('performance_summary', {})
    failures = failure_detector.detect_failures(events_dict, metrics)
    
    failure_summary = failures.get('summary', {})
    print(f"      ✓ Stage Failures: {failure_summary.get('total_stage_failures', 0)}")
    print(f"      ✓ Task Failures: {failure_summary.get('total_task_failures', 0)}")
    print(f"      ✓ OOM Events: {failure_summary.get('total_oom_events', 0)}")
    print(f"      ✓ Fetch Failures: {failure_summary.get('total_fetch_failures', 0)}")
    
    # Step 4: Determine root causes
    print("\n[5/5] Determining root causes with confidence scoring...")
    root_causes = determine_root_cause(analysis)
    print(f"      ✓ Identified {len(root_causes)} root cause(s)")
    
    # Generate comprehensive report
    output_path = os.path.join(output_dir, 'analyzer_report_new')
    generate_report(analysis, root_causes, output_path, failures)
    
    print("\n" + "="*80)
    print("ANALYSIS RESULTS")
    print("="*80)
    
    if bottlenecks:
        print("\n[BOTTLENECKS]")
        for i, b in enumerate(bottlenecks, 1):
            confidence_bar = "█" * int(b['confidence'] * 20)
            print(f"\n{i}. {b['bottleneck']}")
            print(f"   Severity:   {b['severity']}")
            print(f"   Confidence: {confidence_bar} {b['confidence']*100:.0f}%")
            print(f"   Type:       {b.get('type', 'UNKNOWN')}")
            print(f"   Impact:     {b.get('impact', 'N/A')}")
            
            if 'reasoning' in b:
                print(f"\n   Analysis:")
                reasoning_lines = b['reasoning'].split('. ')
                for line in reasoning_lines[:3]:
                    if line.strip():
                        print(f"   {line.strip()}.\n")
    else:
        print("\n✓ No significant bottlenecks detected!")
    
    if root_causes:
        print("\n[ROOT CAUSES]")
        for i, rc in enumerate(root_causes, 1):
            print(f"\n{i}. {rc['root_cause']} [{rc.get('severity', 'UNKNOWN')}]")
            print(f"   Confidence: {rc['confidence']*100:.0f}%")
            print(f"   {rc.get('recommendation', 'No recommendation')}")
    
    if failure_summary.get('has_critical_failures'):
        print("\n[FAILURES DETECTED]")
        if failure_summary.get('total_oom_events', 0) > 0:
            print(f"   ⚠️  {failure_summary['total_oom_events']} Out of Memory event(s)")
        if failure_summary.get('total_stage_failures', 0) > 0:
            print(f"   ⚠️  {failure_summary['total_stage_failures']} Stage failure(s)")
        if failure_summary.get('total_fetch_failures', 0) > 0:
            print(f"   ⚠️  {failure_summary['total_fetch_failures']} Fetch failure(s)")
    
    print("\n" + "="*80)

if __name__ == '__main__':
    analyze_new_log()
