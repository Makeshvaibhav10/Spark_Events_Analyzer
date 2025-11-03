"""
Complete Spark Log Analyzer - Main Analysis Script

Usage:
    python analyze.py --input sample_log.json
    python analyze.py --input sample_log.json --output results/ 
"""

import json
import argparse
import sys
from pathlib import Path

# Import all components
from spark_event_analyzer.parser import parse_event_log
from spark_event_analyzer.metrics_engine import compute_metrics
from spark_event_analyzer.bottleneck_detector import detect_bottlenecks, identify_performance_hotspots
from spark_event_analyzer.root_cause_engine import determine_root_cause
from spark_event_analyzer.executor_monitor import monitor_executors
from spark_event_analyzer.report_generator import generate_report

def analyze_spark_log(log_data, output_dir='output'):
    """
    Complete analysis of Spark event log.
    
    Args:
        log_data: Either file path (str) or list of event dicts
        output_dir: Directory to save results
    """
    print("=" * 80)
    print("SPARK EVENT LOG ANALYZER")
    print("=" * 80)
    
    # Step 1: Parse events
    print("\n[1/6] Parsing events...")
    if isinstance(log_data, str):
        events = list(parse_event_log(log_data))
        events_dict = [e.model_dump(by_alias=True) for e in events]
    else:
        events_dict = log_data
        events = log_data
    
    print(f"      ✓ Parsed {len(events_dict)} events")
    
    # Step 2: Compute metrics
    print("\n[2/6] Computing metrics...")
    metrics = compute_metrics(events_dict)
    print(f"      ✓ Computed metrics for {metrics['global_metrics']['total_tasks']} tasks")
    
    # Step 3: Detect bottlenecks
    print("\n[3/6] Detecting bottlenecks...")
    bottlenecks_result = detect_bottlenecks(metrics, events_dict)
    print(f"      ✓ Found {bottlenecks_result['total_bottlenecks']} bottleneck(s)")
    
    # Step 4: Identify root causes
    print("\n[4/6] Identifying root causes...")
    root_causes_list = determine_root_cause({
        'events': events_dict,
        'metrics': metrics,
        'bottlenecks': bottlenecks_result.get('bottlenecks', [])
    })
    root_causes = {
        'total_causes': len(root_causes_list),
        'root_causes': root_causes_list
    }
    print(f"      ✓ Identified {root_causes['total_causes']} root cause(s)")
    
    # Step 5: Monitor executors
    print("\n[5/6] Monitoring executors...")
    executor_monitor_result = monitor_executors(events_dict)
    print(f"      ✓ Tracked {executor_monitor_result['summary']['total_executors']} executor(s)")
    
    # Step 6: Identify hotspots
    print("\n[6/6] Identifying performance hotspots...")
    hotspots = identify_performance_hotspots(events_dict, metrics)
    print(f"      ✓ Identified {len(hotspots)} performance hotspot(s)")
    
    # Generate comprehensive report
    report = {
        'metadata': {
            'total_events': len(events_dict),
            'analysis_timestamp': None  # Can add datetime
        },
        'metrics': metrics,
        'bottlenecks': bottlenecks_result,
        'root_causes': root_causes,
        'executor_monitoring': executor_monitor_result,
        'performance_hotspots': hotspots
    }

    # Create a dictionary that matches the structure expected by generate_report
    analysis_for_report = {
        'performance_summary': metrics,
        'bottlenecks': bottlenecks_result.get('bottlenecks', []),
        'anomalies': executor_monitor_result.get('anomalies', []),
        'warnings': [],
        'analysis_metadata': {}
    }

    # Generate reports
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    report_base_path = Path(output_dir) / 'analysis_report'
    generate_report(analysis_for_report, root_causes.get('root_causes', []), str(report_base_path))

    print_analysis_summary(report)
    
    return report

def print_analysis_summary(report: dict):
    """Print human-readable analysis summary."""
    print("\n" + "=" * 80)
    print("ANALYSIS SUMMARY")
    print("=" * 80)
    
    # Metrics summary
    metrics = report['metrics']
    print("\n📊 PERFORMANCE METRICS:")
    print(f"   Tasks: {metrics['global_metrics']['total_tasks']}")
    print(f"   Executors: {metrics['executor_metrics'].get('total_executors', 0)}")
    print(f"   CPU Efficiency: {metrics['cpu_metrics'].get('cpu_efficiency', 0)*100:.1f}%")
    
    memory_metrics = metrics.get('memory_metrics', {})
    if memory_metrics:
        print(f"   GC Overhead: {memory_metrics.get('gc_overhead_percentage', 0):.1f}%")
    
    shuffle_metrics = metrics.get('shuffle_metrics', {})
    if shuffle_metrics.get('total_shuffle_bytes', 0) > 0:
        print(f"   Shuffle Data: {shuffle_metrics.get('total_shuffle_bytes_formatted', '0 B')}")
    
    # Bottlenecks
    bottlenecks = report['bottlenecks']
    print(f"\n⚠️  BOTTLENECKS DETECTED: {bottlenecks['total_bottlenecks']}")
    
    if bottlenecks['total_bottlenecks'] > 0:
        print(f"   Critical: {bottlenecks.get('critical_count', 0)}")
        print(f"   High: {bottlenecks.get('high_count', 0)}")
        
        print("\n   Details:")
        for b in bottlenecks['bottlenecks'][:5]:  # Top 5
            print(f"   • [{b['severity']}] {b['type']}: {b['description']}")
    
    # Root causes
    root_causes = report['root_causes']
    if root_causes['total_causes'] > 0:
        print(f"\n🎯 ROOT CAUSES: {root_causes['total_causes']}")
        
        for rc in root_causes['root_causes'][:3]:  # Top 3
            print(f"\n   [{rc['severity']}] {rc['root_cause']}")
            print(f"   Description: {rc['description']}")
            print(f"   Recommendation: {rc['recommendation']}")
    
    # Executor monitoring
    executor_mon = report['executor_monitoring']
    executor_summary = executor_mon['summary']
    
    print(f"\n🖥️  EXECUTOR MONITORING:")
    print(f"   Total: {executor_summary['total_executors']}")
    print(f"   Added: {executor_summary['executors_added']}")
    print(f"   Removed: {executor_summary['executors_removed']}")
    print(f"   Stability: {executor_summary['executor_stability']}")
    
    if executor_mon['anomalies']:
        print(f"\n   Anomalies Detected: {len(executor_mon['anomalies'])}")
        for anomaly in executor_mon['anomalies']:
            print(f"   • [{anomaly['severity']}] {anomaly['type']}: {anomaly['description']}")
    
    # Hotspots
    hotspots = report['performance_hotspots']
    if hotspots:
        print(f"\n🔥 PERFORMANCE HOTSPOTS: {len(hotspots)}")
        print("\n   Top 3 Slowest Tasks:")
        for i, hotspot in enumerate(hotspots[:3], 1):
            print(f"   {i}. Task {hotspot['task_id']} (Stage {hotspot['stage_id']})")
            print(f"      Duration: {hotspot['duration_ms']}ms")
            print(f"      Deviation: {hotspot['deviation_from_mean']}")
    
    print("\n" + "=" * 80)

def main():
    parser = argparse.ArgumentParser(description='Analyze Spark event logs')
    parser.add_argument('--input', required=True, help='Path to Spark event log JSON file')
    parser.add_argument('--output', default='output', help='Output directory for results')
    
    args = parser.parse_args()
    
    try:
        analyze_spark_log(args.input, args.output)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()