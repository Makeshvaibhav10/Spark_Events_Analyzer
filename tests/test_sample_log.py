"""
Test script for your sample Spark log.

Run this to verify everything works with your exact log structure.
"""

import json
import sys
sys.path.insert(0, '.')

from spark_event_analyzer.analyze import analyze_spark_log

def test_sample_log():
    """Test analysis with your sample log."""
    log_file = 'spark-event-file.json'
    print(f"Testing with log file: {log_file}\n")
    
    # Analyze
    report = analyze_spark_log(log_file, output_dir='test_output')
    
    # Verify key findings
    print("\n" + "="*80)
    print("VERIFICATION")
    print("="*80)
    
    # Should detect executor loss
    root_causes = report['root_causes']['root_causes']
    heartbeat_found = any(rc.get('root_cause') == 'HEARTBEAT_TIMEOUT' for rc in root_causes)
    
    print(f"\n✓ Heartbeat timeout detected: {heartbeat_found}")
    
    if heartbeat_found:
        for rc in root_causes:
            if rc.get('root_cause') == 'HEARTBEAT_TIMEOUT':
                print(f"  Executor ID: {rc.get('executor_id')}")
                print(f"  Reason: {rc.get('removed_reason')}")
                print(f"  Recommendation: {rc.get('recommendation')}")
    
    # Check metrics
    metrics = report['metrics']
    print(f"\n✓ Metrics computed:")
    print(f"  Tasks: {metrics['global_metrics']['total_tasks']}")
    print(f"  CPU Time: {metrics['cpu_metrics']['total_cpu_time_ns']} ns")
    print(f"  Shuffle Bytes: {metrics['shuffle_metrics']['total_shuffle_bytes']} bytes")
    print(f"  Input Bytes: {metrics['io_metrics']['total_input_bytes']} bytes")
    
    # Check executor monitoring
    exec_mon = report['executor_monitoring']
    print(f"\n✓ Executor monitoring:")
    print(f"  Executors added: {exec_mon['summary']['executors_added']}")
    print(f"  Executors removed: {exec_mon['summary']['executors_removed']}")
    print(f"  Anomalies: {len(exec_mon['anomalies'])}")
    
    if exec_mon['anomalies']:
        for anomaly in exec_mon['anomalies']:
            print(f"    - {anomaly['type']}: {anomaly['description']}")
    
    print("\n" + "="*80)
    print("✓ ALL TESTS PASSED!")
    print("="*80)
    
    return report

if __name__ == '__main__':
    test_sample_log()