"""
Bottleneck Detector - Identifies performance issues from your logs.
"""

from typing import Dict, Any, List

def detect_bottlenecks(metrics: Dict[str, Any], events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Detect performance bottlenecks from metrics.
    
    Returns:
        Dictionary with detected bottlenecks and their severity
    """
    bottlenecks = []
    
    # 1. Check CPU efficiency
    cpu_metrics = metrics.get('cpu_metrics', {})
    cpu_eff = cpu_metrics.get('cpu_efficiency', 1.0)
    
    if cpu_eff < 0.5:
        bottlenecks.append({
            'type': 'LOW_CPU_UTILIZATION',
            'severity': 'HIGH',
            'description': f'CPU utilization is only {cpu_eff*100:.1f}%',
            'metric_value': cpu_eff,
            'recommendation': 'Increase executor cores or check for I/O bottlenecks'
        })
    
    # 2. Check task skew
    task_metrics = metrics.get('task_metrics', {})
    task_skew = task_metrics.get('task_skew_ratio', 0)
    
    if task_skew > 0.5:
        bottlenecks.append({
            'type': 'DATA_SKEW',
            'severity': 'CRITICAL' if task_skew > 1.0 else 'HIGH',
            'description': f'High task duration variance: {task_skew:.2f}',
            'metric_value': task_skew,
            'recommendation': 'Repartition data or use salting for skewed keys'
        })
    
    # 3. Check shuffle overhead
    shuffle_metrics = metrics.get('shuffle_metrics', {})
    shuffle_ratio = shuffle_metrics.get('shuffle_time_ratio', 0)
    
    if shuffle_ratio > 0.3:
        bottlenecks.append({
            'type': 'EXCESSIVE_SHUFFLE',
            'severity': 'HIGH' if shuffle_ratio > 0.5 else 'MEDIUM',
            'description': f'Shuffle consumes {shuffle_ratio*100:.1f}% of execution time',
            'metric_value': shuffle_ratio,
            'recommendation': 'Use broadcast joins or reduce shuffle partitions'
        })
    
    # 4. Check memory pressure
    memory_metrics = metrics.get('memory_metrics', {})
    gc_overhead = memory_metrics.get('gc_overhead_ratio', 0)
    
    if gc_overhead > 0.2:
        bottlenecks.append({
            'type': 'HIGH_GC_OVERHEAD',
            'severity': 'CRITICAL' if gc_overhead > 0.4 else 'HIGH',
            'description': f'GC overhead is {gc_overhead*100:.1f}%',
            'metric_value': gc_overhead,
            'recommendation': 'Increase executor memory or tune GC settings'
        })
    
    # 5. Check executor stability
    executor_metrics = metrics.get('executor_metrics', {})
    if executor_metrics.get('has_executor_loss', False):
        removal_reasons = executor_metrics.get('removal_reasons', [])
        
        bottlenecks.append({
            'type': 'EXECUTOR_LOSS',
            'severity': 'CRITICAL',
            'description': f'Executor(s) lost: {len(removal_reasons)} removal(s)',
            'metric_value': len(removal_reasons),
            'removal_reasons': removal_reasons,
            'recommendation': 'Check executor logs, increase memory, or adjust timeout settings'
        })
    
    # 6. Identify slow tasks
    if task_metrics.get('max_task_duration', 0) > 0:
        max_dur = task_metrics['max_task_duration']
        mean_dur = task_metrics.get('mean_task_duration', 0)
        
        if mean_dur > 0 and max_dur / mean_dur > 5:
            bottlenecks.append({
                'type': 'STRAGGLER_TASKS',
                'severity': 'MEDIUM',
                'description': f'Slowest task is {max_dur/mean_dur:.1f}x slower than average',
                'metric_value': max_dur / mean_dur,
                'recommendation': 'Investigate data skew or resource contention'
            })
    
    # Sort by severity
    severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
    bottlenecks.sort(key=lambda x: severity_order.get(x['severity'], 4))
    
    return {
        'bottlenecks': bottlenecks,
        'total_bottlenecks': len(bottlenecks),
        'critical_count': sum(1 for b in bottlenecks if b['severity'] == 'CRITICAL'),
        'high_count': sum(1 for b in bottlenecks if b['severity'] == 'HIGH')
    }

def identify_performance_hotspots(events: List[Dict[str, Any]], metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Identify stages/tasks that took abnormally long."""
    task_metrics = metrics.get('task_metrics', {})
    
    if not task_metrics:
        return []
    
    mean_duration = task_metrics.get('mean_task_duration', 0)
    p95_duration = task_metrics.get('percentiles', {}).get('p95', 0)
    
    hotspots = []
    
    # Identify tasks above P95
    for event in events:
        if event.get('Event') == 'SparkListenerTaskEnd':
            task_info = event.get('Task Info', {})
            if isinstance(task_info, dict):
                duration = task_info.get('Finish Time', 0) - task_info.get('Launch Time', 0)
                
                if duration > p95_duration and p95_duration > 0:
                    hotspots.append({
                        'task_id': task_info.get('Task ID'),
                        'stage_id': event.get('Stage ID'),
                        'duration_ms': duration,
                        'executor_id': task_info.get('Executor ID'),
                        'deviation_from_mean': f"{(duration / mean_duration):.1f}x" if mean_duration > 0 else 'N/A'
                    })
    
    # Sort by duration
    hotspots.sort(key=lambda x: x['duration_ms'], reverse=True)
    
    return hotspots[:10]  # Top 10