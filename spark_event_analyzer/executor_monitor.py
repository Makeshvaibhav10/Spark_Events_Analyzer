"""
Executor Monitor - Tracks executor lifecycle and detects anomalies.
"""

from typing import Dict, Any, List
from collections import defaultdict

def monitor_executors(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Monitor executor lifecycle and detect anomalies.
    
    Tracks:
    - Executor additions and removals
    - Heartbeat timeouts
    - Resource utilization
    - Frequent executor loss patterns
    """
    executor_timeline = []
    executor_stats = defaultdict(lambda: {
        'added_at': None,
        'removed_at': None,
        'removal_reason': None,
        'tasks_executed': 0,
        'total_duration_ms': 0,
        'hosts': set()
    })
    
    # Track executor events
    for event in events:
        event_type = event.get('Event')
        timestamp = event.get('Timestamp', 0)
        
        if event_type == 'SparkListenerExecutorAdded':
            executor_id = event.get('Executor ID')
            executor_info = event.get('Executor Info', {})
            host = executor_info.get('Host', 'unknown')
            cores = executor_info.get('Total Cores', 0)
            
            executor_stats[executor_id]['added_at'] = timestamp
            executor_stats[executor_id]['hosts'].add(host)
            executor_stats[executor_id]['cores'] = cores
            
            executor_timeline.append({
                'timestamp': timestamp,
                'event': 'ADDED',
                'executor_id': executor_id,
                'host': host,
                'cores': cores
            })
        
        elif event_type == 'SparkListenerExecutorRemoved':
            executor_id = event.get('Executor ID')
            removed_reason = event.get('Removed Reason', '')
            
            executor_stats[executor_id]['removed_at'] = timestamp
            executor_stats[executor_id]['removal_reason'] = removed_reason
            
            # Calculate lifetime
            added_at = executor_stats[executor_id]['added_at']
            if added_at:
                lifetime_ms = timestamp - added_at
                executor_stats[executor_id]['lifetime_ms'] = lifetime_ms
            
            executor_timeline.append({
                'timestamp': timestamp,
                'event': 'REMOVED',
                'executor_id': executor_id,
                'reason': removed_reason,
                'lifetime_ms': executor_stats[executor_id].get('lifetime_ms', 0)
            })
        
        elif event_type == 'SparkListenerTaskEnd':
            task_info = event.get('Task Info', {})
            if isinstance(task_info, dict):
                executor_id = task_info.get('Executor ID')
                duration = task_info.get('Finish Time', 0) - task_info.get('Launch Time', 0)
                
                executor_stats[executor_id]['tasks_executed'] += 1
                executor_stats[executor_id]['total_duration_ms'] += duration
    
    # Detect anomalies
    anomalies = _detect_executor_anomalies(executor_stats, executor_timeline)
    
    # Generate summary
    summary = _generate_executor_summary(executor_stats, executor_timeline)
    
    return {
        'executor_timeline': executor_timeline,
        'executor_stats': {k: dict(v) for k, v in executor_stats.items()},
        'anomalies': anomalies,
        'summary': summary
    }

def _detect_executor_anomalies(executor_stats: Dict, timeline: List[Dict]) -> List[Dict[str, Any]]:
    """Detect anomalies in executor behavior."""
    anomalies = []
    
    # 1. Frequent heartbeat timeouts
    heartbeat_timeouts = sum(1 for event in timeline 
                            if event.get('event') == 'REMOVED' and 
                            'heartbeat' in event.get('reason', '').lower())
    
    if heartbeat_timeouts > 0:
        anomalies.append({
            'type': 'FREQUENT_HEARTBEAT_TIMEOUTS',
            'severity': 'CRITICAL' if heartbeat_timeouts > 2 else 'HIGH',
            'count': heartbeat_timeouts,
            'description': f'{heartbeat_timeouts} executor(s) lost due to heartbeat timeout',
            'probable_cause': 'Network issues, long GC pauses, or overloaded executors',
            'recommendation': 'Increase spark.executor.heartbeatInterval and spark.network.timeout'
        })
    
    # 2. Short-lived executors
    short_lived = []
    for executor_id, stats in executor_stats.items():
        lifetime = stats.get('lifetime_ms', 0)
        if stats.get('removed_at') and lifetime < 60000:  # Less than 1 minute
            short_lived.append({
                'executor_id': executor_id,
                'lifetime_ms': lifetime,
                'reason': stats.get('removal_reason')
            })
    
    if short_lived:
        anomalies.append({
            'type': 'SHORT_LIVED_EXECUTORS',
            'severity': 'HIGH',
            'count': len(short_lived),
            'description': f'{len(short_lived)} executor(s) lived less than 1 minute',
            'executors': short_lived,
            'probable_cause': 'Executor crashes, OOM, or configuration issues',
            'recommendation': 'Check executor logs for crashes and increase memory if needed'
        })
    
    # 3. Idle executors (executors with no tasks)
    idle_executors = [
        executor_id for executor_id, stats in executor_stats.items()
        if stats.get('tasks_executed', 0) == 0 and stats.get('removed_at')
    ]
    
    if idle_executors:
        anomalies.append({
            'type': 'IDLE_EXECUTORS',
            'severity': 'MEDIUM',
            'count': len(idle_executors),
            'description': f'{len(idle_executors)} executor(s) executed no tasks',
            'executor_ids': idle_executors,
            'probable_cause': 'Executor added but not utilized, or removed before task assignment',
            'recommendation': 'Check dynamic allocation settings and task scheduling'
        })
    
    return anomalies

def _generate_executor_summary(executor_stats: Dict, timeline: List[Dict]) -> Dict[str, Any]:
    """Generate executor summary statistics."""
    total_executors = len(executor_stats)
    removed_executors = sum(1 for stats in executor_stats.values() if stats.get('removed_at'))
    
    # Calculate average lifetime
    lifetimes = [stats.get('lifetime_ms', 0) for stats in executor_stats.values() 
                if stats.get('lifetime_ms')]
    avg_lifetime = sum(lifetimes) / len(lifetimes) if lifetimes else 0
    
    # Total tasks executed
    total_tasks = sum(stats.get('tasks_executed', 0) for stats in executor_stats.values())
    
    # Removal reasons breakdown
    removal_reasons = {}
    for stats in executor_stats.values():
        reason = stats.get('removal_reason')
        if reason:
            removal_reasons[reason] = removal_reasons.get(reason, 0) + 1
    
    return {
        'total_executors': total_executors,
        'executors_added': sum(1 for event in timeline if event.get('event') == 'ADDED'),
        'executors_removed': removed_executors,
        'avg_lifetime_ms': int(avg_lifetime),
        'avg_lifetime_formatted': _format_duration(avg_lifetime),
        'total_tasks_executed': total_tasks,
        'removal_reasons_breakdown': removal_reasons,
        'executor_stability': 'STABLE' if removed_executors == 0 else 'UNSTABLE'
    }

def _format_duration(ms: float) -> str:
    """Format milliseconds to human-readable duration."""
    if ms < 1000:
        return f"{ms:.0f}ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)}m {int(seconds % 60)}s"
    hours = minutes / 60
    return f"{int(hours)}h {int(minutes % 60)}m"