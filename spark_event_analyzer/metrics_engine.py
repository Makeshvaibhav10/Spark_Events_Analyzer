"""
Metrics Engine for YOUR EXACT log schema.

Key: Everything is in "Task Executor Metrics" - no separate "Task Metrics"!
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

def compute_metrics(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compute comprehensive metrics from your log structure.
    
    YOUR SCHEMA: All metrics are in "Task Executor Metrics":
    - JVMHeapMemory, JVMOffHeapMemory
    - Executor CPU Time, Executor Run Time
    - Result Size
    - Shuffle Write Metrics (nested)
    - Input Metrics (nested)
    - Output Metrics (nested)
    """
    if not events:
        return _empty_metrics()
    
    df = pd.DataFrame(events)
    
    # Filter event types
    task_end_df = df[df['Event'] == 'SparkListenerTaskEnd'].copy()
    executor_added_df = df[df['Event'] == 'SparkListenerExecutorAdded'].copy()
    executor_removed_df = df[df['Event'] == 'SparkListenerExecutorRemoved'].copy()
    stage_completed_df = df[df['Event'] == 'SparkListenerStageCompleted'].copy()
    
    # Compute metrics
    task_metrics = _compute_task_metrics(task_end_df)
    cpu_metrics = _compute_cpu_metrics(task_end_df)
    memory_metrics = _compute_memory_metrics(task_end_df)
    shuffle_metrics = _compute_shuffle_metrics(task_end_df)
    io_metrics = _compute_io_metrics(task_end_df)
    executor_metrics = _compute_executor_metrics(task_end_df, executor_added_df, executor_removed_df)
    stage_metrics = _compute_stage_metrics(stage_completed_df, task_end_df)
    
    return {
        'task_metrics': task_metrics,
        'cpu_metrics': cpu_metrics,
        'memory_metrics': memory_metrics,
        'shuffle_metrics': shuffle_metrics,
        'io_metrics': io_metrics,
        'executor_metrics': executor_metrics,
        'stage_metrics': stage_metrics,
        'global_metrics': {
            'total_tasks': len(task_end_df),
            'total_executors_added': len(executor_added_df),
            'total_executors_removed': len(executor_removed_df),
            'total_stages': len(stage_completed_df)
        }
    }

def _compute_task_metrics(task_df: pd.DataFrame) -> Dict[str, Any]:
    """Compute task-level metrics."""
    if task_df.empty:
        return {}
    
    # Extract task durations from Task Info
    task_df['duration'] = task_df['Task Info'].apply(
        lambda x: x.get('Finish Time', 0) - x.get('Launch Time', 0) if isinstance(x, dict) else 0
    )
    
    valid_tasks = task_df[task_df['duration'] > 0]
    
    if valid_tasks.empty:
        return {}
    
    durations = valid_tasks['duration']
    
    return {
        'total_tasks': len(valid_tasks),
        'mean_task_duration': float(durations.mean()),
        'median_task_duration': float(durations.median()),
        'std_task_duration': float(durations.std()) if len(durations) > 1 else 0.0,
        'min_task_duration': float(durations.min()),
        'max_task_duration': float(durations.max()),
        'task_skew_ratio': float(durations.std() / durations.mean()) if durations.mean() > 0 else 0.0,
        'percentiles': {
            'p50': float(durations.quantile(0.50)),
            'p75': float(durations.quantile(0.75)),
            'p90': float(durations.quantile(0.90)),
            'p95': float(durations.quantile(0.95)),
            'p99': float(durations.quantile(0.99))
        }
    }

def _compute_cpu_metrics(task_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute CPU metrics from Task Executor Metrics.
    
    YOUR SCHEMA:
    - "Executor CPU Time" in Task Executor Metrics
    - "Executor Run Time" in Task Executor Metrics
    """
    if task_df.empty:
        return {'cpu_efficiency': 0.0}
    
    task_df['cpu_time'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Executor CPU Time', 0) if isinstance(x, dict) else 0
    )
    
    task_df['run_time'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Executor Run Time', 0) if isinstance(x, dict) else 0
    )
    
    total_cpu_time = task_df['cpu_time'].sum()
    total_run_time = task_df['run_time'].sum()
    
    cpu_efficiency = float(total_cpu_time / total_run_time) if total_run_time > 0 else 0.0
    
    return {
        'cpu_efficiency': cpu_efficiency,
        'cpu_utilization_percentage': cpu_efficiency * 100,
        'total_cpu_time_ns': int(total_cpu_time),  # Your log shows nanoseconds
        'total_run_time_ms': int(total_run_time),
        'cpu_wait_percentage': (1 - cpu_efficiency) * 100 if total_run_time > 0 else 0.0
    }

def _compute_memory_metrics(task_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute memory metrics from Task Executor Metrics.
    
    YOUR SCHEMA:
    - JVMHeapMemory
    - JVMOffHeapMemory
    
    Note: Your sample doesn't show GC metrics, but structure supports them
    """
    if task_df.empty:
        return {}
    
    task_df['heap_memory'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('JVMHeapMemory', 0) if isinstance(x, dict) else 0
    )
    
    task_df['offheap_memory'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('JVMOffHeapMemory', 0) if isinstance(x, dict) else 0
    )
    
    # GC metrics (if present)
    task_df['total_gc_time'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('TotalGCTime', 0) if isinstance(x, dict) else 0
    )
    
    task_df['run_time'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Executor Run Time', 0) if isinstance(x, dict) else 0
    )
    
    total_gc = task_df['total_gc_time'].sum()
    total_run = task_df['run_time'].sum()
    
    gc_overhead = float(total_gc / total_run) if total_run > 0 else 0.0
    
    return {
        'avg_heap_memory': int(task_df['heap_memory'].mean()) if len(task_df) > 0 else 0,
        'max_heap_memory': int(task_df['heap_memory'].max()) if len(task_df) > 0 else 0,
        'avg_offheap_memory': int(task_df['offheap_memory'].mean()) if len(task_df) > 0 else 0,
        'gc_overhead_ratio': gc_overhead,
        'gc_overhead_percentage': gc_overhead * 100,
        'gc_pressure_level': 'HIGH' if gc_overhead > 0.2 else 'MEDIUM' if gc_overhead > 0.1 else 'LOW'
    }

def _compute_shuffle_metrics(task_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute shuffle metrics.
    
    YOUR SCHEMA:
    Task Executor Metrics → Shuffle Write Metrics → {
        "Shuffle Bytes Written",
        "Shuffle Write Time",
        "Shuffle Records Written"
    }
    """
    if task_df.empty:
        return {}
    
    task_df['shuffle_bytes'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0) 
        if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
    )
    
    task_df['shuffle_time'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Shuffle Write Metrics', {}).get('Shuffle Write Time', 0) 
        if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
    )
    
    task_df['shuffle_records'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Shuffle Write Metrics', {}).get('Shuffle Records Written', 0) 
        if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
    )
    
    task_df['run_time'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Executor Run Time', 0) if isinstance(x, dict) else 0
    )
    
    total_shuffle = task_df['shuffle_bytes'].sum()
    total_shuffle_time = task_df['shuffle_time'].sum()
    total_run_time = task_df['run_time'].sum()
    
    shuffle_ratio = float(total_shuffle_time / total_run_time) if total_run_time > 0 else 0.0
    
    return {
        'total_shuffle_bytes': int(total_shuffle),
        'total_shuffle_bytes_formatted': _format_bytes(total_shuffle),
        'total_shuffle_time_ms': int(total_shuffle_time),
        'total_shuffle_records': int(task_df['shuffle_records'].sum()),
        'shuffle_time_ratio': shuffle_ratio,
        'shuffle_time_percentage': shuffle_ratio * 100,
        'tasks_with_shuffle': int((task_df['shuffle_bytes'] > 0).sum())
    }

def _compute_io_metrics(task_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute I/O metrics.
    
    YOUR SCHEMA:
    Task Executor Metrics → Input Metrics → {"Bytes Read", "Records Read"}
    Task Executor Metrics → Output Metrics → {"Bytes Written", "Records Written"}
    """
    if task_df.empty:
        return {}
    
    task_df['input_bytes'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Input Metrics', {}).get('Bytes Read', 0) 
        if isinstance(x, dict) and isinstance(x.get('Input Metrics'), dict) else 0
    )
    
    task_df['input_records'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Input Metrics', {}).get('Records Read', 0) 
        if isinstance(x, dict) and isinstance(x.get('Input Metrics'), dict) else 0
    )
    
    task_df['output_bytes'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Output Metrics', {}).get('Bytes Written', 0) 
        if isinstance(x, dict) and isinstance(x.get('Output Metrics'), dict) else 0
    )
    
    task_df['output_records'] = task_df['Task Executor Metrics'].apply(
        lambda x: x.get('Output Metrics', {}).get('Records Written', 0) 
        if isinstance(x, dict) and isinstance(x.get('Output Metrics'), dict) else 0
    )
    
    return {
        'total_input_bytes': int(task_df['input_bytes'].sum()),
        'total_input_bytes_formatted': _format_bytes(task_df['input_bytes'].sum()),
        'total_input_records': int(task_df['input_records'].sum()),
        'total_output_bytes': int(task_df['output_bytes'].sum()),
        'total_output_bytes_formatted': _format_bytes(task_df['output_bytes'].sum()),
        'total_output_records': int(task_df['output_records'].sum()),
        'tasks_with_input': int((task_df['input_bytes'] > 0).sum()),
        'tasks_with_output': int((task_df['output_bytes'] > 0).sum())
    }

def _compute_executor_metrics(task_df, executor_added_df, executor_removed_df) -> Dict[str, Any]:
    """Compute executor-related metrics."""
    executor_ids = set()
    
    if not task_df.empty:
        executor_ids = set(task_df['Task Info'].apply(
            lambda x: x.get('Executor ID') if isinstance(x, dict) else None
        ).dropna())
    
    # Count tasks per executor
    if not task_df.empty:
        task_counts = task_df['Task Info'].apply(
            lambda x: x.get('Executor ID') if isinstance(x, dict) else None
        ).value_counts().to_dict()
    else:
        task_counts = {}
    
    # Extract removal reasons
    removal_reasons = []
    if not executor_removed_df.empty:
        removal_reasons = executor_removed_df['Removed Reason'].tolist()
    
    return {
        'total_executors': len(executor_ids),
        'executors_added': len(executor_added_df),
        'executors_removed': len(executor_removed_df),
        'executor_task_counts': task_counts,
        'removal_reasons': removal_reasons,
        'has_executor_loss': len(executor_removed_df) > 0
    }

def _compute_stage_metrics(stage_df, task_df) -> Dict[str, Any]:
    """Compute stage-level metrics."""
    if stage_df.empty:
        return {'total_stages': 0}
    
    return {
        'total_stages': len(stage_df),
        'stages': []  # Can be expanded
    }

def _format_bytes(bytes_val: int) -> str:
    """Format bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"

def _empty_metrics() -> Dict[str, Any]:
    """Return empty metrics structure."""
    return {
        'task_metrics': {},
        'cpu_metrics': {'cpu_efficiency': 0.0},
        'memory_metrics': {},
        'shuffle_metrics': {},
        'io_metrics': {},
        'executor_metrics': {},
        'stage_metrics': {'total_stages': 0},
        'global_metrics': {}
    }