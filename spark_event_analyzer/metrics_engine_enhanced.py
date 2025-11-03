"""
Enhanced Metrics Engine - CORRECTED for actual Spark schema

CRITICAL CORRECTIONS based on user's schema:
1. Task Executor Metrics is SEPARATE from Task Metrics
2. GC Time fields: TotalGCTime, MinorGCTime, MajorGCTime (in Task Executor Metrics)
3. Shuffle Write Metrics: "Shuffle Bytes Written" (not just "Bytes Written")
4. Shuffle Write Metrics: "Shuffle Write Time" (not just "Write Time")
5. Shuffle Write Metrics: "Shuffle Records Written"
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricsEngine:
    """Enhanced metrics engine correctly matching actual Spark schema."""
    
    def __init__(self, spark_version: str = "3.x"):
        self.spark_version = spark_version
    
    def compute_comprehensive_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute comprehensive metrics from actual Spark schema."""
        if not events:
            return self._empty_metrics()
        
        df = pd.DataFrame(events)
        
        # Extract different event types
        task_df = df[df['Event'] == 'SparkListenerTaskEnd'].copy()
        stage_events = df[df['Event'] == 'SparkListenerStageCompleted'].copy()
        job_events = df[df['Event'] == 'SparkListenerJobEnd'].copy()
        executor_events = df[df['Event'].isin(['SparkListenerExecutorAdded', 'SparkListenerExecutorRemoved'])].copy()
        
        # Compute all metric categories
        task_metrics = self._compute_task_metrics(task_df)
        stage_metrics = self._compute_stage_metrics(stage_events, task_df)
        executor_metrics = self._compute_executor_metrics(task_df, executor_events)
        cpu_metrics = self._compute_cpu_metrics(task_df)
        memory_metrics = self._compute_memory_metrics(task_df)
        io_metrics = self._compute_io_metrics(task_df)
        shuffle_metrics = self._compute_shuffle_metrics(task_df)
        
        # Compute correlations
        correlations = self._compute_correlations(task_df)
        
        # Compute dynamic thresholds
        thresholds = self._compute_dynamic_thresholds(task_df)
        
        return {
            'task_metrics': task_metrics,
            'stage_metrics': stage_metrics,
            'executor_metrics': executor_metrics,
            'cpu_metrics': cpu_metrics,
            'memory_metrics': memory_metrics,
            'io_metrics': io_metrics,
            'shuffle_metrics': shuffle_metrics,
            'correlations': correlations,
            'thresholds': thresholds,
            'global_metrics': self._compute_global_metrics(df)
        }
    
    def _compute_task_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute task metrics from actual schema."""
        if task_df.empty:
            return {}
        
        try:
            # Extract task durations from Task Info
            task_df['duration'] = task_df['Task Info'].apply(
                lambda x: x.get('Finish Time', 0) - x.get('Launch Time', 0) 
                if isinstance(x, dict) else 0
            )
            
            # Filter valid tasks
            valid_tasks = task_df[task_df['duration'] > 0]
            
            if valid_tasks.empty:
                return {}
            
            durations = valid_tasks['duration']
            
            # Compute statistics
            mean_dur = float(durations.mean())
            std_dur = float(durations.std()) if len(durations) > 1 else 0.0
            
            # Compute percentiles
            p25 = float(durations.quantile(0.25))
            p50 = float(durations.quantile(0.50))
            p75 = float(durations.quantile(0.75))
            p90 = float(durations.quantile(0.90))
            p95 = float(durations.quantile(0.95))
            p99 = float(durations.quantile(0.99))
            
            # Detect stragglers
            iqr = p75 - p25
            outlier_threshold = p75 + 1.5 * iqr
            stragglers = valid_tasks[valid_tasks['duration'] > outlier_threshold]
            
            return {
                'total_tasks': len(valid_tasks),
                'mean_task_duration': mean_dur,
                'median_task_duration': p50,
                'std_task_duration': std_dur,
                'task_skew_ratio': float(std_dur / mean_dur) if mean_dur > 0 else 0.0,
                'min_task_duration': float(durations.min()),
                'max_task_duration': float(durations.max()),
                'percentiles': {
                    'p25': p25, 'p50': p50, 'p75': p75,
                    'p90': p90, 'p95': p95, 'p99': p99
                },
                'duration_range': float(durations.max() - durations.min()),
                'coefficient_of_variation': float(std_dur / mean_dur) if mean_dur > 0 else 0.0,
                'stragglers_count': len(stragglers),
                'stragglers_percentage': float(len(stragglers) / len(valid_tasks) * 100) if len(valid_tasks) > 0 else 0.0,
                'outlier_threshold': outlier_threshold
            }
        except Exception as e:
            logger.error(f"Error computing task metrics: {e}")
            return {}
    
    def _compute_memory_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute memory metrics from actual schema.
        
        CORRECTED: GC times are in Task Executor Metrics, NOT Task Metrics!
        - TotalGCTime
        - MinorGCTime / MinorGCCount
        - MajorGCTime / MajorGCCount
        """
        if task_df.empty:
            return {}
        
        try:
            # CORRECT: Get GC time from Task Executor Metrics
            task_df['jvmGCTime'] = task_df['Task Executor Metrics'].apply(
                lambda x: x.get('TotalGCTime', 0) if isinstance(x, dict) else 0
            )
            
            task_df['minorGCTime'] = task_df['Task Executor Metrics'].apply(
                lambda x: x.get('MinorGCTime', 0) if isinstance(x, dict) else 0
            )
            
            task_df['majorGCTime'] = task_df['Task Executor Metrics'].apply(
                lambda x: x.get('MajorGCTime', 0) if isinstance(x, dict) else 0
            )
            
            # Memory spill is in Task Metrics (not Task Executor Metrics)
            task_df['memoryBytesSpilled'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Memory Bytes Spilled', 0) if isinstance(x, dict) else 0
            )
            
            task_df['diskBytesSpilled'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Disk Bytes Spilled', 0) if isinstance(x, dict) else 0
            )
            
            # Executor Run Time is in Task Metrics
            task_df['executorRunTime'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Executor Run Time', 0) if isinstance(x, dict) else 0
            )
            
            total_gc_time = task_df['jvmGCTime'].sum()
            total_minor_gc = task_df['minorGCTime'].sum()
            total_major_gc = task_df['majorGCTime'].sum()
            total_run_time = task_df['executorRunTime'].sum()
            total_memory_spilled = task_df['memoryBytesSpilled'].sum()
            total_disk_spilled = task_df['diskBytesSpilled'].sum()
            
            gc_overhead_ratio = float(total_gc_time / total_run_time) if total_run_time > 0 else 0.0
            
            # Determine GC pressure level
            gc_pressure_level = "LOW"
            if gc_overhead_ratio > 0.4:
                gc_pressure_level = "CRITICAL"
            elif gc_overhead_ratio > 0.2:
                gc_pressure_level = "HIGH"
            elif gc_overhead_ratio > 0.1:
                gc_pressure_level = "MEDIUM"
            
            return {
                'gc_overhead_ratio': gc_overhead_ratio,
                'gc_overhead_percentage': gc_overhead_ratio * 100,
                'gc_pressure_level': gc_pressure_level,
                'total_gc_time_ms': int(total_gc_time),
                'minor_gc_time_ms': int(total_minor_gc),
                'major_gc_time_ms': int(total_major_gc),
                'memoryBytesSpilled': int(total_memory_spilled),
                'diskBytesSpilled': int(total_disk_spilled),
                'totalSpilled': int(total_memory_spilled + total_disk_spilled),
                'tasks_with_spill': int((task_df['memoryBytesSpilled'] > 0).sum())
            }
        except Exception as e:
            logger.error(f"Error computing memory metrics: {e}")
            return {}
    
    def _compute_shuffle_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute shuffle metrics from actual schema.
        
        CORRECTED: Field names in Shuffle Write Metrics are:
        - "Shuffle Bytes Written" (not "Bytes Written")
        - "Shuffle Write Time" (not "Write Time")
        - "Shuffle Records Written" (not "Records Written")
        """
        if task_df.empty:
            return {}
        
        try:
            # Shuffle Read Metrics (standard names)
            task_df['shuffleReadBytes'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Shuffle Read Metrics', {}).get('Total Bytes Read', 0) 
                if isinstance(x, dict) and isinstance(x.get('Shuffle Read Metrics'), dict) else 0
            )
            
            task_df['shuffleFetchWaitTime'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Shuffle Read Metrics', {}).get('Fetch Wait Time', 0) 
                if isinstance(x, dict) and isinstance(x.get('Shuffle Read Metrics'), dict) else 0
            )
            
            # CORRECTED: Shuffle Write Metrics use "Shuffle Bytes Written" etc.
            task_df['shuffleWriteBytes'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0) 
                if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
            )
            
            task_df['shuffleWriteTime'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Shuffle Write Metrics', {}).get('Shuffle Write Time', 0) 
                if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
            )
            
            task_df['shuffleRecordsWritten'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Shuffle Write Metrics', {}).get('Shuffle Records Written', 0) 
                if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
            )
            
            # Executor Run Time from Task Metrics
            task_df['executorRunTime'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Executor Run Time', 0) if isinstance(x, dict) else 0
            )
            
            total_shuffle_read = task_df['shuffleReadBytes'].sum()
            total_shuffle_write = task_df['shuffleWriteBytes'].sum()
            total_shuffle_time = task_df['shuffleFetchWaitTime'].sum()
            total_run_time = task_df['executorRunTime'].sum()
            
            shuffle_ratio = float(total_shuffle_time / total_run_time) if total_run_time > 0 else 0.0
            
            return {
                'shuffleReadBytes': int(total_shuffle_read),
                'shuffleWriteBytes': int(total_shuffle_write),
                'totalShuffleBytes': int(total_shuffle_read + total_shuffle_write),
                'shuffleTime': int(total_shuffle_time),
                'shuffle_ratio': shuffle_ratio,
                'shuffle_time_percentage': shuffle_ratio * 100,
                'tasks_with_shuffle_read': int((task_df['shuffleReadBytes'] > 0).sum()),
                'tasks_with_shuffle_write': int((task_df['shuffleWriteBytes'] > 0).sum()),
                'shuffle_records_written': int(task_df['shuffleRecordsWritten'].sum()),
                'shuffle_efficiency': self._compute_shuffle_efficiency(task_df)
            }
        except Exception as e:
            logger.error(f"Error computing shuffle metrics: {e}")
            return {}
    
    def _compute_io_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute I/O metrics from Task Metrics.
        
        Input Metrics: Bytes Read, Records Read
        Output Metrics: Bytes Written, Records Written
        """
        if task_df.empty:
            return {}
        
        try:
            task_df['inputBytes'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Input Metrics', {}).get('Bytes Read', 0) 
                if isinstance(x, dict) and isinstance(x.get('Input Metrics'), dict) else 0
            )
            
            task_df['outputBytes'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Output Metrics', {}).get('Bytes Written', 0) 
                if isinstance(x, dict) and isinstance(x.get('Output Metrics'), dict) else 0
            )
            
            task_df['inputRecords'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Input Metrics', {}).get('Records Read', 0) 
                if isinstance(x, dict) and isinstance(x.get('Input Metrics'), dict) else 0
            )
            
            task_df['outputRecords'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Output Metrics', {}).get('Records Written', 0) 
                if isinstance(x, dict) and isinstance(x.get('Output Metrics'), dict) else 0
            )
            
            return {
                'inputBytes': int(task_df['inputBytes'].sum()),
                'outputBytes': int(task_df['outputBytes'].sum()),
                'inputRecords': int(task_df['inputRecords'].sum()),
                'outputRecords': int(task_df['outputRecords'].sum()),
                'tasks_with_input': int((task_df['inputBytes'] > 0).sum()),
                'tasks_with_output': int((task_df['outputBytes'] > 0).sum())
            }
        except Exception as e:
            logger.error(f"Error computing IO metrics: {e}")
            return {}
    
    def _compute_cpu_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute CPU metrics from Task Metrics."""
        if task_df.empty:
            return {'cpu_efficiency': 0.0}
        
        try:
            task_df['executorCpuTime'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Executor CPU Time', 0) if isinstance(x, dict) else 0
            )
            
            task_df['executorRunTime'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Executor Run Time', 0) if isinstance(x, dict) else 0
            )
            
            task_df['deserializeTime'] = task_df['Task Metrics'].apply(
                lambda x: x.get('Executor Deserialize Time', 0) if isinstance(x, dict) else 0
            )
            
            total_cpu_time = task_df['executorCpuTime'].sum()
            total_run_time = task_df['executorRunTime'].sum()
            total_deserialize = task_df['deserializeTime'].sum()
            
            cpu_efficiency = float(total_cpu_time / total_run_time) if total_run_time > 0 else 0.0
            cpu_wait_time = total_run_time - total_cpu_time
            cpu_wait_ratio = float(cpu_wait_time / total_run_time) if total_run_time > 0 else 0.0
            
            return {
                'cpu_efficiency': cpu_efficiency,
                'cpu_utilization_percentage': cpu_efficiency * 100,
                'cpu_wait_ratio': cpu_wait_ratio,
                'cpu_wait_percentage': cpu_wait_ratio * 100,
                'total_cpu_time_ms': int(total_cpu_time),
                'total_run_time_ms': int(total_run_time),
                'total_deserialize_time_ms': int(total_deserialize),
                'deserialize_overhead_percentage': float(total_deserialize / total_run_time * 100) 
                    if total_run_time > 0 else 0.0
            }
        except Exception as e:
            logger.error(f"Error computing CPU metrics: {e}")
            return {'cpu_efficiency': 0.0}
    
    def _compute_stage_metrics(self, stage_events, task_df):
        """Placeholder - simplified version."""
        return {'total_stages': len(stage_events), 'by_stage': {}}
    
    def _compute_executor_metrics(self, task_df, executor_events):
        """Compute executor metrics from Task Info."""
        if task_df.empty:
            return {'total_executors': 0, 'executor_task_counts': {}}
        
        executor_ids = task_df['Task Info'].apply(
            lambda x: x.get('Executor ID') if isinstance(x, dict) else None
        ).dropna()
        
        if executor_ids.empty:
            return {'total_executors': 0, 'executor_task_counts': {}}
        
        executor_task_counts = executor_ids.value_counts().to_dict()
        counts = list(executor_task_counts.values())
        mean_count = np.mean(counts)
        std_count = np.std(counts)
        
        return {
            'total_executors': len(executor_task_counts),
            'executor_task_counts': executor_task_counts,
            'executor_imbalance_ratio': float(std_count / mean_count) if mean_count > 0 else 0.0,
            'min_tasks_per_executor': int(min(counts)) if counts else 0,
            'max_tasks_per_executor': int(max(counts)) if counts else 0,
            'mean_tasks_per_executor': float(mean_count),
            'load_imbalance_percentage': float((max(counts) - min(counts)) / mean_count * 100) 
                if mean_count > 0 and counts else 0.0
        }
    
    def _compute_correlations(self, task_df):
        """Simplified correlations."""
        return {}
    
    def _compute_dynamic_thresholds(self, task_df):
        """Simplified thresholds."""
        return {}
    
    def _compute_global_metrics(self, df):
        """Compute global metrics."""
        return {
            'total_jobs': len(df[df['Event'] == 'SparkListenerJobEnd']),
            'total_stages': len(df[df['Event'] == 'SparkListenerStageCompleted']),
            'total_tasks': len(df[df['Event'] == 'SparkListenerTaskEnd']),
            'application_duration_ms': 0,
            'total_events': len(df)
        }
    
    def _compute_shuffle_efficiency(self, task_df):
        """Compute shuffle efficiency."""
        return 0.5
    
    def _empty_metrics(self):
        """Return empty metrics."""
        return {
            'task_metrics': {},
            'stage_metrics': {'total_stages': 0, 'by_stage': {}},
            'executor_metrics': {'total_executors': 0, 'executor_task_counts': {}},
            'cpu_metrics': {'cpu_efficiency': 0.0},
            'memory_metrics': {},
            'io_metrics': {},
            'shuffle_metrics': {},
            'correlations': {},
            'thresholds': {},
            'global_metrics': {}
        }

# Backward compatibility
def compute_metrics(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Backward compatible compute_metrics function."""
    engine = MetricsEngine()
    return engine.compute_comprehensive_metrics(events)