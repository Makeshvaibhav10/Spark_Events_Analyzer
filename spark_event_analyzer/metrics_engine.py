import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetricsEngine:
    """Enhanced metrics engine with correlation and dynamic thresholds."""
    
    def __init__(self, spark_version: str = "3.x"):
        self.spark_version = spark_version
        self.version_map = self._get_version_map(spark_version)
    
    def _get_version_map(self, version: str) -> Dict[str, str]:
        """Map field names across Spark versions."""
        if version.startswith("2"):
            return {
                "Executor Run Time": "executorRunTime",
                "Executor CPU Time": "executorCpuTime",
                "JVM GC Time": "jvmGCTime"
            }
        return {}  # 3.x uses standard names
    
    def compute_comprehensive_metrics(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute comprehensive metrics with correlation analysis."""
        if not events:
            return self._empty_metrics()
        
        df = pd.DataFrame(events)
        
        # Extract different event types
        task_events = df[df['Event'] == 'SparkListenerTaskEnd'].copy()
        stage_events = df[df['Event'] == 'SparkListenerStageCompleted'].copy()
        job_events = df[df['Event'] == 'SparkListenerJobEnd'].copy()
        executor_events = df[df['Event'].isin(['SparkListenerExecutorAdded', 'SparkListenerExecutorRemoved'])].copy()
        
        # Compute all metric categories
        task_metrics = self._compute_task_metrics(task_events)
        stage_metrics = self._compute_stage_metrics(stage_events, task_events)
        executor_metrics = self._compute_executor_metrics(task_events, executor_events)
        cpu_metrics = self._compute_cpu_metrics(task_events)
        memory_metrics = self._compute_memory_metrics(task_events)
        io_metrics = self._compute_io_metrics(task_events)
        shuffle_metrics = self._compute_shuffle_metrics(task_events)
        
        # Compute correlations and insights
        correlations = self._compute_correlations(task_events)
        
        # Compute dynamic thresholds
        thresholds = self._compute_dynamic_thresholds(task_events)
        
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
        """Enhanced task metrics with statistical analysis."""
        if task_df.empty:
            return {}
        
        # Extract task durations
        task_df['duration'] = task_df['Task Info'].apply(
            lambda x: x.get('Finish Time', 0) - x.get('Launch Time', 0) 
            if isinstance(x, dict) else 0
        )
        
        valid_tasks = task_df[task_df['duration'] > 0]
        
        if valid_tasks.empty:
            return {}
        
        durations = valid_tasks['duration']
        
        # Compute statistical measures
        mean_dur = float(durations.mean())
        std_dur = float(durations.std()) if len(durations) > 1 else 0.0
        
        # Compute percentiles for outlier detection
        p25 = float(durations.quantile(0.25))
        p50 = float(durations.quantile(0.50))
        p75 = float(durations.quantile(0.75))
        p90 = float(durations.quantile(0.90))
        p95 = float(durations.quantile(0.95))
        p99 = float(durations.quantile(0.99))
        
        # Detect stragglers (tasks significantly slower than median)
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
                'p25': p25,
                'p50': p50,
                'p75': p75,
                'p90': p90,
                'p95': p95,
                'p99': p99
            },
            'duration_range': float(durations.max() - durations.min()),
            'coefficient_of_variation': float(std_dur / mean_dur) if mean_dur > 0 else 0.0,
            'stragglers_count': len(stragglers),
            'stragglers_percentage': float(len(stragglers) / len(valid_tasks) * 100) if len(valid_tasks) > 0 else 0.0,
            'outlier_threshold': outlier_threshold
        }
    
    def _compute_stage_metrics(self, stage_df: pd.DataFrame, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute stage-level metrics and detect failures."""
        if stage_df.empty:
            return {'total_stages': 0, 'by_stage': {}}
        
        stages_data = {}
        total_failures = 0
        total_retries = 0
        
        for _, stage_event in stage_df.iterrows():
            stage_info = stage_event.get('Stage Info', {})
            if not isinstance(stage_info, dict):
                continue
            
            stage_id = stage_info.get('Stage ID', -1)
            stage_attempt = stage_info.get('Stage Attempt ID', 0)
            
            # Get tasks for this stage
            stage_tasks = task_df[task_df.get('Stage ID', -1) == stage_id]
            
            # Check for failures
            failure_reason = stage_info.get('Failure Reason')
            has_failed = failure_reason is not None
            
            if has_failed:
                total_failures += 1
            
            if stage_attempt > 0:
                total_retries += 1
            
            # Compute stage-specific metrics
            if not stage_tasks.empty:
                stage_tasks['duration'] = stage_tasks['Task Info'].apply(
                    lambda x: x.get('Finish Time', 0) - x.get('Launch Time', 0) 
                    if isinstance(x, dict) else 0
                )
                
                stages_data[f"stage_{stage_id}"] = {
                    'stage_id': int(stage_id),
                    'stage_attempt': int(stage_attempt),
                    'task_count': len(stage_tasks),
                    'failed': has_failed,
                    'failure_reason': failure_reason,
                    'mean_task_duration': float(stage_tasks['duration'].mean()),
                    'max_task_duration': float(stage_tasks['duration'].max()),
                    'task_skew': float(stage_tasks['duration'].std() / stage_tasks['duration'].mean()) 
                        if stage_tasks['duration'].mean() > 0 else 0.0
                }
        
        return {
            'total_stages': len(stage_df),
            'failed_stages': total_failures,
            'retried_stages': total_retries,
            'failure_rate': float(total_failures / len(stage_df)) if len(stage_df) > 0 else 0.0,
            'by_stage': stages_data
        }
    
    def _compute_executor_metrics(self, task_df: pd.DataFrame, executor_df: pd.DataFrame) -> Dict[str, Any]:
        """Enhanced executor metrics with load balancing analysis."""
        if task_df.empty:
            return {'total_executors': 0, 'executor_task_counts': {}}
        
        # Extract executor IDs from tasks
        executor_ids = task_df['Task Info'].apply(
            lambda x: x.get('Executor ID') if isinstance(x, dict) else None
        ).dropna()
        
        if executor_ids.empty:
            return {'total_executors': 0, 'executor_task_counts': {}}
        
        # Count tasks per executor
        executor_task_counts = executor_ids.value_counts().to_dict()
        counts = list(executor_task_counts.values())
        
        # Statistical analysis of executor load
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
                if mean_count > 0 and counts else 0.0,
            'executors_added': len(executor_df[executor_df['Event'] == 'SparkListenerExecutorAdded']) 
                if not executor_df.empty else 0,
            'executors_removed': len(executor_df[executor_df['Event'] == 'SparkListenerExecutorRemoved']) 
                if not executor_df.empty else 0
        }
    
    def _compute_cpu_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute CPU utilization and efficiency metrics."""
        if task_df.empty:
            return {'cpu_efficiency': 0.0}
        
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
        
        # Compute CPU wait time (idle time)
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
    
    def _compute_memory_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Enhanced memory metrics with GC and spill analysis."""
        if task_df.empty:
            return {}
        
        task_df['jvmGCTime'] = task_df['Task Metrics'].apply(
            lambda x: x.get('JVM GC Time', 0) if isinstance(x, dict) else 0
        )
        task_df['memoryBytesSpilled'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Memory Bytes Spilled', 0) if isinstance(x, dict) else 0
        )
        task_df['diskBytesSpilled'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Disk Bytes Spilled', 0) if isinstance(x, dict) else 0
        )
        task_df['peakExecutionMemory'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Peak Execution Memory', 0) if isinstance(x, dict) else 0
        )
        task_df['executorRunTime'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Executor Run Time', 0) if isinstance(x, dict) else 0
        )
        
        total_gc_time = task_df['jvmGCTime'].sum()
        total_run_time = task_df['executorRunTime'].sum()
        total_memory_spilled = task_df['memoryBytesSpilled'].sum()
        total_disk_spilled = task_df['diskBytesSpilled'].sum()
        peak_memory = task_df['peakExecutionMemory'].max()
        
        gc_overhead_ratio = float(total_gc_time / total_run_time) if total_run_time > 0 else 0.0
        
        # Detect GC pressure level
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
            'memoryBytesSpilled': int(total_memory_spilled),
            'diskBytesSpilled': int(total_disk_spilled),
            'totalSpilled': int(total_memory_spilled + total_disk_spilled),
            'peakExecutionMemory': int(peak_memory),
            'spill_to_disk_ratio': float(total_disk_spilled / total_memory_spilled) 
                if total_memory_spilled > 0 else 0.0,
            'tasks_with_spill': int((task_df['memoryBytesSpilled'] > 0).sum())
        }
    
    def _compute_io_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute I/O metrics."""
        if task_df.empty:
            return {}
        
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
        
        return {
            'inputBytes': int(task_df['inputBytes'].sum()),
            'outputBytes': int(task_df['outputBytes'].sum()),
            'inputRecords': int(task_df['inputRecords'].sum()),
            'tasks_with_input': int((task_df['inputBytes'] > 0).sum()),
            'tasks_with_output': int((task_df['outputBytes'] > 0).sum())
        }
    
    def _compute_shuffle_metrics(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Enhanced shuffle metrics with efficiency analysis."""
        if task_df.empty:
            return {}
        
        task_df['shuffleReadBytes'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Shuffle Read Metrics', {}).get('Total Bytes Read', 0) 
            if isinstance(x, dict) and isinstance(x.get('Shuffle Read Metrics'), dict) else 0
        )
        task_df['shuffleWriteBytes'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Shuffle Write Metrics', {}).get('Bytes Written', 0) 
            if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
        )
        task_df['shuffleFetchWaitTime'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Shuffle Read Metrics', {}).get('Fetch Wait Time', 0) 
            if isinstance(x, dict) and isinstance(x.get('Shuffle Read Metrics'), dict) else 0
        )
        task_df['shuffleWriteTime'] = task_df['Task Metrics'].apply(
            lambda x: x.get('Shuffle Write Metrics', {}).get('Write Time', 0) 
            if isinstance(x, dict) and isinstance(x.get('Shuffle Write Metrics'), dict) else 0
        )
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
            'shuffle_efficiency': self._compute_shuffle_efficiency(task_df)
        }
    
    def _compute_shuffle_efficiency(self, task_df: pd.DataFrame) -> float:
        """Compute shuffle efficiency score."""
        if 'shuffleReadBytes' not in task_df.columns or 'inputBytes' not in task_df.columns:
            return 0.0
        
        total_shuffle = task_df['shuffleReadBytes'].sum() + task_df['shuffleWriteBytes'].sum()
        total_io = task_df['inputBytes'].sum() + task_df['outputBytes'].sum()
        
        if total_io == 0:
            return 0.0
        
        # Lower shuffle-to-io ratio is better
        shuffle_intensity = float(total_shuffle / total_io)
        
        # Convert to efficiency score (inverse, normalized)
        efficiency = 1.0 / (1.0 + shuffle_intensity)
        return float(efficiency)
    
    def _compute_correlations(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute correlations between key metrics."""
        if task_df.empty or len(task_df) < 2:
            return {}
        
        try:
            # Extract key metrics for correlation
            metrics_data = {
                'duration': task_df['Task Info'].apply(
                    lambda x: x.get('Finish Time', 0) - x.get('Launch Time', 0) 
                    if isinstance(x, dict) else 0
                ),
                'gc_time': task_df['Task Metrics'].apply(
                    lambda x: x.get('JVM GC Time', 0) if isinstance(x, dict) else 0
                ),
                'memory_spill': task_df['Task Metrics'].apply(
                    lambda x: x.get('Memory Bytes Spilled', 0) if isinstance(x, dict) else 0
                ),
                'shuffle_read': task_df['Task Metrics'].apply(
                    lambda x: x.get('Shuffle Read Metrics', {}).get('Total Bytes Read', 0)
                    if isinstance(x, dict) and isinstance(x.get('Shuffle Read Metrics'), dict) else 0
                ),
                'cpu_time': task_df['Task Metrics'].apply(
                    lambda x: x.get('Executor CPU Time', 0) if isinstance(x, dict) else 0
                )
            }
            
            corr_df = pd.DataFrame(metrics_data)
            
            # Compute key correlations
            correlations = {
                'gc_vs_duration': float(corr_df['gc_time'].corr(corr_df['duration'])) 
                    if corr_df['gc_time'].std() > 0 and corr_df['duration'].std() > 0 else 0.0,
                'spill_vs_gc': float(corr_df['memory_spill'].corr(corr_df['gc_time'])) 
                    if corr_df['memory_spill'].std() > 0 and corr_df['gc_time'].std() > 0 else 0.0,
                'shuffle_vs_duration': float(corr_df['shuffle_read'].corr(corr_df['duration'])) 
                    if corr_df['shuffle_read'].std() > 0 and corr_df['duration'].std() > 0 else 0.0,
                'cpu_vs_duration': float(corr_df['cpu_time'].corr(corr_df['duration'])) 
                    if corr_df['cpu_time'].std() > 0 and corr_df['duration'].std() > 0 else 0.0
            }
            
            return correlations
            
        except Exception as e:
            logger.warning(f"Error computing correlations: {e}")
            return {}
    
    def _compute_dynamic_thresholds(self, task_df: pd.DataFrame) -> Dict[str, Any]:
        """Compute dynamic thresholds based on data distribution."""
        if task_df.empty:
            return {}
        
        # Extract durations
        durations = task_df['Task Info'].apply(
            lambda x: x.get('Finish Time', 0) - x.get('Launch Time', 0) 
            if isinstance(x, dict) else 0
        )
        durations = durations[durations > 0]
        
        if durations.empty:
            return {}
        
        # Compute z-score based thresholds
        mean_dur = durations.mean()
        std_dur = durations.std()
        
        return {
            'task_duration_threshold_1std': float(mean_dur + std_dur),
            'task_duration_threshold_2std': float(mean_dur + 2 * std_dur),
            'task_duration_threshold_3std': float(mean_dur + 3 * std_dur),
            'skew_threshold_p90': float(durations.quantile(0.90)),
            'skew_threshold_p95': float(durations.quantile(0.95)),
            'skew_threshold_p99': float(durations.quantile(0.99))
        }
    
    def _compute_global_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Compute global application-level metrics."""
        app_start = df[df['Event'] == 'SparkListenerApplicationStart']
        app_end = df[df['Event'] == 'SparkListenerApplicationEnd']
        
        total_jobs = len(df[df['Event'] == 'SparkListenerJobEnd'])
        total_stages = len(df[df['Event'] == 'SparkListenerStageCompleted'])
        total_tasks = len(df[df['Event'] == 'SparkListenerTaskEnd'])
        
        # Compute application duration
        app_duration = 0
        if not app_start.empty and not app_end.empty:
            start_time = app_start.iloc[0].get('Timestamp', 0)
            end_time = app_end.iloc[-1].get('Timestamp', 0)
            app_duration = end_time - start_time
        
        return {
            'total_jobs': total_jobs,
            'total_stages': total_stages,
            'total_tasks': total_tasks,
            'application_duration_ms': int(app_duration),
            'total_events': len(df)
        }
    
    def _empty_metrics(self) -> Dict[str, Any]:
        """Return empty metrics structure."""
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

# Backward compatibility wrapper
def compute_metrics(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Backward compatible compute_metrics function."""
    engine = MetricsEngine()
    return engine.compute_comprehensive_metrics(events)