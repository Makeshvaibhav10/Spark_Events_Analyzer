"""
Failure and Anomaly Detection Module

Detects Spark failures and links them to performance metrics:
- OOM errors
- FetchFailure events  
- Stage retries
- Task failures
"""

from typing import Dict, Any, List
import logging
import re

logger = logging.getLogger(__name__)

class FailureDetector:
    """Detects and analyzes failures in Spark event logs."""
    
    def __init__(self):
        self.failure_patterns = {
            'OOM': r'OutOfMemoryError|Java heap space|GC overhead limit exceeded',
            'FetchFailed': r'FetchFailed|ShuffleFetchFailed',
            'ExecutorLost': r'ExecutorLostFailure|Executor .* lost',
            'NetworkIssue': r'Connection refused|Connection reset|IOException',
            'DiskFull': r'No space left on device|Disk full',
            'Timeout': r'TimeoutException|Timeout waiting',
        }
    
    def detect_failures(self, events: List[Dict[str, Any]], metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect failures and anomalies, linking them to performance metrics.
        
        Args:
            events: List of Spark events
            metrics: Computed performance metrics
            
        Returns:
            Dictionary with failure analysis
        """
        failures = {
            'stage_failures': self._detect_stage_failures(events, metrics),
            'task_failures': self._detect_task_failures(events, metrics),
            'executor_failures': self._detect_executor_failures(events, metrics),
            'oom_events': self._detect_oom_events(events, metrics),
            'fetch_failures': self._detect_fetch_failures(events, metrics),
            'summary': {}
        }
        
        # Generate summary
        failures['summary'] = self._generate_failure_summary(failures)
        
        return failures
    
    def _detect_stage_failures(self, events: List[Dict[str, Any]], 
                                metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect stage-level failures."""
        stage_failures = []
        
        for event in events:
            if event.get('Event') != 'SparkListenerStageCompleted':
                continue
            
            stage_info = event.get('Stage Info', {})
            if not isinstance(stage_info, dict):
                continue
            
            failure_reason = stage_info.get('Failure Reason')
            if not failure_reason:
                continue
            
            stage_id = stage_info.get('Stage ID', -1)
            stage_attempt = stage_info.get('Stage Attempt ID', 0)
            
            # Classify failure type
            failure_type = self._classify_failure(failure_reason)
            
            # Get stage metrics at failure time
            stage_metrics = self._get_stage_metrics_at_failure(
                stage_id, metrics
            )
            
            failure = {
                'stage_id': stage_id,
                'stage_attempt': stage_attempt,
                'failure_type': failure_type,
                'failure_reason': failure_reason,
                'timestamp': stage_info.get('Completion Time', 0),
                'metrics_at_failure': stage_metrics,
                'root_cause_hypothesis': self._hypothesize_cause(
                    failure_type, failure_reason, stage_metrics
                )
            }
            
            stage_failures.append(failure)
        
        return stage_failures
    
    def _detect_task_failures(self, events: List[Dict[str, Any]], 
                              metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect task-level failures."""
        task_failures = []
        
        for event in events:
            if event.get('Event') != 'SparkListenerTaskEnd':
                continue
            
            task_end_reason = event.get('Task End Reason', {})
            if not isinstance(task_end_reason, dict):
                continue
            
            reason = task_end_reason.get('Reason')
            if reason not in ['Success', None]:
                task_info = event.get('Task Info', {})
                task_metrics = event.get('Task Metrics', {})
                
                failure_type = self._classify_failure(str(task_end_reason))
                
                failure = {
                    'task_id': task_info.get('Task ID') if isinstance(task_info, dict) else None,
                    'stage_id': event.get('Stage ID', -1),
                    'executor_id': task_info.get('Executor ID') if isinstance(task_info, dict) else None,
                    'failure_type': failure_type,
                    'failure_reason': reason,
                    'task_end_reason': task_end_reason,
                    'timestamp': task_info.get('Finish Time', 0) if isinstance(task_info, dict) else 0,
                    'duration_ms': (task_info.get('Finish Time', 0) - task_info.get('Launch Time', 0))
                        if isinstance(task_info, dict) else 0,
                    'metrics_at_failure': self._extract_task_failure_metrics(task_metrics)
                }
                
                task_failures.append(failure)
        
        return task_failures
    
    def _detect_executor_failures(self, events: List[Dict[str, Any]], 
                                   metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect executor removal events."""
        executor_failures = []
        
        for event in events:
            if event.get('Event') != 'SparkListenerExecutorRemoved':
                continue
            
            removed_reason = event.get('Removed Reason', '')
            executor_id = event.get('Executor ID', 'unknown')
            
            # Check if removal was due to failure
            is_failure = any(pattern in removed_reason.lower() 
                           for pattern in ['lost', 'failed', 'killed', 'error'])
            
            if is_failure:
                failure_type = self._classify_failure(removed_reason)
                
                failure = {
                    'executor_id': executor_id,
                    'failure_type': failure_type,
                    'removed_reason': removed_reason,
                    'timestamp': event.get('Timestamp', 0),
                    'suspected_cause': self._hypothesize_executor_failure(
                        failure_type, removed_reason, metrics
                    )
                }
                
                executor_failures.append(failure)
        
        return executor_failures
    
    def _detect_oom_events(self, events: List[Dict[str, Any]], 
                           metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect Out of Memory events."""
        oom_events = []
        
        memory_metrics = metrics.get('memory_metrics', {})
        gc_pressure = memory_metrics.get('gc_pressure_level', 'LOW')
        
        for event in events:
            event_type = event.get('Event')
            
            # Check task failures for OOM
            if event_type == 'SparkListenerTaskEnd':
                task_end_reason = event.get('Task End Reason', {})
                if isinstance(task_end_reason, dict):
                    reason_str = str(task_end_reason)
                    if re.search(self.failure_patterns['OOM'], reason_str, re.IGNORECASE):
                        task_info = event.get('Task Info', {})
                        
                        oom_event = {
                            'type': 'Task OOM',
                            'task_id': task_info.get('Task ID') if isinstance(task_info, dict) else None,
                            'stage_id': event.get('Stage ID', -1),
                            'executor_id': task_info.get('Executor ID') if isinstance(task_info, dict) else None,
                            'timestamp': task_info.get('Finish Time', 0) if isinstance(task_info, dict) else 0,
                            'gc_pressure_at_time': gc_pressure,
                            'memory_metrics': memory_metrics,
                            'likely_cause': self._determine_oom_cause(memory_metrics)
                        }
                        
                        oom_events.append(oom_event)
            
            # Check executor failures for OOM
            elif event_type == 'SparkListenerExecutorRemoved':
                removed_reason = event.get('Removed Reason', '')
                if re.search(self.failure_patterns['OOM'], removed_reason, re.IGNORECASE):
                    oom_event = {
                        'type': 'Executor OOM',
                        'executor_id': event.get('Executor ID', 'unknown'),
                        'removed_reason': removed_reason,
                        'timestamp': event.get('Timestamp', 0),
                        'gc_pressure_at_time': gc_pressure,
                        'memory_metrics': memory_metrics,
                        'likely_cause': self._determine_oom_cause(memory_metrics)
                    }
                    
                    oom_events.append(oom_event)
        
        return oom_events
    
    def _detect_fetch_failures(self, events: List[Dict[str, Any]], 
                                metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect shuffle fetch failures."""
        fetch_failures = []
        
        shuffle_metrics = metrics.get('shuffle_metrics', {})
        
        for event in events:
            if event.get('Event') != 'SparkListenerTaskEnd':
                continue
            
            task_end_reason = event.get('Task End Reason', {})
            if not isinstance(task_end_reason, dict):
                continue
            
            reason = task_end_reason.get('Reason', '')
            if 'FetchFailed' in reason:
                task_info = event.get('Task Info', {})
                
                fetch_failure = {
                    'task_id': task_info.get('Task ID') if isinstance(task_info, dict) else None,
                    'stage_id': event.get('Stage ID', -1),
                    'executor_id': task_info.get('Executor ID') if isinstance(task_info, dict) else None,
                    'timestamp': task_info.get('Finish Time', 0) if isinstance(task_info, dict) else 0,
                    'failure_reason': task_end_reason,
                    'shuffle_metrics': shuffle_metrics,
                    'likely_cause': self._determine_fetch_failure_cause(
                        task_end_reason, shuffle_metrics
                    )
                }
                
                fetch_failures.append(fetch_failure)
        
        return fetch_failures
    
    def _classify_failure(self, failure_text: str) -> str:
        """Classify failure type based on text pattern."""
        if not failure_text:
            return "UNKNOWN"
        
        failure_lower = failure_text.lower()
        
        for failure_type, pattern in self.failure_patterns.items():
            if re.search(pattern, failure_text, re.IGNORECASE):
                return failure_type
        
        return "UNKNOWN"
    
    def _get_stage_metrics_at_failure(self, stage_id: int, 
                                      metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Get metrics for a specific stage."""
        stage_metrics = metrics.get('stage_metrics', {})
        by_stage = stage_metrics.get('by_stage', {})
        
        stage_key = f"stage_{stage_id}"
        if stage_key in by_stage:
            return by_stage[stage_key]
        
        return {}
    
    def _extract_task_failure_metrics(self, task_metrics: Any) -> Dict[str, Any]:
        """Extract relevant metrics from failed task."""
        if not isinstance(task_metrics, dict):
            return {}
        
        return {
            'executor_run_time': task_metrics.get('Executor Run Time', 0),
            'jvm_gc_time': task_metrics.get('JVM GC Time', 0),
            'memory_bytes_spilled': task_metrics.get('Memory Bytes Spilled', 0),
            'shuffle_read_bytes': task_metrics.get('Shuffle Read Metrics', {}).get('Total Bytes Read', 0)
                if isinstance(task_metrics.get('Shuffle Read Metrics'), dict) else 0
        }
    
    def _hypothesize_cause(self, failure_type: str, failure_reason: str, 
                          stage_metrics: Dict[str, Any]) -> str:
        """Generate hypothesis for failure cause."""
        if failure_type == "OOM":
            return ("Out of Memory due to insufficient executor memory or memory leak. "
                   "Check executor memory configuration and GC logs.")
        elif failure_type == "FetchFailed":
            return ("Shuffle fetch failure due to network issues or executor loss. "
                   "Check network stability and shuffle service configuration.")
        elif failure_type == "ExecutorLost":
            return ("Executor lost due to crash, OOM, or container preemption. "
                   "Review executor logs and resource manager logs.")
        elif failure_type == "NetworkIssue":
            return ("Network connectivity issues between executors. "
                   "Check network configuration and bandwidth.")
        elif failure_type == "DiskFull":
            return ("Insufficient disk space for shuffle/spill operations. "
                   "Increase disk space or reduce data volume per executor.")
        elif failure_type == "Timeout":
            return ("Operation timeout, possibly due to slow executors or network congestion. "
                   "Review timeout configurations and system load.")
        else:
            return f"Failure of type {failure_type}. Review logs for details."
    
    def _hypothesize_executor_failure(self, failure_type: str, removed_reason: str,
                                      metrics: Dict[str, Any]) -> str:
        """Generate hypothesis for executor failure."""
        memory_metrics = metrics.get('memory_metrics', {})
        gc_pressure = memory_metrics.get('gc_pressure_level', 'LOW')
        
        if failure_type == "OOM" or gc_pressure in ['CRITICAL', 'HIGH']:
            return (f"Executor likely failed due to memory pressure (GC pressure: {gc_pressure}). "
                   "Increase spark.executor.memory and spark.executor.memoryOverhead.")
        else:
            return self._hypothesize_cause(failure_type, removed_reason, {})
    
    def _determine_oom_cause(self, memory_metrics: Dict[str, Any]) -> str:
        """Determine likely cause of OOM."""
        gc_ratio = memory_metrics.get('gc_overhead_ratio', 0)
        memory_spilled = memory_metrics.get('memoryBytesSpilled', 0)
        
        causes = []
        
        if gc_ratio > 0.3:
            causes.append("Excessive GC overhead indicating severe memory pressure")
        
        if memory_spilled > 1_000_000_000:  # > 1GB
            causes.append(f"Large memory spill ({self._format_bytes(memory_spilled)}) indicating insufficient memory")
        
        if not causes:
            causes.append("Insufficient executor memory for workload")
        
        return ". ".join(causes) + ". Recommendation: Increase spark.executor.memory significantly."
    
    def _determine_fetch_failure_cause(self, task_end_reason: Dict[str, Any],
                                       shuffle_metrics: Dict[str, Any]) -> str:
        """Determine likely cause of fetch failure."""
        shuffle_ratio = shuffle_metrics.get('shuffle_ratio', 0)
        total_shuffle = shuffle_metrics.get('totalShuffleBytes', 0)
        
        if shuffle_ratio > 0.5:
            return (f"Heavy shuffle operations ({shuffle_ratio*100:.1f}% of time, "
                   f"{self._format_bytes(total_shuffle)} data) likely causing network congestion. "
                   "Consider reducing shuffle volume or enabling external shuffle service.")
        else:
            return ("Network instability or executor failure during shuffle. "
                   "Check network configuration and executor stability.")
    
    def _generate_failure_summary(self, failures: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary of all failures."""
        return {
            'total_stage_failures': len(failures.get('stage_failures', [])),
            'total_task_failures': len(failures.get('task_failures', [])),
            'total_executor_failures': len(failures.get('executor_failures', [])),
            'total_oom_events': len(failures.get('oom_events', [])),
            'total_fetch_failures': len(failures.get('fetch_failures', [])),
            'has_critical_failures': (
                len(failures.get('oom_events', [])) > 0 or
                len(failures.get('stage_failures', [])) > 0
            ),
            'failure_rate': self._compute_failure_rate(failures)
        }
    
    def _compute_failure_rate(self, failures: Dict[str, Any]) -> float:
        """Compute overall failure rate."""
        total_failures = (
            len(failures.get('stage_failures', [])) +
            len(failures.get('task_failures', [])) +
            len(failures.get('executor_failures', []))
        )
        
        # This is a simplified rate; in practice, would compare against total attempts
        return float(total_failures)
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes into human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
