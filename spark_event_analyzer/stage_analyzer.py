"""
Stage and Job Level Analyzer

Maps performance issues to specific jobs, stages, and tasks.
Identifies problematic transformations and actions.
"""

from typing import Dict, Any, List, Tuple, Optional
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class StageJobAnalyzer:
    """Analyzes performance at stage and job granularity."""
    
    def __init__(self):
        self.transformation_patterns = {
            'join': ['join', 'leftOuterJoin', 'rightOuterJoin', 'fullOuterJoin'],
            'groupBy': ['groupBy', 'groupByKey', 'aggregateByKey'],
            'shuffle': ['repartition', 'coalesce', 'sortBy', 'sortByKey'],
            'cache': ['cache', 'persist'],
            'wide': ['reduceByKey', 'combineByKey', 'distinct']
        }
    
    def analyze_stages_and_jobs(self, events: List[Dict[str, Any]], 
                                metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze stages and jobs to map performance issues.
        
        Args:
            events: List of parsed Spark events
            metrics: Computed performance metrics
            
        Returns:
            Comprehensive stage/job analysis
        """
        # Extract stage and job information
        stages = self._extract_stage_info(events)
        jobs = self._extract_job_info(events)
        tasks = self._extract_task_info(events)
        
        # Analyze each stage
        stage_analysis = {}
        for stage_id, stage_info in stages.items():
            stage_analysis[stage_id] = self._analyze_single_stage(
                stage_id, stage_info, tasks, metrics
            )
        
        # Analyze each job
        job_analysis = {}
        for job_id, job_info in jobs.items():
            job_analysis[job_id] = self._analyze_single_job(
                job_id, job_info, stages, stage_analysis
            )
        
        # Identify problematic transformations
        problematic_transformations = self._identify_problematic_transformations(
            stage_analysis, stages
        )
        
        # Map bottlenecks to stages/jobs
        bottleneck_mapping = self._map_bottlenecks_to_stages(
            stage_analysis, job_analysis, metrics
        )
        
        return {
            'stage_summary': self._generate_stage_summary(stage_analysis),
            'job_summary': self._generate_job_summary(job_analysis),
            'stage_details': stage_analysis,
            'job_details': job_analysis,
            'problematic_transformations': problematic_transformations,
            'bottleneck_mapping': bottleneck_mapping,
            'performance_hotspots': self._identify_hotspots(stage_analysis, job_analysis)
        }
    
    def _extract_stage_info(self, events: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        """Extract stage information from events."""
        stages = {}
        
        for event in events:
            event_type = event.get('Event')
            
            if event_type == 'SparkListenerStageSubmitted':
                stage_info = event.get('Stage Info', {})
                if isinstance(stage_info, dict):
                    stage_id = stage_info.get('Stage ID')
                    stages[stage_id] = {
                        'stage_id': stage_id,
                        'stage_name': stage_info.get('Stage Name', ''),
                        'stage_attempt': stage_info.get('Stage Attempt ID', 0),
                        'num_tasks': stage_info.get('Number of Tasks', 0),
                        'parent_ids': stage_info.get('Parent IDs', []),
                        'rdd_info': stage_info.get('RDD Info', []),
                        'submission_time': stage_info.get('Submission Time'),
                        'status': 'SUBMITTED'
                    }
            
            elif event_type == 'SparkListenerStageCompleted':
                stage_info = event.get('Stage Info', {})
                if isinstance(stage_info, dict):
                    stage_id = stage_info.get('Stage ID')
                    if stage_id in stages:
                        stages[stage_id].update({
                            'completion_time': stage_info.get('Completion Time'),
                            'failure_reason': stage_info.get('Failure Reason'),
                            'status': 'FAILED' if stage_info.get('Failure Reason') else 'SUCCESS',
                            'stage_metrics': stage_info.get('Task Metrics', {})
                        })
        
        return stages
    
    def _extract_job_info(self, events: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
        """Extract job information from events."""
        jobs = {}
        
        for event in events:
            event_type = event.get('Event')
            
            if event_type == 'SparkListenerJobStart':
                job_id = event.get('Job ID')
                jobs[job_id] = {
                    'job_id': job_id,
                    'submission_time': event.get('Submission Time'),
                    'stage_ids': event.get('Stage IDs', []),
                    'stage_infos': event.get('Stage Infos', []),
                    'status': 'RUNNING'
                }
            
            elif event_type == 'SparkListenerJobEnd':
                job_id = event.get('Job ID')
                if job_id in jobs:
                    job_result = event.get('Job Result', {})
                    jobs[job_id].update({
                        'completion_time': event.get('Completion Time'),
                        'result': job_result,
                        'status': 'FAILED' if job_result.get('Result') == 'JobFailed' else 'SUCCESS'
                    })
        
        return jobs
    
    def _extract_task_info(self, events: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
        """Extract task information grouped by stage."""
        tasks_by_stage = defaultdict(list)
        
        for event in events:
            if event.get('Event') == 'SparkListenerTaskEnd':
                stage_id = event.get('Stage ID')
                task_info = event.get('Task Info', {})
                task_metrics = event.get('Task Metrics', {})
                
                if isinstance(task_info, dict) and isinstance(task_metrics, dict):
                    task_data = {
                        'task_id': task_info.get('Task ID'),
                        'executor_id': task_info.get('Executor ID'),
                        'host': task_info.get('Host'),
                        'launch_time': task_info.get('Launch Time'),
                        'finish_time': task_info.get('Finish Time'),
                        'duration': task_info.get('Finish Time', 0) - task_info.get('Launch Time', 0),
                        'metrics': task_metrics,
                        'locality': task_info.get('Locality'),
                        'speculative': task_info.get('Speculative', False)
                    }
                    tasks_by_stage[stage_id].append(task_data)
        
        return dict(tasks_by_stage)
    
    def _analyze_single_stage(self, stage_id: int, stage_info: Dict[str, Any],
                              tasks: Dict[int, List[Dict[str, Any]]],
                              metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze a single stage in detail."""
        stage_tasks = tasks.get(stage_id, [])
        
        if not stage_tasks:
            return {
                'stage_id': stage_id,
                'stage_name': stage_info.get('stage_name', ''),
                'status': stage_info.get('status', 'UNKNOWN'),
                'issues': [],
                'metrics': {}
            }
        
        # Compute stage-level metrics
        durations = [t['duration'] for t in stage_tasks if t['duration'] > 0]
        gc_times = [t['metrics'].get('JVM GC Time', 0) for t in stage_tasks if isinstance(t['metrics'], dict)]
        memory_spilled = [t['metrics'].get('Memory Bytes Spilled', 0) for t in stage_tasks if isinstance(t['metrics'], dict)]
        shuffle_read = [t['metrics'].get('Shuffle Read Metrics', {}).get('Total Bytes Read', 0) 
                       for t in stage_tasks if isinstance(t['metrics'], dict)]
        
        total_duration = sum(durations)
        total_gc = sum(gc_times)
        total_spill = sum(memory_spilled)
        total_shuffle = sum(shuffle_read)
        
        stage_metrics = {
            'total_tasks': len(stage_tasks),
            'total_duration_ms': total_duration,
            'mean_task_duration_ms': sum(durations) / len(durations) if durations else 0,
            'max_task_duration_ms': max(durations) if durations else 0,
            'min_task_duration_ms': min(durations) if durations else 0,
            'task_skew': (max(durations) - min(durations)) / sum(durations) * len(durations) if durations and sum(durations) > 0 else 0,
            'total_gc_time_ms': total_gc,
            'gc_ratio': total_gc / total_duration if total_duration > 0 else 0,
            'total_memory_spilled': total_spill,
            'total_shuffle_read': total_shuffle,
            'shuffle_ratio': total_shuffle / (total_shuffle + sum([t['metrics'].get('Input Metrics', {}).get('Bytes Read', 0) 
                                                                    for t in stage_tasks if isinstance(t['metrics'], dict)])) if total_shuffle > 0 else 0
        }
        
        # Identify stage-specific issues
        issues = self._identify_stage_issues(stage_id, stage_info, stage_metrics, stage_tasks)
        
        # Identify transformation type
        transformation = self._identify_transformation(stage_info)
        
        return {
            'stage_id': stage_id,
            'stage_name': stage_info.get('stage_name', ''),
            'stage_attempt': stage_info.get('stage_attempt', 0),
            'status': stage_info.get('status', 'UNKNOWN'),
            'transformation': transformation,
            'metrics': stage_metrics,
            'issues': issues,
            'parent_stages': stage_info.get('parent_ids', []),
            'task_details': self._summarize_tasks(stage_tasks),
            'failure_reason': stage_info.get('failure_reason')
        }
    
    def _identify_stage_issues(self, stage_id: int, stage_info: Dict[str, Any],
                               metrics: Dict[str, Any], tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify issues specific to this stage."""
        issues = []
        
        # Check for high GC
        if metrics.get('gc_ratio', 0) > 0.2:
            issues.append({
                'type': 'HIGH_GC',
                'severity': 'HIGH' if metrics['gc_ratio'] > 0.3 else 'MEDIUM',
                'description': f"High GC overhead: {metrics['gc_ratio']*100:.1f}% of stage time",
                'metric': 'gc_ratio',
                'value': metrics['gc_ratio']
            })
        
        # Check for data skew
        if metrics.get('task_skew', 0) > 0.5:
            issues.append({
                'type': 'DATA_SKEW',
                'severity': 'HIGH' if metrics['task_skew'] > 1.0 else 'MEDIUM',
                'description': f"Data skew detected: task duration variance {metrics['task_skew']*100:.1f}%",
                'metric': 'task_skew',
                'value': metrics['task_skew']
            })
        
        # Check for memory spill
        if metrics.get('total_memory_spilled', 0) > 100_000_000:  # 100MB
            issues.append({
                'type': 'MEMORY_SPILL',
                'severity': 'HIGH' if metrics['total_memory_spilled'] > 1_000_000_000 else 'MEDIUM',
                'description': f"Memory spill: {self._format_bytes(metrics['total_memory_spilled'])}",
                'metric': 'total_memory_spilled',
                'value': metrics['total_memory_spilled']
            })
        
        # Check for excessive shuffle
        if metrics.get('shuffle_ratio', 0) > 0.5:
            issues.append({
                'type': 'EXCESSIVE_SHUFFLE',
                'severity': 'HIGH',
                'description': f"Excessive shuffle: {metrics['shuffle_ratio']*100:.1f}% of data",
                'metric': 'shuffle_ratio',
                'value': metrics['shuffle_ratio']
            })
        
        # Check for stragglers
        max_duration = metrics.get('max_task_duration_ms', 0)
        mean_duration = metrics.get('mean_task_duration_ms', 0)
        if max_duration > mean_duration * 2 and mean_duration > 0:
            issues.append({
                'type': 'STRAGGLER_TASKS',
                'severity': 'MEDIUM',
                'description': f"Straggler tasks detected: max task is {max_duration/mean_duration:.1f}x slower than mean",
                'metric': 'max_vs_mean_ratio',
                'value': max_duration/mean_duration
            })
        
        # Check for stage failure
        if stage_info.get('status') == 'FAILED':
            issues.append({
                'type': 'STAGE_FAILURE',
                'severity': 'CRITICAL',
                'description': f"Stage failed: {stage_info.get('failure_reason', 'Unknown')}",
                'metric': 'failure',
                'value': stage_info.get('failure_reason')
            })
        
        return issues
    
    def _identify_transformation(self, stage_info: Dict[str, Any]) -> Dict[str, Any]:
        """Identify the transformation type from stage name and RDD info."""
        stage_name = stage_info.get('stage_name', '').lower()
        
        transformation_type = 'unknown'
        operation = 'unknown'
        
        # Check common patterns
        for trans_type, patterns in self.transformation_patterns.items():
            for pattern in patterns:
                if pattern in stage_name:
                    transformation_type = trans_type
                    operation = pattern
                    break
        
        # Check RDD info for more details
        rdd_info = stage_info.get('rdd_info', [])
        rdd_names = []
        if isinstance(rdd_info, list):
            for rdd in rdd_info:
                if isinstance(rdd, dict):
                    rdd_names.append(rdd.get('Name', ''))
        
        return {
            'type': transformation_type,
            'operation': operation,
            'stage_name': stage_info.get('stage_name', ''),
            'rdd_names': rdd_names,
            'is_shuffle_stage': 'shuffle' in stage_name or 'exchange' in stage_name,
            'is_wide_transformation': transformation_type in ['join', 'groupBy', 'shuffle', 'wide']
        }
    
    def _summarize_tasks(self, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize task-level information."""
        if not tasks:
            return {}
        
        executors = set(t['executor_id'] for t in tasks if t.get('executor_id'))
        localities = [t['locality'] for t in tasks if t.get('locality')]
        
        return {
            'total_tasks': len(tasks),
            'unique_executors': len(executors),
            'locality_distribution': {
                loc: localities.count(loc) for loc in set(localities)
            },
            'speculative_tasks': sum(1 for t in tasks if t.get('speculative'))
        }
    
    def _analyze_single_job(self, job_id: int, job_info: Dict[str, Any],
                           stages: Dict[int, Dict[str, Any]],
                           stage_analysis: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze a single job."""
        job_stages = job_info.get('stage_ids', [])
        
        # Aggregate metrics from all stages
        total_duration = 0
        total_tasks = 0
        failed_stages = []
        problematic_stages = []
        
        for stage_id in job_stages:
            if stage_id in stage_analysis:
                stage = stage_analysis[stage_id]
                total_duration += stage.get('metrics', {}).get('total_duration_ms', 0)
                total_tasks += stage.get('metrics', {}).get('total_tasks', 0)
                
                if stage.get('status') == 'FAILED':
                    failed_stages.append(stage_id)
                
                if stage.get('issues'):
                    problematic_stages.append({
                        'stage_id': stage_id,
                        'issues': stage['issues']
                    })
        
        return {
            'job_id': job_id,
            'status': job_info.get('status', 'UNKNOWN'),
            'total_stages': len(job_stages),
            'stage_ids': job_stages,
            'total_duration_ms': total_duration,
            'total_tasks': total_tasks,
            'failed_stages': failed_stages,
            'problematic_stages': problematic_stages,
            'submission_time': job_info.get('submission_time'),
            'completion_time': job_info.get('completion_time')
        }
    
    def _identify_problematic_transformations(self, stage_analysis: Dict[int, Dict[str, Any]],
                                             stages: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify which transformations caused problems."""
        problematic = []
        
        for stage_id, analysis in stage_analysis.items():
            if analysis.get('issues'):
                transformation = analysis.get('transformation', {})
                
                # Group issues by transformation type
                problematic.append({
                    'stage_id': stage_id,
                    'stage_name': analysis.get('stage_name', ''),
                    'transformation_type': transformation.get('type', 'unknown'),
                    'operation': transformation.get('operation', 'unknown'),
                    'is_wide_transformation': transformation.get('is_wide_transformation', False),
                    'issues': analysis['issues'],
                    'severity': max([i.get('severity', 'LOW') for i in analysis['issues']], 
                                   key=lambda x: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'].index(x))
                })
        
        # Sort by severity
        problematic.sort(key=lambda x: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'].index(x['severity']))
        
        return problematic
    
    def _map_bottlenecks_to_stages(self, stage_analysis: Dict[int, Dict[str, Any]],
                                   job_analysis: Dict[int, Dict[str, Any]],
                                   metrics: Dict[str, Any]) -> Dict[str, List[int]]:
        """Map global bottlenecks to specific stages."""
        mapping = {
            'gc_pressure_stages': [],
            'data_skew_stages': [],
            'memory_spill_stages': [],
            'shuffle_heavy_stages': [],
            'failed_stages': []
        }
        
        for stage_id, analysis in stage_analysis.items():
            for issue in analysis.get('issues', []):
                issue_type = issue.get('type')
                
                if issue_type == 'HIGH_GC':
                    mapping['gc_pressure_stages'].append(stage_id)
                elif issue_type == 'DATA_SKEW':
                    mapping['data_skew_stages'].append(stage_id)
                elif issue_type == 'MEMORY_SPILL':
                    mapping['memory_spill_stages'].append(stage_id)
                elif issue_type == 'EXCESSIVE_SHUFFLE':
                    mapping['shuffle_heavy_stages'].append(stage_id)
                elif issue_type == 'STAGE_FAILURE':
                    mapping['failed_stages'].append(stage_id)
        
        return mapping
    
    def _identify_hotspots(self, stage_analysis: Dict[int, Dict[str, Any]],
                          job_analysis: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify performance hotspots (stages consuming most time/resources)."""
        hotspots = []
        
        for stage_id, analysis in stage_analysis.items():
            metrics = analysis.get('metrics', {})
            duration = metrics.get('total_duration_ms', 0)
            
            if duration > 0:
                hotspots.append({
                    'stage_id': stage_id,
                    'stage_name': analysis.get('stage_name', ''),
                    'duration_ms': duration,
                    'duration_formatted': self._format_duration(duration),
                    'task_count': metrics.get('total_tasks', 0),
                    'issues': analysis.get('issues', []),
                    'transformation': analysis.get('transformation', {}).get('type', 'unknown')
                })
        
        # Sort by duration
        hotspots.sort(key=lambda x: x['duration_ms'], reverse=True)
        
        return hotspots[:10]  # Top 10 hotspots
    
    def _generate_stage_summary(self, stage_analysis: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate overall stage summary."""
        total_stages = len(stage_analysis)
        failed_stages = sum(1 for s in stage_analysis.values() if s.get('status') == 'FAILED')
        stages_with_issues = sum(1 for s in stage_analysis.values() if s.get('issues'))
        
        total_duration = sum(s.get('metrics', {}).get('total_duration_ms', 0) 
                           for s in stage_analysis.values())
        
        return {
            'total_stages': total_stages,
            'failed_stages': failed_stages,
            'stages_with_issues': stages_with_issues,
            'healthy_stages': total_stages - stages_with_issues,
            'total_duration_ms': total_duration,
            'total_duration_formatted': self._format_duration(total_duration)
        }
    
    def _generate_job_summary(self, job_analysis: Dict[int, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate overall job summary."""
        total_jobs = len(job_analysis)
        failed_jobs = sum(1 for j in job_analysis.values() if j.get('status') == 'FAILED')
        
        return {
            'total_jobs': total_jobs,
            'failed_jobs': failed_jobs,
            'successful_jobs': total_jobs - failed_jobs
        }
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes into human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
    
    def _format_duration(self, ms: float) -> str:
        """Format milliseconds into human-readable duration."""
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
