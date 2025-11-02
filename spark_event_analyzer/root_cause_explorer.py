"""
Interactive Root Cause Explorer

Enables exploration of root cause chains for failures and performance issues.
Shows correlated metrics and builds "probable cause chains".
"""

from typing import Dict, Any, List, Optional, Tuple
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class RootCauseExplorer:
    """Interactive root cause chain exploration."""
    
    def __init__(self):
        self.cause_chain_rules = {
            'executor_lost': {
                'check_metrics': ['gc_time', 'memory_spill', 'oom_events'],
                'probable_causes': ['memory_pressure', 'oom', 'resource_exhaustion']
            },
            'stage_failure': {
                'check_metrics': ['task_failures', 'executor_lost', 'fetch_failures'],
                'probable_causes': ['executor_failure', 'network_issue', 'data_corruption']
            },
            'task_failure': {
                'check_metrics': ['memory_spill', 'gc_time', 'shuffle_fetch_time'],
                'probable_causes': ['oom', 'fetch_failure', 'timeout']
            }
        }
    
    def build_cause_chain(self, stage_id: int, stage_analysis: Dict[str, Any],
                         failures: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build a probable cause chain for a stage failure or issue.
        
        Args:
            stage_id: The stage to analyze
            stage_analysis: Stage-level analysis results
            failures: Failure detection results
            metrics: Global metrics
            
        Returns:
            Cause chain with correlated metrics
        """
        stage_data = stage_analysis.get('stage_details', {}).get(stage_id, {})
        
        if not stage_data:
            return {'error': f'Stage {stage_id} not found'}
        
        # Initialize cause chain
        cause_chain = {
            'stage_id': stage_id,
            'stage_name': stage_data.get('stage_name', ''),
            'primary_symptom': self._identify_primary_symptom(stage_data),
            'chain': [],
            'correlated_metrics': {},
            'confidence': 0.0
        }
        
        # Build the chain based on issues
        if stage_data.get('status') == 'FAILED':
            cause_chain['chain'] = self._build_failure_chain(
                stage_id, stage_data, failures, metrics
            )
        elif stage_data.get('issues'):
            cause_chain['chain'] = self._build_performance_chain(
                stage_id, stage_data, metrics
            )
        
        # Extract correlated metrics
        cause_chain['correlated_metrics'] = self._extract_correlated_metrics(
            stage_id, stage_data, metrics
        )
        
        # Compute confidence
        cause_chain['confidence'] = self._compute_chain_confidence(cause_chain['chain'])
        
        return cause_chain
    
    def explore_all_failures(self, stage_analysis: Dict[str, Any],
                            failures: Dict[str, Any], 
                            metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Explore all failures and build cause chains for each.
        
        Returns:
            List of cause chains for all failed/problematic stages
        """
        cause_chains = []
        
        # Get all problematic stages
        bottleneck_mapping = stage_analysis.get('bottleneck_mapping', {})
        
        # Failed stages
        for stage_id in bottleneck_mapping.get('failed_stages', []):
            chain = self.build_cause_chain(stage_id, stage_analysis, failures, metrics)
            cause_chains.append(chain)
        
        # Stages with critical issues
        for stage_id, stage_data in stage_analysis.get('stage_details', {}).items():
            critical_issues = [i for i in stage_data.get('issues', []) 
                             if i.get('severity') == 'CRITICAL']
            if critical_issues and stage_id not in bottleneck_mapping.get('failed_stages', []):
                chain = self.build_cause_chain(stage_id, stage_analysis, failures, metrics)
                cause_chains.append(chain)
        
        return cause_chains
    
    def generate_interactive_report(self, cause_chains: List[Dict[str, Any]]) -> str:
        """
        Generate interactive text report for cause chains.
        
        Returns:
            Formatted text report
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("ROOT CAUSE CHAIN ANALYSIS")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        if not cause_chains:
            report_lines.append("✓ No critical failures or issues detected")
            return "\n".join(report_lines)
        
        for i, chain in enumerate(cause_chains, 1):
            report_lines.append(f"\n[CHAIN {i}] Stage {chain['stage_id']}: {chain['stage_name']}")
            report_lines.append("-" * 80)
            
            # Primary symptom
            symptom = chain['primary_symptom']
            report_lines.append(f"Primary Symptom: {symptom['type']} [{symptom['severity']}]")
            report_lines.append(f"Description: {symptom['description']}")
            report_lines.append("")
            
            # Cause chain
            report_lines.append("Probable Cause Chain:")
            for j, cause in enumerate(chain['chain'], 1):
                indent = "  " * j
                arrow = "└─>" if j == len(chain['chain']) else "├─>"
                report_lines.append(f"{indent}{arrow} {cause['event']}")
                if cause.get('metrics'):
                    for key, value in cause['metrics'].items():
                        report_lines.append(f"{indent}    • {key}: {value}")
            
            report_lines.append("")
            
            # Correlated metrics
            if chain['correlated_metrics']:
                report_lines.append("Correlated Metrics at Failure:")
                for metric, value in chain['correlated_metrics'].items():
                    report_lines.append(f"  • {metric}: {value}")
            
            report_lines.append("")
            report_lines.append(f"Chain Confidence: {chain['confidence']*100:.0f}%")
            report_lines.append("")
        
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)
    
    def _identify_primary_symptom(self, stage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Identify the primary symptom for this stage."""
        if stage_data.get('status') == 'FAILED':
            return {
                'type': 'STAGE_FAILURE',
                'severity': 'CRITICAL',
                'description': f"Stage failed: {stage_data.get('failure_reason', 'Unknown')}"
            }
        
        issues = stage_data.get('issues', [])
        if not issues:
            return {
                'type': 'NO_ISSUE',
                'severity': 'INFO',
                'description': 'Stage completed successfully'
            }
        
        # Get highest severity issue
        critical_issues = [i for i in issues if i.get('severity') == 'CRITICAL']
        high_issues = [i for i in issues if i.get('severity') == 'HIGH']
        
        if critical_issues:
            issue = critical_issues[0]
        elif high_issues:
            issue = high_issues[0]
        else:
            issue = issues[0]
        
        return {
            'type': issue['type'],
            'severity': issue['severity'],
            'description': issue['description']
        }
    
    def _build_failure_chain(self, stage_id: int, stage_data: Dict[str, Any],
                            failures: Dict[str, Any], 
                            metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build cause chain for stage failure."""
        chain = []
        
        # Start with stage failure
        failure_reason = stage_data.get('failure_reason', '')
        chain.append({
            'event': f"Stage {stage_id} failed",
            'reason': failure_reason,
            'timestamp': 'at_failure',
            'metrics': {}
        })
        
        # Check for executor loss
        executor_failures = failures.get('executor_failures', [])
        stage_tasks = stage_data.get('task_details', {})
        
        if executor_failures:
            # Find executor failures around stage failure time
            chain.append({
                'event': f"Executor(s) lost: {len(executor_failures)} executor failure(s)",
                'reason': 'Executor failure preceded stage failure',
                'metrics': {
                    'failed_executors': len(executor_failures)
                }
            })
            
            # Check if executor failure was due to OOM
            oom_events = failures.get('oom_events', [])
            if oom_events:
                chain.append({
                    'event': "Out of Memory (OOM) detected",
                    'reason': 'Memory exhaustion caused executor loss',
                    'metrics': {
                        'oom_count': len(oom_events)
                    }
                })
                
                # Link to high GC or memory spill
                stage_metrics = stage_data.get('metrics', {})
                gc_ratio = stage_metrics.get('gc_ratio', 0)
                memory_spill = stage_metrics.get('total_memory_spilled', 0)
                
                if gc_ratio > 0.2 or memory_spill > 0:
                    chain.append({
                        'event': "High memory pressure detected",
                        'reason': 'Memory pressure led to OOM',
                        'metrics': {
                            'gc_overhead': f"{gc_ratio*100:.1f}%",
                            'memory_spilled': self._format_bytes(memory_spill)
                        }
                    })
                    
                    # Root cause
                    chain.append({
                        'event': "ROOT CAUSE: Insufficient executor memory",
                        'reason': 'Executor memory allocation too low for workload',
                        'metrics': {
                            'recommendation': 'Increase spark.executor.memory'
                        }
                    })
        
        # Check for fetch failures
        fetch_failures = failures.get('fetch_failures', [])
        if fetch_failures and 'FetchFailed' in failure_reason:
            chain.append({
                'event': f"Shuffle fetch failure: {len(fetch_failures)} fetch failure(s)",
                'reason': 'Network or executor issues during shuffle',
                'metrics': {
                    'fetch_failures': len(fetch_failures)
                }
            })
            
            # Check for network issues
            if stage_data.get('metrics', {}).get('shuffle_ratio', 0) > 0.5:
                chain.append({
                    'event': "Heavy shuffle operations detected",
                    'reason': 'Large data movement increases failure probability',
                    'metrics': {
                        'shuffle_ratio': f"{stage_data['metrics']['shuffle_ratio']*100:.1f}%"
                    }
                })
                
                chain.append({
                    'event': "ROOT CAUSE: Network instability or shuffle service issues",
                    'reason': 'Shuffle service may be overwhelmed or network unstable',
                    'metrics': {
                        'recommendation': 'Enable external shuffle service, reduce shuffle volume'
                    }
                })
        
        # If no specific pattern found, provide generic analysis
        if len(chain) == 1:
            stage_metrics = stage_data.get('metrics', {})
            
            # Check various metrics
            if stage_metrics.get('gc_ratio', 0) > 0.3:
                chain.append({
                    'event': "Critical GC overhead detected",
                    'metrics': {'gc_overhead': f"{stage_metrics['gc_ratio']*100:.1f}%"}
                })
                chain.append({
                    'event': "ROOT CAUSE: Memory pressure / GC thrashing",
                    'metrics': {'recommendation': 'Increase executor memory and tune GC'}
                })
            elif stage_metrics.get('task_skew', 0) > 1.0:
                chain.append({
                    'event': "Severe data skew detected",
                    'metrics': {'task_skew': f"{stage_metrics['task_skew']:.2f}"}
                })
                chain.append({
                    'event': "ROOT CAUSE: Uneven data distribution",
                    'metrics': {'recommendation': 'Repartition data and handle skewed keys'}
                })
            else:
                chain.append({
                    'event': "ROOT CAUSE: Unknown - Review stage logs for details",
                    'metrics': {'recommendation': 'Check application logs and Spark UI'}
                })
        
        return chain
    
    def _build_performance_chain(self, stage_id: int, stage_data: Dict[str, Any],
                                 metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build cause chain for performance issues."""
        chain = []
        issues = stage_data.get('issues', [])
        stage_metrics = stage_data.get('metrics', {})
        
        # Identify primary performance issue
        primary_issue = issues[0] if issues else None
        
        if not primary_issue:
            return chain
        
        issue_type = primary_issue['type']
        
        if issue_type == 'HIGH_GC':
            chain.append({
                'event': f"Stage {stage_id} experiencing high GC overhead",
                'metrics': {
                    'gc_overhead': f"{stage_metrics.get('gc_ratio', 0)*100:.1f}%"
                }
            })
            
            if stage_metrics.get('total_memory_spilled', 0) > 0:
                chain.append({
                    'event': "Memory spill detected",
                    'metrics': {
                        'memory_spilled': self._format_bytes(stage_metrics['total_memory_spilled'])
                    }
                })
            
            chain.append({
                'event': "ROOT CAUSE: Insufficient memory allocation",
                'metrics': {
                    'recommendation': 'Increase spark.executor.memory by 30-50%'
                }
            })
        
        elif issue_type == 'DATA_SKEW':
            chain.append({
                'event': f"Stage {stage_id} has uneven task distribution",
                'metrics': {
                    'task_skew': f"{stage_metrics.get('task_skew', 0):.2f}",
                    'max_task_duration': f"{stage_metrics.get('max_task_duration_ms', 0):.0f}ms",
                    'mean_task_duration': f"{stage_metrics.get('mean_task_duration_ms', 0):.0f}ms"
                }
            })
            
            transformation = stage_data.get('transformation', {})
            if transformation.get('is_wide_transformation'):
                chain.append({
                    'event': f"Wide transformation detected: {transformation.get('operation', 'unknown')}",
                    'metrics': {
                        'transformation_type': transformation.get('type', 'unknown')
                    }
                })
            
            chain.append({
                'event': "ROOT CAUSE: Data skew in partition distribution",
                'metrics': {
                    'recommendation': 'Enable AQE skew handling or use salting technique'
                }
            })
        
        elif issue_type == 'MEMORY_SPILL':
            chain.append({
                'event': f"Stage {stage_id} spilling data to disk",
                'metrics': {
                    'memory_spilled': self._format_bytes(stage_metrics.get('total_memory_spilled', 0))
                }
            })
            
            chain.append({
                'event': "Memory pressure exceeds available executor memory",
                'metrics': {
                    'gc_overhead': f"{stage_metrics.get('gc_ratio', 0)*100:.1f}%"
                }
            })
            
            chain.append({
                'event': "ROOT CAUSE: Working set too large for memory",
                'metrics': {
                    'recommendation': 'Increase executor memory or reduce data volume per partition'
                }
            })
        
        elif issue_type == 'EXCESSIVE_SHUFFLE':
            chain.append({
                'event': f"Stage {stage_id} has heavy shuffle operations",
                'metrics': {
                    'shuffle_ratio': f"{stage_metrics.get('shuffle_ratio', 0)*100:.1f}%",
                    'shuffle_data': self._format_bytes(stage_metrics.get('total_shuffle_read', 0))
                }
            })
            
            transformation = stage_data.get('transformation', {})
            chain.append({
                'event': f"Transformation causing shuffle: {transformation.get('operation', 'unknown')}",
                'metrics': {
                    'transformation_type': transformation.get('type', 'unknown')
                }
            })
            
            chain.append({
                'event': "ROOT CAUSE: Inefficient shuffle strategy",
                'metrics': {
                    'recommendation': 'Use broadcast join for small tables or reduce shuffle partitions'
                }
            })
        
        return chain
    
    def _extract_correlated_metrics(self, stage_id: int, stage_data: Dict[str, Any],
                                   metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Extract correlated metrics for the stage."""
        stage_metrics = stage_data.get('metrics', {})
        
        correlated = {
            'GC Overhead %': f"{stage_metrics.get('gc_ratio', 0)*100:.1f}%",
            'Task Skew Ratio': f"{stage_metrics.get('task_skew', 0):.2f}",
            'Memory Spilled': self._format_bytes(stage_metrics.get('total_memory_spilled', 0)),
            'Shuffle Read': self._format_bytes(stage_metrics.get('total_shuffle_read', 0)),
            'Total Duration': self._format_duration(stage_metrics.get('total_duration_ms', 0)),
            'Task Count': stage_metrics.get('total_tasks', 0),
            'Mean Task Duration': f"{stage_metrics.get('mean_task_duration_ms', 0):.0f}ms",
            'Max Task Duration': f"{stage_metrics.get('max_task_duration_ms', 0):.0f}ms"
        }
        
        # Add global correlations if available
        global_correlations = metrics.get('correlations', {})
        if global_correlations:
            correlated['GC vs Duration Correlation'] = f"{global_correlations.get('gc_vs_duration', 0):.3f}"
            correlated['Spill vs GC Correlation'] = f"{global_correlations.get('spill_vs_gc', 0):.3f}"
        
        return correlated
    
    def _compute_chain_confidence(self, chain: List[Dict[str, Any]]) -> float:
        """Compute confidence score for the cause chain."""
        if not chain:
            return 0.0
        
        # Base confidence
        confidence = 0.5
        
        # More links in chain = higher confidence
        chain_length_bonus = min(len(chain) * 0.1, 0.3)
        confidence += chain_length_bonus
        
        # Check for strong evidence
        has_metrics = any('metrics' in c and c['metrics'] for c in chain)
        if has_metrics:
            confidence += 0.15
        
        # Check for root cause identification
        has_root_cause = any('ROOT CAUSE' in c.get('event', '') for c in chain)
        if has_root_cause:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes into human-readable format."""
        if bytes_val == 0:
            return "0 B"
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
