"""
Enhanced Analyzer Module with Correlation-Based Bottleneck Detection

This module analyzes Spark metrics to identify bottlenecks using:
- Correlation analysis between metrics
- Dynamic confidence scoring
- Severity classification
- Detailed reasoning generation
"""

from typing import Dict, Any, List
from spark_event_analyzer.metrics_engine import compute_metrics
import logging
import math

logger = logging.getLogger(__name__)

class BottleneckAnalyzer:
    """Enhanced analyzer with correlation-based bottleneck detection."""
    
    def __init__(self):
        self.confidence_calibration = {
            'high_correlation': 0.15,
            'metric_severity': 0.50,
            'statistical_significance': 0.20,
            'temporal_consistency': 0.15
        }
    
    def analyze_events(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyzes Spark event logs with correlation-based bottleneck detection.
        
        Args:
            events: List of Spark events as dictionaries
            
        Returns:
            Comprehensive analysis with bottlenecks, correlations, and insights
        """
        metrics = compute_metrics(events)
        
        # Detect bottlenecks with correlation analysis
        bottlenecks = []
        bottlenecks.extend(self._detect_gc_pressure(metrics))
        bottlenecks.extend(self._detect_data_skew(metrics))
        bottlenecks.extend(self._detect_cpu_underuse(metrics))
        bottlenecks.extend(self._detect_spill_events(metrics))
        bottlenecks.extend(self._detect_shuffle_inefficiency(metrics))
        bottlenecks.extend(self._detect_executor_imbalance(metrics))
        
        # Detect anomalies and failures
        anomalies = self._detect_anomalies(metrics)
        
        # Generate warnings for moderate issues
        warnings = self._generate_warnings(metrics)
        
        # Sort bottlenecks by severity and confidence
        bottlenecks.sort(key=lambda x: (
            self._severity_order(x.get('severity', 'LOW')),
            -x.get('confidence', 0)
        ))
        
        return {
            "performance_summary": metrics,
            "bottlenecks": bottlenecks,
            "anomalies": anomalies,
            "warnings": warnings,
            "analysis_metadata": {
                "total_bottlenecks": len(bottlenecks),
                "critical_bottlenecks": sum(1 for b in bottlenecks if b.get('severity') == 'CRITICAL'),
                "high_bottlenecks": sum(1 for b in bottlenecks if b.get('severity') == 'HIGH'),
                "total_anomalies": len(anomalies)
            }
        }
    
    def _detect_gc_pressure(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect GC pressure with correlation analysis: GC↑ + Spill↑ → Memory Pressure."""
        memory_metrics = metrics.get('memory_metrics', {})
        correlations = metrics.get('correlations', {})
        
        gc_ratio = memory_metrics.get('gc_overhead_ratio', 0)
        memory_spilled = memory_metrics.get('memoryBytesSpilled', 0)
        gc_pressure_level = memory_metrics.get('gc_pressure_level', 'LOW')
        
        # Correlation strength between GC and memory spill
        spill_gc_correlation = abs(correlations.get('spill_vs_gc', 0))
        
        bottlenecks = []
        
        if gc_ratio > 0.1:  # Dynamic threshold
            # Compute confidence based on multiple factors
            impact_score = min(gc_ratio * 2, 1.0)  # Higher GC = higher impact
            correlation_strength = spill_gc_correlation
            
            # Check for memory spill as supporting evidence
            has_spill_evidence = memory_spilled > 0
            
            confidence = self._compute_confidence(
                impact_score=impact_score,
                correlation_strength=correlation_strength,
                has_supporting_evidence=has_spill_evidence
            )
            
            severity = "CRITICAL" if gc_ratio > 0.4 else "HIGH" if gc_ratio > 0.2 else "MEDIUM"
            
            reasoning = self._generate_gc_reasoning(
                gc_ratio=gc_ratio,
                memory_spilled=memory_spilled,
                correlation=spill_gc_correlation,
                pressure_level=gc_pressure_level
            )
            
            bottlenecks.append({
                "bottleneck": "GC Overhead / Memory Pressure",
                "type": "MEMORY",
                "severity": severity,
                "confidence": confidence,
                "impact_score": impact_score,
                "metric_evidence": {
                    "gc_overhead_ratio": gc_ratio,
                    "gc_overhead_percentage": f"{gc_ratio*100:.2f}%",
                    "memoryBytesSpilled": memory_spilled,
                    "memory_spilled_formatted": self._format_bytes(memory_spilled),
                    "gc_pressure_level": gc_pressure_level,
                    "total_gc_time_ms": memory_metrics.get('total_gc_time_ms', 0)
                },
                "correlation_evidence": {
                    "spill_vs_gc_correlation": spill_gc_correlation,
                    "correlation_strength": "STRONG" if spill_gc_correlation > 0.7 else "MODERATE" if spill_gc_correlation > 0.4 else "WEAK"
                },
                "reasoning": reasoning,
                "impact": f"GC consumes {gc_ratio*100:.1f}% of executor runtime, causing performance degradation"
            })
        
        return bottlenecks
    
    def _detect_data_skew(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect data skew using percentile analysis and variance."""
        task_metrics = metrics.get('task_metrics', {})
        thresholds = metrics.get('thresholds', {})
        
        task_skew = task_metrics.get('task_skew_ratio', 0)
        stragglers_pct = task_metrics.get('stragglers_percentage', 0)
        cv = task_metrics.get('coefficient_of_variation', 0)
        
        bottlenecks = []
        
        # Use dynamic thresholds
        if task_skew > 0.3 or stragglers_pct > 5:
            impact_score = min(task_skew, 1.0)
            
            confidence = self._compute_confidence(
                impact_score=impact_score,
                correlation_strength=cv,
                has_supporting_evidence=stragglers_pct > 5
            )
            
            severity = "CRITICAL" if task_skew > 1.0 else "HIGH" if task_skew > 0.5 else "MEDIUM"
            
            reasoning = self._generate_skew_reasoning(
                task_skew=task_skew,
                stragglers_pct=stragglers_pct,
                percentiles=task_metrics.get('percentiles', {})
            )
            
            bottlenecks.append({
                "bottleneck": "Data Skew / Partition Imbalance",
                "type": "DATA_SKEW",
                "severity": severity,
                "confidence": confidence,
                "impact_score": impact_score,
                "metric_evidence": {
                    "task_skew_ratio": task_skew,
                    "coefficient_of_variation": cv,
                    "stragglers_count": task_metrics.get('stragglers_count', 0),
                    "stragglers_percentage": stragglers_pct,
                    "min_duration_ms": task_metrics.get('min_task_duration', 0),
                    "median_duration_ms": task_metrics.get('median_task_duration', 0),
                    "max_duration_ms": task_metrics.get('max_task_duration', 0),
                    "p99_duration_ms": task_metrics.get('percentiles', {}).get('p99', 0)
                },
                "reasoning": reasoning,
                "impact": f"Task duration variance is {task_skew*100:.1f}% of mean, with {stragglers_pct:.1f}% straggler tasks"
            })
        
        return bottlenecks
    
    def _detect_cpu_underuse(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect CPU underutilization: CPU↓ + Shuffle↑ → I/O Bottleneck."""
        cpu_metrics = metrics.get('cpu_metrics', {})
        shuffle_metrics = metrics.get('shuffle_metrics', {})
        correlations = metrics.get('correlations', {})
        
        cpu_eff = cpu_metrics.get('cpu_efficiency', 1)
        cpu_wait_ratio = cpu_metrics.get('cpu_wait_ratio', 0)
        shuffle_ratio = shuffle_metrics.get('shuffle_ratio', 0)
        
        bottlenecks = []
        
        if cpu_eff < 0.75:
            # Check if low CPU correlates with high shuffle
            is_io_bottleneck = shuffle_ratio > 0.3
            
            impact_score = 1.0 - cpu_eff
            correlation_strength = shuffle_ratio if is_io_bottleneck else cpu_wait_ratio
            
            confidence = self._compute_confidence(
                impact_score=impact_score,
                correlation_strength=correlation_strength,
                has_supporting_evidence=is_io_bottleneck
            )
            
            severity = "HIGH" if cpu_eff < 0.4 else "MEDIUM"
            
            probable_cause = "I/O Bottleneck" if is_io_bottleneck else "CPU Underutilization"
            
            reasoning = self._generate_cpu_reasoning(
                cpu_eff=cpu_eff,
                shuffle_ratio=shuffle_ratio,
                is_io_bottleneck=is_io_bottleneck
            )
            
            bottlenecks.append({
                "bottleneck": f"CPU Underutilization / {probable_cause}",
                "type": "CPU",
                "severity": severity,
                "confidence": confidence,
                "impact_score": impact_score,
                "metric_evidence": {
                    "cpu_efficiency": cpu_eff,
                    "cpu_utilization_percentage": f"{cpu_eff*100:.2f}%",
                    "cpu_wait_percentage": f"{cpu_wait_ratio*100:.2f}%",
                    "shuffle_ratio": shuffle_ratio,
                    "deserialize_overhead_percentage": cpu_metrics.get('deserialize_overhead_percentage', 0)
                },
                "correlation_evidence": {
                    "is_io_bottleneck": is_io_bottleneck,
                    "shuffle_correlation": "HIGH" if shuffle_ratio > 0.5 else "MODERATE" if shuffle_ratio > 0.3 else "LOW"
                },
                "reasoning": reasoning,
                "impact": f"CPUs are {(1-cpu_eff)*100:.1f}% idle, indicating {'I/O bottleneck' if is_io_bottleneck else 'inefficient resource utilization'}"
            })
        
        return bottlenecks
    
    def _detect_spill_events(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect memory spill: Large Spill↑ + Reduced Memory → Insufficient Memory."""
        memory_metrics = metrics.get('memory_metrics', {})
        io_metrics = metrics.get('io_metrics', {})
        
        memory_spilled = memory_metrics.get('memoryBytesSpilled', 0)
        disk_spilled = memory_metrics.get('diskBytesSpilled', 0)
        total_input = io_metrics.get('inputBytes', 0)
        
        bottlenecks = []
        
        if memory_spilled > 0:
            spill_ratio = memory_spilled / total_input if total_input > 0 else 0
            
            if spill_ratio > 0.05:  # 5% threshold
                impact_score = min(spill_ratio * 2, 1.0)
                
                confidence = self._compute_confidence(
                    impact_score=impact_score,
                    correlation_strength=spill_ratio,
                    has_supporting_evidence=disk_spilled > 0
                )
                
                severity = "CRITICAL" if spill_ratio > 0.3 else "HIGH" if spill_ratio > 0.1 else "MEDIUM"
                
                reasoning = self._generate_spill_reasoning(
                    memory_spilled=memory_spilled,
                    disk_spilled=disk_spilled,
                    spill_ratio=spill_ratio
                )
                
                bottlenecks.append({
                    "bottleneck": "Memory Spill to Disk",
                    "type": "MEMORY",
                    "severity": severity,
                    "confidence": confidence,
                    "impact_score": impact_score,
                    "metric_evidence": {
                        "memoryBytesSpilled": memory_spilled,
                        "memory_spilled_formatted": self._format_bytes(memory_spilled),
                        "diskBytesSpilled": disk_spilled,
                        "disk_spilled_formatted": self._format_bytes(disk_spilled),
                        "spill_ratio": spill_ratio,
                        "spill_percentage": f"{spill_ratio*100:.2f}%",
                        "tasks_with_spill": memory_metrics.get('tasks_with_spill', 0)
                    },
                    "reasoning": reasoning,
                    "impact": f"{self._format_bytes(memory_spilled)} spilled to disk ({spill_ratio*100:.1f}% of input data)"
                })
        
        return bottlenecks
    
    def _detect_shuffle_inefficiency(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect shuffle inefficiency: Excessive Shuffle vs Total Data."""
        shuffle_metrics = metrics.get('shuffle_metrics', {})
        io_metrics = metrics.get('io_metrics', {})
        
        shuffle_ratio = shuffle_metrics.get('shuffle_ratio', 0)
        total_shuffle = shuffle_metrics.get('totalShuffleBytes', 0)
        total_io = io_metrics.get('inputBytes', 0) + io_metrics.get('outputBytes', 0)
        shuffle_efficiency = shuffle_metrics.get('shuffle_efficiency', 1.0)
        
        bottlenecks = []
        
        if shuffle_ratio > 0.3:
            shuffle_intensity = total_shuffle / total_io if total_io > 0 else 0
            
            impact_score = min(shuffle_ratio, 1.0)
            
            confidence = self._compute_confidence(
                impact_score=impact_score,
                correlation_strength=shuffle_intensity,
                has_supporting_evidence=shuffle_efficiency < 0.5
            )
            
            severity = "CRITICAL" if shuffle_ratio > 0.7 else "HIGH" if shuffle_ratio > 0.5 else "MEDIUM"
            
            reasoning = self._generate_shuffle_reasoning(
                shuffle_ratio=shuffle_ratio,
                shuffle_intensity=shuffle_intensity,
                shuffle_efficiency=shuffle_efficiency
            )
            
            bottlenecks.append({
                "bottleneck": "Excessive Shuffle Overhead",
                "type": "SHUFFLE",
                "severity": severity,
                "confidence": confidence,
                "impact_score": impact_score,
                "metric_evidence": {
                    "shuffle_ratio": shuffle_ratio,
                    "shuffle_time_percentage": f"{shuffle_ratio*100:.2f}%",
                    "shuffleReadBytes": shuffle_metrics.get('shuffleReadBytes', 0),
                    "shuffleWriteBytes": shuffle_metrics.get('shuffleWriteBytes', 0),
                    "totalShuffleBytes": total_shuffle,
                    "total_shuffle_formatted": self._format_bytes(total_shuffle),
                    "shuffle_efficiency": shuffle_efficiency,
                    "shuffle_intensity": shuffle_intensity
                },
                "reasoning": reasoning,
                "impact": f"Shuffle operations consume {shuffle_ratio*100:.1f}% of execution time"
            })
        
        return bottlenecks
    
    def _detect_executor_imbalance(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect executor load imbalance."""
        executor_metrics = metrics.get('executor_metrics', {})
        
        imbalance_ratio = executor_metrics.get('executor_imbalance_ratio', 0)
        load_imbalance_pct = executor_metrics.get('load_imbalance_percentage', 0)
        
        bottlenecks = []
        
        if imbalance_ratio > 0.3:
            impact_score = min(imbalance_ratio, 1.0)
            
            confidence = self._compute_confidence(
                impact_score=impact_score,
                correlation_strength=load_imbalance_pct / 100,
                has_supporting_evidence=load_imbalance_pct > 30
            )
            
            severity = "HIGH" if imbalance_ratio > 0.6 else "MEDIUM"
            
            reasoning = self._generate_imbalance_reasoning(
                imbalance_ratio=imbalance_ratio,
                load_imbalance_pct=load_imbalance_pct,
                executor_counts=executor_metrics.get('executor_task_counts', {})
            )
            
            bottlenecks.append({
                "bottleneck": "Executor Load Imbalance",
                "type": "EXECUTOR",
                "severity": severity,
                "confidence": confidence,
                "impact_score": impact_score,
                "metric_evidence": {
                    "executor_imbalance_ratio": imbalance_ratio,
                    "load_imbalance_percentage": load_imbalance_pct,
                    "min_tasks": executor_metrics.get('min_tasks_per_executor', 0),
                    "max_tasks": executor_metrics.get('max_tasks_per_executor', 0),
                    "mean_tasks": executor_metrics.get('mean_tasks_per_executor', 0),
                    "total_executors": executor_metrics.get('total_executors', 0)
                },
                "reasoning": reasoning,
                "impact": f"Task distribution variance is {imbalance_ratio*100:.1f}% across executors"
            })
        
        return bottlenecks
    
    def _detect_anomalies(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect anomalies and failures in the application."""
        stage_metrics = metrics.get('stage_metrics', {})
        
        anomalies = []
        
        failed_stages = stage_metrics.get('failed_stages', 0)
        retried_stages = stage_metrics.get('retried_stages', 0)
        failure_rate = stage_metrics.get('failure_rate', 0)
        
        if failed_stages > 0:
            anomalies.append({
                "type": "STAGE_FAILURES",
                "severity": "CRITICAL" if failure_rate > 0.2 else "HIGH",
                "count": failed_stages,
                "failure_rate": failure_rate,
                "description": f"{failed_stages} stage(s) failed ({failure_rate*100:.1f}% failure rate)"
            })
        
        if retried_stages > 0:
            anomalies.append({
                "type": "STAGE_RETRIES",
                "severity": "MEDIUM",
                "count": retried_stages,
                "description": f"{retried_stages} stage(s) were retried"
            })
        
        return anomalies
    
    def _generate_warnings(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate warnings for moderate issues."""
        warnings = []
        
        memory_metrics = metrics.get('memory_metrics', {})
        shuffle_metrics = metrics.get('shuffle_metrics', {})
        cpu_metrics = metrics.get('cpu_metrics', {})
        
        gc_ratio = memory_metrics.get('gc_overhead_ratio', 0)
        if 0.05 < gc_ratio <= 0.1:
            warnings.append({
                "warning": "Moderate GC Overhead",
                "gc_overhead_percentage": f"{gc_ratio*100:.2f}%"
            })
        
        shuffle_ratio = shuffle_metrics.get('shuffle_ratio', 0)
        if 0.2 < shuffle_ratio <= 0.3:
            warnings.append({
                "warning": "Moderate Shuffle Overhead",
                "shuffle_time_percentage": f"{shuffle_ratio*100:.2f}%"
            })
        
        cpu_eff = cpu_metrics.get('cpu_efficiency', 1)
        if 0.75 <= cpu_eff < 0.85:
            warnings.append({
                "warning": "Suboptimal CPU Utilization",
                "cpu_efficiency_percentage": f"{cpu_eff*100:.2f}%"
            })
        
        return warnings
    
    def _compute_confidence(self, impact_score: float, correlation_strength: float, 
                           has_supporting_evidence: bool) -> float:
        """Compute confidence score using calibrated formula."""
        base_confidence = 0.5
        
        # Sigmoid function for impact
        impact_contribution = 1 / (1 + math.exp(-5 * (impact_score - 0.5)))
        
        # Correlation contribution
        corr_contribution = abs(correlation_strength)
        
        # Evidence boost
        evidence_boost = 0.15 if has_supporting_evidence else 0.0
        
        confidence = (
            base_confidence +
            impact_contribution * 0.30 +
            corr_contribution * 0.15 +
            evidence_boost
        )
        
        return min(max(confidence, 0.0), 1.0)
    
    def _generate_gc_reasoning(self, gc_ratio: float, memory_spilled: int, 
                               correlation: float, pressure_level: str) -> str:
        """Generate reasoning text for GC pressure."""
        reasoning = f"Probable Cause: Memory pressure due to {pressure_level.lower()} garbage collection overhead. "
        reasoning += f"Supporting Evidence: GC consumes {gc_ratio*100:.1f}% of executor runtime"
        
        if memory_spilled > 0:
            reasoning += f", {self._format_bytes(memory_spilled)} spilled to disk"
            if correlation > 0.5:
                reasoning += f" (strong correlation: {correlation:.2f})"
        
        reasoning += f". Confidence based on GC impact severity and correlation with memory metrics."
        
        return reasoning
    
    def _generate_skew_reasoning(self, task_skew: float, stragglers_pct: float, 
                                 percentiles: Dict[str, float]) -> str:
        """Generate reasoning text for data skew."""
        reasoning = f"Probable Cause: Data skew causing uneven partition distribution. "
        reasoning += f"Supporting Evidence: Task duration variance is {task_skew*100:.1f}% of mean, "
        reasoning += f"{stragglers_pct:.1f}% of tasks are stragglers. "
        
        p99 = percentiles.get('p99', 0)
        p50 = percentiles.get('p50', 0)
        if p50 > 0:
            ratio = p99 / p50
            reasoning += f"P99 task is {ratio:.1f}x slower than median. "
        
        reasoning += "Indicates severe partition imbalance requiring repartitioning."
        
        return reasoning
    
    def _generate_cpu_reasoning(self, cpu_eff: float, shuffle_ratio: float, 
                                is_io_bottleneck: bool) -> str:
        """Generate reasoning text for CPU underutilization."""
        reasoning = f"Probable Cause: {'I/O bottleneck' if is_io_bottleneck else 'CPU underutilization'} "
        reasoning += f"with {(1-cpu_eff)*100:.1f}% CPU idle time. "
        reasoning += f"Supporting Evidence: CPU efficiency is {cpu_eff*100:.1f}%"
        
        if is_io_bottleneck:
            reasoning += f", shuffle operations consume {shuffle_ratio*100:.1f}% of time, "
            reasoning += "indicating network/disk I/O saturation."
        else:
            reasoning += ". Low CPU utilization without I/O pressure suggests serialization overhead or insufficient parallelism."
        
        return reasoning
    
    def _generate_spill_reasoning(self, memory_spilled: int, disk_spilled: int, 
                                  spill_ratio: float) -> str:
        """Generate reasoning text for memory spill."""
        reasoning = f"Probable Cause: Insufficient executor memory causing data spill. "
        reasoning += f"Supporting Evidence: {self._format_bytes(memory_spilled)} spilled to memory"
        
        if disk_spilled > 0:
            reasoning += f", {self._format_bytes(disk_spilled)} spilled to disk"
        
        reasoning += f" ({spill_ratio*100:.1f}% of input data). "
        reasoning += "Indicates memory pressure requiring increased executor memory allocation."
        
        return reasoning
    
    def _generate_shuffle_reasoning(self, shuffle_ratio: float, shuffle_intensity: float, 
                                    shuffle_efficiency: float) -> str:
        """Generate reasoning text for shuffle inefficiency."""
        reasoning = f"Probable Cause: Excessive shuffle operations consuming {shuffle_ratio*100:.1f}% of execution time. "
        reasoning += f"Supporting Evidence: Shuffle intensity is {shuffle_intensity:.2f}x the I/O volume, "
        reasoning += f"shuffle efficiency score is {shuffle_efficiency:.2f}. "
        reasoning += "Indicates need for broadcast joins or repartitioning strategy."
        
        return reasoning
    
    def _generate_imbalance_reasoning(self, imbalance_ratio: float, load_imbalance_pct: float, 
                                      executor_counts: Dict[str, int]) -> str:
        """Generate reasoning text for executor imbalance."""
        reasoning = f"Probable Cause: Uneven task distribution across executors. "
        reasoning += f"Supporting Evidence: Load variance is {imbalance_ratio:.2f}, "
        reasoning += f"imbalance is {load_imbalance_pct:.1f}%. "
        
        if executor_counts:
            min_tasks = min(executor_counts.values())
            max_tasks = max(executor_counts.values())
            reasoning += f"Max loaded executor has {max_tasks} tasks vs min {min_tasks}. "
        
        reasoning += "Suggests data skew or partitioning issues."
        
        return reasoning
    
    def _severity_order(self, severity: str) -> int:
        """Convert severity to numeric order for sorting."""
        order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        return order.get(severity, 4)
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes into human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"

# Backward compatibility wrapper
def analyze_events(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Backward compatible analyze_events function."""
    analyzer = BottleneckAnalyzer()
    return analyzer.analyze_events(events)
