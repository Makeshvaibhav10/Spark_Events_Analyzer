from typing import Dict, Any, List

def determine_root_cause(analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Determines the root cause of performance issues based on the analysis report.

    Args:
        analysis: The analysis report from the analyzer module.

    Returns:
        A list of root causes with confidence, recommendations, and supporting metrics.
    """
    root_causes = []
    bottlenecks = analysis.get('bottlenecks', [])
    performance_summary = analysis.get('performance_summary', {})
    
    if not bottlenecks:
        return [{
            "root_cause": "No dominant bottleneck detected",
            "confidence": 1.0,
            "evidence": {},
            "supporting_metrics": _get_healthy_metrics(performance_summary),
            "recommendation": "Application appears to be running efficiently. Continue monitoring for anomalies.",
            "action_items": []
        }]

    for bottleneck in bottlenecks:
        bottleneck_type = bottleneck.get('bottleneck', '')
        severity = bottleneck.get('severity', 'MEDIUM')
        evidence = bottleneck.get('metric_evidence', {})
        confidence = bottleneck.get('confidence', 0.5)
        
        if bottleneck_type == "GC Overhead":
            gc_ratio = evidence.get('gc_overhead_ratio', 0)
            memory_spilled = evidence.get('memoryBytesSpilled', 0)
            
            root_causes.append({
                "root_cause": "Memory Pressure / Excessive GC Overhead",
                "severity": severity,
                "confidence": confidence,
                "evidence": evidence,
                "supporting_metrics": {
                    "gc_overhead_percentage": f"{gc_ratio*100:.2f}%",
                    "memory_spilled_bytes": memory_spilled,
                    "memory_spilled_formatted": _format_bytes(memory_spilled),
                    "impact": bottleneck.get('impact', '')
                },
                "recommendation": _get_gc_recommendation(gc_ratio, memory_spilled),
                "action_items": [
                    f"Increase executor memory: spark.executor.memory (current impact: {gc_ratio*100:.1f}% GC time)",
                    "Tune GC settings: Consider G1GC with -XX:+UseG1GC",
                    "Review spark.memory.fraction (default: 0.6)",
                    "Check for memory leaks in UDFs or transformations",
                    "Consider increasing spark.memory.storageFraction if caching is heavy"
                ]
            })
            
        elif bottleneck_type == "Excessive Shuffle Overhead":
            shuffle_ratio = evidence.get('shuffle_ratio', 0)
            shuffle_read = evidence.get('shuffleReadBytes', 0)
            shuffle_write = evidence.get('shuffleWriteBytes', 0)
            
            root_causes.append({
                "root_cause": "Excessive Shuffle Operations",
                "severity": severity,
                "confidence": confidence,
                "evidence": evidence,
                "supporting_metrics": {
                    "shuffle_time_percentage": f"{shuffle_ratio*100:.2f}%",
                    "shuffle_read_bytes": shuffle_read,
                    "shuffle_read_formatted": _format_bytes(shuffle_read),
                    "shuffle_write_bytes": shuffle_write,
                    "shuffle_write_formatted": _format_bytes(shuffle_write),
                    "total_shuffle_formatted": _format_bytes(shuffle_read + shuffle_write),
                    "impact": bottleneck.get('impact', '')
                },
                "recommendation": _get_shuffle_recommendation(shuffle_ratio, shuffle_read, shuffle_write),
                "action_items": [
                    "Enable Adaptive Query Execution: spark.sql.adaptive.enabled=true",
                    "Optimize shuffle partitions: spark.sql.shuffle.partitions (default: 200)",
                    "Use broadcast joins for small tables: spark.sql.autoBroadcastJoinThreshold",
                    "Repartition data before shuffle-heavy operations",
                    "Consider using reduceByKey instead of groupByKey where applicable",
                    "Enable shuffle compression: spark.shuffle.compress=true"
                ]
            })
            
        elif bottleneck_type == "Data Skew Detected":
            skew_ratio = evidence.get('task_skew_ratio', 0)
            mean_duration = evidence.get('mean_task_duration', 0)
            max_duration = evidence.get('max_task_duration', 0)
            min_duration = evidence.get('min_task_duration', 0)
            total_tasks = evidence.get('total_tasks', 0)
            
            root_causes.append({
                "root_cause": "Data Skew / Uneven Partition Distribution",
                "severity": severity,
                "confidence": confidence,
                "evidence": evidence,
                "supporting_metrics": {
                    "task_skew_ratio": f"{skew_ratio:.2f}",
                    "mean_task_duration_ms": mean_duration,
                    "max_task_duration_ms": max_duration,
                    "min_task_duration_ms": min_duration,
                    "duration_range_ms": max_duration - min_duration,
                    "slowest_vs_fastest_ratio": f"{max_duration/min_duration:.2f}x" if min_duration > 0 else "N/A",
                    "total_tasks": total_tasks,
                    "impact": bottleneck.get('impact', '')
                },
                "recommendation": _get_skew_recommendation(skew_ratio, max_duration, min_duration),
                "action_items": [
                    "Enable Adaptive Query Execution for skew handling: spark.sql.adaptive.enabled=true",
                    "Enable skew join optimization: spark.sql.adaptive.skewJoin.enabled=true",
                    "Repartition by key with salt: df.repartition('key', 'salt')",
                    "Use approximate percentiles to identify skewed keys",
                    "Consider filtering out or handling outlier keys separately",
                    "Increase parallelism for skewed stages"
                ]
            })
            
        elif bottleneck_type == "Underutilized CPU":
            cpu_eff = evidence.get('cpu_efficiency', 0)
            
            root_causes.append({
                "root_cause": "CPU Underutilization",
                "severity": severity,
                "confidence": confidence,
                "evidence": evidence,
                "supporting_metrics": {
                    "cpu_efficiency_percentage": f"{cpu_eff*100:.2f}%",
                    "cpu_waste_percentage": f"{(1-cpu_eff)*100:.2f}%",
                    "impact": bottleneck.get('impact', '')
                },
                "recommendation": _get_cpu_recommendation(cpu_eff),
                "action_items": [
                    f"Increase cores per executor: spark.executor.cores (current utilization: {cpu_eff*100:.1f}%)",
                    "Check for I/O bottlenecks causing CPU idle time",
                    "Verify data locality: review spark.locality.wait",
                    "Increase parallelism: spark.default.parallelism",
                    "Review serialization overhead: use Kryo serializer",
                    "Check network bandwidth if shuffle-heavy"
                ]
            })
            
        elif bottleneck_type == "Executor Load Imbalance":
            imbalance = evidence.get('executor_imbalance_ratio', 0)
            task_counts = evidence.get('executor_task_counts', {})
            total_executors = evidence.get('total_executors', 0)
            
            if task_counts:
                min_tasks = min(task_counts.values())
                max_tasks = max(task_counts.values())
                avg_tasks = sum(task_counts.values()) / len(task_counts)
            else:
                min_tasks = max_tasks = avg_tasks = 0
            
            root_causes.append({
                "root_cause": "Executor Load Imbalance",
                "severity": severity,
                "confidence": confidence,
                "evidence": evidence,
                "supporting_metrics": {
                    "imbalance_ratio": f"{imbalance:.2f}",
                    "total_executors": total_executors,
                    "min_tasks_per_executor": min_tasks,
                    "max_tasks_per_executor": max_tasks,
                    "avg_tasks_per_executor": f"{avg_tasks:.1f}",
                    "task_distribution_range": max_tasks - min_tasks,
                    "impact": bottleneck.get('impact', '')
                },
                "recommendation": _get_imbalance_recommendation(imbalance, min_tasks, max_tasks),
                "action_items": [
                    "Ensure even data distribution across partitions",
                    "Check for skewed joins or aggregations",
                    "Review partitioning strategy: consider repartition() or coalesce()",
                    "Enable dynamic allocation: spark.dynamicAllocation.enabled=true",
                    "Verify no stragglers due to data locality issues",
                    "Check executor loss patterns in Spark UI"
                ]
            })
            
        elif bottleneck_type == "Memory Spill to Disk":
            memory_spilled = evidence.get('memoryBytesSpilled', 0)
            spill_ratio = evidence.get('spill_ratio', 0)
            
            root_causes.append({
                "root_cause": "Insufficient Memory / Memory Spill",
                "severity": severity,
                "confidence": confidence,
                "evidence": evidence,
                "supporting_metrics": {
                    "memory_spilled_bytes": memory_spilled,
                    "memory_spilled_formatted": _format_bytes(memory_spilled),
                    "spill_ratio_percentage": f"{spill_ratio*100:.2f}%",
                    "impact": bottleneck.get('impact', '')
                },
                "recommendation": _get_spill_recommendation(memory_spilled, spill_ratio),
                "action_items": [
                    "Increase executor memory: spark.executor.memory",
                    "Adjust memory overhead: spark.executor.memoryOverhead",
                    "Tune off-heap memory: spark.memory.offHeap.enabled=true",
                    "Reduce shuffle partition size to fit in memory",
                    "Consider external shuffle service",
                    "Review sorting and aggregation operations"
                ]
            })

    # Add overall system health metrics
    if root_causes:
        root_causes.append(_get_system_overview(performance_summary, len(bottlenecks)))

    return root_causes

def _get_gc_recommendation(gc_ratio: float, memory_spilled: int) -> str:
    """Generate specific GC overhead recommendation."""
    if gc_ratio > 0.4:
        return (f"CRITICAL: GC overhead is {gc_ratio*100:.1f}%, severely impacting performance. "
                f"Immediately increase executor memory by at least 50% and enable G1GC.")
    elif gc_ratio > 0.2:
        return (f"HIGH: GC overhead is {gc_ratio*100:.1f}%. Increase executor memory by 20-30% "
                f"and tune GC settings. Consider reviewing caching strategy.")
    else:
        return f"Moderate GC overhead detected ({gc_ratio*100:.1f}%). Monitor and increase memory if it worsens."

def _get_shuffle_recommendation(shuffle_ratio: float, read_bytes: int, write_bytes: int) -> str:
    """Generate specific shuffle recommendation."""
    total_shuffle = read_bytes + write_bytes
    if shuffle_ratio > 0.7:
        return (f"CRITICAL: Shuffle operations consume {shuffle_ratio*100:.1f}% of execution time "
                f"({_format_bytes(total_shuffle)} shuffled). Enable AQE and optimize join strategy immediately.")
    elif shuffle_ratio > 0.5:
        return (f"HIGH: Excessive shuffle overhead ({shuffle_ratio*100:.1f}% of time, "
                f"{_format_bytes(total_shuffle)} data). Enable broadcast joins and AQE.")
    else:
        return f"Moderate shuffle detected. Consider optimizing if shuffle grows."

def _get_skew_recommendation(skew_ratio: float, max_dur: float, min_dur: float) -> str:
    """Generate specific data skew recommendation."""
    ratio = max_dur / min_dur if min_dur > 0 else float('inf')
    if skew_ratio > 1.0 or ratio > 10:
        return (f"CRITICAL: Severe data skew detected. Slowest task is {ratio:.1f}x slower than fastest. "
                f"Enable skew join optimization and use salting technique.")
    elif skew_ratio > 0.5:
        return (f"HIGH: Significant data skew (variance {skew_ratio:.2f}). "
                f"Enable AQE skew handling and repartition data.")
    else:
        return "Moderate skew detected. Monitor partition sizes."

def _get_cpu_recommendation(cpu_eff: float) -> str:
    """Generate specific CPU utilization recommendation."""
    waste = (1 - cpu_eff) * 100
    if cpu_eff < 0.4:
        return (f"CRITICAL: CPUs are {waste:.1f}% idle. Likely I/O bound or severe serialization overhead. "
                f"Investigate disk/network bottlenecks immediately.")
    elif cpu_eff < 0.6:
        return (f"HIGH: CPU utilization is only {cpu_eff*100:.1f}%. "
                f"Increase parallelism and check for I/O bottlenecks.")
    else:
        return f"Suboptimal CPU usage ({cpu_eff*100:.1f}%). Consider increasing cores per executor."

def _get_imbalance_recommendation(imbalance: float, min_tasks: int, max_tasks: int) -> str:
    """Generate specific executor imbalance recommendation."""
    if imbalance > 0.6:
        return (f"CRITICAL: Severe load imbalance. Max executor has {max_tasks} tasks vs min {min_tasks}. "
                f"Repartition data immediately and check for skewed keys.")
    elif imbalance > 0.4:
        return (f"HIGH: Significant load imbalance (ratio {imbalance:.2f}). "
                f"Review partitioning strategy and enable dynamic allocation.")
    else:
        return "Moderate imbalance detected. Monitor executor utilization."

def _get_spill_recommendation(memory_spilled: int, spill_ratio: float) -> str:
    """Generate specific memory spill recommendation."""
    if spill_ratio > 0.3:
        return (f"CRITICAL: {_format_bytes(memory_spilled)} spilled to disk ({spill_ratio*100:.1f}% of data). "
                f"Increase executor memory by at least 50% immediately.")
    elif spill_ratio > 0.1:
        return (f"HIGH: {_format_bytes(memory_spilled)} spilled. "
                f"Increase executor memory by 20-30%.")
    else:
        return f"Memory spill detected ({_format_bytes(memory_spilled)}). Monitor and adjust if needed."

def _get_healthy_metrics(performance_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key metrics showing healthy operation."""
    task_metrics = performance_summary.get('task_metrics', {})
    cpu_metrics = performance_summary.get('cpu_metrics', {})
    memory_metrics = performance_summary.get('memory_metrics', {})
    
    return {
        "cpu_efficiency": f"{cpu_metrics.get('cpu_efficiency', 0)*100:.2f}%",
        "gc_overhead": f"{memory_metrics.get('gc_overhead_ratio', 0)*100:.2f}%",
        "task_count": task_metrics.get('total_tasks', 0),
        "avg_task_duration_ms": task_metrics.get('mean_task_duration', 0)
    }

def _get_system_overview(performance_summary: Dict[str, Any], bottleneck_count: int) -> Dict[str, Any]:
    """Provide overall system health overview."""
    task_metrics = performance_summary.get('task_metrics', {})
    io_metrics = performance_summary.get('io_metrics', {})
    shuffle_metrics = performance_summary.get('shuffle_metrics', {})
    
    return {
        "root_cause": "System Overview",
        "severity": "INFO",
        "confidence": 1.0,
        "evidence": {},
        "supporting_metrics": {
            "total_tasks": task_metrics.get('total_tasks', 0),
            "total_input_data": _format_bytes(io_metrics.get('inputBytes', 0)),
            "total_output_data": _format_bytes(io_metrics.get('outputBytes', 0)),
            "total_shuffle_data": _format_bytes(
                shuffle_metrics.get('shuffleReadBytes', 0) + 
                shuffle_metrics.get('shuffleWriteBytes', 0)
            ),
            "identified_bottlenecks": bottleneck_count
        },
        "recommendation": f"Found {bottleneck_count} performance bottleneck(s). Review and address in priority order above.",
        "action_items": []
    }

def _format_bytes(bytes_val: int) -> str:
    """Format bytes into human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"