import json
import numpy as np
from typing import Dict, Any, List
from datetime import datetime
import os

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def generate_report(analysis: Dict[str, Any], root_causes: List[Dict[str, Any]], 
                   output_path: str, failures: Dict[str, Any] = None):
    """
    Generates comprehensive JSON and Markdown reports.

    Args:
        analysis: The analysis report from the analyzer module
        root_causes: The root causes from the root cause engine
        output_path: The path to save the report
        failures: Optional failure detection results
    """
    performance_summary = analysis.get('performance_summary', {})
    
    # Extract metrics
    task_metrics = performance_summary.get('task_metrics', {})
    executor_metrics = performance_summary.get('executor_metrics', {})
    io_metrics = performance_summary.get('io_metrics', {})
    memory_metrics = performance_summary.get('memory_metrics', {})
    shuffle_metrics = performance_summary.get('shuffle_metrics', {})
    cpu_metrics = performance_summary.get('cpu_metrics', {})
    correlations = performance_summary.get('correlations', {})
    global_metrics = performance_summary.get('global_metrics', {})
    
    # Build comprehensive report
    report = {
        "report_metadata": {
            "generated_at": datetime.now().isoformat(),
            "report_version": "2.0",
            "analysis_engine": "Spark Event Analyzer - Phase 2",
            "analyzer_capabilities": [
                "Bottleneck Detection",
                "Root Cause Analysis",
                "Failure Detection",
                "Correlation Analysis",
                "Dynamic Thresholding"
            ]
        },
        "executive_summary": _generate_executive_summary(
            analysis, global_metrics, failures
        ),
        "performance_metrics": {
            "global": {
                "total_jobs": global_metrics.get('total_jobs', 0),
                "total_stages": global_metrics.get('total_stages', 0),
                "total_tasks": global_metrics.get('total_tasks', 0),
                "application_duration": _format_duration(global_metrics.get('application_duration_ms', 0)),
                "application_duration_ms": global_metrics.get('application_duration_ms', 0)
            },
            "task_metrics": {
                "total_tasks": task_metrics.get('total_tasks', 0),
                "mean_duration_ms": round(task_metrics.get('mean_task_duration', 0), 2),
                "median_duration_ms": round(task_metrics.get('median_task_duration', 0), 2),
                "std_duration_ms": round(task_metrics.get('std_task_duration', 0), 2),
                "skew_ratio": round(task_metrics.get('task_skew_ratio', 0), 3),
                "coefficient_of_variation": round(task_metrics.get('coefficient_of_variation', 0), 3),
                "percentiles": task_metrics.get('percentiles', {}),
                "stragglers": {
                    "count": task_metrics.get('stragglers_count', 0),
                    "percentage": round(task_metrics.get('stragglers_percentage', 0), 2)
                }
            },
            "cpu_metrics": {
                "efficiency": round(cpu_metrics.get('cpu_efficiency', 0), 3),
                "efficiency_percentage": f"{cpu_metrics.get('cpu_efficiency', 0)*100:.2f}%",
                "wait_percentage": f"{cpu_metrics.get('cpu_wait_percentage', 0):.2f}%",
                "deserialize_overhead_percentage": f"{cpu_metrics.get('deserialize_overhead_percentage', 0):.2f}%"
            },
            "memory_metrics": {
                "gc_overhead_ratio": round(memory_metrics.get('gc_overhead_ratio', 0), 3),
                "gc_overhead_percentage": f"{memory_metrics.get('gc_overhead_percentage', 0):.2f}%",
                "gc_pressure_level": memory_metrics.get('gc_pressure_level', 'UNKNOWN'),
                "memory_bytes_spilled": memory_metrics.get('memoryBytesSpilled', 0),
                "memory_spilled_formatted": _format_bytes(memory_metrics.get('memoryBytesSpilled', 0)),
                "disk_bytes_spilled": memory_metrics.get('diskBytesSpilled', 0),
                "total_spilled_formatted": _format_bytes(memory_metrics.get('totalSpilled', 0)),
                "tasks_with_spill": memory_metrics.get('tasks_with_spill', 0)
            },
            "io_metrics": {
                "input_bytes": io_metrics.get('inputBytes', 0),
                "input_formatted": _format_bytes(io_metrics.get('inputBytes', 0)),
                "output_bytes": io_metrics.get('outputBytes', 0),
                "output_formatted": _format_bytes(io_metrics.get('outputBytes', 0)),
                "input_records": io_metrics.get('inputRecords', 0)
            },
            "shuffle_metrics": {
                "shuffle_read_bytes": shuffle_metrics.get('shuffleReadBytes', 0),
                "shuffle_read_formatted": _format_bytes(shuffle_metrics.get('shuffleReadBytes', 0)),
                "shuffle_write_bytes": shuffle_metrics.get('shuffleWriteBytes', 0),
                "shuffle_write_formatted": _format_bytes(shuffle_metrics.get('shuffleWriteBytes', 0)),
                "total_shuffle_formatted": _format_bytes(shuffle_metrics.get('totalShuffleBytes', 0)),
                "shuffle_time_ms": shuffle_metrics.get('shuffleTime', 0),
                "shuffle_ratio": round(shuffle_metrics.get('shuffle_ratio', 0), 3),
                "shuffle_ratio_percentage": f"{shuffle_metrics.get('shuffle_ratio', 0)*100:.2f}%",
                "shuffle_efficiency": round(shuffle_metrics.get('shuffle_efficiency', 0), 3)
            },
            "executor_metrics": {
                "total_executors": executor_metrics.get('total_executors', 0),
                "imbalance_ratio": round(executor_metrics.get('executor_imbalance_ratio', 0), 3),
                "load_imbalance_percentage": round(executor_metrics.get('load_imbalance_percentage', 0), 2),
                "min_tasks_per_executor": executor_metrics.get('min_tasks_per_executor', 0),
                "max_tasks_per_executor": executor_metrics.get('max_tasks_per_executor', 0),
                "mean_tasks_per_executor": round(executor_metrics.get('mean_tasks_per_executor', 0), 2)
            },
            "correlations": correlations
        },
        "bottlenecks": analysis.get('bottlenecks', []),
        "anomalies": analysis.get('anomalies', []),
        "warnings": analysis.get('warnings', []),
        "failures": failures if failures else {},
        "root_cause_analysis": root_causes,
        "recommendations": _extract_recommendations(root_causes, analysis),
        "action_items": _extract_action_items(root_causes),
        "analysis_metadata": analysis.get('analysis_metadata', {})
    }

    # Generate JSON report
    json_path = f"{output_path}.json"
    with open(json_path, 'w') as f:
        json.dump(report, f, indent=2, cls=NpEncoder)
    
    # Generate Markdown report
    md_path = f"{output_path}.md"
    _generate_markdown_report(report, md_path)
    
    # Generate text summary
    txt_path = f"{output_path}_summary.txt"
    _generate_text_summary(report, txt_path)
    
    print(f"\n{'='*80}")
    print("REPORT GENERATION COMPLETE")
    print(f"{ '='*80}")
    print(f"✓ JSON Report:     {json_path}")
    print(f"✓ Markdown Report: {md_path}")
    print(f"✓ Text Summary:    {txt_path}")
    print(f"{ '='*80}\n")

def _generate_executive_summary(analysis: Dict[str, Any], global_metrics: Dict[str, Any],
                                failures: Dict[str, Any] = None) -> Dict[str, Any]:
    """Generate executive summary."""
    bottlenecks = analysis.get('bottlenecks', [])
    anomalies = analysis.get('anomalies', [])
    
    critical_issues = sum(1 for b in bottlenecks if b.get('severity') == 'CRITICAL')
    high_issues = sum(1 for b in bottlenecks if b.get('severity') == 'HIGH')
    
    health_score = _compute_health_score(analysis)
    health_status = "CRITICAL" if health_score < 40 else "POOR" if health_score < 60 else "FAIR" if health_score < 80 else "GOOD"
    
    summary = {
        "health_score": health_score,
        "health_status": health_status,
        "total_bottlenecks": len(bottlenecks),
        "critical_bottlenecks": critical_issues,
        "high_priority_bottlenecks": high_issues,
        "total_anomalies": len(anomalies),
        "requires_immediate_action": critical_issues > 0,
        "primary_concerns": _identify_primary_concerns(bottlenecks)
    }
    
    if failures:
        failure_summary = failures.get('summary', {})
        summary['total_failures'] = (
            failure_summary.get('total_stage_failures', 0) +
            failure_summary.get('total_task_failures', 0)
        )
        summary['has_oom_events'] = failure_summary.get('total_oom_events', 0) > 0
    
    return summary

def _compute_health_score(analysis: Dict[str, Any]) -> int:
    """Compute overall application health score (0-100)."""
    score = 100
    
    bottlenecks = analysis.get('bottlenecks', [])
    
    for bottleneck in bottlenecks:
        severity = bottleneck.get('severity', 'LOW')
        confidence = bottleneck.get('confidence', 1.0)
        
        # Deduct points based on severity and confidence
        deduction = 0
        if severity == 'CRITICAL':
            deduction = 25 * confidence
        elif severity == 'HIGH':
            deduction = 15 * confidence
        elif severity == 'MEDIUM':
            deduction = 8 * confidence
        else:
            deduction = 3 * confidence
        
        score -= deduction
    
    return max(int(score), 0)

def _identify_primary_concerns(bottlenecks: List[Dict[str, Any]]) -> List[str]:
    """Identify primary concerns from bottlenecks."""
    concerns = []
    
    # Get top 3 most severe bottlenecks
    sorted_bottlenecks = sorted(
        bottlenecks,
        key=lambda x: (
            0 if x.get('severity') == 'CRITICAL' else
            1 if x.get('severity') == 'HIGH' else 2,
            -x.get('confidence', 0)
        )
    )[:3]
    
    for b in sorted_bottlenecks:
                    concerns.append(f"{b.get('type', 'Unknown')} [{b.get('severity', 'UNKNOWN')}]")    
    return concerns

def _generate_markdown_report(report: Dict[str, Any], output_path: str):
    """Generate comprehensive Markdown report."""
    with open(output_path, 'w') as f:
        # Header
        f.write("# Spark Event Log Analysis Report\n\n")
        f.write(f"**Generated:** {report['report_metadata']['generated_at']}  \n")
        f.write(f"**Analyzer Version:** {report['report_metadata']['report_version']}  \n\n")
        
        # Executive Summary
        exec_summary = report['executive_summary']
        f.write("## 📊 Executive Summary\n\n")
        f.write(f"### Application Health: **{exec_summary['health_status']}** ({exec_summary['health_score']}/100)\n\n")
        
        if exec_summary['requires_immediate_action']:
            f.write("⚠️ **IMMEDIATE ACTION REQUIRED** - Critical issues detected\n\n")
        
        f.write(f"- **Total Bottlenecks:** {exec_summary['total_bottlenecks']}\n")
        f.write(f"- **Critical Issues:** {exec_summary['critical_bottlenecks']}\n")
        f.write(f"- **High Priority Issues:** {exec_summary['high_priority_bottlenecks']}\n")
        f.write(f"- **Anomalies Detected:** {exec_summary['total_anomalies']}\n\n")
        
        if exec_summary.get('primary_concerns'):
            f.write("**Primary Concerns:**\n")
            for concern in exec_summary['primary_concerns']:
                f.write(f"- {concern}\n")
            f.write("\n")
        
        # Performance Metrics
        f.write("## 📈 Performance Metrics\n\n")
        
        # Global metrics
        global_m = report['performance_metrics']['global']
        f.write("### Application Overview\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| Total Jobs | {global_m['total_jobs']} |\n")
        f.write(f"| Total Stages | {global_m['total_stages']} |\n")
        f.write(f"| Total Tasks | {global_m['total_tasks']} |\n")
        f.write(f"| Total Duration | {global_m['application_duration']} |\n\n")
        
        # Key Performance Indicators
        f.write("### Key Performance Indicators\n\n")
        cpu_m = report['performance_metrics']['cpu_metrics']
        mem_m = report['performance_metrics']['memory_metrics']
        shuffle_m = report['performance_metrics']['shuffle_metrics']
        
        f.write(f"| KPI | Value | Status |\n")
        f.write(f"|-----|-------|--------|\n")
        
        # CPU efficiency
        cpu_eff = float(cpu_m['efficiency'])
        cpu_status = "✅ Good" if cpu_eff > 0.8 else "⚠️ Fair" if cpu_eff > 0.6 else "🔴 Poor"
        f.write(f"| CPU Efficiency | {cpu_m['efficiency_percentage']} | {cpu_status} |\n")
        
        # GC overhead
        gc_level = mem_m['gc_pressure_level']
        gc_status = "✅ Good" if gc_level == "LOW" else "⚠️ Fair" if gc_level == "MEDIUM" else "🔴 Poor"
        f.write(f"| GC Overhead | {mem_m['gc_overhead_percentage']} | {gc_status} |\n")
        
        # Shuffle efficiency
        shuffle_eff = float(shuffle_m['shuffle_efficiency'])
        shuffle_status = "✅ Good" if shuffle_eff > 0.7 else "⚠️ Fair" if shuffle_eff > 0.5 else "🔴 Poor"
        f.write(f"| Shuffle Efficiency | {shuffle_eff:.3f} | {shuffle_status} |\n")
        
        # Memory spill
        mem_spilled = mem_m['memory_bytes_spilled']
        spill_status = "✅ None" if mem_spilled == 0 else "⚠️ Moderate" if mem_spilled < 1_000_000_000 else "🔴 High"
        f.write(f"| Memory Spilled | {mem_m['memory_spilled_formatted']} | {spill_status} |\n\n")
        
        # Bottlenecks
        bottlenecks = report['bottlenecks']
        if bottlenecks:
            f.write(f"## 🔍 Identified Bottlenecks ({len(bottlenecks)})\n\n")
            
            for i, b in enumerate(bottlenecks, 1):
                severity_emoji = "🔴" if b['severity'] == "CRITICAL" else "🟠" if b['severity'] == "HIGH" else "🟡"
                f.write(f"### {i}. {severity_emoji} {b['type']}\n\n")
                f.write(f"**Severity:** {b['severity']}  \n")
                f.write(f"**Confidence:** {b.get('confidence', 1.0)*100:.0f}%  \n")
                f.write(f"**Type:** {b.get('type', 'UNKNOWN')}  \n")
                f.write(f"**Impact:** {b.get('impact', 'N/A')}  \n\n")
                
                if 'reasoning' in b:
                    f.write(f"**Analysis:**  \n{b['reasoning']}\n\n")
                
                f.write("**Evidence:**\n")
                for key, value in b.get('metric_evidence', {}).items():
                    f.write(f"- `{key}`: {value}\n")
                f.write("\n")
        
        # Root Cause Analysis
        root_causes = [rc for rc in report['root_cause_analysis'] if rc.get('severity') != 'INFO']
        if root_causes:
            f.write(f"## 🎯 Root Cause Analysis ({len(root_causes)})\n\n")
            
            for i, rc in enumerate(root_causes, 1):
                f.write(f"### {i}. {rc['root_cause']}\n\n")
                f.write(f"**Severity:** {rc.get('severity', 'UNKNOWN')}  \n")
                f.write(f"**Confidence:** {rc['confidence']*100:.0f}%  \n\n")
                
                f.write(f"**Recommendation:**  \n{rc['recommendation']}\n\n")
                
                if 'supporting_metrics' in rc:
                    f.write("**Supporting Metrics:**\n")
                    for key, value in rc['supporting_metrics'].items():
                        f.write(f"- `{key}`: {value}\n")
                    f.write("\n")
        
        # Action Items
        action_items = report['action_items']
        f.write("## ✅ Recommended Actions\n\n")
        
        for severity in ['CRITICAL', 'HIGH', 'MEDIUM']:
            items = action_items.get(severity, [])
            if items:
                emoji = "🔴" if severity == "CRITICAL" else "🟠" if severity == "HIGH" else "🟡"
                f.write(f"### {emoji} {severity} Priority\n\n")
                for item in items:
                    f.write(f"- **{item['root_cause']}:** {item['action']}\n")
                f.write("\n")
        
        # Failures
        failures = report.get('failures', {})
        if failures and failures.get('summary', {}).get('has_critical_failures'):
            f.write("## ⚠️ Failure Analysis\n\n")
            
            summary = failures.get('summary', {})
            f.write(f"- **Stage Failures:** {summary.get('total_stage_failures', 0)}\n")
            f.write(f"- **Task Failures:** {summary.get('total_task_failures', 0)}\n")
            f.write(f"- **Executor Failures:** {summary.get('total_executor_failures', 0)}\n")
            f.write(f"- **OOM Events:** {summary.get('total_oom_events', 0)}\n")
            f.write(f"- **Fetch Failures:** {summary.get('total_fetch_failures', 0)}\n\n")
        
        # Footer
        f.write("---\n\n")
        f.write("*Report generated by Spark Event Analyzer - Phase 2*\n")

def _generate_text_summary(report: Dict[str, Any], output_path: str):
    """Generate human-readable text summary."""
    with open(output_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("SPARK EVENT ANALYZER - COMPREHENSIVE PERFORMANCE REPORT\n")
        f.write("=" * 80 + "\n\n")
        
        # Executive Summary
        exec_summary = report['executive_summary']
        f.write("EXECUTIVE SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"Health Status: {exec_summary['health_status']} ({exec_summary['health_score']}/100)\n")
        f.write(f"Total Bottlenecks: {exec_summary['total_bottlenecks']}\n")
        f.write(f"Critical Issues: {exec_summary['critical_bottlenecks']}\n")
        f.write(f"High Priority Issues: {exec_summary['high_priority_bottlenecks']}\n\n")
        
        if exec_summary['requires_immediate_action']:
            f.write("*** IMMEDIATE ACTION REQUIRED ***\n\n")
        
        # Performance Summary
        global_m = report['performance_metrics']['global']
        f.write("APPLICATION OVERVIEW\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Jobs: {global_m['total_jobs']}\n")
        f.write(f"Total Stages: {global_m['total_stages']}\n")
        f.write(f"Total Tasks: {global_m['total_tasks']}\n")
        f.write(f"Total Duration: {global_m['application_duration']}\n\n")
        
        # Key metrics
        cpu_m = report['performance_metrics']['cpu_metrics']
        mem_m = report['performance_metrics']['memory_metrics']
        shuffle_m = report['performance_metrics']['shuffle_metrics']
        
        f.write("KEY PERFORMANCE INDICATORS\n")
        f.write("-" * 80 + "\n")
        f.write(f"CPU Efficiency: {cpu_m['efficiency_percentage']}\n")
        f.write(f"GC Overhead: {mem_m['gc_overhead_percentage']} ({mem_m['gc_pressure_level']})\n")
        f.write(f"Shuffle Efficiency: {shuffle_m['shuffle_efficiency']}\n")
        f.write(f"Memory Spilled: {mem_m['memory_spilled_formatted']}\n\n")
        
        # Bottlenecks
        bottlenecks = report['bottlenecks']
        if bottlenecks:
            f.write(f"IDENTIFIED BOTTLENECKS ({len(bottlenecks)})\n")
            f.write("-" * 80 + "\n")
            for i, b in enumerate(bottlenecks, 1):
                f.write(f"{i}. {b['type']} [{b['severity']}] - Confidence: {b.get('confidence', 1.0)*100:.0f}%\n")
                f.write(f"   Impact: {b.get('impact', 'N/A')}\n")
                if 'reasoning' in b:
                    reasoning_short = b['reasoning'][:200] if len(b['reasoning']) > 200 else b['reasoning']
                    f.write(f"   {reasoning_short}...\n")
                f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write(f"Report generated: {report['report_metadata']['generated_at']}\n")
        f.write("=" * 80 + "\n")

def _extract_recommendations(root_causes: List[Dict[str, Any]], 
                             analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract recommendations with priorities."""
    recommendations = []
    
    for rc in root_causes:
        if rc.get('severity') != 'INFO' and rc.get('recommendation'):
            recommendations.append({
                "root_cause": rc.get('root_cause'),
                "severity": rc.get('severity'),
                "confidence": rc.get('confidence'),
                "recommendation": rc.get('recommendation')
            })
    
    # Sort by severity and confidence
    recommendations.sort(key=lambda x: (
        0 if x['severity'] == 'CRITICAL' else 1 if x['severity'] == 'HIGH' else 2,
        -x['confidence']
    ))
    
    return recommendations

def _extract_action_items(root_causes: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Extract and organize action items by severity."""
    action_items_by_severity = {
        "CRITICAL": [],
        "HIGH": [],
        "MEDIUM": [],
        "LOW": []
    }
    
    for rc in root_causes:
        severity = rc.get('severity', 'LOW')
        items = rc.get('action_items', [])
        if items and severity in action_items_by_severity:
            action_items_by_severity[severity].extend([
                {"root_cause": rc.get('root_cause', 'Unknown'), "action": item}
                for item in items
            ])
    
    return action_items_by_severity

def _format_duration(ms: float) -> str:
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
    return f"{int(hours)}h {int(minutes % 60)}m {int(seconds % 60)}s"

def _format_bytes(bytes_val: int) -> str:
    """Format bytes into human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"
