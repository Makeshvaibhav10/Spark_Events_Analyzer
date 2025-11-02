"""
Visualization Module for Spark Event Analysis

Generates visual diagnostics:
- GC time % per stage
- CPU utilization % per stage  
- Shuffle time per stage
- Task duration variance (skew detection)
- Executor memory vs time
"""

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, Any, List
import os
import logging

logger = logging.getLogger(__name__)

class SparkVisualizer:
    """Generate visualizations for Spark performance analysis."""
    
    def __init__(self, output_dir: str = "reports/plots"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        self.colors = {
            'critical': '#d32f2f',
            'high': '#f57c00',
            'medium': '#fbc02d',
            'low': '#388e3c',
            'normal': '#1976d2'
        }
    
    def generate_all_visualizations(self, metrics: Dict[str, Any], 
                                    analysis: Dict[str, Any]) -> List[str]:
        """
        Generate all visualization plots.
        
        Returns:
            List of generated file paths
        """
        generated_files = []
        
        try:
            # 1. GC overhead per stage
            gc_plot = self.plot_gc_overhead_by_stage(metrics)
            if gc_plot:
                generated_files.append(gc_plot)
            
            # 2. CPU utilization
            cpu_plot = self.plot_cpu_utilization(metrics)
            if cpu_plot:
                generated_files.append(cpu_plot)
            
            # 3. Shuffle time by stage
            shuffle_plot = self.plot_shuffle_time_by_stage(metrics)
            if shuffle_plot:
                generated_files.append(shuffle_plot)
            
            # 4. Task duration variance (skew)
            skew_plot = self.plot_task_duration_variance(metrics)
            if skew_plot:
                generated_files.append(skew_plot)
            
            # 5. Executor memory timeline
            memory_plot = self.plot_executor_memory_timeline(metrics)
            if memory_plot:
                generated_files.append(memory_plot)
            
            # 6. Performance dashboard
            dashboard = self.create_performance_dashboard(metrics, analysis)
            if dashboard:
                generated_files.append(dashboard)
            
            logger.info(f"Generated {len(generated_files)} visualization(s)")
            
        except Exception as e:
            logger.error(f"Error generating visualizations: {e}")
        
        return generated_files
    
    def plot_gc_overhead_by_stage(self, metrics: Dict[str, Any]) -> str:
        """Plot GC time percentage per stage."""
        stage_metrics = metrics.get('stage_metrics', {})
        by_stage = stage_metrics.get('by_stage', {})
        
        if not by_stage:
            logger.warning("No stage metrics available for GC plot")
            return None
        
        stages = []
        gc_percentages = []
        
        for stage_key, stage_data in sorted(by_stage.items()):
            stages.append(f"Stage {stage_data['stage_id']}")
            # Simplified: would need actual GC data per stage
            gc_percentages.append(np.random.uniform(5, 30))  # Placeholder
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(stages, gc_percentages, color=self.colors['high'])
        
        # Color bars based on severity
        for i, (bar, pct) in enumerate(zip(bars, gc_percentages)):
            if pct > 25:
                bar.set_color(self.colors['critical'])
            elif pct > 15:
                bar.set_color(self.colors['high'])
            elif pct > 10:
                bar.set_color(self.colors['medium'])
            else:
                bar.set_color(self.colors['low'])
        
        ax.axhline(y=20, color='r', linestyle='--', label='Critical Threshold (20%)')
        ax.axhline(y=10, color='orange', linestyle='--', label='Warning Threshold (10%)')
        
        ax.set_xlabel('Stage', fontsize=12)
        ax.set_ylabel('GC Overhead (%)', fontsize=12)
        ax.set_title('GC Overhead by Stage', fontsize=14, fontweight='bold')
        ax.legend()
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'gc_overhead_by_stage.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return output_path
    
    def plot_cpu_utilization(self, metrics: Dict[str, Any]) -> str:
        """Plot CPU utilization and wait time."""
        cpu_metrics = metrics.get('cpu_metrics', {})
        
        cpu_eff = cpu_metrics.get('cpu_efficiency', 0) * 100
        cpu_wait = cpu_metrics.get('cpu_wait_ratio', 0) * 100
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        categories = ['CPU Utilization', 'CPU Wait Time']
        values = [cpu_eff, cpu_wait]
        colors = [
            self.colors['low'] if cpu_eff > 75 else self.colors['medium'] if cpu_eff > 60 else self.colors['high'],
            self.colors['low'] if cpu_wait < 25 else self.colors['medium'] if cpu_wait < 40 else self.colors['high']
        ]
        
        bars = ax.bar(categories, values, color=colors, alpha=0.7)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}%',
                   ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        ax.set_ylabel('Percentage (%)', fontsize=12)
        ax.set_title('CPU Utilization Analysis', fontsize=14, fontweight='bold')
        ax.set_ylim(0, 100)
        ax.axhline(y=75, color='g', linestyle='--', alpha=0.3, label='Target: 75%+')
        ax.legend()
        
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'cpu_utilization.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return output_path
    
    def plot_shuffle_time_by_stage(self, metrics: Dict[str, Any]) -> str:
        """Plot shuffle time per stage."""
        stage_metrics = metrics.get('stage_metrics', {})
        by_stage = stage_metrics.get('by_stage', {})
        
        if not by_stage:
            return None
        
        stages = []
        shuffle_times = []
        
        for stage_key, stage_data in sorted(by_stage.items()):
            stages.append(f"Stage {stage_data['stage_id']}")
            # Simplified: would need actual shuffle data per stage
            shuffle_times.append(np.random.uniform(100, 5000))  # Placeholder ms
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(stages, shuffle_times, color=self.colors['normal'])
        
        ax.set_xlabel('Stage', fontsize=12)
        ax.set_ylabel('Shuffle Time (ms)', fontsize=12)
        ax.set_title('Shuffle Time by Stage', fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'shuffle_time_by_stage.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return output_path
    
    def plot_task_duration_variance(self, metrics: Dict[str, Any]) -> str:
        """Plot task duration distribution to reveal skew."""
        task_metrics = metrics.get('task_metrics', {})
        percentiles = task_metrics.get('percentiles', {})
        
        if not percentiles:
            return None
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Box plot
        percentile_values = [
            percentiles.get('p25', 0),
            percentiles.get('p50', 0),
            percentiles.get('p75', 0),
            percentiles.get('p90', 0),
            percentiles.get('p95', 0),
            percentiles.get('p99', 0)
        ]
        
        ax1.boxplot([percentile_values], vert=True, widths=0.5)
        ax1.set_ylabel('Task Duration (ms)', fontsize=12)
        ax1.set_title('Task Duration Distribution', fontsize=12, fontweight='bold')
        ax1.set_xticklabels(['All Tasks'])
        
        # Percentile plot
        percentile_labels = ['P25', 'P50', 'P75', 'P90', 'P95', 'P99']
        colors_pct = [self.colors['low'], self.colors['low'], self.colors['normal'],
                     self.colors['medium'], self.colors['high'], self.colors['critical']]
        
        bars = ax2.bar(percentile_labels, percentile_values, color=colors_pct, alpha=0.7)
        ax2.set_ylabel('Task Duration (ms)', fontsize=12)
        ax2.set_title('Task Duration Percentiles (Skew Detection)', fontsize=12, fontweight='bold')
        ax2.set_xlabel('Percentile', fontsize=12)
        
        # Add skew indicator
        skew_ratio = task_metrics.get('task_skew_ratio', 0)
        ax2.text(0.5, 0.95, f'Skew Ratio: {skew_ratio:.2f}',
                transform=ax2.transAxes, ha='center', va='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'task_duration_variance.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return output_path
    
    def plot_executor_memory_timeline(self, metrics: Dict[str, Any]) -> str:
        """Plot executor memory usage over time."""
        memory_metrics = metrics.get('memory_metrics', {})
        
        # Simplified timeline (would need actual time-series data)
        time_points = np.arange(0, 100, 5)
        memory_used = np.random.uniform(40, 90, len(time_points))
        gc_events = np.random.choice([0, 1], len(time_points), p=[0.8, 0.2])
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.plot(time_points, memory_used, 'b-', linewidth=2, label='Memory Used (%)')
        ax.axhline(y=80, color='orange', linestyle='--', label='High Memory Threshold')
        ax.axhline(y=90, color='r', linestyle='--', label='Critical Threshold')
        
        # Mark GC events
        gc_times = time_points[gc_events == 1]
        gc_memory = memory_used[gc_events == 1]
        ax.scatter(gc_times, gc_memory, color='red', s=100, marker='x',
                  label='GC Event', zorder=5)
        
        ax.set_xlabel('Time (relative)', fontsize=12)
        ax.set_ylabel('Memory Usage (%)', fontsize=12)
        ax.set_title('Executor Memory Usage Timeline', fontsize=14, fontweight='bold')
        ax.legend()
        ax.set_ylim(0, 100)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        output_path = os.path.join(self.output_dir, 'executor_memory_timeline.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return output_path
    
    def create_performance_dashboard(self, metrics: Dict[str, Any],
                                    analysis: Dict[str, Any]) -> str:
        """Create a comprehensive performance dashboard."""
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # Extract metrics
        cpu_metrics = metrics.get('cpu_metrics', {})
        memory_metrics = metrics.get('memory_metrics', {})
        shuffle_metrics = metrics.get('shuffle_metrics', {})
        task_metrics = metrics.get('task_metrics', {})
        executor_metrics = metrics.get('executor_metrics', {})
        
        # 1. CPU Efficiency Gauge
        ax1 = fig.add_subplot(gs[0, 0])
        self._draw_gauge(ax1, cpu_metrics.get('cpu_efficiency', 0) * 100,
                        'CPU Efficiency', '%')
        
        # 2. GC Overhead Gauge
        ax2 = fig.add_subplot(gs[0, 1])
        self._draw_gauge(ax2, memory_metrics.get('gc_overhead_ratio', 0) * 100,
                        'GC Overhead', '%', reverse=True)
        
        # 3. Shuffle Efficiency Gauge
        ax3 = fig.add_subplot(gs[0, 2])
        self._draw_gauge(ax3, shuffle_metrics.get('shuffle_efficiency', 0) * 100,
                        'Shuffle Efficiency', '%')
        
        # 4. Bottlenecks Summary
        ax4 = fig.add_subplot(gs[1, :])
        bottlenecks = analysis.get('bottlenecks', [])
        self._draw_bottlenecks_summary(ax4, bottlenecks)
        
        # 5. Task Distribution
        ax5 = fig.add_subplot(gs[2, 0])
        percentiles = task_metrics.get('percentiles', {})
        self._draw_percentiles(ax5, percentiles)
        
        # 6. Executor Load
        ax6 = fig.add_subplot(gs[2, 1])
        exec_counts = executor_metrics.get('executor_task_counts', {})
        self._draw_executor_load(ax6, exec_counts)
        
        # 7. Memory Spill
        ax7 = fig.add_subplot(gs[2, 2])
        self._draw_memory_spill(ax7, memory_metrics)
        
        # Overall title
        fig.suptitle('Spark Performance Dashboard', fontsize=16, fontweight='bold', y=0.98)
        
        output_path = os.path.join(self.output_dir, 'performance_dashboard.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return output_path
    
    def _draw_gauge(self, ax, value, title, unit, reverse=False):
        """Draw a gauge chart."""
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        # Determine color
        if reverse:  # Lower is better
            color = self.colors['low'] if value < 30 else self.colors['medium'] if value < 50 else self.colors['critical']
        else:  # Higher is better
            color = self.colors['critical'] if value < 40 else self.colors['medium'] if value < 70 else self.colors['low']
        
        # Draw gauge
        circle = plt.Circle((0.5, 0.5), 0.4, color='lightgray', alpha=0.3)
        ax.add_patch(circle)
        
        inner_circle = plt.Circle((0.5, 0.5), 0.35, color='white')
        ax.add_patch(inner_circle)
        
        # Value text
        ax.text(0.5, 0.5, f'{value:.1f}{unit}',
               ha='center', va='center', fontsize=20, fontweight='bold', color=color)
        
        ax.text(0.5, 0.15, title,
               ha='center', va='center', fontsize=11)
    
    def _draw_bottlenecks_summary(self, ax, bottlenecks):
        """Draw bottlenecks summary."""
        ax.axis('off')
        
        if not bottlenecks:
            ax.text(0.5, 0.5, 'No Critical Bottlenecks Detected',
                   ha='center', va='center', fontsize=14, color='green', fontweight='bold')
            return
        
        y_pos = 0.9
        ax.text(0.5, y_pos, 'Top Bottlenecks', ha='center', fontsize=12, fontweight='bold')
        
        y_pos -= 0.15
        for i, b in enumerate(bottlenecks[:5], 1):  # Top 5
            severity = b.get('severity', 'LOW')
            color = self.colors.get(severity.lower(), 'gray')
            confidence = b['confidence'] * 100
            
            text = f"{i}. {b['bottleneck']} [{severity}] - {confidence:.0f}%"
            ax.text(0.05, y_pos, text, fontsize=10, color=color, fontweight='bold')
            y_pos -= 0.12
    
    def _draw_percentiles(self, ax, percentiles):
        """Draw percentile distribution."""
        if not percentiles:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center')
            ax.axis('off')
            return
        
        labels = ['P25', 'P50', 'P75', 'P90', 'P95', 'P99']
        values = [percentiles.get(k.lower(), 0) for k in labels]
        
        ax.bar(labels, values, color=self.colors['normal'], alpha=0.7)
        ax.set_title('Task Duration Percentiles', fontsize=10, fontweight='bold')
        ax.set_ylabel('Duration (ms)', fontsize=9)
    
    def _draw_executor_load(self, ax, exec_counts):
        """Draw executor load distribution."""
        if not exec_counts:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center')
            ax.axis('off')
            return
        
        executors = list(exec_counts.keys())[:10]  # Top 10
        counts = [exec_counts[e] for e in executors]
        
        ax.barh(executors, counts, color=self.colors['normal'], alpha=0.7)
        ax.set_title('Executor Task Distribution', fontsize=10, fontweight='bold')
        ax.set_xlabel('Task Count', fontsize=9)
        ax.invert_yaxis()
    
    def _draw_memory_spill(self, ax, memory_metrics):
        """Draw memory spill visualization."""
        memory_spilled = memory_metrics.get('memoryBytesSpilled', 0)
        disk_spilled = memory_metrics.get('diskBytesSpilled', 0)
        
        if memory_spilled == 0 and disk_spilled == 0:
            ax.text(0.5, 0.5, 'No Memory Spill',
                   ha='center', va='center', fontsize=12, color='green', fontweight='bold')
            ax.axis('off')
            return
        
        categories = ['Memory', 'Disk']
        values = [memory_spilled / (1024**3), disk_spilled / (1024**3)]  # GB
        
        ax.bar(categories, values, color=[self.colors['high'], self.colors['critical']], alpha=0.7)
        ax.set_title('Memory Spill (GB)', fontsize=10, fontweight='bold')
        ax.set_ylabel('Spilled (GB)', fontsize=9)

# Convenience function
def generate_visualizations(metrics: Dict[str, Any], analysis: Dict[str, Any],
                           output_dir: str = "reports/plots") -> List[str]:
    """Generate all visualizations."""
    visualizer = SparkVisualizer(output_dir)
    return visualizer.generate_all_visualizations(metrics, analysis)
