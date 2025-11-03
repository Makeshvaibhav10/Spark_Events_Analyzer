"""
Pydantic models matching YOUR EXACT log structure.

Critical differences from standard Spark:
1. Task Executor Metrics contains BOTH task metrics AND GC metrics
2. No separate "Task Metrics" field - everything is in "Task Executor Metrics"
3. Shuffle/Input/Output metrics are nested inside Task Executor Metrics
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ConfigDict

class SparkEvent(BaseModel):
    """Base model for all Spark events."""
    model_config = ConfigDict(extra='allow')
    Event: str

class SparkListenerApplicationStart(SparkEvent):
    """Event sent when the application starts."""
    AppName: str = Field(..., alias='App Name')
    AppID: Optional[str] = Field(None, alias='App ID')
    Timestamp: int
    User: str
    AppAttemptID: Optional[str] = Field(None, alias='App Attempt ID')
    SparkVersion: Optional[str] = Field(None, alias='Spark Version')

class SparkListenerApplicationEnd(SparkEvent):
    """Event sent when the application ends."""
    Timestamp: int

class SparkListenerJobStart(SparkEvent):
    """Event sent when a job starts."""
    model_config = ConfigDict(extra='allow')
    JobID: int = Field(..., alias='Job ID')
    SubmissionTime: Optional[int] = Field(None, alias='Submission Time')
    StageIDs: List[int] = Field(default_factory=list, alias='Stage IDs')
    StageInfos: Optional[List[Dict[str, Any]]] = Field(default_factory=list, alias='Stage Infos')

class SparkListenerJobEnd(SparkEvent):
    """Event sent when a job ends."""
    model_config = ConfigDict(extra='allow')
    JobID: int = Field(..., alias='Job ID')
    CompletionTime: int = Field(..., alias='Completion Time')
    JobResult: Optional[Dict[str, Any]] = Field(None, alias='Job Result')

class SparkListenerStageSubmitted(SparkEvent):
    """Event sent when a stage is submitted."""
    StageInfo: Dict[str, Any] = Field(..., alias='Stage Info')
    Timestamp: Optional[int] = None

class SparkListenerStageCompleted(SparkEvent):
    """Event sent when a stage is completed."""
    StageInfo: Dict[str, Any] = Field(..., alias='Stage Info')

class SparkListenerTaskStart(SparkEvent):
    """Event sent when a task starts."""
    StageID: int = Field(..., alias='Stage ID')
    StageAttemptID: int = Field(..., alias='Stage Attempt ID')
    TaskInfo: Dict[str, Any] = Field(..., alias='Task Info')

class SparkListenerTaskEnd(SparkEvent):
    """
    Event sent when a task ends.
    
    YOUR SCHEMA:
    - Task Executor Metrics contains EVERYTHING (not separate Task Metrics)
    - Includes: CPU Time, Run Time, Result Size, GC metrics
    - Also includes: Shuffle Write Metrics, Input Metrics, Output Metrics as NESTED objects
    """
    StageID: int = Field(..., alias='Stage ID')
    StageAttemptID: int = Field(..., alias='Stage Attempt ID')
    TaskType: str = Field(..., alias='Task Type')
    TaskEndReason: Dict[str, Any] = Field(..., alias='Task End Reason')
    TaskInfo: Dict[str, Any] = Field(..., alias='Task Info')
    TaskExecutorMetrics: Dict[str, Any] = Field(..., alias='Task Executor Metrics')

class SparkListenerExecutorAdded(SparkEvent):
    """Event sent when an executor is added."""
    Timestamp: int
    ExecutorID: str = Field(..., alias='Executor ID')
    ExecutorInfo: Dict[str, Any] = Field(..., alias='Executor Info')

class SparkListenerExecutorRemoved(SparkEvent):
    """Event sent when an executor is removed."""
    Timestamp: int
    ExecutorID: str = Field(..., alias='Executor ID')
    RemovedReason: str = Field(..., alias='Removed Reason')

class SparkListenerBlockManagerAdded(SparkEvent):
    """Event sent when a block manager is added."""
    Timestamp: int
    BlockManagerID: Dict[str, Any] = Field(..., alias='Block Manager ID')
    MaximumOnHeapMemory: Optional[int] = Field(None, alias='Maximum OnHeap Memory')
    MaximumOffHeapMemory: Optional[int] = Field(None, alias='Maximum OffHeap Memory')

class SparkListenerBlockManagerRemoved(SparkEvent):
    """Event sent when a block manager is removed."""
    Timestamp: int
    BlockManagerID: Dict[str, Any] = Field(..., alias='Block Manager ID')