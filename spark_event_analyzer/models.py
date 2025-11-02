"""
Pydantic models for Spark event log parsing.

These models define the structure of various Spark listener events.
Compatible with Spark 2.x and 3.x event formats.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator

class SparkEvent(BaseModel):
    """Base model for all Spark events."""
    Event: str

class SparkListenerApplicationStart(SparkEvent):
    """
    Event sent when the application starts.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerApplicationStart.html
    """
    AppName: str = Field(..., alias='App Name')
    AppID: Optional[str] = Field(None, alias='App ID')
    Timestamp: int = Field(..., alias='Timestamp')
    User: str

class SparkListenerApplicationEnd(SparkEvent):
    """
    Event sent when the application ends.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerApplicationEnd.html
    """
    Timestamp: int = Field(..., alias='Timestamp')

class SparkListenerJobStart(SparkEvent):
    """
    Event sent when a job starts.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerJobStart.html
    """
    JobID: int = Field(..., alias='Job ID')
    SubmissionTime: Optional[int] = Field(None, alias='Submission Time')
    StageIDs: List[int] = Field(default_factory=list, alias='Stage IDs')
    StageInfos: List[Dict[str, Any]] = Field(..., alias='Stage Infos')

class SparkListenerJobEnd(SparkEvent):
    """
    Event sent when a job ends.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerJobEnd.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    JobID: int = Field(..., alias='Job ID')
    CompletionTime: int = Field(..., alias='Completion Time')
    JobResult: Dict[str, Any] = Field(..., alias='Job Result')

class SparkListenerStageSubmitted(SparkEvent):
    """
    Event sent when a stage is submitted.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerStageSubmitted.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    StageInfo: Dict[str, Any] = Field(..., alias='Stage Info')

class SparkListenerStageCompleted(SparkEvent):
    """
    Event sent when a stage is completed.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerStageCompleted.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    StageInfo: Dict[str, Any] = Field(..., alias='Stage Info')

class TaskMetrics(BaseModel):
    """
    Metrics for a single task.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/executor/TaskMetrics.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    ExecutorRunTime: Optional[int] = Field(None, alias='Executor Run Time')
    ExecutorCpuTime: Optional[int] = Field(None, alias='Executor CPU Time')
    ExecutorDeserializeTime: Optional[int] = Field(None, alias='Executor Deserialize Time')
    ExecutorDeserializeCpuTime: Optional[int] = Field(None, alias='Executor Deserialize CPU Time')
    ResultSize: Optional[int] = Field(None, alias='Result Size')
    JvmGCTime: Optional[int] = Field(None, alias='JVM GC Time')
    ResultSerializationTime: Optional[int] = Field(None, alias='Result Serialization Time')
    MemoryBytesSpilled: Optional[int] = Field(None, alias='Memory Bytes Spilled')
    DiskBytesSpilled: Optional[int] = Field(None, alias='Disk Bytes Spilled')
    PeakExecutionMemory: Optional[int] = Field(None, alias='Peak Execution Memory')
    InputMetrics: Optional[Dict[str, Any]] = Field(None, alias='Input Metrics')
    OutputMetrics: Optional[Dict[str, Any]] = Field(None, alias='Output Metrics')
    ShuffleReadMetrics: Optional[Dict[str, Any]] = Field(None, alias='Shuffle Read Metrics')
    ShuffleWriteMetrics: Optional[Dict[str, Any]] = Field(None, alias='Shuffle Write Metrics')

class SparkListenerTaskEnd(SparkEvent):
    """
    Event sent when a task ends.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerTaskEnd.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    StageID: int = Field(..., alias='Stage ID')
    TaskType: Optional[str] = Field(None, alias='Task Type')
    TaskEndReason: Optional[Dict[str, Any]] = Field(None, alias='Task End Reason')
    TaskInfo: Optional[Dict[str, Any]] = Field(None, alias='Task Info')
    TaskMetrics: Optional[Dict[str, Any]] = Field(None, alias='Task Metrics')

    @field_validator('TaskMetrics', mode='before')
    @classmethod
    def parse_task_metrics(cls, v):
        """
        Keep Task Metrics as dict for easier processing.
        
        This avoids Pydantic validation issues and makes it simpler
        to extract nested metrics in the metrics_engine.
        """
        if v is not None and isinstance(v, dict):
            return v
        return None

class SparkListenerExecutorAdded(SparkEvent):
    """
    Event sent when an executor is added.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerExecutorAdded.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    Timestamp: int = Field(..., alias='Timestamp')
    ExecutorID: str = Field(..., alias='Executor ID')
    ExecutorInfo: Dict[str, Any] = Field(..., alias='Executor Info')

class SparkListenerExecutorRemoved(SparkEvent):
    """
    Event sent when an executor is removed.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerExecutorRemoved.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    Timestamp: int = Field(..., alias='Timestamp')
    ExecutorID: str = Field(..., alias='Executor ID')
    RemovedReason: str = Field(..., alias='Removed Reason')

class SparkListenerBlockManagerAdded(SparkEvent):
    """
    Event sent when a block manager is added.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerBlockManagerAdded.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    Timestamp: int = Field(..., alias='Timestamp')
    BlockManagerID: Dict[str, Any] = Field(..., alias='Block Manager ID')
    MaxMem: int = Field(..., alias='Maximum Memory')

class SparkListenerBlockManagerRemoved(SparkEvent):
    """
    Event sent when a block manager is removed.

    Reference:
    - https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerBlockManagerRemoved.html
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)
    Timestamp: int = Field(..., alias='Timestamp')
    BlockManagerID: Dict[str, Any] = Field(..., alias='Block Manager ID')
