"""
Pydantic models for Spark event log parsing.
CORRECTED to match the ACTUAL schema provided by user.

Key corrections:
1. Stage Attempt ID (not Stage Attempt)
2. Task Executor Metrics structure
3. Shuffle Write Metrics field names
4. Block Manager ID structure
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, ConfigDict

class SparkEvent(BaseModel):
    """Base model for all Spark events."""
    model_config = ConfigDict(extra='allow')
    Event: str

class SparkListenerApplicationStart(SparkEvent):
    """Event sent when the application starts."""
    AppName: Optional[str] = Field(None, alias='App Name')
    AppID: Optional[str] = Field(None, alias='App ID')
    Timestamp: Optional[int] = Field(None, alias='Timestamp')
    User: Optional[str] = None

class SparkListenerApplicationEnd(SparkEvent):
    """Event sent when the application ends."""
    Timestamp: int = Field(..., alias='Timestamp')

class SparkListenerJobStart(SparkEvent):
    """Event sent when a job starts."""
    JobID: int = Field(..., alias='Job ID')
    SubmissionTime: Optional[int] = Field(None, alias='Submission Time')
    StageIDs: List[int] = Field(default_factory=list, alias='Stage IDs')
    StageInfos: Optional[List[Dict[str, Any]]] = Field(default_factory=list, alias='Stage Infos')

class SparkListenerJobEnd(SparkEvent):
    """Event sent when a job ends."""
    JobID: int = Field(..., alias='Job ID')
    CompletionTime: int = Field(..., alias='Completion Time')
    JobResult: Optional[Dict[str, Any]] = Field(None, alias='Job Result')

class SparkListenerStageSubmitted(SparkEvent):
    """Event sent when a stage is submitted."""
    StageInfo: Dict[str, Any] = Field(..., alias='Stage Info')

class SparkListenerStageCompleted(SparkEvent):
    """Event sent when a stage is completed."""
    StageInfo: Dict[str, Any] = Field(..., alias='Stage Info')

class SparkListenerTaskStart(SparkEvent):
    """
    Event sent when a task starts.
    
    Schema from user:
    - Stage ID
    - Stage Attempt ID (NOT "Stage Attempt")
    - Task Info
    """
    StageID: int = Field(..., alias='Stage ID')
    StageAttemptID: int = Field(..., alias='Stage Attempt ID')
    TaskInfo: Optional[Dict[str, Any]] = Field(None, alias='Task Info')

class SparkListenerTaskEnd(SparkEvent):
    """
    Event sent when a task ends.
    
    CORRECTED Schema from user:
    - Stage ID
    - Stage Attempt ID (NOT "Stage Attempt")
    - Task Type
    - Task End Reason
    - Task Info (with Task ID, Index, Attempt, Partition ID, etc.)
    - Task Executor Metrics (separate from Task Metrics!)
    - Task Metrics (Executor Deserialize Time, Executor Run Time, etc.)
    """
    StageID: int = Field(..., alias='Stage ID')
    StageAttemptID: int = Field(..., alias='Stage Attempt ID')
    TaskType: Optional[str] = Field(None, alias='Task Type')
    TaskEndReason: Optional[Dict[str, Any]] = Field(None, alias='Task End Reason')
    TaskInfo: Optional[Dict[str, Any]] = Field(None, alias='Task Info')
    TaskExecutorMetrics: Optional[Dict[str, Any]] = Field(None, alias='Task Executor Metrics')
    TaskMetrics: Optional[Dict[str, Any]] = Field(None, alias='Task Metrics')

class SparkListenerExecutorAdded(SparkEvent):
    """
    Event sent when an executor is added.
    
    CORRECTED Schema from user:
    - Timestamp
    - Executor ID
    - Executor Info (Host, Total Cores, Log Urls, Attributes, Resources, Resource Profile Id, Registration Time)
    """
    Timestamp: int = Field(..., alias='Timestamp')
    ExecutorID: str = Field(..., alias='Executor ID')
    ExecutorInfo: Optional[Dict[str, Any]] = Field(None, alias='Executor Info')

class SparkListenerExecutorRemoved(SparkEvent):
    """
    Event sent when an executor is removed.
    
    Schema from user:
    - Timestamp
    - Executor ID
    - Removed Reason
    """
    Timestamp: int = Field(..., alias='Timestamp')
    ExecutorID: str = Field(..., alias='Executor ID')
    RemovedReason: Optional[str] = Field(None, alias='Removed Reason')

class SparkListenerBlockManagerAdded(SparkEvent):
    """
    Event sent when a block manager is added.
    
    CORRECTED Schema from user:
    - Block Manager ID (contains Executor ID, Host, Port)
    - Maximum Memory
    - Timestamp
    - Maximum Onheap Memory
    - Maximum Offheap Memory
    """
    BlockManagerID: Dict[str, Any] = Field(..., alias='Block Manager ID')
    MaximumMemory: Optional[int] = Field(None, alias='Maximum Memory')
    Timestamp: int = Field(..., alias='Timestamp')
    MaximumOnheapMemory: Optional[int] = Field(None, alias='Maximum Onheap Memory')
    MaximumOffheapMemory: Optional[int] = Field(None, alias='Maximum Offheap Memory')

class SparkListenerBlockManagerRemoved(SparkEvent):
    """
    Event sent when a block manager is removed.
    
    Schema from user:
    - Block Manager ID (contains Executor ID, Host, Port)
    - Timestamp
    """
    BlockManagerID: Dict[str, Any] = Field(..., alias='Block Manager ID')
    Timestamp: int = Field(..., alias='Timestamp')