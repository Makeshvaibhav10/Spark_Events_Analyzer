"""
Unit tests for the Spark event log parser.
"""
import os
import pytest
from spark_event_analyzer.parser import parse_event_log
from spark_event_analyzer.models import (
    SparkListenerApplicationStart,
    SparkListenerApplicationEnd,
    SparkListenerJobStart,
    SparkListenerJobEnd,
    SparkListenerStageSubmitted,
    SparkListenerStageCompleted,
    SparkListenerTaskEnd,
    SparkListenerExecutorAdded,
    SparkListenerExecutorRemoved,
    SparkListenerBlockManagerAdded,
    SparkListenerBlockManagerRemoved,
)

SAMPLE_LOGS_DIR = os.path.join(os.path.dirname(__file__), 'sample_logs')


@pytest.fixture
def json_log_path():
    return os.path.join(SAMPLE_LOGS_DIR, 'sample_log.json')


@pytest.fixture
def ndjson_log_path():
    return os.path.join(SAMPLE_LOGS_DIR, 'sample_log.ndjson')


@pytest.fixture
def gzipped_ndjson_log_path():
    return os.path.join(SAMPLE_LOGS_DIR, 'sample_log.ndjson.gz')


def test_parse_json_log(json_log_path):
    """Tests parsing of a JSON array event log."""
    events = list(parse_event_log(json_log_path))
    assert len(events) == 4
    assert isinstance(events[0], SparkListenerApplicationStart)
    assert isinstance(events[1], SparkListenerJobStart)
    assert isinstance(events[2], SparkListenerStageCompleted)
    assert isinstance(events[3], SparkListenerTaskEnd)


def test_parse_ndjson_log(ndjson_log_path):
    """Tests parsing of a newline-delimited JSON event log."""
    events = list(parse_event_log(ndjson_log_path))
    assert len(events) == 4
    assert isinstance(events[0], SparkListenerApplicationStart)
    assert events[0].AppName == "test-app"


def test_parse_gzipped_ndjson_log(gzipped_ndjson_log_path):
    """Tests parsing of a gzipped newline-delimited JSON event log."""
    events = list(parse_event_log(gzipped_ndjson_log_path))
    assert len(events) == 4
    assert isinstance(events[1], SparkListenerJobStart)
    assert events[1].JobID == 0


@pytest.fixture
def json_log_2_path():
    return os.path.join(SAMPLE_LOGS_DIR, 'sample_log_2.json')

def test_parse_json_log_2(json_log_2_path):
    """Tests parsing of a JSON array event log with more event types."""
    events = list(parse_event_log(json_log_2_path))
    assert len(events) == 10
    assert isinstance(events[0], SparkListenerApplicationStart)
    assert isinstance(events[1], SparkListenerExecutorAdded)
    assert isinstance(events[2], SparkListenerBlockManagerAdded)
    assert isinstance(events[3], SparkListenerJobStart)
    assert isinstance(events[4], SparkListenerStageSubmitted)
    assert isinstance(events[5], SparkListenerTaskEnd)
    assert isinstance(events[6], SparkListenerStageCompleted)
    assert isinstance(events[7], SparkListenerJobEnd)
    assert isinstance(events[8], SparkListenerExecutorRemoved)
    assert isinstance(events[9], SparkListenerApplicationEnd)

    task_end_event = events[5]
    assert task_end_event.task_metrics is not None
    assert task_end_event.task_metrics.ExecutorRunTime == 500
    assert task_end_event.task_metrics.InputMetrics['Bytes Read'] == 1024

def test_malformed_log():
    """Tests that the parser handles malformed JSON lines gracefully."""
    log_path = os.path.join(SAMPLE_LOGS_DIR, 'malformed_log.ndjson')
    with open(log_path, 'w') as f:
        f.write('{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":1635671201000,"Stage IDs":[0, 1]}\n')
        f.write('this is not json\n')
        f.write('{"Event":"SparkListenerTaskEnd","Stage ID":0}\n')

    events = list(parse_event_log(log_path))
    assert len(events) == 2
    assert isinstance(events[0], SparkListenerJobStart)

    os.remove(log_path)
