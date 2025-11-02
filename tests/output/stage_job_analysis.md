# Stage and Job Analysis Report

## Stage Summary

- **Total Stages:** 2
- **Failed Stages:** 1
- **Stages with Issues:** 2
- **Total Duration:** 17.0s

## Job Summary

- **Total Jobs:** 1
- **Failed Jobs:** 1
- **Successful Jobs:** 0

## Problematic Transformations

### Stage 34: aggregateByKey

- **Transformation:** unknown (unknown)
- **Severity:** MEDIUM
- **Issues:**
  - Memory spill: 500.00 MB

### Stage 35: join

- **Transformation:** join (join)
- **Severity:** CRITICAL
- **Issues:**
  - Memory spill: 1.00 GB
  - Stage failed: FetchFailedException (executor lost due to OOM)

## Performance Hotspots

| Rank | Stage ID | Duration | Tasks | Transformation | Issues |
|------|----------|----------|-------|----------------|--------|
| 1 | 35 | 10.0s | 1 | join | 2 |
| 2 | 34 | 7.0s | 1 | unknown | 1 |

