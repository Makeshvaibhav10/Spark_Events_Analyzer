# Stage and Job Analysis Report

## Stage Summary

- **Total Stages:** 3
- **Failed Stages:** 0
- **Stages with Issues:** 3
- **Total Duration:** 9.9s

## Job Summary

- **Total Jobs:** 0
- **Failed Jobs:** 0
- **Successful Jobs:** 0

## Problematic Transformations

### Stage 3: 

- **Transformation:** unknown (unknown)
- **Severity:** MEDIUM
- **Issues:**
  - Data skew detected: task duration variance 90.9%

### Stage 1: 

- **Transformation:** unknown (unknown)
- **Severity:** HIGH
- **Issues:**
  - Data skew detected: task duration variance 309.5%
  - Straggler tasks detected: max task is 3.2x slower than mean

### Stage 2: 

- **Transformation:** unknown (unknown)
- **Severity:** HIGH
- **Issues:**
  - Data skew detected: task duration variance 135.3%

## Performance Hotspots

| Rank | Stage ID | Duration | Tasks | Transformation | Issues |
|------|----------|----------|-------|----------------|--------|
| 1 | 1 | 6.3s | 5 | unknown | 2 |
| 2 | 2 | 2.5s | 3 | unknown | 1 |
| 3 | 3 | 1.1s | 2 | unknown | 1 |

