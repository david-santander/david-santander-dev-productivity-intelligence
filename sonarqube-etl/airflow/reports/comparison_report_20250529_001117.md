# SonarQube ETL Data Quality Report

**Generated:** 2025-05-29 00:11:17 UTC
**Execution Date:** 2025-05-29T00:11:14.991215+00:00
**DAG Run ID:** dataset_triggered__2025-05-29T00:11:14.991215+00:00

## Health Status

### Overall Status: FAIL CRITICAL

#### Critical Issues

- 2 projects failed to fetch from SonarQube

### Health Metrics

| Category | Metric | Value | Status |
|----------|--------|-------|--------|
| Data Freshness | Latest PostgreSQL Date | 2025-05-29 | healthy |
| Data Freshness | Data Age (days) | 0 | - |
| Data Freshness | Carried Forward Projects | 0 (0.0%) | - |
| Data Completeness | Total Unique Projects | 2 | - |
| Data Completeness | Missing in PostgreSQL | 0 | - |
| Data Completeness | Missing in SonarQube | 0 | - |
| Data Completeness | Completeness % | 100.0% | - |
| Sync Status | Projects in Sync | 2 | - |
| Sync Status | Projects with Discrepancies | 0 | - |
| Sync Status | Total Discrepancies | 0 | - |
| Sync Status | Sync Percentage | 100.0% | - |

## Summary

| Metric | Value |
|--------|-------|
| Total Projects in PostgreSQL | 2 |
| Total Projects in SonarQube | 2 |
| Projects with Discrepancies | 0 |
| Total Discrepancies | 0 |
| Metrics Compared | 75 |

## PostgreSQL Database Statistics

| Statistic | Value |
|-----------|-------|
| Total Projects | 2 |
| Latest Metric Date | 2025-05-29 |
| Earliest Metric Date | 2025-01-01 |
| Total Records | 298 |

## Data Quality Metrics

### Metric Coverage Analysis

| Metric Type | Total Fields | Fields with Data | Coverage % |
|-------------|--------------|------------------|------------|
| Bugs | 13 | 0 | 0.0% |
| Vulnerabilities | 13 | 0 | 0.0% |
| Code Smells | 13 | 0 | 0.0% |
| Security Hotspots | 8 | 0 | 0.0% |
| Coverage | 1 | 0 | 0.0% |
| Duplication | 1 | 0 | 0.0% |
| New Code - Bugs | 6 | 0 | 0.0% |
| New Code - Vulnerabilities | 6 | 0 | 0.0% |
| New Code - Code Smells | 6 | 0 | 0.0% |
| New Code - Security Hotspots | 5 | 0 | 0.0% |
| New Code - Quality | 3 | 0 | 0.0% |

## Metric Validation Report

### New Metrics Validation

| Metric Category | Status | Details |
|-----------------|--------|---------|
| Vulnerabilities BLOCKER Severity | ⚠️ error | Metrics not found in comparison data |
| Issues FALSE-POSITIVE Status | ⚠️ error | Metrics not found in comparison data |
| Issues WONTFIX Status | ⚠️ error | Metrics not found in comparison data |
| Code Smells Status Tracking | ⚠️ error | Metrics not found in comparison data |
| Security Hotspots SAFE Status | ⚠️ error | Metrics not found in comparison data |
| New Code Severity Breakdown | ⚠️ error | Metrics not found in comparison data |

## ETL Process Health

| ETL Metric | Value |
|------------|-------|
| Data Accuracy Rate | 100.0% |
| Total Metrics Processed | 150 |
| Metrics with Discrepancies | 0 |
| Average Discrepancies per Project | 0.0 |

### ETL Health Recommendations

- ETL pipeline is operating normally with 100% data accuracy.

## Project Comparison Details

### All Projects in Sync ✅

Great news! All 2 projects have matching metrics between PostgreSQL and SonarQube.

| Project | Last Updated | Data Status |
|---------|--------------|-------------|
| dev.productivity:django-sample-app | N/A | ✅ Fresh Data |
| dev.productivity:fastapi-sample-app | N/A | ✅ Fresh Data |

### Projects with Fetch Errors

The following projects encountered errors during SonarQube data fetch:

- dev.productivity:django-sample-app: 404 Client Error:  for url: http://sonarqube:9000/api/measures/component?component=dev.productivity%3Adjango-sample-app&metricKeys=lines%2Cncloc%2Cclasses%2Cfunctions%2Cstatements%2Cfiles%2Cdirectories%2Cbugs%2Cvulnerabilities%2Ccode_smells%2Csecurity_hotspots%2Creliability_rating%2Csecurity_rating%2Csqale_rating%2Creliability_remediation_effort%2Csecurity_remediation_effort%2Ctechnical_debt%2Csqale_debt_ratio%2Ccoverage%2Cline_coverage%2Cbranch_coverage%2Ccovered_lines%2Cuncovered_lines%2Ccovered_conditions%2Cuncovered_conditions%2Clines_to_cover%2Cconditions_to_cover%2Cduplicated_lines_density%2Cduplicated_lines%2Cduplicated_blocks%2Cduplicated_files%2Ccomplexity%2Ccognitive_complexity%2Ccomment_lines%2Ccomment_lines_density%2Cnew_lines%2Cnew_ncloc%2Cnew_bugs%2Cnew_vulnerabilities%2Cnew_code_smells%2Cnew_security_hotspots%2Cnew_reliability_remediation_effort%2Cnew_security_remediation_effort%2Cnew_technical_debt%2Cnew_sqale_debt_ratio%2Cnew_coverage%2Cnew_line_coverage%2Cnew_branch_coverage%2Cnew_covered_lines%2Cnew_uncovered_lines%2Cnew_covered_conditions%2Cnew_uncovered_conditions%2Cnew_lines_to_cover%2Cnew_conditions_to_cover%2Cnew_duplicated_lines_density%2Cnew_duplicated_lines%2Cnew_duplicated_blocks%2Cnew_complexity%2Cnew_cognitive_complexity
- dev.productivity:fastapi-sample-app: 404 Client Error:  for url: http://sonarqube:9000/api/measures/component?component=dev.productivity%3Afastapi-sample-app&metricKeys=lines%2Cncloc%2Cclasses%2Cfunctions%2Cstatements%2Cfiles%2Cdirectories%2Cbugs%2Cvulnerabilities%2Ccode_smells%2Csecurity_hotspots%2Creliability_rating%2Csecurity_rating%2Csqale_rating%2Creliability_remediation_effort%2Csecurity_remediation_effort%2Ctechnical_debt%2Csqale_debt_ratio%2Ccoverage%2Cline_coverage%2Cbranch_coverage%2Ccovered_lines%2Cuncovered_lines%2Ccovered_conditions%2Cuncovered_conditions%2Clines_to_cover%2Cconditions_to_cover%2Cduplicated_lines_density%2Cduplicated_lines%2Cduplicated_blocks%2Cduplicated_files%2Ccomplexity%2Ccognitive_complexity%2Ccomment_lines%2Ccomment_lines_density%2Cnew_lines%2Cnew_ncloc%2Cnew_bugs%2Cnew_vulnerabilities%2Cnew_code_smells%2Cnew_security_hotspots%2Cnew_reliability_remediation_effort%2Cnew_security_remediation_effort%2Cnew_technical_debt%2Cnew_sqale_debt_ratio%2Cnew_coverage%2Cnew_line_coverage%2Cnew_branch_coverage%2Cnew_covered_lines%2Cnew_uncovered_lines%2Cnew_covered_conditions%2Cnew_uncovered_conditions%2Cnew_lines_to_cover%2Cnew_conditions_to_cover%2Cnew_duplicated_lines_density%2Cnew_duplicated_lines%2Cnew_duplicated_blocks%2Cnew_complexity%2Cnew_cognitive_complexity


---
*Report generated by SonarQube ETL Data Quality Report DAG*