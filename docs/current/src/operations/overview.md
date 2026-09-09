# Operations

## Operational Overview

```d2
direction: right

DailyOps: {
  label: "Daily Operations"

  monitor: "Monitor\nReplication Lag"
  verify: "Verify Event\nCounts"
  alerts: "Review\nAlerts"
}

WeeklyOps: {
  label: "Weekly Operations"

  optimize: "Run\nOptimization"
  storage: "Review\nStorage Usage"
  costs: "AWS Cost\nReview"
}

OnDemand: {
  label: "On-Demand Operations"

  retrieve: "Historical\nRetrieval"
  investigate: "Security\nInvestigation"
  audit: "Compliance\nAudit"
}

DailyOps.monitor -> DailyOps.verify -> DailyOps.alerts
WeeklyOps.optimize -> WeeklyOps.storage -> WeeklyOps.costs

OnDemand.retrieve -> OnDemand.investigate
OnDemand.retrieve -> OnDemand.audit
```

## Runbooks

### Daily Health Check

```bash
#!/bin/bash
# daily-health-check.sh

echo "=== Cyberphy Daily Health Check ==="
echo "Date: $(date)"
echo

# 1. Check replication lag
echo "--- Replication Status ---"
replication-manager status cloudtrail-aws-to-onprem | grep -E "(lag|pending|error)"

# 2. Compare event counts (last 24h)
echo
echo "--- Event Count Comparison (24h) ---"
impala-shell -q "
SELECT
  'AWS' as source, count(*) as events
FROM aws_catalog.cybersec.cloudtrail_events
WHERE event_time >= now() - interval 1 day
UNION ALL
SELECT
  'OnPrem' as source, count(*) as events
FROM cybersec.cloudtrail_events
WHERE event_time >= now() - interval 1 day;
"

# 3. Check Flink job status
echo
echo "--- Flink Job Status ---"
flink list -r 2>/dev/null | grep -E "(cloudtrail|RUNNING|FAILED)"

# 4. Check optimization status
echo
echo "--- Table Optimization Status ---"
lakehouse-optimizer status cybersec.cloudtrail_events --summary
```

### Incident Response

When a security incident requires historical data:

1. **Identify time range** and data requirements
2. **Check data location**:
   - Hot (0-90 days): Query directly
   - Cold (90+ days): Initiate Glacier restore
3. **Execute analysis** on-prem
4. **Document findings** and data accessed

```bash
#!/bin/bash
# incident-data-request.sh

INCIDENT_ID=$1
START_DATE=$2
END_DATE=$3

echo "=== Incident Data Request ==="
echo "Incident: $INCIDENT_ID"
echo "Date Range: $START_DATE to $END_DATE"

# Check if data is in Glacier
aws s3api head-object \
  --bucket cybersec-cloudtrail-iceberg \
  --key "iceberg/warehouse/cloudtrail_events/data/year=${START_DATE:0:4}/month=${START_DATE:5:2}/day=${START_DATE:8:2}/sample.parquet" \
  2>&1 | grep -q "GLACIER"

if [ $? -eq 0 ]; then
  echo "Data is in Glacier - initiating restore..."
  python3 scripts/glacier_restore.py \
    --start-date $START_DATE \
    --end-date $END_DATE \
    --tier Standard \
    --notify security-team@company.com
else
  echo "Data is accessible - proceed with analysis"
fi
```

## SLAs and Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Ingestion latency | < 5 minutes | CloudTrail → S3 Iceberg |
| Replication lag | < 15 minutes | AWS → On-prem |
| Query response (hot) | < 30 seconds | 24h aggregate query |
| Glacier restore (bulk) | < 12 hours | Full day restore |
| Data retention | 7 years | Compliance requirement |
| Availability | 99.9% | Query service uptime |

## Contacts and Escalation

| Role | Team | Contact |
|------|------|---------|
| L1 Operations | Platform Ops | platform-ops@company.com |
| L2 Data Engineering | Data Platform | data-platform@company.com |
| L3 Security Analysis | Security Team | security-team@company.com |
| AWS Account Owner | Cloud Team | cloud-team@company.com |
