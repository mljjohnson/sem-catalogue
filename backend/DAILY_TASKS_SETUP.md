# Daily Tasks Setup

## Overview

The SEM Catalogue has a fully automated daily workflow that runs 4 tasks in sequence:

1. **Airtable Sync** - Sync new URLs and page status updates from Airtable
2. **Crawler Check** - Check for URLs updated in the crawler
3. **Zero Sessions Check** - Find URLs with 0 sessions and pause them in Airtable
4. **LLM Cataloguing** - Process all uncatalogued URLs through the LLM

## Files

### Main Orchestrator
- **`scripts/daily_tasks/run_daily_tasks.py`** - Master script that runs all 4 tasks in sequence

### Individual Task Scripts
- **`scripts/daily_tasks/scheduled_airtable_sync_optimized.py`** - Airtable sync
- **`scripts/daily_tasks/check_updated_urls.py`** - Crawler check
- **`scripts/daily_tasks/check_zero_sessions.py`** - Zero sessions check
- **`app/tools/process_uncatalogued.py`** - LLM cataloguing

### Supporting Services
- **`app/services/airtable.py`** - Airtable API integration
- **`app/services/bigquery_integration.py`** - BigQuery session data
- **`app/services/crawler_graphql.py`** - Crawler GraphQL API
- **`app/services/task_logger.py`** - Task execution logging
- **`app/ai/process.py`** - LLM cataloguing logic

## Running Manually

### Run All Tasks
```bash
python scripts/daily_tasks/run_daily_tasks.py
```

### Run Individual Tasks

**Airtable Sync:**
```bash
python scripts/daily_tasks/scheduled_airtable_sync_optimized.py
```

**Crawler Check:**
```bash
python scripts/daily_tasks/check_updated_urls.py
```

**Zero Sessions Check:**
```bash
# With confirmation prompt:
python scripts/daily_tasks/check_zero_sessions.py

# Auto-update without prompt (for automation):
python scripts/daily_tasks/check_zero_sessions.py --auto-update

# Use cached BigQuery data (for testing):
python scripts/daily_tasks/check_zero_sessions.py --cache
```

**LLM Cataloguing:**
```bash
# Process all uncatalogued URLs:
python -m app.tools.process_uncatalogued

# Process specific count:
python -m app.tools.process_uncatalogued --count 10

# With task logging:
python -m app.tools.process_uncatalogued --log
```

## Task Logging

All tasks log their execution to the `task_logs` table:
- Start time
- End time
- Status (success/failed)
- Statistics (URLs processed, errors, etc.)
- Error details (if failed)

View logs at: http://localhost:3000/task-logs

## Deployment on AWS ECS

### Recommended Setup: ECS + EventBridge

1. **Create ECS Task Definition**
   - Container: Your backend Docker image
   - Command: `["python", "scripts/daily_tasks/run_daily_tasks.py"]`
   - Environment variables: All required credentials (DB, Airtable, BigQuery, OpenAI)
   - CPU/Memory: Sufficient for LLM processing (recommend 2 vCPU, 4GB RAM)

2. **Create EventBridge Rule**
   - Schedule: `cron(0 2 * * ? *)` (runs at 2 AM UTC daily)
   - Target: ECS Task
   - Launch Type: Fargate
   - Task count: 1

3. **CloudWatch Logs**
   - All output is printed to stdout/stderr
   - Captured by CloudWatch for monitoring

### Environment Variables Required
```
# Database
MYSQL_HOST=your-rds-endpoint
MYSQL_USER=your-user
MYSQL_PASSWORD=your-password
MYSQL_DATABASE=sem_catalogue

# Airtable
AIRTABLE_PAT=your-personal-access-token
AIRTABLE_BASE_ID=appfGPddh3kvKXSkx
AIRTABLE_TABLE_ID=tbl5X32JvqrSwWaH
AIRTABLE_VIEW_ID=viwyvspX2WSxsmEBg

# BigQuery
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
BIGQUERY_PROJECT=your-project
BIGQUERY_DATASET=your-dataset

# OpenAI
OPENAI_API_KEY=your-api-key

# ScrapingBee
SCRAPINGBEE_API_KEY=your-api-key

# Crawler GraphQL
CRAWLER_GRAPHQL_ENDPOINT=your-endpoint
CRAWLER_GRAPHQL_KEY=your-key
```

## Task Flow

```
┌─────────────────────┐
│  1. Airtable Sync   │
│  - Fetch new URLs   │
│  - Update statuses  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  2. Crawler Check   │
│  - Get updated URLs │
│  - Mark for recat.  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 3. Zero Sessions    │
│  - Query BigQuery   │
│  - Pause 0-session  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ 4. LLM Cataloguing  │
│  - Process new URLs │
│  - Extract data     │
└─────────────────────┘
```

## Monitoring

### Check Task Logs
```bash
# Via UI
http://localhost:3000/task-logs

# Via Database
SELECT * FROM task_logs ORDER BY started_at DESC LIMIT 10;
```

### Check for Failures
```sql
SELECT task_name, started_at, status, error_message 
FROM task_logs 
WHERE status = 'failed' 
ORDER BY started_at DESC;
```

### Expected Daily Stats
- **Airtable**: 0-50 new URLs, 0-100 status updates
- **Crawler**: 10-100 updated URLs
- **Zero Sessions**: 50-500 URLs paused
- **Cataloguing**: 50-500 URLs processed

## Important Notes

1. **301/302 Redirects**: Now properly detected and skipped (not sent to LLM)
2. **URL Matching**: Trailing slashes are normalized for consistent matching
3. **Error Handling**: Tasks continue even if one fails
4. **Idempotency**: Safe to run multiple times - duplicate work is avoided
5. **Cost Control**: Only 200/0 status codes are catalogued via LLM

## Troubleshooting

### No URLs Being Catalogued
- Check `catalogued = 0 AND status_code IN (0, 200)` in database
- Verify OpenAI API key is valid
- Check ScrapingBee credits

### Zero Sessions Check Not Working
- Verify BigQuery credentials
- Check Airtable PAT has write permissions
- Confirm URL matching is working (check trailing slashes)

### Airtable Sync Failing
- Verify PAT, base ID, table ID are correct
- Check Airtable API rate limits
- Ensure page_status field exists and is spelled correctly
