# Complete Daily Workflow for SEM Catalogue

This document outlines the comprehensive daily automation workflow including Airtable sync, crawler updates, LLM cataloguing, and BigQuery session analysis.

## 🔄 **Complete Daily Workflow**

### **Current Setup Overview**

The daily workflow now consists of 4 main components:

1. **Airtable API Integration** - Sync new URLs and metadata
2. **Crawler GraphQL Integration** - Check for updated pages  
3. **LLM Cataloguing** - Process uncatalogued URLs
4. **BigQuery Session Analysis** - Deactivate zero-session pages

---

## 📊 **1. Airtable API Integration**

### **Current Implementation**
- **Service**: `app/services/airtable.py`
- **API**: Uses `pyairtable` library with PAT authentication
- **Endpoint**: Airtable REST API via `Table.all(view=view_id)`

### **Field Mappings**
```python
"DE: Landing Page" → landing_page (URL)
"DE: Channel" → channel  
"DE: Team" → team
"DE: Brand: " → brand (note trailing space)
"DE: Vertical" → vertical
"DE: Category" → primary_category
"Page Status" → page_status (Active/Inactive)
```

### **Process Flow**
1. Fetch all records from specified Airtable view
2. Normalize URLs for comparison (remove www, trailing slashes, etc.)
3. Compare against existing database URLs
4. **New URLs**: Add with `status_code=0` (uncatalogued)
5. **Existing URLs**: Update metadata and Airtable record ID

### **Environment Variables Required**
```env
AIRTABLE_PAT=your_personal_access_token
AIRTABLE_BASE_ID=app123456789
AIRTABLE_TABLE_ID=tbl123456789  
AIRTABLE_VIEW_ID=viw123456789
```

---

## 🕷️ **2. Crawler GraphQL Integration**

### **Current Implementation**
- **Service**: `app/services/crawler_graphql.py`
- **Endpoint**: `http://sandbox-crawler-alb-567754458.us-east-1.elb.amazonaws.com/graphql`
- **Authentication**: Raw token in Authorization header

### **GraphQL Query Structure**
```graphql
query GetPosts($siteName: String, $postType: String, $lastModifiedPrefix: String) {
  postsConnection(
    condition: {siteName: $siteName, postType: $postType}, 
    filter: { 
      lastModifiedDate: { includesInsensitive: $lastModifiedPrefix } 
    }
  ) {
    nodes {
      postUrl
      publishedDate
      lastModifiedDate
      crawlMetadatum { htmlPath, crawlDate }
    }
  }
}
```

### **Default Parameters**
- `siteName`: "health"
- `postType`: "SEM"
- `lastModifiedPrefix`: Yesterday's date (YYYY-MM-DD)
- Pagination: 100 results per request

### **Environment Variables Required**
```env
CRAWLER_API_TOKEN=aCitiAVAl9PCBvlB5fKwHhEMXdDvNBHfPdoI2EKK
```

---

## 🔍 **3. BigQuery Session Analysis Integration**

### **New Implementation**
- **Service**: `app/services/bigquery_integration.py`
- **BigQuery Service**: `app/services/bigquery_page_views.py`
- **Purpose**: Identify and deactivate pages with 0 sessions

### **BigQuery Data Sources**
Queries across 4 Forbes properties:
- **Advisor**: `fm-gold-layer.fm_advisor_reporting.00_reports_fm_adv_page_views`
- **Home**: `fm-gold-layer.fm_home_reporting.00_reports_fm_hom_page_views`  
- **Health**: `fm-gold-layer.fm_health_reporting.00_reports_fm_hea_page_views`
- **Betting**: `fm-gold-layer.fm_betting_reporting.00_reports_fm_bet_page_views`

### **URL Matching Process**
1. **Normalize URLs** for comparison between BigQuery and database
2. **Extract sessions data** from BigQuery (last 30 days)
3. **Match database URLs** with BigQuery URLs by normalized format
4. **Identify zero-session URLs** (not found in BigQuery = 0 sessions)

### **Airtable Status Updates**
For URLs with 0 sessions:
1. **Update Airtable** `Page Status` field from "Active" to "Inactive"
2. **Update Database** `page_status` field to "Inactive"
3. **Log all changes** for audit trail

### **Environment Variables Required**
```env
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
GOOGLE_CLOUD_PROJECT=fm-gold-layer
```

---

## ⏰ **4. Automated Scheduling**

### **AWS EventBridge Schedule**

| Task | Schedule | Description |
|------|----------|-------------|
| **Airtable Sync** | 2:00 AM UTC | Sync new URLs, update metadata |
| **Content Updates** | 3:00 AM UTC | Check crawler for updated pages |
| **BigQuery Cleanup** | 4:00 AM UTC | Analyze sessions, deactivate dead pages |

### **ECS Task Definitions**
- `sem-catalogue-airtable-sync-prod` - 256 CPU, 512 MB
- `sem-catalogue-content-updates-prod` - 512 CPU, 1024 MB  
- `sem-catalogue-bigquery-cleanup-prod` - 256 CPU, 512 MB

---

## 🚀 **Usage Instructions**

### **Test Individual Components**
```bash
# Test Airtable integration
python test_airtable_only.py

# Test BigQuery integration  
python test_bigquery_integration.py

# Test complete workflow (dry run)
python scheduled_complete_workflow.py
```

### **Run Daily Tasks Manually**
```bash
# Individual tasks
python scheduled_airtable_sync.py
python scheduled_content_updates.py
python scheduled_bigquery_cleanup.py

# Complete workflow
python scheduled_complete_workflow.py
```

### **Deploy to AWS**
```bash
# Deploy CloudFormation stack
aws cloudformation deploy \
  --template-file infra/eventbridge-schedule.yaml \
  --stack-name sem-catalogue-daily-tasks-prod \
  --parameter-overrides \
    Environment=prod \
    ImageRepository=YOUR_ECR_REPO \
    ImageTag=latest \
  --capabilities CAPABILITY_NAMED_IAM \
  --region eu-west-2
```

---

## 📋 **Workflow Summary**

### **Daily Process Flow**
1. **2:00 AM**: Airtable sync adds new URLs (status_code=0)
2. **2:30 AM**: LLM processes all uncatalogued URLs 
3. **3:00 AM**: Crawler check identifies updated pages
4. **4:00 AM**: BigQuery analysis deactivates dead pages

### **Data Flow**
```
Airtable URLs → Database (new/updated) → LLM Cataloguing → 
Crawler Updates → BigQuery Analysis → Airtable Status Updates
```

### **Key Benefits**
- ✅ **Automated discovery** of new landing pages
- ✅ **Real-time content updates** from crawler
- ✅ **AI-powered cataloguing** with metadata extraction
- ✅ **Data-driven cleanup** based on actual traffic
- ✅ **Bidirectional sync** keeps Airtable updated

This creates a fully automated, self-maintaining catalogue system that grows with new content and cleans up dead pages automatically.



