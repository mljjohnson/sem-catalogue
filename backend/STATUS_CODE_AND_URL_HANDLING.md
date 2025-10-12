# Status Code and URL Handling - Current State & Limitations

## Current Issues

### 1. URL Normalization
- **Problem**: URLs with trailing slashes may be treated as different URLs
  - Example: `/best-insurance/` vs `/best-insurance`
- **Problem**: URLs with query parameters or suffixes may redirect to canonical versions
  - Example: `/best-car-insurance-for-seniors-eq` → `/best-car-insurance-for-seniors/`
  
**Current Handling**:
- BigQuery query strips UTM parameters: `CASE WHEN STRPOS(visit_page, '?') > 0 THEN SUBSTR(visit_page, 1, STRPOS(visit_page, '?') - 1)`
- Python normalization in `normalize_url()` removes trailing slashes for comparison
- Database stores URLs as provided by Airtable (with trailing slashes)
- Data Gaps page groups by normalized URL to show unique URLs only

**Strategy**:
- ✅ Store URLs WITH trailing slashes in database (matches Airtable format)
- ✅ Normalize WITHOUT trailing slashes for comparison/matching
- ✅ Display URLs consistently in UI

### 2. Status Code Recording - NOW FIXED! ✅
**Previous Problem**: We thought we couldn't detect redirects (301/302).

**The Fix**: 
- ScrapingBee DOES expose redirect information via response headers:
  - `Spb-Initial-Status-Code`: The actual status code before any redirects (301, 302, 200, etc.)
  - `Spb-Resolved-Url`: The final URL after following redirects
- We now capture both of these values from ScrapingBee

**What This Means**:
- ✅ We can now correctly record 301/302 redirect status codes
- ✅ We know the exact final URL after redirects (`canonical_url`)
- ✅ We can distinguish between:
  - A URL that returns 200 directly (status_code=200, url == canonical_url)
  - A URL that redirects (301) to another URL (status_code=301, url != canonical_url)
- Example: `/best-insurance-eq` → redirects 301 to `/best-insurance/`
  - We now record: status_code=301, canonical_url=`/best-insurance/`

**Implementation**:
- `fetch_html()` now returns `(status_code, html, resolved_url)`
- `status_code` = `Spb-Initial-Status-Code` (the true status before redirects)
- `resolved_url` = `Spb-Resolved-Url` (used as canonical_url)
- If `url` != `canonical_url`, we know it redirected

### 3. Page Status vs Status Code
- **Page Status** (Airtable): Business decision - "Active" or "Paused"
- **Status Code** (Database): HTTP response - 200, 301, 302, 404, etc.
- These are independent - an "Active" page can have status_code=301 (intentional redirect)

## What We Can Do Now

### ✅ Implemented
1. **Redirect Detection**: Correctly capture 301/302 status codes via `Spb-Initial-Status-Code`
2. **Resolved URL Tracking**: Store final URL after redirects via `Spb-Resolved-Url`
3. **Data Gaps Page**: Identifies URLs in Airtable but not BigQuery, and vice versa
4. **URL Deduplication**: Groups by normalized URL to show unique URLs
5. **Duplicate Detection**: Flags when same URL appears multiple times with different `page_id`s
6. **Filter Excluded Domains**: Removes carshieldplans.com and gorenewalbyandersen.com from views

### ⚠️ Still Cannot Do
1. **Track URL Status Changes Over Time**: Would require recrawling existing URLs (expensive)
   - If a URL was catalogued as 200, we won't know if it later became a 301
   - Solution: Periodic recrawling of high-value URLs, or checking on manual request

## How Redirects Are Now Handled

### Detection
- When cataloguing a URL, ScrapingBee returns:
  - `Spb-Initial-Status-Code`: 301, 302, 200, 404, etc.
  - `Spb-Resolved-Url`: Final URL if it redirected
- We store:
  - `status_code`: The initial status (301, 302, or 200)
  - `canonical_url`: The resolved URL (where it actually goes)

### Analysis
- If `status_code` = 301/302: The URL is a redirect
- If `url` != `canonical_url`: The URL goes somewhere else
- If `status_code` = 200 AND `url` == `canonical_url`: Direct 200, no redirect
- If `status_code` = 200 AND `url` != `canonical_url`: 200 but has a different canonical URL (usually from HTML tag)

### Example
URL: `https://www.forbes.com/advisor/l/best-car-insurance-for-seniors-eq`
- ScrapingBee returns:
  - Initial status: 301
  - Resolved URL: `https://www.forbes.com/advisor/l/best-car-insurance-for-seniors/`
- We store:
  - `url`: `/advisor/l/best-car-insurance-for-seniors-eq`
  - `status_code`: 301
  - `canonical_url`: `https://www.forbes.com/advisor/l/best-car-insurance-for-seniors/`

## Files Modified

### Backend:
- `backend/app/crawler/scrape.py` - **CRITICAL FIX**: Now captures `Spb-Initial-Status-Code` and `Spb-Resolved-Url` from ScrapingBee
- `backend/app/ai/process.py` - Updated to use resolved URL as canonical URL
- `backend/app/api/data_gaps.py` - API endpoint for data source gaps
- `backend/STATUS_CODE_AND_URL_HANDLING.md` - This documentation

### Frontend:
- `frontend/app/data-gaps/page.tsx` - UI page to display data gaps
- `frontend/app/page.tsx` - Added "Data Gaps" navigation button

## URL Consistency Rules

**Storage** (in database):
- Store URL exactly as provided by Airtable
- Usually includes trailing slash: `https://www.forbes.com/advisor/l/best-insurance/`

**Comparison** (for matching/deduplication):
- Remove trailing slash using `TRIM(TRAILING '/' FROM url)`
- Convert to lowercase
- Remove UTM parameters
- Normalize `forbes.com` → `www.forbes.com`

**Display** (in UI):
- Show as stored (with trailing slash for consistency)
- For table display, can show just pathname: `/advisor/l/best-insurance/`
