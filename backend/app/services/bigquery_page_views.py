"""
BigQuery API script to run page views query across Forbes properties.
Optimized for daily automated execution with service account authentication.
"""

import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from typing import Optional
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging with timestamp for automated runs
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'bigquery_run_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BigQueryPageViews:
    def __init__(self, project_id: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Initialize BigQuery client with service account authentication.
        
        Args:
            project_id: GCP project ID. If None, will use from environment or credentials.
            credentials_path: Path to service account JSON file. If None, will use environment variable.
        """
        try:
            # Get credentials path from parameter, environment, or default
            creds_path = credentials_path or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            
            if creds_path and os.path.exists(creds_path):
                # Use service account credentials
                credentials = service_account.Credentials.from_service_account_file(creds_path)
                project_id = project_id or os.getenv('GOOGLE_CLOUD_PROJECT') or credentials.project_id
                self.client = bigquery.Client(credentials=credentials, project=project_id)
                logger.info(f"BigQuery client initialized with service account for project: {self.client.project}")
            else:
                # Fall back to default credentials (for development)
                self.client = bigquery.Client(project=project_id)
                logger.info(f"BigQuery client initialized with default credentials for project: {self.client.project}")
                logger.warning("Using default credentials - consider using service account for production")
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            logger.error("Make sure GOOGLE_APPLICATION_CREDENTIALS points to a valid service account JSON file")
            raise

    def get_page_views_query(self) -> str:
        """
        Returns the page views query across all Forbes properties including DollarGeek and Expertise.
        """
        return """
        WITH _page_views AS (
        SELECT
          'advisor' AS property,
          CASE 
            WHEN STRPOS(visit_page, '?') > 0 THEN SUBSTR(visit_page, 1, STRPOS(visit_page, '?') - 1)
            ELSE visit_page
          END AS visit_page,
          COUNT(DISTINCT session_id) AS sessions
        FROM
          `fm-gold-layer.fm_advisor_reporting.00_reports_fm_adv_page_views`
        WHERE
          partition_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND visit_page LIKE '%/l/%'
        GROUP BY
          ALL
        UNION ALL
        SELECT
          'home' AS property,
          CASE 
            WHEN STRPOS(visit_page, '?') > 0 THEN SUBSTR(visit_page, 1, STRPOS(visit_page, '?') - 1)
            ELSE visit_page
          END AS visit_page,
          COUNT(DISTINCT session_id) AS sessions
        FROM
          `fm-gold-layer.fm_home_reporting.00_reports_fm_hom_page_views`
        WHERE
          partition_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND visit_page LIKE '%/l/%'
        GROUP BY
          ALL
        UNION ALL
        SELECT
          'health' AS property,
          CASE 
            WHEN STRPOS(visit_page, '?') > 0 THEN SUBSTR(visit_page, 1, STRPOS(visit_page, '?') - 1)
            ELSE visit_page
          END AS visit_page,
          COUNT(DISTINCT session_id) AS sessions
        FROM
          `fm-gold-layer.fm_health_reporting.00_reports_fm_hea_page_views`
        WHERE
          partition_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND visit_page LIKE '%/l/%'
        GROUP BY
          ALL
        UNION ALL
        SELECT
          'betting' AS property,
          CASE 
            WHEN STRPOS(visit_page, '?') > 0 THEN SUBSTR(visit_page, 1, STRPOS(visit_page, '?') - 1)
            ELSE visit_page
          END AS visit_page,
          COUNT(DISTINCT session_id) AS sessions
        FROM
          `fm-gold-layer.fm_betting_reporting.00_reports_fm_bet_page_views`
        WHERE
          partition_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND visit_page LIKE '%/l/%'
        GROUP BY
          ALL
        UNION ALL
        SELECT
          'dollargeek' AS property,
          CASE 
            WHEN STRPOS(visit_page, '?') > 0 THEN SUBSTR(visit_page, 1, STRPOS(visit_page, '?') - 1)
            ELSE visit_page
          END AS visit_page,
          COUNT(DISTINCT session_id) AS sessions
        FROM
          `fm-gold-layer.fm_dollargeek_tracking.fm_dol_ga_page_views`
        WHERE
          partition_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND visit_page LIKE '%/l/%'
        GROUP BY
          ALL
        UNION ALL
        SELECT
          'expertise' AS property,
          CASE 
            WHEN STRPOS(visit_page, '?') > 0 THEN SUBSTR(visit_page, 1, STRPOS(visit_page, '?') - 1)
            ELSE visit_page
          END AS visit_page,
          COUNT(DISTINCT session_id) AS sessions
        FROM
          `fm-gold-layer.fm_expertise_tracking.fm_exp_ga_page_views`
        WHERE
          partition_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND visit_page LIKE '%/l/%'
        GROUP BY
          ALL
        )
        SELECT 
          *, 
          CASE 
            WHEN _page_views.sessions > 0 THEN "Live" 
            ELSE "Dead" 
          END AS live 
        FROM _page_views 
        ORDER BY sessions ASC
        """

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute BigQuery SQL and return results as pandas DataFrame.
        
        Args:
            query: SQL query string
            
        Returns:
            pandas DataFrame with query results
        """
        try:
            logger.info("Executing BigQuery...")
            
            # Configure query job
            job_config = bigquery.QueryJobConfig()
            job_config.use_legacy_sql = False
            
            # Run query
            query_job = self.client.query(query, job_config=job_config)
            
            logger.info(f"Query job started: {query_job.job_id}")
            
            # Wait for results
            results = query_job.result()
            
            # Convert to DataFrame
            df = results.to_dataframe()
            
            logger.info(f"Query completed successfully. Rows returned: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def run_page_views_analysis(self) -> pd.DataFrame:
        """
        Run the page views analysis and return results.
        
        Returns:
            pandas DataFrame with page views analysis
        """
        query = self.get_page_views_query()
        return self.run_query(query)

    def save_results(self, df: pd.DataFrame, filename: Optional[str] = None):
        """
        Save results to CSV file with timestamp for automated runs.
        
        Args:
            df: DataFrame to save
            filename: Output filename. If None, will use timestamped filename.
        """
        try:
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"page_views_results_{timestamp}.csv"
            
            df.to_csv(filename, index=False)
            logger.info(f"Results saved to {filename}")
            
            # Also save a "latest" version for easy access
            latest_filename = "page_views_results_latest.csv"
            df.to_csv(latest_filename, index=False)
            logger.info(f"Latest results saved to {latest_filename}")
            
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            raise

def main():
    """
    Main execution function.
    """
    try:
        # Initialize BigQuery client with the new credentials
        bq = BigQueryPageViews(
            project_id="fm-gold-layer",
            credentials_path="fm-gold-layer-34786f9cc208.json"
        )
        
        # Run page views analysis
        logger.info("Starting page views analysis...")
        results_df = bq.run_page_views_analysis()
        
        # Display summary
        print(f"\n=== Page Views Analysis Results ===")
        print(f"Total rows: {len(results_df)}")
        print(f"Properties analyzed: {results_df['property'].unique()}")
        print(f"Live pages: {len(results_df[results_df['live'] == 'Live'])}")
        print(f"eWaste pages: {len(results_df[results_df['live'] == 'eWaste'])}")
        
        # Show top 10 pages by sessions
        print(f"\n=== Top 10 Pages by Sessions ===")
        top_pages = results_df.nlargest(10, 'sessions')
        print(top_pages[['property', 'visit_page', 'sessions', 'live']].to_string(index=False))
        
        # Save results
        bq.save_results(results_df)
        
        print(f"\nFull results saved to page_views_results.csv")
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
