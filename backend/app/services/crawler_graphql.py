"""
GraphQL service for the crawler endpoint to get updated URLs
"""
import os
import requests
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from ..core.config import settings

logger = logging.getLogger(__name__)

class CrawlerGraphQLService:
    """Service to interact with the GraphQL crawler endpoint"""
    
    def __init__(self):
        self.base_url = "http://sandbox-crawler-alb-567754458.us-east-1.elb.amazonaws.com/graphql"
        self.api_token = settings.crawler_api_token
        if not self.api_token:
            logger.warning("CRAWLER_API_TOKEN not set - GraphQL calls will fail")
    
    def _make_graphql_request(self, query: str, variables: dict) -> Optional[dict]:
        """
        Make a GraphQL request to the crawler endpoint
        
        Args:
            query: GraphQL query string
            variables: Query variables
            
        Returns:
            Response data or None if failed
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.api_token  # Raw token based on Postman working
        }
        
        payload = {
            "query": query,
            "variables": variables
        }
        
        try:
            logger.info(f"Making GraphQL request to {self.base_url}")
            response = requests.post(
                self.base_url, 
                json=payload, 
                headers=headers,
                timeout=30
            )
            
            logger.info(f"GraphQL response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                if "errors" in data:
                    logger.error(f"GraphQL errors: {data['errors']}")
                    return None
                return data.get("data")
            else:
                logger.error(f"GraphQL request failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"GraphQL request exception: {e}")
            return None
    
    def get_updated_pages(self, date: str, site_name: str = "health", post_type: str = "SEM") -> List[Dict]:
        """
        Get pages updated on a specific date
        
        Args:
            date: Date in YYYY-MM-DD format
            site_name: Site name to filter by (default: "health")
            post_type: Post type to filter by (default: "SEM")
            
        Returns:
            List of updated page dictionaries
        """
        query = """
        query GetPosts($siteName: String, $postType: String, $postUrlContains: String, $lastModifiedPrefix: String, $first: Int!, $offset: Int!) {
            postsConnection(
                condition: {siteName: $siteName, postType: $postType}, 
                first: $first, 
                offset: $offset, 
                filter: { 
                    postUrl: { includesInsensitive: $postUrlContains }, 
                    lastModifiedDate: { includesInsensitive: $lastModifiedPrefix } 
                }, 
                orderBy: LAST_MODIFIED_DATE_DESC
            ) {
                nodes {
                    postUrl
                    publishedDate
                    lastModifiedDate
                    annotationsConnection {
                        totalCount
                    }
                    crawlMetadatum {
                        htmlPath
                        crawlDate
                    }
                }
            }
        }
        """
        
        variables = {
            "siteName": site_name,
            "postType": post_type,
            "postUrlContains": None,  # No URL filtering
            "lastModifiedPrefix": date,  # Filter by date
            "first": 100,  # Get up to 100 results per request
            "offset": 0
        }
        
        all_pages = []
        
        # Paginate through results
        while True:
            logger.info(f"Fetching pages for {date}, offset: {variables['offset']}")
            
            data = self._make_graphql_request(query, variables)
            if not data:
                logger.error("Failed to get GraphQL data")
                break
            
            posts_connection = data.get("postsConnection")
            if not posts_connection:
                logger.error("No postsConnection in response")
                break
            
            nodes = posts_connection.get("nodes", [])
            if not nodes:
                logger.info(f"No more pages found, stopping pagination")
                break
            
            # Process nodes and add to results
            for node in nodes:
                page_data = {
                    "url": node.get("postUrl"),
                    "published_date": node.get("publishedDate"),
                    "last_modified_date": node.get("lastModifiedDate"),
                    "annotations_count": node.get("annotationsConnection", {}).get("totalCount", 0),
                    "crawl_data": node.get("crawlMetadatum")
                }
                all_pages.append(page_data)
            
            # Update offset for next iteration
            variables["offset"] += len(nodes)
            
            # Break if we got fewer results than requested (end of data)
            if len(nodes) < variables["first"]:
                break
        
        logger.info(f"Found {len(all_pages)} updated pages for {date}")
        return all_pages
    
    def get_all_updated_pages(self, target_date: str = None) -> List[Dict]:
        """
        Get all pages updated on the target date (defaults to yesterday)
        
        Args:
            target_date: Date in YYYY-MM-DD format (defaults to yesterday)
            
        Returns:
            List of updated page dictionaries
        """
        if not target_date:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        logger.info(f"Getting all updated pages for {target_date}")
        return self.get_updated_pages(target_date)
    
    def get_updated_pages_last_n_days(self, days: int = 1) -> List[Dict]:
        """
        Get all pages updated in the last N days
        
        Args:
            days: Number of days to look back
            
        Returns:
            List of updated page dictionaries
        """
        all_pages = []
        for i in range(days):
            date = (datetime.now() - timedelta(days=i+1)).strftime("%Y-%m-%d")
            pages = self.get_updated_pages(date)
            all_pages.extend(pages)
        
        logger.info(f"Found {len(all_pages)} pages updated in last {days} day(s)")
        return all_pages
    
    def get_posts_by_url(self, site_name: str, post_url_contains: str, limit: int = 1) -> List[Dict]:
        """
        Get posts by URL pattern
        
        Args:
            site_name: Site name to filter by
            post_url_contains: URL pattern to search for
            limit: Maximum number of results
            
        Returns:
            List of matching page dictionaries
        """
        query = """
        query GetPosts($siteName: String, $postType: String, $postUrlContains: String, $first: Int!, $offset: Int!) {
            postsConnection(
                condition: {siteName: $siteName, postType: $postType}, 
                first: $first, 
                offset: $offset, 
                filter: { 
                    postUrl: { includesInsensitive: $postUrlContains }
                }, 
                orderBy: LAST_MODIFIED_DATE_DESC
            ) {
                nodes {
                    postUrl
                    publishedDate
                    lastModifiedDate
                    annotationsConnection {
                        totalCount
                    }
                    crawlMetadatum {
                        htmlPath
                        crawlDate
                    }
                }
            }
        }
        """
        
        variables = {
            "siteName": site_name,
            "postType": "SEM",
            "postUrlContains": post_url_contains,
            "first": limit,
            "offset": 0
        }
        
        data = self._make_graphql_request(query, variables)
        if not data:
            return []
        
        posts_connection = data.get("postsConnection")
        if not posts_connection:
            return []
        
        nodes = posts_connection.get("nodes", [])
        
        results = []
        for node in nodes:
            page_data = {
                "url": node.get("postUrl"),
                "published_date": node.get("publishedDate"),
                "last_modified_date": node.get("lastModifiedDate"),
                "annotations_count": node.get("annotationsConnection", {}).get("totalCount", 0),
                "crawl_data": node.get("crawlMetadatum")
            }
            results.append(page_data)
        
        return results
    
    def test_connection(self) -> bool:
        """
        Test the GraphQL connection with a simple query
        
        Returns:
            True if connection successful, False otherwise
        """
        logger.info("Testing GraphQL connection...")
        
        # Simple query to test connectivity
        query = """
        query TestConnection {
            postsConnection(first: 1, offset: 0) {
                nodes {
                    postUrl
                }
            }
        }
        """
        
        variables = {}
        
        data = self._make_graphql_request(query, variables)
        success = data is not None
        
        if success:
            logger.info("✅ GraphQL connection test successful")
        else:
            logger.error("❌ GraphQL connection test failed")
        
        return success

# Global service instance
crawler_graphql_service = CrawlerGraphQLService()




