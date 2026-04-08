import requests
import json
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class CSIRODapClient:
    """Client for the CSIRO Data Access Portal (DAP) v2 REST API."""
    
    BASE_URL = "https://data.csiro.au/dap/ws/v2"

    def __init__(self):
        self.session = requests.Session()

    def search_collections(self, query: str, keyword_operator: str = "AND") -> List[Dict]:
        """
        Search for data collections using the `q` parameter.
        Returns a list of collection metadata dictionaries.
        """
        url = f"{self.BASE_URL}/collections.json"
        
        # Format the query appropriately. 
        # Example for Storm Bay hydrodynamic: q=("Storm Bay") AND (hydrodynamic OR model)
        params = {"q": query}
        
        logger.info(f"Searching DAP collections with params: {params}")
        response = self.session.get(url, params=params)
        
        if response.status_code != 200:
            logger.error(f"DAP search failed: {response.status_code} - {response.text}")
            return []
            
        data = response.json()
        return data.get("dataCollections", [])

    def get_collection_details(self, collection_url: str) -> Optional[Dict]:
        """
        Fetch full details for a specific collection utilizing its self URI.
        Expects a .json suffix or injects one if needed.
        """
        # Ensure we ask for JSON
        if ".xml" in collection_url:
            collection_url = collection_url.replace(".xml", ".json", 1)
        elif not collection_url.endswith(".json"):
            collection_url = f"{collection_url}.json"
            
        response = self.session.get(collection_url)
        if response.status_code == 200:
            return response.json().get("dataCollection", {})
        else:
            logger.error(f"Failed to fetch collection details from {collection_url}: {response.status_code}")
            return None

    def list_files_for_collection(self, collection_data: Dict) -> List[Dict]:
        """
        Given the collection details dictionary, fetch the file list.
        """
        data_url = collection_data.get("data")
        if not data_url:
            logger.warning("No data URI found for collection.")
            return []
            
        if ".xml" in data_url:
            data_url = data_url.replace(".xml", ".json", 1)
        elif not data_url.endswith(".json"):
            data_url = f"{data_url}.json"
            
        response = self.session.get(data_url)
        if response.status_code == 200:
            return response.json().get("files", [])
        else:
            logger.error(f"Failed to fetch files from {data_url}: {response.status_code}")
            return []
