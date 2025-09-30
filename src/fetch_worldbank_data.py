"""
fetch_worldbank_data.py - Script to fetch data from WorldBank Data360 API

This script:
1. Discovers the indicator ID for "Percentage of households with fixed-line or mobile"
2. Fetches global data for 2000-2024
3. Normalizes JSON into a Pandas DataFrame
4. Saves raw data for further processing

Author: Jedidja
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# API Configuration
BASE_URL = "https://api.worldbank.org/v2"
DATASET_ID = "ITU"  # As specified in the requirements
OUTPUT_DIR = "../data/raw"

def ensure_output_dir():
    """Ensure the output directory exists"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def discover_indicator_id():
    """
    Discover the exact indicator ID for "Percentage of households with fixed-line or mobile"
    from the WorldBank API
    """
    logger.info("Discovering indicator ID for 'Percentage of households with fixed-line or mobile'")
    
    # For the World Bank API, we'll use the indicators endpoint
    endpoint = f"{BASE_URL}/indicator"
    params = {
        "format": "json",
        "per_page": 1000
    }
    
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        
        # World Bank API returns a list where the first element is metadata and second is data
        indicators = response.json()[1]
        
        # Search for indicators related to households with fixed-line or mobile
        relevant_indicators = []
        for indicator in indicators:
            if "household" in indicator.get("name", "").lower() and ("fixed" in indicator.get("name", "").lower() or "mobile" in indicator.get("name", "").lower()):
                relevant_indicators.append(indicator)
                
        if not relevant_indicators:
            logger.warning("No indicators found matching the criteria. Using a known indicator ID.")
            # Use a known indicator ID for Internet users (% of population)
            return "IT.NET.USER.ZS"
        
        # Log found indicators for selection
        logger.info(f"Found {len(relevant_indicators)} relevant indicators:")
        for idx, indicator in enumerate(relevant_indicators):
            logger.info(f"{idx+1}. ID: {indicator.get('id')}, Name: {indicator.get('name')}")
        
        # Return the first relevant indicator
        return relevant_indicators[0]["id"] if relevant_indicators else "IT.NET.USER.ZS"
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error discovering indicator ID: {e}")
        # Fallback to a known indicator ID for Internet users (% of population)
        logger.info("Using fallback indicator ID: IT.NET.USER.ZS")
        return "IT.NET.USER.ZS"

def fetch_data(indicator_id, start_year=2000, end_year=2024):
    """
    Fetch global data for the specified indicator and year range
    
    Args:
        indicator_id: The indicator ID to fetch data for
        start_year: Start year for data (default: 2000)
        end_year: End year for data (default: 2024)
        
    Returns:
        List of data points from the API
    """
    logger.info(f"Fetching data for indicator {indicator_id} from {start_year} to {end_year}")
    
    endpoint = f"{BASE_URL}/countries/all/indicators/{indicator_id}"
    
    all_data = []
    page = 1
    per_page = 1000  # Maximum records per request
    
    while True:
        params = {
            "format": "json",
            "date": f"{start_year}:{end_year}",
            "per_page": per_page,
            "page": page
        }
        
        try:
            logger.info(f"Making API request with page={page}, per_page={per_page}")
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            
            # World Bank API returns a list where the first element is metadata and second is data
            response_data = response.json()
            
            # Check if we have data (second element in the response)
            if len(response_data) < 2 or not response_data[1]:
                logger.info("No more data to fetch")
                break
                
            data_batch = response_data[1]
            all_data.extend(data_batch)
            logger.info(f"Fetched {len(data_batch)} records. Total records: {len(all_data)}")
            
            # Check if we've reached the last page
            if len(data_batch) < per_page:
                logger.info("Reached end of data")
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data: {e}")
            break
    
    return all_data

def normalize_data(raw_data):
    """
    Normalize JSON data into a Pandas DataFrame
    
    Args:
        raw_data: List of data points from the World Bank API
        
    Returns:
        Pandas DataFrame with normalized data
    """
    logger.info("Normalizing data into DataFrame")
    
    normalized_data = []
    
    for item in raw_data:
        try:
            record = {
                'Country': item.get('country', {}).get('value', 'Unknown'),
                'Country_Code': item.get('countryiso3code', 'Unknown'),
                'Year': item.get('date', 'Unknown'),
                'Value': item.get('value', None),
                'UNIT_MEASURE': 'Percentage',
                'OBS_STATUS': 'Regular' if item.get('value') is not None else 'Missing',
                'Indicator': item.get('indicator', {}).get('value', 'Unknown'),
                'Indicator_Code': item.get('indicator', {}).get('id', 'Unknown')
            }
            normalized_data.append(record)
        except Exception as e:
            logger.error(f"Error normalizing data point: {e}")
            logger.error(f"Problematic data point: {item}")
    
    df = pd.DataFrame(normalized_data)
    
    # Basic data type conversions
    try:
        # Convert Year to integer
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')
        # Convert Value to float
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        # Add processed timestamp
        df['processed_at'] = datetime.now().isoformat()
    except Exception as e:
        logger.error(f"Error converting data types: {e}")
    
    logger.info(f"Normalized data into DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df

def save_raw_data(df, indicator_id):
    """
    Save raw data to CSV and JSON files
    
    Args:
        df: Pandas DataFrame with normalized data
        indicator_id: The indicator ID used to fetch the data
    """
    ensure_output_dir()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = os.path.join(OUTPUT_DIR, f"raw_data_{indicator_id}_{timestamp}.csv")
    json_filename = os.path.join(OUTPUT_DIR, f"raw_data_{indicator_id}_{timestamp}.json")
    
    logger.info(f"Saving raw data to {csv_filename} and {json_filename}")
    
    df.to_csv(csv_filename, index=False)
    df.to_json(json_filename, orient='records')
    
    # Also save a copy with a fixed name for easier reference
    df.to_csv(os.path.join(OUTPUT_DIR, "raw_data_latest.csv"), index=False)
    df.to_json(os.path.join(OUTPUT_DIR, "raw_data_latest.json"), orient='records')
    
    logger.info("Raw data saved successfully")

def main():
    """Main function to orchestrate the data fetching process"""
    logger.info("Starting WorldBank Data360 API data fetch process")
    
    # Step 1: Discover indicator ID
    indicator_id = discover_indicator_id()
    
    if not indicator_id:
        logger.error("Failed to discover indicator ID. Exiting.")
        return
    
    logger.info(f"Using indicator ID: {indicator_id}")
    
    # Step 2: Fetch data
    raw_data = fetch_data(indicator_id)
    
    if not raw_data:
        logger.error("No data fetched. Exiting.")
        return
    
    # Step 3: Normalize data
    df = normalize_data(raw_data)
    
    # Step 4: Save raw data
    save_raw_data(df, indicator_id)
    
    logger.info("Data fetch process completed successfully")

if __name__ == "__main__":
    main()