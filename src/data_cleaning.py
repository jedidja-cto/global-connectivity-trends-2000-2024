"""
data_cleaning.py - Script to clean and transform the raw data

This script:
1. Loads raw data from CSV/JSON
2. Cleans and transforms the data
3. Saves the cleaned data for dbt processing

Author: Jedidja
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Configuration
INPUT_DIR = "../data/raw"
OUTPUT_DIR = "../data/processed"

def load_raw_data():
    """
    Load the latest raw data from CSV
    
    Returns:
        Pandas DataFrame with raw data
    """
    latest_file = os.path.join(INPUT_DIR, "raw_data_latest.csv")
    
    if not os.path.exists(latest_file):
        logger.error(f"Raw data file not found: {latest_file}")
        return None
    
    logger.info(f"Loading raw data from {latest_file}")
    df = pd.read_csv(latest_file)
    logger.info(f"Loaded {len(df)} rows of raw data")
    
    return df

def clean_data(df):
    """
    Clean and transform the raw data
    
    Args:
        df: Pandas DataFrame with raw data
        
    Returns:
        Pandas DataFrame with cleaned data
    """
    if df is None or df.empty:
        logger.error("No data to clean")
        return None
    
    logger.info("Starting data cleaning process")
    
    # Make a copy to avoid modifying the original
    cleaned_df = df.copy()
    
    # 1. Handle missing values
    logger.info("Handling missing values")
    missing_before = cleaned_df.isna().sum().sum()
    
    # Fill missing country names with 'Unknown'
    cleaned_df['Country'] = cleaned_df['Country'].fillna('Unknown')
    
    # Log missing values by column
    for col in cleaned_df.columns:
        missing_count = cleaned_df[col].isna().sum()
        if missing_count > 0:
            logger.info(f"Column '{col}' has {missing_count} missing values ({missing_count/len(cleaned_df)*100:.2f}%)")
    
    # 2. Standardize country names
    logger.info("Standardizing country names")
    
    # Common country name variations to standardize
    country_mapping = {
        'United States': 'United States of America',
        'USA': 'United States of America',
        'US': 'United States of America',
        'UK': 'United Kingdom',
        'Great Britain': 'United Kingdom',
        'Republic of Korea': 'South Korea',
        'Korea, Rep.': 'South Korea',
        'Korea, Dem. Rep.': 'North Korea',
        'Congo, Dem. Rep.': 'Democratic Republic of the Congo',
        'Congo, Rep.': 'Republic of Congo',
        'Viet Nam': 'Vietnam',
        'Russian Federation': 'Russia',
        'Iran, Islamic Rep.': 'Iran',
        'Egypt, Arab Rep.': 'Egypt',
        'Hong Kong SAR, China': 'Hong Kong',
        'Macao SAR, China': 'Macao'
    }
    
    # Apply country name standardization
    cleaned_df['Country'] = cleaned_df['Country'].replace(country_mapping)
    
    # 3. Convert percentages to numeric
    logger.info("Converting values to numeric")
    cleaned_df['Value'] = pd.to_numeric(cleaned_df['Value'], errors='coerce')
    
    # 4. Create year as integer
    if 'Year' in cleaned_df.columns:
        cleaned_df['Year'] = pd.to_numeric(cleaned_df['Year'], errors='coerce').astype('Int64')
    
    # 5. Add metadata columns
    cleaned_df['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 6. Drop duplicates
    logger.info("Removing duplicates")
    dups_before = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates()
    dups_removed = dups_before - len(cleaned_df)
    logger.info(f"Removed {dups_removed} duplicate rows")
    
    # 7. Create a clean indicator name
    if 'Indicator' in cleaned_df.columns:
        # Extract a cleaner name from the indicator
        indicator_name = cleaned_df['Indicator'].iloc[0] if not cleaned_df.empty else "Unknown"
        cleaned_df['indicator_clean'] = indicator_name
        
        # Try to determine if this is fixed-line or mobile data
        if 'fixed' in indicator_name.lower():
            cleaned_df['connection_type'] = 'fixed-line'
        elif 'mobile' in indicator_name.lower():
            cleaned_df['connection_type'] = 'mobile'
        else:
            cleaned_df['connection_type'] = 'unknown'
    
    # Log cleaning results
    missing_after = cleaned_df.isna().sum().sum()
    logger.info(f"Data cleaning complete. Rows: {len(cleaned_df)}, Missing values before: {missing_before}, after: {missing_after}")
    
    return cleaned_df

def save_cleaned_data(df):
    """
    Save cleaned data to CSV and JSON files
    
    Args:
        df: Pandas DataFrame with cleaned data
    """
    if df is None or df.empty:
        logger.error("No cleaned data to save")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = os.path.join(OUTPUT_DIR, f"cleaned_data_{timestamp}.csv")
    json_filename = os.path.join(OUTPUT_DIR, f"cleaned_data_{timestamp}.json")
    
    logger.info(f"Saving cleaned data to {csv_filename} and {json_filename}")
    
    df.to_csv(csv_filename, index=False)
    df.to_json(json_filename, orient='records')
    
    # Also save a copy with a fixed name for easier reference
    df.to_csv(os.path.join(OUTPUT_DIR, "cleaned_data_latest.csv"), index=False)
    df.to_json(os.path.join(OUTPUT_DIR, "cleaned_data_latest.json"), orient='records')
    
    logger.info("Cleaned data saved successfully")

def main():
    """Main function to orchestrate the data cleaning process"""
    logger.info("Starting data cleaning process")
    
    # Step 1: Load raw data
    raw_df = load_raw_data()
    
    if raw_df is None:
        logger.error("Failed to load raw data. Exiting.")
        return
    
    # Step 2: Clean data
    cleaned_df = clean_data(raw_df)
    
    if cleaned_df is None:
        logger.error("Data cleaning failed. Exiting.")
        return
    
    # Step 3: Save cleaned data
    save_cleaned_data(cleaned_df)
    
    logger.info("Data cleaning process completed successfully")

if __name__ == "__main__":
    main()