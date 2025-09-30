"""
generate_eda.py - Script to perform EDA and generate visualizations

This script:
1. Loads the analytics-ready data
2. Performs exploratory data analysis (EDA)
3. Creates visualizations

Author: Jedidja
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
DATA_DIR = "../data"
PLOTS_DIR = "../analysis/plots"

def ensure_dirs():
    """Ensure output directories exist"""
    os.makedirs(PLOTS_DIR, exist_ok=True)

def load_data():
    """Load the analytics-ready data"""
    latest_file = os.path.join(DATA_DIR, "cleaned_data_latest.csv")
    
    if not os.path.exists(latest_file):
        logger.error(f"Data file not found: {latest_file}")
        return None
    
    logger.info(f"Loading data from {latest_file}")
    df = pd.read_csv(latest_file)
    return df

def create_visualizations(df):
    """Create visualizations for the EDA results"""
    if df is None or df.empty:
        logger.error("No data for visualizations")
        return []
    
    logger.info("Creating visualizations")
    ensure_dirs()
    
    visualizations = []
    
    # Set the style
    sns.set(style="whitegrid")
    
    # 1. Global trend over time
    try:
        plt.figure(figsize=(12, 8))
        global_avg = df.groupby('Year')['Value'].mean().reset_index()
        plt.plot(global_avg['Year'], global_avg['Value'], marker='o', linewidth=2)
        plt.title('Global Average Connectivity Percentage (2000-2024)')
        plt.xlabel('Year')
        plt.ylabel('Connectivity Percentage')
        plt.grid(True, alpha=0.3)
        
        filename = os.path.join(PLOTS_DIR, 'global_trend.png')
        plt.savefig(filename)
        plt.close()
        visualizations.append(filename)
    except Exception as e:
        logger.error(f"Error creating global trend visualization: {e}")
    
    # 2. Namibia trend
    try:
        namibia_data = df[df['Country'] == 'Namibia'].sort_values('Year')
        if not namibia_data.empty:
            plt.figure(figsize=(12, 8))
            plt.plot(namibia_data['Year'], namibia_data['Value'], marker='o', linewidth=2, color='orange')
            global_avg = df.groupby('Year')['Value'].mean().reset_index()
            plt.plot(global_avg['Year'], global_avg['Value'], marker='', linewidth=2, color='blue', alpha=0.7, linestyle='--')
            plt.title('Namibia vs Global Average Connectivity Percentage')
            plt.xlabel('Year')
            plt.ylabel('Connectivity Percentage')
            plt.legend(['Namibia', 'Global Average'])
            
            filename = os.path.join(PLOTS_DIR, 'namibia_trend.png')
            plt.savefig(filename)
            plt.close()
            visualizations.append(filename)
    except Exception as e:
        logger.error(f"Error creating Namibia trend visualization: {e}")
    
    logger.info(f"Created {len(visualizations)} visualizations")
    return visualizations

def main():
    """Main function to orchestrate the EDA process"""
    logger.info("Starting EDA process")
    
    # Load data
    df = load_data()
    
    if df is None:
        logger.error("Failed to load data. Exiting.")
        return
    
    # Create visualizations
    create_visualizations(df)
    
    logger.info("EDA process completed successfully")

if __name__ == "__main__":
    main()