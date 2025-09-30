"""
generate_visualizations.py - Script to create visualizations from processed data

This script:
1. Loads processed data
2. Creates visualizations for global connectivity trends
3. Saves plots to the plots directory

Author: Jedidja
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np
from datetime import datetime

# Set style
sns.set(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 8)

# Configuration
INPUT_DIR = "data/processed"
OUTPUT_DIR = "analysis/plots"

def ensure_output_dir():
    """Ensure the output directory exists"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_processed_data():
    """
    Load the latest processed data
    
    Returns:
        Pandas DataFrame with processed data
    """
    # Find the latest file by sorting filenames
    files = [f for f in os.listdir(INPUT_DIR) if f.startswith('cleaned_data_') and f.endswith('.csv')]
    if not files:
        print("No processed data files found")
        return None
    
    latest_file = sorted(files)[-1]
    file_path = os.path.join(INPUT_DIR, latest_file)
    
    print(f"Loading processed data from {file_path}")
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows of processed data")
    
    return df

def plot_global_trend(df):
    """Create a plot showing global connectivity trend over time"""
    print("Creating global trend plot")
    
    # Calculate global average by year
    global_avg = df.groupby('Year')['Value'].mean().reset_index()
    
    plt.figure(figsize=(12, 6))
    plt.plot(global_avg['Year'], global_avg['Value'], marker='o', linewidth=2)
    plt.title('Global Internet Usage (% of Population) 2000-2024', fontsize=16)
    plt.xlabel('Year', fontsize=12)
    plt.ylabel('Internet Users (% of Population)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Save the plot
    output_path = os.path.join(OUTPUT_DIR, 'global_trend.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved global trend plot to {output_path}")

def plot_top_countries(df, year=2020, n=10):
    """Create a plot showing top countries by connectivity"""
    print(f"Creating top countries plot for year {year}")
    
    # Filter for the specified year and get top countries
    year_data = df[df['Year'] == year].copy()
    if year_data.empty:
        print(f"No data available for year {year}")
        # Try to find the most recent year with data
        available_years = sorted(df['Year'].unique())
        if not available_years:
            print("No data available for any year")
            return
        year = available_years[-1]
        year_data = df[df['Year'] == year].copy()
        print(f"Using most recent year with data: {year}")
    
    # Get top countries
    top_countries = year_data.sort_values('Value', ascending=False).head(n)
    
    plt.figure(figsize=(10, 8))
    bars = plt.barh(top_countries['Country'], top_countries['Value'], color='skyblue')
    plt.title(f'Top {n} Countries by Internet Usage (% of Population) in {year}', fontsize=16)
    plt.xlabel('Internet Users (% of Population)', fontsize=12)
    plt.ylabel('Country', fontsize=12)
    plt.grid(True, alpha=0.3, axis='x')
    
    # Add value labels to the bars
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 1, bar.get_y() + bar.get_height()/2, f'{width:.1f}%', 
                 ha='left', va='center', fontsize=10)
    
    plt.tight_layout()
    
    # Save the plot
    output_path = os.path.join(OUTPUT_DIR, f'top_countries_{year}.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved top countries plot to {output_path}")

def plot_regional_comparison(df, year=2020):
    """Create a plot comparing regions"""
    print(f"Creating regional comparison plot for year {year}")
    
    # Define regions (simplified)
    regions = {
        'North America': ['United States', 'Canada', 'Mexico'],
        'Europe': ['United Kingdom', 'Germany', 'France', 'Italy', 'Spain'],
        'Asia': ['China', 'Japan', 'India', 'South Korea', 'Indonesia'],
        'Africa': ['South Africa', 'Nigeria', 'Kenya', 'Egypt', 'Morocco'],
        'South America': ['Brazil', 'Argentina', 'Colombia', 'Chile', 'Peru'],
        'Oceania': ['Australia', 'New Zealand']
    }
    
    # Filter for the specified year
    year_data = df[df['Year'] == year].copy()
    if year_data.empty:
        print(f"No data available for year {year}")
        # Try to find the most recent year with data
        available_years = sorted(df['Year'].unique())
        if not available_years:
            print("No data available for any year")
            return
        year = available_years[-1]
        year_data = df[df['Year'] == year].copy()
        print(f"Using most recent year with data: {year}")
    
    # Calculate regional averages
    regional_avgs = []
    for region, countries in regions.items():
        region_data = year_data[year_data['Country'].isin(countries)]
        if not region_data.empty:
            avg = region_data['Value'].mean()
            regional_avgs.append({'Region': region, 'Value': avg})
    
    regional_df = pd.DataFrame(regional_avgs)
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(regional_df['Region'], regional_df['Value'], color=sns.color_palette("viridis", len(regional_df)))
    plt.title(f'Regional Comparison of Internet Usage (% of Population) in {year}', fontsize=16)
    plt.xlabel('Region', fontsize=12)
    plt.ylabel('Internet Users (% of Population)', fontsize=12)
    plt.grid(True, alpha=0.3, axis='y')
    plt.xticks(rotation=45)
    
    # Add value labels to the bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 1, f'{height:.1f}%', 
                 ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    
    # Save the plot
    output_path = os.path.join(OUTPUT_DIR, f'regional_comparison_{year}.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved regional comparison plot to {output_path}")

def main():
    """Main function to orchestrate the visualization process"""
    print("Starting visualization generation process")
    
    # Ensure output directory exists
    ensure_output_dir()
    
    # Load processed data
    df = load_processed_data()
    
    if df is None or df.empty:
        print("No data available for visualization")
        return
    
    # Create visualizations
    plot_global_trend(df)
    
    # Find the most recent year with data
    available_years = sorted(df['Year'].unique())
    if available_years:
        recent_year = available_years[-1]
        plot_top_countries(df, year=recent_year)
        plot_regional_comparison(df, year=recent_year)
    
    print("Visualization generation process completed successfully")

if __name__ == "__main__":
    main()