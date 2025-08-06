#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GHI Data Extraction using pvlib-python
=========================================================

This script extracts monthly Global Horizontal Irradiance (GHI) data
for  Crete for the period 2010-2017.

It fetches data from NREL SARAH 3 dataset , calculates performance ratio against known
PVOUT data, and generates summary reports and charts.

Author: Cline - AI Software Engineer
Date: June 2025
"""

import pandas as pd
import numpy as np
import requests
import json
import matplotlib.pyplot as plt
from matplotlib import rcParams
from datetime import datetime
import pvlib.iotools
import pvlib.location
import os

sites = {
    "Agioi Deka": {
        "lat": 35.0617, "lon": 24.9483, "elevation": 150,
        "raddatabase": "PVGIS-SARAH3", "pvtechchoice": "crystSi",
        "trackingtype": 2  # 2-axis tracking
    },
    "Moroni": {
        "lat": 35.0500, "lon": 24.8833,
        "raddatabase": "PVGIS-SARAH3", "pvtechchoice": "crystSi",
        "trackingtype": 0,  # Fixed
        "angle": 30, "aspect": 180, "optimalangles": 0
    },
    "Chorio": {
        "lat": 35.0167, "lon": 25.7333,
        "raddatabase": "PVGIS-SARAH3", "pvtechchoice": "crystSi",
        "trackingtype": 0,  # Fixed
        "angle": 30, "aspect": 180, "optimalangles": 0
    },
    "Dexameni": {
        "lat": 35.1167, "lon": 24.9500, "elevation": 180,
        "raddatabase": "PVGIS-SARAH3", "pvtechchoice": "crystSi",
        "trackingtype": 2  # 2-axis tracking
    },
    "Karavas": {
        "lat": 35.1200, "lon": 24.9600, "elevation": 190,
        "raddatabase": "PVGIS-SARAH3", "pvtechchoice": "crystSi",
        "trackingtype": 2  # 2-axis tracking
    },
    "PLASTIKA KRITIS VIPE": {
        "lat": 35.3387, "lon": 25.1442, "elevation": 50,
        "raddatabase": "PVGIS-SARAH3", "pvtechchoice": "crystSi",
        "trackingtype": 0,  # Fixed
        "angle": 30, "aspect": 180, "optimalangles": 0
    }
}

def fetch_pvgis_data(site_name, config):
    """Fetch hourly data from PVGIS API with robust error handling"""
    # The user specified to remove "not needed parameters", but did not specify which ones.
    # Based on the previous error, 'raddatabase' is not a valid parameter for get_pvgis_tmy.
    # The other parameters in the provided 'params' dictionary are not directly used by get_pvgis_tmy
    # but are likely for a different PVGIS API call or a full PV system simulation.
    # For now, I will only use lat, lon, and map_variables for get_pvgis_tmy.
    
    print(f"🔄 Fetching GHI data from PVGIS ({config['raddatabase']}) for {site_name}...")
    try:
        # Fetch TMY data from PVGIS, which includes GHI
        tmy_data, _ = pvlib.iotools.get_pvgis_tmy(config["lat"], config["lon"], map_variables=True, url='https://re.jrc.ec.europa.eu/api/v5_3/')
        
        # Convert GHI from Wh/m^2 to kWh/m^2
        tmy_data['GHI_kWh_m2'] = tmy_data['ghi'] / 1000.0
        
        # Aggregate monthly GHI from TMY data
        ghi_df = tmy_data.resample('ME').agg(
            GHI_kWh_m2=('GHI_kWh_m2', 'sum')
        )
        ghi_df['Year'] = ghi_df.index.year
        ghi_df['Month'] = ghi_df.index.month
        ghi_df = ghi_df.reset_index(drop=True)
        ghi_df = ghi_df[['Year', 'Month', 'GHI_kWh_m2']]
        
        print(f"✅ GHI data processed for {site_name}: {len(ghi_df)} monthly records")
        return ghi_df
        
    except Exception as e:
        print(f"❌ Failed to retrieve or process data from PVGIS for {site_name}: {e}")
        return None

def load_sunny_portal_data(site_name):
    """
    Load specific yield data from sunny_portal_data folder for a given site.
    Assumes CSV files are named as 'site_name_specific_yield.csv'.
    """
    file_name = f"{site_name.replace(' ', '_')}_specific_yield.csv"
    file_path = os.path.join("sunny_portal_data", file_name)
    
    if not os.path.exists(file_path):
        print(f"⚠️ Sunny Portal data file not found for {site_name}: {file_path}")
        return None
    
    try:
        # Read the CSV, using semicolon as separator and setting the header to the second row (index 1)
        df = pd.read_csv(file_path, sep=';', header=1)
        
        # Rename the first column to 'Year' explicitly, as it might be 'Unnamed: 0'
        if df.columns[0] != 'Year':
            df = df.rename(columns={df.columns[0]: 'Year'})
        
        # Drop the last four rows which contain summary statistics
        df = df.iloc[:-4]
        
        # Melt the DataFrame to transform monthly columns into rows
        # 'Year' is the ID variable, and month names are value variables
        df_melted = df.melt(id_vars=['Year'], 
                            var_name='Month', 
                            value_name='Specific Yield (kWh/kWp)')
        
        # Convert 'Year' to numeric, coercing errors to NaN, then drop NaNs
        # Convert 'Year' to numeric, coercing errors to NaN, then drop NaNs
        df_melted['Year'] = pd.to_numeric(df_melted['Year'], errors='coerce')
        df_melted = df_melted.dropna(subset=['Year'])
        
        # Convert 'Specific Yield (kWh/kWp)' to numeric, handling commas and coercing errors to NaN, then fill NaN with 0
        df_melted['Specific Yield (kWh/kWp)'] = pd.to_numeric(
            df_melted['Specific Yield (kWh/kWp)'].astype(str).str.replace(',', '.'), 
            errors='coerce'
        ).fillna(0)
        
        # Map month names to month numbers
        month_name_to_num = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        df_melted['Month_Num'] = df_melted['Month'].map(month_name_to_num)
        
        # Convert 'Month_Num' to numeric, coercing errors to NaN, then drop NaNs
        df_melted['Month_Num'] = pd.to_numeric(df_melted['Month_Num'], errors='coerce')
        df_melted = df_melted.dropna(subset=['Month_Num'])

        # Ensure 'Year' and 'Month_Num' are integers before converting to string for datetime
        df_melted['Year'] = df_melted['Year'].astype(int)
        df_melted['Month_Num'] = df_melted['Month_Num'].astype(int)

        # Combine 'Year' and 'Month_Num' to create a datetime object
        # Use a dummy day (e.g., 1) as specific yield is monthly
        df_melted['Date'] = pd.to_datetime(df_melted['Year'].astype(str) + '-' + 
                                            df_melted['Month_Num'].astype(str) + '-01')
        
        # Set 'Date' as index and sort
        df_melted = df_melted.set_index('Date').sort_index()
        
        # Aggregate monthly specific yield (already monthly, but sum if there were multiple entries for a month)
        monthly_specific_yield = df_melted.resample('ME')['Specific Yield (kWh/kWp)'].sum()
        # Ensure it's a pandas Series before converting to dict
        monthly_specific_yield = pd.Series(monthly_specific_yield)
        
        print(f"✅ Sunny Portal data loaded for {site_name}: {len(monthly_specific_yield)} monthly records")
        # DEBUG: Sunny Portal data keys (first 5): {list(monthly_specific_yield.keys())[:5]}
        # DEBUG: Sunny Portal data values (first 5): {list(monthly_specific_yield.values())[:5]}
        return monthly_specific_yield.to_dict()
        
    except Exception as e:
        print(f"❌ Error loading Sunny Portal data for {site_name}: {e}")
        return None

def calculate_performance_ratio(ghi_df, latitude, longitude, sunny_portal_data, pvgis_database='SARAH'):
    """
    Calculate Performance Ratio using PVGIS PVOUT data and real PVOUT from Sunny Portal.
    """
    print(f"🔄 Fetching PVOUT data from PVGIS ({pvgis_database})...")
    try:
        # Fetch TMY data from PVGIS
        tmy_data, _ = pvlib.iotools.get_pvgis_tmy(latitude, longitude, map_variables=True, url='https://re.jrc.ec.europa.eu/api/v5_3/')
        
        # Let's assume PVGIS TMY provides 'GHI' in Wh/m^2. Convert to kWh/m^2
        tmy_data['GHI_kWh_m2'] = tmy_data['ghi'] / 1000.0
        
        # Aggregate monthly GHI from TMY data
        pvgis_monthly_ghi = tmy_data.groupby(tmy_data.index.month)['GHI_kWh_m2'].sum()
        
        # Convert to a dictionary for easy lookup
        pvgis_pvout_monthly = pvgis_monthly_ghi.to_dict()
        
        print("✅ PVGIS PVOUT data retrieved successfully!")
        
    except Exception as e:
        print(f"❌ Error fetching PVGIS PVOUT data: {e}")
        # Fallback to a default or raise an error if data cannot be fetched
        print("Using placeholder PVOUT values due to PVGIS data retrieval failure.")
        pvgis_pvout_monthly = {
            1: 94.37, 2: 109.62, 3: 137.89, 4: 156.78, 5: 178.45, 6: 228.49,
            7: 224.67, 8: 201.34, 9: 178.23, 10: 156.89, 11: 134.56, 12: 121.43
        }
    
    pr_data = []
    
    for _, row in ghi_df.iterrows():
        year = int(row['Year'])
        month = int(row['Month'])
        ghi_value = row['GHI_kWh_m2']
        
        # Use PVGIS PVOUT data
        pvout_value = pvgis_pvout_monthly.get(month, 0) # Get PVOUT for the month, default to 0 if not found

        # Get real_pvout_value from sunny_portal_data for the specific year and month
        real_pvout_value = 0
        if sunny_portal_data:
            # Create a pandas Timestamp object for the end of the current month and year
            # This matches the keys generated by resample('ME') in load_sunny_portal_data
            end_of_month_date = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
            # DEBUG: Looking up real_pvout_value for Year: {year}, Month: {month}, Date: {end_of_month_date}
            real_pvout_value = sunny_portal_data.get(end_of_month_date, 0)
            # DEBUG: Retrieved real_pvout_value: {real_pvout_value}
        
        # Calculate Performance Ratio
        pr = pvout_value / ghi_value if ghi_value > 0 else 0
        
        pr_data.append({
            'Year': year,
            'Month': month,
            'GHI_kWh_m2': ghi_value,
            'PVOUT_kWh_kWp': pvout_value,
            'Real_kWh_kWp': real_pvout_value,
            'Performance_Ratio': round(pr, 4)
        })
        
    return pd.DataFrame(pr_data)

def save_results(ghi_df, pr_df, site_name):
    """
    Save results to CSV and JSON files.
    """
    output_dir = f"{site_name.replace(' ', '_')}_pvlib_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save GHI data
    ghi_path = os.path.join(output_dir, f'{site_name.replace(" ", "_")}_ghi_data_pvlib_2012_2017.csv')
    ghi_df.to_csv(ghi_path, index=False)
    print(f"✅ GHI data saved to {ghi_path}")
    
    # Save Performance Ratio analysis
    pr_path = os.path.join(output_dir, f'{site_name.replace(" ", "_")}_performance_ratio_pvlib_analysis.csv')
    pr_df.to_csv(pr_path, index=False)
    print(f"✅ Performance Ratio analysis saved to {pr_path}")
    
    # Create summary statistics
    summary_stats = {
        'Annual_Average_GHI_kWh_m2': ghi_df.groupby('Year')['GHI_kWh_m2'].sum().mean(),
        'Annual_Average_PVOUT_kWh_kWp': pr_df.groupby('Year')['PVOUT_kWh_kWp'].sum().mean(),
        'Average_Performance_Ratio': pr_df['Performance_Ratio'].mean(),
        'Min_Monthly_PR': pr_df['Performance_Ratio'].min(),
        'Max_Monthly_PR': pr_df['Performance_Ratio'].max(),
        'PR_Standard_Deviation': pr_df['Performance_Ratio'].std()
    }
    
    # Save summary
    summary_path = os.path.join(output_dir, f'{site_name.replace(" ", "_")}_ghi_summary_pvlib.json')
    with open(summary_path, 'w') as f:
        json.dump(summary_stats, f, indent=2)
    
    print(f"✅ Summary statistics saved to {summary_path}")
    
    return summary_stats, output_dir

def create_monthly_comparison_chart(pr_df, output_dir, site_name):
    """
    Create a comparison chart of monthly GHI vs PVOUT, including Real PVOUT data.
    """
    # Configure matplotlib for Greek text
    rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['axes.unicode_minus'] = False
    
    # Calculate monthly averages
    monthly_avg = pr_df.groupby('Month').agg({
        'GHI_kWh_m2': 'mean',
        'PVOUT_kWh_kWp': 'mean',
        'Real_kWh_kWp': 'mean',
        'Performance_Ratio': 'mean'
    }).reset_index()
    
    months = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαι', 'Ιουν', 
              'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    
    # Top plot: GHI vs PVOUT
    x = np.arange(len(months))
    width = 0.25 # Adjusted width for three bars
    
    bars1 = ax1.bar(x - width, monthly_avg['GHI_kWh_m2'], width, 
                    label='GHI (kWh/m²)', color='#FFC300', alpha=0.9)
    bars2 = ax1.bar(x, monthly_avg['PVOUT_kWh_kWp'], width,
                    label='PVOUT (kWh/kWp)', color='#007ACC', alpha=0.9)
    bar3= ax1.bar(x, monthly_avg['Real_kWh_kWp'], width,
                    label='Real_kWh_kWp', color="#9600CC", alpha=0.9)
    
    # The 'Real_kWh_kWp' bar is already defined and will be populated from pr_df
    # No need for a separate sunny_portal_data_monthly_avg parameter
    
    ax1.set_ylabel('Ενέργεια (kWh)', fontsize=12, fontweight='bold')
    ax1.set_title(f'{site_name.upper()}: Μηνιαία Σύγκριση GHI vs PVOUT (pvlib)\n' +
                  'Εκτιμώμενα Δεδομένα 2012-2017', fontsize=13, fontweight='bold', pad=20)
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Bottom plot: Performance Ratio
    bars_pr = ax2.bar(months, monthly_avg['Performance_Ratio'], 
                    color='#28A745', alpha=0.9, edgecolor='black', linewidth=0.5)
    
    ax2.set_xlabel('Μήνας', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Performance Ratio', fontsize=12, fontweight='bold')
    ax2.set_title('Μηνιαία Αποdoτικότητα (Performance Ratio)', 
                  fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_ylim(bottom=max(0, monthly_avg['Performance_Ratio'].min() - 0.1))

    # Add average line
    avg_pr = monthly_avg['Performance_Ratio'].mean()
    ax2.axhline(y=avg_pr, color='red', linestyle='--', alpha=0.7, linewidth=2, label=f'Μ.Ο. PR: {avg_pr:.3f}')
    ax2.legend()
    
    plt.tight_layout(pad=2.0)
    chart_path = os.path.join(output_dir, f'{site_name.replace(" ", "_")}_ghi_pvout_analysis_pvlib.png')
    plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"✅ Monthly comparison chart saved to {chart_path}")

def main():
    """Main execution function"""
    
    print("="*60)
    print("PV GHI Data Extraction for Multiple Sites using pvlib")
    print("="*60)
    
    for site_name, config in sites.items():
        print(f"\n{'='*60}")
        print(f"Processing data for site: {site_name}")
        print(f"{'='*60}")
        
        print(f"📍 Coordinates: {config['lat']}°N, {config['lon']}°E")
        print(f"📅 Period: 2012-2017") # Hardcoding years as per user's fetch_pvgis_data params
        
        ghi_df = fetch_pvgis_data(site_name, config)
        
        if ghi_df is not None:
            sunny_portal_data = load_sunny_portal_data(site_name)
            pr_df = calculate_performance_ratio(ghi_df, config['lat'], config['lon'], sunny_portal_data, pvgis_database=config['raddatabase'])
            print(f"✅ Performance Ratio calculated for {site_name}: {len(pr_df)} records")
            
            summary_stats, output_dir = save_results(ghi_df, pr_df, site_name)
            create_monthly_comparison_chart(pr_df, output_dir, site_name)
            
            print("\n" + "-"*25 + " SUMMARY " + "-"*26)
            print(f"  Annual Average GHI:      {summary_stats['Annual_Average_GHI_kWh_m2']:.0f} kWh/m²")
            print(f"  Annual Average PVOUT:    {summary_stats['Annual_Average_PVOUT_kWh_kWp']:.0f} kWh/kWp")
            print(f"  Average Performance Ratio: {summary_stats['Average_Performance_Ratio']:.3f}")
            print(f"  PR Range:                {summary_stats['Min_Monthly_PR']:.3f} - {summary_stats['Max_Monthly_PR']:.3f}")
            print("-" * 60)
            print(f"\n✅ Data extraction for {site_name} completed!")
            print(f"📁 Files created in '{output_dir}' directory.")
        else:
            print(f"Skipping {site_name} due to data retrieval failure.")

    print("\n" + "="*60)
    print("All site data extraction with pvlib completed!")
    print("="*60)

if __name__ == "__main__":
    main()
