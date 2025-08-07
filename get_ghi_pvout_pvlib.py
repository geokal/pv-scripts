#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GHI Data Extraction using pvlib-python
=========================================================

This script extracts monthly Global Horizontal Irradiance (GHI) data
for  Crete for the period 2010-2017.

It fetches data from NREL SARAH 3 dataset , calculates performance ratio against known
PVOUT data, and generates summary reports and charts.
Date: June 2025
"""

import pandas as pd
import numpy as np
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
        "raddatabase": "PVGIS-SARAH3",
        "pvtechchoice": "crystSi",
        "trackingtype": 0,  # Fixed
        "angle": 30, "aspect": 180, "optimalangles": 0
    }
}

def fetch_pvgis_data(site_name, config):
    """Fetch typical meteorological year (TMY) irradiance data from PVGIS API and compute POA (plane‑of‑array) irradiance
    reflecting the site’s tracking configuration."""
    print(f"🔄 Fetching PVGIS TMY data for {site_name} ({config['raddatabase']})...")
    try:
        # Fetch hourly PVGIS data (2012-2017) which includes GHI, DNI, DHI
        # Use get_pvgis_hourly to obtain actual yearly data instead of a typical year (TMY)
        optional_kwargs = {
            "map_variables": True,
            "pvcalculation": False,
            "url": 'https://re.jrc.ec.europa.eu/api/v5_3/'
        }
        # Retrieve hourly data for the period 2012-2017
        hourly_data, _ = pvlib.iotools.get_pvgis_hourly(
            config["lat"], config["lon"],
            2012,  # startyear
            2017,  # endyear
            config["raddatabase"],
            **optional_kwargs
        )
        
        # Ensure datetime index is timezone-naive
        if hourly_data.index.tz is not None:
            hourly_data = hourly_data.tz_convert(None)
        
        # DEBUG: Print the range of years in the hourly_data index
        if not hourly_data.empty:
            print(f"DEBUG: Years in hourly_data index for {site_name}: {hourly_data.index.year.min()} - {hourly_data.index.year.max()}")
        else:
            print(f"DEBUG: hourly_data for {site_name} is empty.")

        # Print column names for debugging
        print(f"DEBUG: PVGIS hourly data columns for {site_name}: {list(hourly_data.columns)}")
        
        # Calculate GHI from available POA components if 'ghi' is not present
        if 'ghi' not in hourly_data.columns:
            print(f"WARNING: 'ghi' column not found. Deriving GHI from POA components for {site_name}.")
            # Assuming poa_direct, poa_sky_diffuse, poa_ground_diffuse are available
            # and represent horizontal components when pvcalculation=False
            if all(col in hourly_data.columns for col in ['poa_direct', 'poa_sky_diffuse', 'poa_ground_diffuse']):
                hourly_data['GHI'] = hourly_data['poa_direct'] + hourly_data['poa_sky_diffuse'] + hourly_data['poa_ground_diffuse']
                # Create dummy DNI and DHI for later calculations if needed
                hourly_data['DNI'] = hourly_data['poa_direct'] # Direct component as DNI proxy
                hourly_data['DHI'] = hourly_data['poa_sky_diffuse'] + hourly_data['poa_ground_diffuse'] # Diffuse component as DHI proxy
            else:
                raise ValueError(f"Neither 'ghi' nor POA components found in PVGIS hourly data for {site_name}.")
        
        # Compute monthly GHI (convert from Wh/m² to kWh/m²)
        ghi_monthly = hourly_data.resample('ME').agg(
            GHI_kWh_m2=('GHI', 'sum') # Use 'GHI' column (either original or derived)
        )
        ghi_monthly['GHI_kWh_m2'] = ghi_monthly['GHI_kWh_m2'] / 1000.0
        ghi_monthly['Year'] = ghi_monthly.index.year
        ghi_monthly['Month'] = ghi_monthly.index.month
        ghi_monthly = ghi_monthly.reset_index(drop=True)
        ghi_monthly = ghi_monthly[['Year', 'Month', 'GHI_kWh_m2']]
        
        # Save GHI data to separate CSV files
        output_dir = f"{site_name.replace(' ', '_')}_pvlib_output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        ghi_path = os.path.join(output_dir, f'{site_name.replace(" ", "_")}_ghi_data_pvlib_2012_2017.csv')
        ghi_monthly.to_csv(ghi_path, index=False)
        print(f"✅ GHI data saved to {ghi_path}")
        
        # Now compute POA irradiance for performance ratio calculations
        # Solar position for the site
        from pvlib import solarposition, irradiance
        solpos = solarposition.get_solarposition(
            hourly_data.index,
            config["lat"],
            config["lon"]
        )
        # Determine surface tilt & azimuth based on tracking type
        if config.get("trackingtype") == 2:  # 2‑axis tracking – panel always faces the sun
            surface_tilt = solpos["apparent_zenith"]
            surface_azimuth = solpos["azimuth"]
        else:  # Fixed or 1‑axis (treated as fixed for POA calculation)
            surface_tilt = config.get("angle", 0)
            surface_azimuth = config.get("aspect", 180)
        
        # Compute POA irradiance
        poa = irradiance.get_total_irradiance(
            surface_tilt,
            surface_azimuth,
            solpos["apparent_zenith"],
            solpos["azimuth"],
            hourly_data["DNI"],
            hourly_data["GHI"],
            hourly_data["DHI"]
        )
        # Convert POA from Wh/m² to kWh/m²
        poa["poa_global_kWh_m2"] = poa["poa_global"] / 1000.0
        # Aggregate monthly POA
        poa_monthly = poa.resample('ME').agg(
            POA_kWh_m2=('poa_global_kWh_m2', 'sum')
        )
        poa_monthly['Year'] = poa_monthly.index.year
        poa_monthly['Month'] = poa_monthly.index.month
        poa_monthly = poa_monthly.reset_index(drop=True)
        poa_monthly = poa_monthly[['Year', 'Month', 'POA_kWh_m2']]
        
        print(f"✅ POA data processed for {site_name}: {len(poa_monthly)} monthly records")
        return poa_monthly
    except Exception as e:
        print(f"❌ Failed to retrieve or process PVGIS data for {site_name}: {e}")
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
        # Read the CSV, using semicolon as separator and setting the header to the first row (index 0)
        df = pd.read_csv(file_path, sep=';', header=0)
        print(f"DEBUG: Initial DataFrame head for {site_name}:\n{df.head()}")
        
        # Rename the first column to 'Year' explicitly, as it might be 'Unnamed: 0'
        # This handles cases where the first column might not be named 'Year' but contains year data
        if df.columns[0] not in ['Year', 'year', 'Έτος']: # Added 'Έτος' for Greek files
            df = df.rename(columns={df.columns[0]: 'Year'})
        print(f"DEBUG: DataFrame head after renaming 'Year' for {site_name}:\n{df.head()}")
        
        # Drop the last four rows which contain summary statistics
        df = df.iloc[:-4]
        print(f"DEBUG: DataFrame head after dropping summary rows for {site_name}:\n{df.head()}")
        
        # Melt the DataFrame to transform monthly columns into rows
        # 'Year' is the ID variable, and month names are value variables
        # Exclude 'Total' column if it exists, as it's a summary column
        id_vars = ['Year']
        if 'Total' in df.columns:
            value_vars = [col for col in df.columns if col not in ['Year', 'Total']]
        else:
            value_vars = [col for col in df.columns if col != 'Year']

        df_melted = df.melt(id_vars=id_vars, 
                            value_vars=value_vars,
                            var_name='Month', 
                            value_name='Specific Yield (kWh/kWp)')
        print(f"DEBUG: Melted DataFrame head for {site_name}:\n{df_melted.head()}")
        
        # Convert 'Year' to numeric, coercing errors to NaN, then drop NaNs
        df_melted['Year'] = pd.to_numeric(df_melted['Year'], errors='coerce')
        df_melted = df_melted.dropna(subset=['Year'])
        print(f"DEBUG: Melted DataFrame head after Year conversion/dropna for {site_name}:\n{df_melted.head()}")
        
        # Convert 'Specific Yield (kWh/kWp)' to numeric, handling commas and coercing errors to NaN, then fill NaN with 0
        df_melted['Specific Yield (kWh/kWp)'] = pd.to_numeric(
            df_melted['Specific Yield (kWh/kWp)'].astype(str).str.replace(',', '.'), 
            errors='coerce'
        ).fillna(0)
        print(f"DEBUG: Melted DataFrame head after Specific Yield conversion/fillna for {site_name}:\n{df_melted.head()}")
        
        # Map month names to month numbers (including Greek month names)
        month_name_to_num = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
            'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12,
            'Ιανουάριος': 1, 'Φεβρουάριος': 2, 'Μάρτιος': 3, 'Απρίλιος': 4, 'Μάιος': 5, 'Ιούνιος': 6,
            'Ιούλιος': 7, 'Αύγουστος': 8, 'Σεπτέμβριος': 9, 'Οκτώβριος': 10, 'Νοέμβριος': 11, 'Δεκέμβριος': 12,
            'Ιαν': 1, 'Φεβ': 2, 'Μαρ': 3, 'Απρ': 4, 'Μαι': 5, 'Ιουν': 6,
            'Ιουλ': 7, 'Αυγ': 8, 'Σεπ': 9, 'Οκτ': 10, 'Νοε': 11, 'Δεκ': 12
        }
        df_melted['Month_Num'] = df_melted['Month'].map(month_name_to_num)
        
        # Convert 'Month_Num' to numeric, coercing errors to NaN, then drop NaNs
        df_melted['Month_Num'] = pd.to_numeric(df_melted['Month_Num'], errors='coerce')
        df_melted = df_melted.dropna(subset=['Month_Num'])
        print(f"DEBUG: Melted DataFrame head after Month_Num conversion/dropna for {site_name}:\n{df_melted.head()}")

        # Ensure 'Year' and 'Month_Num' are integers before converting to string for datetime
        df_melted['Year'] = df_melted['Year'].astype(int)
        df_melted['Month_Num'] = df_melted['Month_Num'].astype(int)

        # Combine 'Year' and 'Month_Num' to create a datetime object
        # Use a dummy day (e.g., 1) as specific yield is monthly
        df_melted['Date'] = pd.to_datetime(df_melted['Year'].astype(str) + '-' + 
                                            df_melted['Month_Num'].astype(str) + '-01')
        
        # Set 'Date' as index and sort
        df_melted = df_melted.set_index('Date').sort_index()
        print(f"DEBUG: Melted DataFrame head after Date creation/index set for {site_name}:\n{df_melted.head()}")
        
        # Calculate monthly average specific yield across all years
        monthly_avg_specific_yield = df_melted.groupby(df_melted.index.month)['Specific Yield (kWh/kWp)'].mean()
        
        print(f"✅ Sunny Portal data loaded for {site_name}: {len(monthly_avg_specific_yield)} monthly average records")
        print(f"DEBUG: Sunny Portal monthly average data keys (first 5): {list(monthly_avg_specific_yield.index[:5])}")
        print(f"DEBUG: Sunny Portal monthly average data values (first 5): {list(monthly_avg_specific_yield.values[:5])}")
        return monthly_avg_specific_yield.to_dict()
        
    except Exception as e:
        print(f"❌ Error loading Sunny Portal data for {site_name}: {e}")
        return None

def calculate_performance_ratio(poa_df, latitude, longitude, sunny_portal_data, pvgis_database='SARAH', config=None):
    """
    Calculate Performance Ratio using PVGIS PVOUT data and real PVOUT from Sunny Portal.
    """
    print(f"🔄 Fetching PVOUT data from PVGIS ({pvgis_database}) using get_pvgis_hourly for PVOUT simulation...")
    try:
        # Prepare keyword arguments for get_pvgis_hourly
        optional_kwargs = {
            "peakpower": 1, # Assuming 1 kWp for specific yield
            "pvtechchoice": config.get("pvtechchoice", "crystSi"),
            "url": 'https://re.jrc.ec.europa.eu/api/v5_3/',
            "pvcalculation": True
        }
        
        # Add tracking type and related parameters if specified
        if config.get("trackingtype") is not None:
            optional_kwargs["trackingtype"] = config["trackingtype"]
        if config.get("angle") is not None:
            optional_kwargs["surface_tilt"] = config["angle"]
        if config.get("aspect") is not None:
            optional_kwargs["surface_azimuth"] = config["aspect"]
        # Add mountingplace parameter for fixed systems (trackingtype=0)
        if config.get("trackingtype") == 0:
            optional_kwargs["mountingplace"] = "free"

        print(f"DEBUG: PVGIS hourly positional args: lat={latitude}, lon={longitude}, startyear=2012, endyear=2017, raddatabase={pvgis_database}")
        print(f"DEBUG: PVGIS hourly optional kwargs: {optional_kwargs}")

        # Fetch hourly data, which includes PVOUT ('P' column)
        # Pass required positional arguments directly, and optional parameters via **kwargs
        pvgis_hourly_data, _ = pvlib.iotools.get_pvgis_hourly(
            latitude, longitude,
            2012, # startyear
            2017, # endyear
            pvgis_database, # raddatabase
            **optional_kwargs
        )
        
        # Aggregate monthly PVOUT from hourly data (convert Wh to kWh)
        pvgis_monthly_pvout = pvgis_hourly_data.resample('ME')['P'].sum() / 1000.0
        
        # Convert to a dictionary for easy lookup by month number (average across years if multiple years are returned)
        pvgis_pvout_monthly = pvgis_monthly_pvout.groupby(pvgis_monthly_pvout.index.month).mean().to_dict()
        
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
    
    # Calculate monthly average POA from the input poa_df (which is POA irradiance)
    # This ensures we have a single average POA value per month for comparison
    monthly_avg_poa_tmy = poa_df.groupby('Month')['POA_kWh_m2'].mean().to_dict()

    # Iterate through the 12 months to create the monthly average PR data
    for month_num in range(1, 13):
        avg_poa_value = monthly_avg_poa_tmy.get(month_num, 0)
        
        # Get PVGIS PVOUT for the month (now actual PVOUT from simulation)
        pvout_value = pvgis_pvout_monthly.get(month_num, 0)
        
        # Get real_pvout_value from sunny_portal_data (which contains monthly averages)
        real_pvout_value = sunny_portal_data.get(month_num, 0)
        
        # Calculate Performance Ratio: Real_kWh_kWp / PVOUT_kWh_kWp
        pr = real_pvout_value / pvout_value if pvout_value > 0 else 0
        
        pr_data.append({
            'Year': 'Monthly_Average', # Indicate this is monthly average data
            'Month': month_num,
            'POA_kWh_m2': avg_poa_value, # This is the average POA from POA calculation
            'PVOUT_kWh_kWp': pvout_value, # This is the PVGIS simulated PVOUT
            'Real_kWh_kWp': real_pvout_value, # This is the actual specific yield from Sunny Portal
            'Performance_Ratio': round(pr, 4)
        })
        
    return pd.DataFrame(pr_data)

def save_results(poa_df, pr_df, site_name):
    """
    Save results to CSV and JSON files.
    """
    output_dir = f"{site_name.replace(' ', '_')}_pvlib_output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save POA data
    poa_path = os.path.join(output_dir, f'{site_name.replace(" ", "_")}_poa_data_pvlib_2012_2017.csv')
    poa_df.to_csv(poa_path, index=False)
    print(f"✅ POA data saved to {poa_path}")
    
    # Save Performance Ratio analysis
    pr_path = os.path.join(output_dir, f'{site_name.replace(" ", "_")}_performance_ratio_pvlib_analysis.csv')
    pr_df.to_csv(pr_path, index=False)
    print(f"✅ Performance Ratio analysis saved to {pr_path}")
    
    # Create summary statistics
    summary_stats = {
        'Annual_Average_POA_kWh_m2': poa_df.groupby('Year')['POA_kWh_m2'].sum().mean(),
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
    Create a comparison chart of monthly POA vs PVOUT, including Real PVOUT data.
    """
    # Configure matplotlib for Greek text
    rcParams['font.family'] = 'DejaVu Sans'
    plt.rcParams['axes.unicode_minus'] = False
    
    # Calculate monthly averages
    monthly_avg = pr_df.groupby('Month').agg({
        'POA_kWh_m2': 'mean',
        'PVOUT_kWh_kWp': 'mean',
        'Real_kWh_kWp': 'mean',
        'Performance_Ratio': 'mean'
    }).reset_index()
    
    months = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαι', 'Ιουν', 
              'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    
    # Top plot: POA vs PVOUT
    x = np.arange(len(months))
    width = 0.25 # Adjusted width for three bars
    
    bars1 = ax1.bar(x - width, monthly_avg['POA_kWh_m2'], width, 
                    label='POA (kWh/m²)', color='#FFC300', alpha=0.9)
    bars2 = ax1.bar(x, monthly_avg['PVOUT_kWh_kWp'], width,
                    label='PVOUT (kWh/kWp)', color='#007ACC', alpha=0.9)
    bars3= ax1.bar(x, monthly_avg['Real_kWh_kWp'], width,
                    label='Real_kWh_kWp', color="#9600CC", alpha=0.9)
    
    ax1.set_ylabel('Ενέργεια (kWh)', fontsize=12, fontweight='bold')
    ax1.set_title(f'{site_name.upper()}: Μηνιαία Σύγκριση POA vs PVOUT (pvlib)\n' +
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
        
        poa_df = fetch_pvgis_data(site_name, config)
        
        if poa_df is not None:
            sunny_portal_data = load_sunny_portal_data(site_name)
            pr_df = calculate_performance_ratio(poa_df, config['lat'], config['lon'], sunny_portal_data, pvgis_database=config['raddatabase'], config=config)
            print(f"✅ Performance Ratio calculated for {site_name}: {len(pr_df)} records")
            
            summary_stats, output_dir = save_results(poa_df, pr_df, site_name)
            create_monthly_comparison_chart(pr_df, output_dir, site_name)
            
            print("\n" + "-"*25 + " SUMMARY " + "-"*26)
            print(f"  Annual Average POA:      {summary_stats['Annual_Average_POA_kWh_m2']:.0f} kWh/m²")
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
