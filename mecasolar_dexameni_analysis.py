#!/usr/bin/env python3
"""
MECASOLAR DEXAMENI PV Plant Analysis
===================================
Comprehensive analysis script for the MECASOLAR 2AXIS DEXAMENI site with:
- 6 x Sunny Mini Central 11000TL inverters
- 3 x Sunny Boy 4000TL-20 inverters
- TRINA SOLAR 405W modules
- 2-axis tracking system
- PR analysis, decomposition, forecasting, and validation

Usage:
    python mecasolar_dexameni_analysis.py --latitude 37.7749 --longitude -122.4194
"""

import argparse
import pvlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import pytz
import statsmodels.api as sm
import os
from pvlib.iotools import get_pvgis_hourly
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS
import requests

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='MECASOLAR DEXAMENI PV Plant Analysis')
    parser.add_argument('--latitude', type=float, required=True, help='Site latitude')
    parser.add_argument('--longitude', type=float, required=True, help='Site longitude')
    parser.add_argument('--start-year', type=int, default=2022, help='Start year for analysis')
    parser.add_argument('--end-year', type=int, default=2023, help='End year for analysis')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory for reports')
    return parser.parse_args()

def setup_environment(output_dir):
    """Create output directory if it doesn't exist"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    sns.set_style('whitegrid')
    plt.rcParams['figure.dpi'] = 300

def get_module_params(module_name_pattern):
    """Retrieve module parameters from CEC database"""
    cec_modules = pvlib.pvsystem.retrieve_sam('cecmod').T
    
    matching_modules = cec_modules[cec_modules.index.str.contains(module_name_pattern, case=False, regex=True)]
    
    if matching_modules.empty:
        print(f"Error: No module found for pattern '{module_name_pattern}'.")
        brand = module_name_pattern.split('_')[0]
        print(f"Available modules for brand '{brand}':")
        similar_modules = cec_modules[cec_modules.index.str.contains(brand, case=False, regex=True)]
        for name in similar_modules.index[:20]:
            print(f"- {name}")
        raise IndexError(f"No module found for pattern '{module_name_pattern}'")
        
    module = matching_modules.iloc[0]
    
    return {
        'pdc0': module['STC'],
        'gamma_pdc': module['gamma_r'] / 100.0, # Convert from %/C to decimal
        'v_mp': module['V_mp_ref'],
        'v_oc': module['V_oc_ref'],
        'i_mp': module['I_mp_ref'],
        'i_sc': module['I_sc_ref'],
        'cells_in_series': module['N_s'],
        'alpha_sc': module.get('alpha_sc', 0.0005),  # Temperature coefficient for short circuit current
        'beta_voc': module.get('beta_voc', -0.003),  # Temperature coefficient for open circuit voltage
        'gamma_pdc': module['gamma_r'] / 100.0,  # Temperature coefficient for power
        'b': module.get('b', 0.05),  # Empirically determined parameter
        'a': module.get('a', 0.0)    # Empirically determined parameter
    }

def calculate_european_efficiency(efficiency_profile):
    """Calculate European efficiency using JRC/Ispra weighting formula"""
    # Euro Efficiency = 0.03 x Eff5% + 0.06 x Eff10% + 0.13 x Eff20% + 0.10 x Eff30% + 0.48 x Eff50% + 0.20 x Eff100%
    power_fractions = efficiency_profile['power_fraction']
    efficiencies = efficiency_profile['efficiency']
    weights = efficiency_profile['weight_euro']
    
    # Calculate weighted average
    euro_efficiency = sum(w * eff for w, eff in zip(weights, efficiencies))
    return euro_efficiency

def get_inverter_params():
    """Return inverter parameters for actual MECASOLAR DEXAMENI plant inverters"""
    return {
        '11000tl': {  # 6 x Sunny Mini Central 11000TL - Official SMA specifications
            'paco': 11000,  # Nominal AC power (@230V, 50Hz)
            'pdc0': 11400,  # Maximum DC power
            'Vdco': 350,   # Nominal input voltage
            'voltage_min': 333,  # Minimum input voltage
            'voltage_max': 700,  # Maximum input voltage (from datasheet)
            'eta_inv_nom': 0.98,  # Maximum efficiency: 98%
            'eta_inv_1': 0.975,   # European efficiency: 97.5%
            'Pso': 0.25,   # Standby consumption: 0.25W
            'current_max': 34,    # Maximum input current: 34A
            # Efficiency profile based on European standard (Greece climate)
            'efficiency_profile': {
                'power_fraction': [0.05, 0.10, 0.20, 0.30, 0.50, 1.00],
                'efficiency': [0.85, 0.90, 0.94, 0.96, 0.98, 0.98],  # Typical SMA profile
                'weight_euro': [0.03, 0.06, 0.13, 0.10, 0.48, 0.20]  # European weighting
            }
        },
        '4000tl': {  # 3 x Sunny Boy 4000TL-20
            'paco': 4000, 'pdc0': 4200, 'Vdco': 360,
            'voltage_min': 200, 'voltage_max': 500,
            'eta_inv_nom': 0.975, 'eta_inv_1': 0.982,
            'Pso': 0.1, 'current_max': 11,
            # Efficiency profile for smaller inverter
            'efficiency_profile': {
                'power_fraction': [0.05, 0.10, 0.20, 0.30, 0.50, 1.00],
                'efficiency': [0.88, 0.92, 0.95, 0.97, 0.98, 0.98],  # Typical SMA profile
                'weight_euro': [0.03, 0.06, 0.13, 0.10, 0.48, 0.20]  # European weighting
            }
        }
    }

def get_system_configurations(module_params, inverter_params):
    """Create PVSystem configurations for both inverter types"""
    # Get temperature model parameters for SAPM (compatible with CEC DC model)
    temperature_model_parameters = TEMPERATURE_MODEL_PARAMETERS['sapm']['open_rack_glass_polymer']
    
    # Create Arrays with 2-axis tracking mounts for MECASOLAR DEXAMENI plant
    # Plant: 80.000 kWp total, 6 x Sunny Mini Central 11000TL + 3 x Sunny Boy 4000TL-20
    # Total modules: 80,000W / 405W = ~198 modules
    # For 2-axis tracking, we use FixedMount but update angles dynamically
    array_11000tl = pvlib.pvsystem.Array(
        mount=pvlib.pvsystem.FixedMount(surface_tilt=0, surface_azimuth=180),
        module_parameters=module_params['trina'],
        temperature_model_parameters=temperature_model_parameters,
        strings=6,  # 6 x Sunny Mini Central 11000TL
        modules_per_string=16  # 96 modules total (6 x 16 = 96 modules)
    )
    
    array_4000tl = pvlib.pvsystem.Array(
        mount=pvlib.pvsystem.FixedMount(surface_tilt=0, surface_azimuth=180),
        module_parameters=module_params['trina'],  # Same Trina modules for both systems
        temperature_model_parameters=temperature_model_parameters,
        strings=3,  # 3 x Sunny Boy 4000TL-20
        modules_per_string=34   # 102 modules total (3 x 34 = 102 modules)
    )
    
    return {
        '11000tl': pvlib.pvsystem.PVSystem(
            arrays=[array_11000tl],
            inverter_parameters=inverter_params['11000tl'],
            losses_parameters={
                'soiling': 0.03,
                'shading': 0.02,
                'wiring': 0.015,
                'mismatch': 0.02,
                'availability': 0.01
            }
        ),
        '4000tl': pvlib.pvsystem.PVSystem(
            arrays=[array_4000tl],
            inverter_parameters=inverter_params['4000tl'],
            losses_parameters={
                'soiling': 0.04,
                'shading': 0.01,
                'wiring': 0.01,
                'mismatch': 0.015,
                'availability': 0.01
            }
        )
    }

def get_pvgis_sarah_data(lat, lon, start_year, end_year):
    """Retrieve SARAH data from PVGIS for a specified period"""
    times = pd.date_range(
        start=datetime(start_year, 1, 1, 0, 0, tzinfo=pytz.UTC),
        end=datetime(end_year, 12, 31, 23, 0, tzinfo=pytz.UTC),
        freq='h'
    )
    try:
        print("Fetching SARAH data from PVGIS...")
        pvgis_result = get_pvgis_hourly(
            latitude=lat,
            longitude=lon,
            start=datetime(start_year, 1, 1, 0, 0, tzinfo=pytz.UTC),
            end=datetime(end_year, 12, 31, 23, 0, tzinfo=pytz.UTC),
            raddatabase='PVGIS-SARAH3',
            surface_azimuth=180, # For 2-axis tracking, this is just a placeholder
            outputformat='json',
            usehorizon=True,
            userhorizon=None,
            timeout=60
        )
        # Handle different return formats from get_pvgis_hourly
        if len(pvgis_result) == 3:
            pvgis_data, _, _ = pvgis_result
        elif len(pvgis_result) == 2:
            pvgis_data, _ = pvgis_result
        else:
            pvgis_data = pvgis_result[0]
        # Handle different PVGIS data formats
        weather_data = {}
        if 'G(h)' in pvgis_data:
            weather_data['ghi'] = pvgis_data['G(h)']
        elif 'G(i)' in pvgis_data:
            weather_data['ghi'] = pvgis_data['G(i)']
        else:
            weather_data['ghi'] = pvgis_data.get('ghi', 0)
            
        if 'D(h)' in pvgis_data:
            weather_data['dhi'] = pvgis_data['D(h)']
        elif 'D(i)' in pvgis_data:
            weather_data['dhi'] = pvgis_data['D(i)']
        else:
            weather_data['dhi'] = pvgis_data.get('dhi', 0)
            
        if 'B(h)' in pvgis_data:
            weather_data['dni'] = pvgis_data['B(h)']
        elif 'B(i)' in pvgis_data:
            weather_data['dni'] = pvgis_data['B(i)']
        else:
            weather_data['dni'] = pvgis_data.get('dni', 0)
            
        if 'T2m' in pvgis_data:
            weather_data['temp_air'] = pvgis_data['T2m']
        else:
            weather_data['temp_air'] = pvgis_data.get('temp_air', 25)
            
        if 'WS10m' in pvgis_data:
            weather_data['wind_speed'] = pvgis_data['WS10m']
        else:
            weather_data['wind_speed'] = pvgis_data.get('wind_speed', 2)
            
        weather = pd.DataFrame(weather_data)
        weather.index = weather.index.tz_convert('UTC')
        
        # Check if weather data contains valid values
        if weather['ghi'].isna().all() or weather['ghi'].max() == 0:
            print("Warning: PVGIS data contains no valid irradiance values, using clearsky fallback")
            raise Exception("Invalid PVGIS data")
        
        return weather.reindex(times), times
    except Exception as e:
        print(f"Error retrieving PVGIS SARAH data: {e}")
        print("Using clearsky as fallback")
        location = pvlib.location.Location(lat, lon, tz='UTC')
        clearsky = location.get_clearsky(times)
        
        # Debug clearsky data
        print(f"Clearsky data shape: {clearsky.shape}")
        print(f"Clearsky columns: {list(clearsky.columns)}")
        print(f"Clearsky GHI range: {clearsky['ghi'].min():.2f} - {clearsky['ghi'].max():.2f} W/m²")
        print(f"Clearsky DHI range: {clearsky['dhi'].min():.2f} - {clearsky['dhi'].max():.2f} W/m²")
        print(f"Clearsky DNI range: {clearsky['dni'].min():.2f} - {clearsky['dni'].max():.2f} W/m²")
        
        # Check for NaN values in clearsky data
        if clearsky['ghi'].isna().all():
            print("Warning: Clearsky GHI is all NaN, generating synthetic data")
            # Generate synthetic irradiance data
            synthetic_ghi = pd.Series(index=times, dtype=float)
            for i, timestamp in enumerate(times):
                # Simple sinusoidal pattern for daily cycle
                hour = timestamp.hour
                day_of_year = timestamp.timetuple().tm_yday
                # Basic solar elevation model
                solar_elevation = max(0, 90 - abs(hour - 12) * 7.5)  # Simplified
                seasonal_factor = 0.8 + 0.2 * np.cos(2 * np.pi * (day_of_year - 172) / 365)
                synthetic_value = max(0, solar_elevation * seasonal_factor * 10)  # Scale to reasonable values
                synthetic_ghi.iloc[i] = synthetic_value
            
            # Generate synthetic DHI and DNI
            synthetic_dhi = synthetic_ghi * 0.2  # 20% diffuse
            synthetic_dni = synthetic_ghi * 0.8   # 80% direct
            
            weather_fallback = pd.DataFrame({
                'ghi': synthetic_ghi,
                'dhi': synthetic_dhi,
                'dni': synthetic_dni,
                'temp_air': 25 + 10 * np.sin(2 * np.pi * times.hour / 24),  # Daily temperature cycle
                'wind_speed': 2 + np.random.normal(0, 0.5, len(times))  # Random wind with mean 2
            })
        else:
            # Use clearsky data if it's valid
            weather_fallback = pd.DataFrame({
                'ghi': clearsky['ghi'],
                'dhi': clearsky['dhi'], 
                'dni': clearsky['dni'],
                'temp_air': 25 + 10 * np.sin(2 * np.pi * times.hour / 24),  # Daily temperature cycle
                'wind_speed': 2 + np.random.normal(0, 0.5, len(times))  # Random wind with mean 2
            })
        
        weather_fallback.index = times
        print(f"Fallback weather data shape: {weather_fallback.shape}")
        print(f"Fallback GHI range: {weather_fallback['ghi'].min():.2f} - {weather_fallback['ghi'].max():.2f} W/m²")
        print(f"Fallback temp range: {weather_fallback['temp_air'].min():.2f} - {weather_fallback['temp_air'].max():.2f} °C")
        return weather_fallback, times

def calculate_pr(modelchain, system, weather):
    """Calculate Performance Ratio for a system"""
    # Get the first array (assuming single array per system)
    array = system.arrays[0]
    dc_capacity = array.module_parameters['pdc0'] * array.modules_per_string * array.strings
    
    # Debug: Check if we have irradiance data
    if hasattr(modelchain.results, 'total_irrad') and 'poa_global' in modelchain.results.total_irrad:
        poa = modelchain.results.total_irrad['poa_global']
        print(f"POA Global range: {poa.min():.2f} - {poa.max():.2f} W/m²")
    else:
        print("Warning: No POA irradiance data found")
        return pd.Series(dtype=float), dc_capacity
    
    # Debug: Check AC power
    if hasattr(modelchain.results, 'ac'):
        actual_ac = modelchain.results.ac
        if isinstance(actual_ac, pd.DataFrame):
            actual_ac = actual_ac.iloc[:, 0]  # Take first column if DataFrame
        print(f"AC power range: {actual_ac.min():.2f} - {actual_ac.max():.2f} W")
    else:
        print("Warning: No AC power data found")
        return pd.Series(dtype=float), dc_capacity
    
    theoretical_dc = (poa / 1000) * dc_capacity
    pr = actual_ac / theoretical_dc
    pr = pr.replace([np.inf, -np.inf], np.nan).dropna()
    
    print(f"PR range: {pr.min():.3f} - {pr.max():.3f}, valid points: {len(pr)}")
    return pr, dc_capacity

def decompose_pr(modelchain, system, weather):
    """Decompose PR into contributing loss factors"""
    # Get the first array (assuming single array per system)
    array = system.arrays[0]
    dc_capacity = array.module_parameters['pdc0'] * array.modules_per_string * array.strings
    poa = modelchain.results.total_irrad['poa_global']
    theoretical_dc = (poa / 1000) * dc_capacity
    temp_effect = 1 + array.module_parameters['gamma_pdc'] * (weather['temp_air'] - 25)
    temp_corrected_dc = theoretical_dc * temp_effect
    
    # Calculate inverter efficiency from AC/DC ratio
    dc_power = modelchain.results.dc
    ac_power = modelchain.results.ac
    # Handle case where dc might be a DataFrame with multiple columns
    if isinstance(dc_power, pd.DataFrame):
        dc_power = dc_power.iloc[:, 0]  # Take first column if multiple arrays
    inv_eff = ac_power / dc_power
    inv_eff = inv_eff.replace([np.inf, -np.inf], np.nan).dropna()
    
    losses = {
        'temperature': 1 - (temp_corrected_dc / theoretical_dc).mean(),
        'inverter': 1 - inv_eff.mean() if not inv_eff.empty else 0.05,  # Default 5% loss if no data
        'soiling': system.losses_parameters['soiling'],
        'shading': system.losses_parameters['shading'],
        'wiring': system.losses_parameters['wiring'],
        'mismatch': system.losses_parameters['mismatch'],
        'availability': system.losses_parameters['availability']
    }
    return losses

def forecast_pr(pr_series, periods=12):
    """Forecast PR using seasonal decomposition or simple trend"""
    monthly_pr = pr_series.resample('ME').mean()
    
    # Check if we have any data at all
    if len(monthly_pr) == 0 or monthly_pr.dropna().empty:
        print("No valid PR data available for forecasting. Using default forecast.")
        # Create a default forecast starting from the last date in the original series
        if not pr_series.empty:
            start_date = pr_series.index[-1]
        else:
            start_date = pd.Timestamp.now()
        forecast_index = pd.date_range(start=start_date, periods=periods+1, freq='ME')[1:]
        return pd.Series([0.8] * periods, index=forecast_index)
    
    if len(monthly_pr) < 24:
        print("Not enough data for seasonal decomposition forecast. Using simple trend forecast.")
        # Simple trend-based forecast
        if len(monthly_pr) >= 6:
            # Calculate simple linear trend
            x = np.arange(len(monthly_pr))
            y = monthly_pr.dropna().values
            if len(y) >= 2:
                slope, intercept = np.polyfit(x, y, 1)
                # Forecast using linear trend
                future_x = np.arange(len(monthly_pr), len(monthly_pr) + periods)
                forecast_values = slope * future_x + intercept
                forecast_index = pd.date_range(start=monthly_pr.index[-1], periods=periods+1, freq='ME')[1:]
                return pd.Series(forecast_values, index=forecast_index)
        
        # If still not enough data, return constant forecast
        last_value = monthly_pr.dropna().iloc[-1] if not monthly_pr.dropna().empty else 0.8
        forecast_index = pd.date_range(start=monthly_pr.index[-1], periods=periods+1, freq='ME')[1:]
        return pd.Series([last_value] * periods, index=forecast_index)
    
    # Full seasonal decomposition
    decomposition = sm.tsa.seasonal_decompose(monthly_pr.dropna(), model='additive', period=12)
    seasonal_pattern = decomposition.seasonal[-12:]
    last_trend = decomposition.trend.dropna().iloc[-1]
    forecast = [last_trend + seasonal_pattern.iloc[i % 12] for i in range(periods)]
    return pd.Series(forecast, index=pd.date_range(start=monthly_pr.index[-1], periods=periods+1, freq='ME')[1:])

def validate_with_pvout(pr_series, pvout_data_path='pvout_data.csv'):
    """Validate PR against pvout.org measurements"""
    if not os.path.exists(pvout_data_path):
        print(f"Warning: pvout.org data file '{pvout_data_path}' not found. Skipping validation.")
        return None, None, None
    try:
        pvout_data = pd.read_csv(pvout_data_path, parse_dates=['timestamp'], index_col='timestamp')
        pvout_data = pvout_data.resample('h').mean()
        
        # Ensure timezone consistency
        if pvout_data.index.tz is None:
            pvout_data.index = pvout_data.index.tz_localize('UTC')
        if pr_series.index.tz is None:
            pr_series.index = pr_series.index.tz_localize('UTC')
        
        measured_pr = pvout_data['ac_power'] / pvout_data['poa_irradiance']
        common_index = pr_series.index.intersection(measured_pr.index)
        
        print(f"Validation data: {len(pvout_data)} records, {len(measured_pr)} valid PR calculations")
        print(f"Common time periods: {len(common_index)}")
        
        if len(common_index) == 0:
            print("Warning: No overlapping time periods between simulation and validation data")
            return None, None, None
            
        pr_sim = pr_series.loc[common_index]
        pr_meas = measured_pr.loc[common_index]
        metrics = {
            'rmse': np.sqrt(((pr_sim - pr_meas) ** 2).mean()),
            'mae': abs(pr_sim - pr_meas).mean(),
            'r2': 1 - ((pr_sim - pr_meas) ** 2).sum() / ((pr_meas - pr_meas.mean()) ** 2).sum(),
            'bias': (pr_sim - pr_meas).mean()
        }
        return metrics, pr_sim, pr_meas
    except Exception as e:
        print(f"Validation error: {e}")
        return None, None, None

def create_validation_report(metrics, pr_sim, pr_meas, output_dir):
    """Create detailed PR validation report"""
    report_path = os.path.join(output_dir, 'pr_validation_report.txt')
    with open(report_path, 'w') as f:
        if metrics is None:
            f.write("PR VALIDATION REPORT\n====================\nNo pvout.org data available for validation\n")
            return
        f.write(f"PR VALIDATION REPORT\n====================\n")
        f.write(f"Time Period: {pr_sim.index.min()} to {pr_sim.index.max()}\n\n")
        f.write(f"Validation Metrics:\n")
        f.write(f"- RMSE: {metrics['rmse']:.4f}\n- MAE: {metrics['mae']:.4f}\n")
        f.write(f"- R²: {metrics['r2']:.4f}\n- Bias: {metrics['bias']:.4f}\n\n")
        f.write("Performance Assessment:\n")
        if metrics['rmse'] < 0.03:
            f.write("Excellent agreement between simulated and measured PR\n")
        elif metrics['rmse'] < 0.05:
            f.write("Good agreement between simulated and measured PR\n")
        else:
            f.write("Significant discrepancies between simulated and measured PR\n")
    print(f"Validation report saved to {report_path}")

def plot_comprehensive_analysis(
    tracking_angles, system_11000tl, system_4000tl, mc_trina, mc_canadian,
    pr_trina, pr_canadian, weighted_pr, trina_losses, canadian_losses,
    weighted_forecast, output_dir
):
    """Plot comprehensive PR analysis"""
    fig_path = os.path.join(output_dir, 'comprehensive_pr_analysis.png')
    fig, axes = plt.subplots(4, 2, figsize=(16, 20))
    fig.suptitle('MECASOLAR DEXAMENI PV Plant Analysis', fontsize=16, fontweight='bold')
    
    # 1. Tracking angles
    ax1 = axes[0, 0]
    ax1.plot(tracking_angles.index, tracking_angles['surface_tilt'], label='Surface Tilt', alpha=0.7)
    ax1.plot(tracking_angles.index, tracking_angles['surface_azimuth'], label='Surface Azimuth', alpha=0.7)
    ax1.set_title('2-Axis Tracking Angles')
    ax1.set_ylabel('Angle (degrees)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. Irradiance
    ax2 = axes[0, 1]
    if hasattr(mc_trina.results, 'total_irrad'):
        irrad = mc_trina.results.total_irrad
        print(f"Plotting irradiance - POA Global range: {irrad['poa_global'].min():.2f} - {irrad['poa_global'].max():.2f}")
        ax2.plot(irrad.index, irrad['poa_global'], label='POA Global', alpha=0.7)
        if 'poa_direct' in irrad.columns:
            ax2.plot(irrad.index, irrad['poa_direct'], label='POA Direct', alpha=0.7)
        if 'poa_diffuse' in irrad.columns:
            ax2.plot(irrad.index, irrad['poa_diffuse'], label='POA Diffuse', alpha=0.7)
    else:
        print("No irradiance data available for plotting")
    ax2.set_title('Plane of Array Irradiance')
    ax2.set_ylabel('Irradiance (W/m²)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # 3. Performance Ratio - Trina
    ax3 = axes[1, 0]
    pr_trina_clean = pr_trina.dropna()
    print(f"Trina PR data: {len(pr_trina_clean)} valid points")
    if not pr_trina_clean.empty:
        ax3.plot(pr_trina_clean.index, pr_trina_clean.values, label='Trina 11000TL', alpha=0.7)
        ax3.axhline(y=pr_trina_clean.mean(), color='red', linestyle='--', alpha=0.7, label=f'Mean: {pr_trina_clean.mean():.3f}')
    else:
        print("No Trina PR data to plot")
    ax3.set_title('Performance Ratio - Trina System')
    ax3.set_ylabel('Performance Ratio')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # 4. Performance Ratio - Canadian Solar
    ax4 = axes[1, 1]
    pr_canadian_clean = pr_canadian.dropna()
    print(f"Canadian PR data: {len(pr_canadian_clean)} valid points")
    if not pr_canadian_clean.empty:
        ax4.plot(pr_canadian_clean.index, pr_canadian_clean.values, label='Canadian 4000TL', alpha=0.7)
        ax4.axhline(y=pr_canadian_clean.mean(), color='red', linestyle='--', alpha=0.7, label=f'Mean: {pr_canadian_clean.mean():.3f}')
    else:
        print("No Canadian PR data to plot")
    ax4.set_title('Performance Ratio - Canadian Solar System')
    ax4.set_ylabel('Performance Ratio')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # 5. Weighted Performance Ratio
    ax5 = axes[2, 0]
    pr_weighted_clean = weighted_pr.dropna()
    print(f"Weighted PR data: {len(pr_weighted_clean)} valid points")
    if not pr_weighted_clean.empty:
        ax5.plot(pr_weighted_clean.index, pr_weighted_clean.values, label='Weighted PR', alpha=0.7, color='green')
        ax5.axhline(y=pr_weighted_clean.mean(), color='red', linestyle='--', alpha=0.7, label=f'Mean: {pr_weighted_clean.mean():.3f}')
    else:
        print("No weighted PR data to plot")
    ax5.set_title('Weighted Performance Ratio')
    ax5.set_ylabel('Performance Ratio')
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    
    # 6. Loss Decomposition - Trina
    ax6 = axes[2, 1]
    if trina_losses:
        losses_names = list(trina_losses.keys())
        losses_values = list(trina_losses.values())
        bars = ax6.bar(losses_names, losses_values, alpha=0.7, color='skyblue')
        ax6.set_title('Loss Decomposition - Trina System')
        ax6.set_ylabel('Loss Fraction')
        ax6.tick_params(axis='x', rotation=45)
        # Add value labels on bars
        for bar, value in zip(bars, losses_values):
            ax6.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                    f'{value:.3f}', ha='center', va='bottom', fontsize=8)
    ax6.grid(True, alpha=0.3)
    
    # 7. Loss Decomposition - Canadian Solar
    ax7 = axes[3, 0]
    if canadian_losses:
        losses_names = list(canadian_losses.keys())
        losses_values = list(canadian_losses.values())
        bars = ax7.bar(losses_names, losses_values, alpha=0.7, color='lightcoral')
        ax7.set_title('Loss Decomposition - Canadian Solar System')
        ax7.set_ylabel('Loss Fraction')
        ax7.tick_params(axis='x', rotation=45)
        # Add value labels on bars
        for bar, value in zip(bars, losses_values):
            ax7.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001, 
                    f'{value:.3f}', ha='center', va='bottom', fontsize=8)
    ax7.grid(True, alpha=0.3)
    
    # 8. PR Forecast
    ax8 = axes[3, 1]
    if weighted_forecast is not None and not weighted_forecast.empty:
        # Plot historical data
        pr_weighted_clean = weighted_pr.dropna()
        if not pr_weighted_clean.empty:
            ax8.plot(pr_weighted_clean.index, pr_weighted_clean.values, label='Historical PR', alpha=0.7, color='blue')
        
        # Plot forecast
        ax8.plot(weighted_forecast.index, weighted_forecast.values, label='Forecast', alpha=0.7, color='red', linestyle='--')
        ax8.set_title('PR Forecast')
        ax8.set_ylabel('Performance Ratio')
        ax8.legend()
    else:
        ax8.text(0.5, 0.5, 'No forecast data available', ha='center', va='center', transform=ax8.transAxes)
        ax8.set_title('PR Forecast')
    ax8.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(fig_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Analysis plot saved to {fig_path}")

def main():
    """Main function to run the analysis"""
    args = parse_arguments()
    setup_environment(args.output_dir)
    
    print("Loading module and inverter parameters...")
    
    # Real Trina Solar TSM-405DE09.08 parameters from actual MECASOLAR DEXAMENI plant
    # Plant: 80.000 kWp, Commissioning: 03/11/2010, Location: ΣΙΤΑΝΟΣ, Greece
    # Inverters: 6 x Sunny Mini Central 11000TL + 3 x Sunny Boy 4000TL-20
    # Total modules: ~198 x 405W = 80.19 kWp
    trina_params = {
        # PVsyst v6 model parameters for Trina Solar TSM-405DE09.08
        'pdc0': 405,  # P_mp_ref from datasheet (TRINA SOLAR 405W)
        'v_mp': 34.4,  # V_mp_ref
        'v_oc': 41.4,  # V_oc_ref
        'i_mp': 11.77,  # I_mp_ref
        'i_sc': 12.34,  # I_sc_ref
        'alpha_sc': 0.0004,  # Temperature coefficient for short circuit current: 0.04%/K
        'beta_voc': -0.0025,  # Temperature coefficient for open circuit voltage: -0.25%/K
        'gamma_pdc': -0.0034,  # Power coefficient: -0.34%/K
        'cells_in_series': 120,  # N_s from datasheet
        'A_c': 1.9215,  # Module area (m²)
        'T_NOCT': 43.0,  # NOCT temperature
        'module_type': 'glass_polymer',  # Module type for temperature modeling
        # CEC model parameters (realistic values for Trina module)
        'a_ref': 2.6363,  # Product of diode ideality factor and thermal voltage
        'I_L_ref': 12.34,  # Light-generated current at reference conditions
        'I_o_ref': 1.0e-12,  # Dark saturation current at reference conditions
        'R_sh_ref': 100,  # Shunt resistance at reference conditions
        'R_s': 0.5,  # Series resistance at reference conditions
        'Adjust': 0.0  # Adjustment to temperature coefficient
    }
    
    module_params = {
        'trina': trina_params,
        'canadian_solar': get_module_params('Canadian_Solar.*375')
    }
    inverter_params = get_inverter_params()
    
    # Calculate and display European efficiency for both inverters
    print("Calculating European efficiency profiles...")
    for inv_name, inv_params in inverter_params.items():
        if 'efficiency_profile' in inv_params:
            euro_eff = calculate_european_efficiency(inv_params['efficiency_profile'])
            print(f"{inv_name}: European Efficiency = {euro_eff:.3f} ({euro_eff*100:.1f}%)")
    
    systems = get_system_configurations(module_params, inverter_params)
    
    weather, times = get_pvgis_sarah_data(args.latitude, args.longitude, args.start_year, args.end_year)
    
    # Debug: Check weather data
    print(f"Weather data shape: {weather.shape}")
    print(f"Weather columns: {list(weather.columns)}")
    print(f"Weather GHI range: {weather['ghi'].min():.2f} - {weather['ghi'].max():.2f} W/m²")
    print(f"Weather temp range: {weather['temp_air'].min():.2f} - {weather['temp_air'].max():.2f} °C")
    print(f"Times range: {times[0]} to {times[-1]}")
    
    location = pvlib.location.Location(args.latitude, args.longitude, tz='UTC')
    solar_position = location.get_solarposition(times)
    print(f"Solar position data shape: {solar_position.shape}")
    
    print("Calculating 2-axis tracking angles...")
    # For 2-axis tracking, we need to calculate both tilt and azimuth
    # 2-axis tracking always points directly at the sun
    tracking_angles = pd.DataFrame({
        'surface_tilt': solar_position['apparent_zenith'],
        'surface_azimuth': solar_position['azimuth']
    })
    
    # Update 2-axis tracking angles for the arrays
    # For 2-axis tracking: tilt = zenith angle, azimuth = solar azimuth
    systems['11000tl'].arrays[0].mount.surface_tilt = tracking_angles['surface_tilt']
    systems['11000tl'].arrays[0].mount.surface_azimuth = tracking_angles['surface_azimuth']
    systems['4000tl'].arrays[0].mount.surface_tilt = tracking_angles['surface_tilt']
    systems['4000tl'].arrays[0].mount.surface_azimuth = tracking_angles['surface_azimuth']
    
    print(f"2-axis tracking: Tilt range {tracking_angles['surface_tilt'].min():.1f}° - {tracking_angles['surface_tilt'].max():.1f}°")
    print(f"2-axis tracking: Azimuth range {tracking_angles['surface_azimuth'].min():.1f}° - {tracking_angles['surface_azimuth'].max():.1f}°")
    
    print("Running simulations...")
    mc_trina = pvlib.modelchain.ModelChain(systems['11000tl'], location, ac_model='pvwatts', aoi_model='physical', dc_model='cec', temperature_model='sapm')
    print("Running Trina simulation...")
    mc_trina.run_model(weather)
    
    # Debug: Check simulation results
    print(f"Trina simulation results:")
    print(f"  - Total irradiance shape: {mc_trina.results.total_irrad.shape if hasattr(mc_trina.results, 'total_irrad') else 'No total_irrad'}")
    print(f"  - AC power shape: {mc_trina.results.ac.shape if hasattr(mc_trina.results, 'ac') else 'No AC power'}")
    if hasattr(mc_trina.results, 'total_irrad') and not mc_trina.results.total_irrad.empty:
        print(f"  - POA Global range: {mc_trina.results.total_irrad['poa_global'].min():.2f} - {mc_trina.results.total_irrad['poa_global'].max():.2f}")
    
    mc_canadian = pvlib.modelchain.ModelChain(systems['4000tl'], location, ac_model='pvwatts', aoi_model='physical', dc_model='cec', temperature_model='sapm')
    print("Running Canadian Solar simulation...")
    mc_canadian.run_model(weather)
    
    print("Calculating PR and decomposition...")
    pr_trina, dc_capacity_trina = calculate_pr(mc_trina, systems['11000tl'], weather)
    pr_canadian, dc_capacity_canadian = calculate_pr(mc_canadian, systems['4000tl'], weather)
    
    total_dc_capacity = dc_capacity_trina * 6 + dc_capacity_canadian * 3
    weighted_pr = (pr_trina * dc_capacity_trina * 6 + pr_canadian * dc_capacity_canadian * 3) / total_dc_capacity
    
    trina_losses = decompose_pr(mc_trina, systems['11000tl'], weather)
    canadian_losses = decompose_pr(mc_canadian, systems['4000tl'], weather)
    
    print("Forecasting PR...")
    weighted_forecast = forecast_pr(weighted_pr)
    
    print("Validating with pvout.org data...")
    metrics, pr_sim, pr_meas = validate_with_pvout(weighted_pr)
    create_validation_report(metrics, pr_sim, pr_meas, args.output_dir)
    
    print("Generating plots...")
    plot_comprehensive_analysis(
        tracking_angles, systems['11000tl'], systems['4000tl'], mc_trina, mc_canadian,
        pr_trina, pr_canadian, weighted_pr, trina_losses, canadian_losses,
        weighted_forecast, args.output_dir
    )
    
    print("\nAnalysis complete.")

if __name__ == '__main__':
    main()
