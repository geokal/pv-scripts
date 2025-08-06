import pandas as pd

# Correct Chorio specific yield data with 2010 starting from May
chorio_data = {
    "Year": [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, "Mean value", "Year portion"],
    "January": ["", 106.70, 53.28, 90.43, 88.31, 91.66, 91.55, 100.03, 110.39, 51.68, 103.32, 32.96, 73.44, 82.81, "5.14%"],
    "February": ["", 106.42, 131.97, 114.76, 100.16, 102.43, 130.42, 115.66, 91.71, "", 104.27, 114.57, 85.88, 108.93, "6.76%"],
    "March": ["", 131.19, 156.10, 149.68, 140.74, 126.96, 153.08, 140.61, 128.21, 109.77, 124.58, 134.70, 99.52, 132.93, "8.25%"],
    "April": ["", 154.54, 168.85, 169.80, 156.87, 163.81, 169.68, 157.98, 159.26, 132.18, 147.66, 139.51, 140.39, 155.04, "9.62%"],
    "May": [145.30, 166.32, 170.02, 166.83, 176.65, 176.26, 156.58, 164.01, 124.09, 148.82, 165.10, 169.38, 148.87, 159.86, "9.92%"],
    "June": [171.36, 176.20, 146.27, 176.01, 169.09, 170.47, 172.20, 170.43, 130.53, 167.30, 157.70, 158.09, 145.11, 162.37, "10.07%"],
    "July": [190.38, 192.51, 228.49, 194.44, 183.92, 180.78, 186.60, 181.62, 139.07, 178.08, 174.79, 167.83, 130.67, 179.17, "11.12%"],
    "August": [187.09, 187.41, 190.48, 184.40, 185.05, 175.98, 178.08, 177.24, 145.91, 177.82, 170.10, 162.52, 170.46, 176.35, "10.94%"],
    "September": [161.42, 146.17, 132.80, 165.33, 146.67, 153.44, 155.98, 156.47, 139.52, 163.07, 144.93, 57.25, 102.07, 140.39, "8.71%"],
    "October": [132.36, 157.94, "", 162.27, 147.89, 133.53, 144.23, 146.03, 14.80, 138.52, 121.76, 97.70, "", 127.00, "7.88%"],
    "November": [113.49, 116.41, "", 103.38, 105.64, 127.29, 108.42, 112.80, 14.04, 97.48, 94.12, 93.28, "", 98.76, "6.13%"],
    "December": [90.02, 105.11, "", 86.22, 97.69, 114.78, 95.02, 89.83, 53.98, 90.45, 71.70, 78.29, "", 88.08, "5.47%"],
    "Total": [1191.42, 1746.92, 1462.17, 1763.54, 1698.68, 1717.37, 1741.84, 1712.69, 1251.50, 1455.16, 1580.04, 1406.07, 1096.41, 1611.70, "100.00%"]
}

# Create DataFrame
df = pd.DataFrame(chorio_data)

# Add the additional summary rows
additional_rows = {
    "Year": ["Yield expectations *", "19823.81"],
    "January": [108.15, ""],
    "February": [121.28, ""],
    "March": [157.50, ""],
    "April": [166.60, ""],
    "May": [175.88, ""],
    "June": [169.40, ""],
    "July": [174.65, ""],
    "August": [172.38, ""],
    "September": [156.28, ""],
    "October": [138.78, ""],
    "November": [108.85, ""],
    "December": [100.28, ""],
    "Total": [1750.00, "19823.81"]
}

# Append additional rows
df = pd.concat([df, pd.DataFrame(additional_rows)], ignore_index=True)

# Set the header
header = "Specific PV System Yield [kWh/kWp];January;February;March;April;May;June;July;August;September;October;November;December;Total"

# Save to CSV with UTF-8 encoding and semicolon delimiter
with open("chorio_specific_yield.csv", "w", encoding="utf-8") as f:
    f.write(header + "\n")
    df.to_csv(f, index=False, sep=";", lineterminator="\n")  # Changed line_terminator to lineterminator

print("Successfully created 'chorio_specific_yield.csv' with correct formatting")