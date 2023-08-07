import pandas as pd
import numpy as np
import dask.dataframe as dd
from math import exp, sqrt
from numba import jit

# A_2. Read Data Files in
# Data Preprocessing Functions
# These functions are designed to convert Unix time to a readable format and clean column names. 
def convert_unix_to_readable(df, unix_cols):
    for col in unix_cols:
        df[col] = pd.to_datetime(df[col], unit='s')
    return df

def clean_column_name(column_name):
    return column_name.replace("[", "").replace("]", "").strip().lower()


# I'm reading the SPY options data for different months of 2022 and selecting specific columns for analysis.
dtypes = {
    ' [C_ASK]': 'object',  
    ' [C_BID]': 'object',
    ' [C_IV]': 'object',
    ' [C_LAST]': 'object',
    ' [P_ASK]': 'object',
    ' [P_BID]': 'object',
    ' [P_IV]': 'object',
    ' [P_LAST]': 'object',
    ' [P_VOLUME]': 'object',
}

necessary_columns = ['[QUOTE_UNIXTIME]', ' [UNDERLYING_LAST]', 
                    ' [DTE]',  ' [C_IV]', ' [C_LAST]', ' [C_BID]', 
                    ' [C_ASK]', ' [STRIKE]', ' [P_IV]', ' [P_LAST]', 
                    ' [QUOTE_DATE]']

# Define file path and base file name
file_path = "C:/Users/Santiago/Desktop/Portfolio/DTE_Project/SPY_2022_DATA/"
base_file_name = "spy_01x_2022"

# Define the epsilon value
epsilon = 0.9

# Initialize an empty list to store dataframes
DF = []

# Loop over the months
for month in range(1, 13):
    # Format the month to have leading zeros
    month_str = f"{month:02d}"
    
    # Construct the file name
    file_name = f"{file_path}{base_file_name}{month_str}.txt"
    
    # Read the file and select necessary columns
    df = dd.read_csv(file_name, dtype=dtypes)[necessary_columns]
    
    # Filter dataframe based on DTE
    temp = df[df[' [DTE]'].abs() <= epsilon]
    
    # Compute the Dask dataframe and add to the list
    DF.append(temp.compute())

# Concatenate the list of dataframes into a single dataframe
results = pd.concat(DF, ignore_index=True)

# Convert Unix time to a readable format
results = convert_unix_to_readable(results, ['[QUOTE_UNIXTIME]'])

# Clean column names
results.columns = [clean_column_name(column) for column in results.columns]    

# Save the final dataframe to a file
results.to_csv(r'C:\Users\Santiago\Desktop\Portfolio\DTE_Project\DTE_Data.txt', sep='\t', index=False)

##############################################################################################################################################################################

# Define the American option pricing function with Numba JIT decorator for optimization
@jit(nopython=True, parallel=True)
def american_option_price(S, K, T, r, sigma, option_type, N=100):
    dt = T / N
    u = exp(sigma * sqrt(dt))
    d = 1 / u
    
    # Return None if the parameters are not valid
    if dt == 0 or sigma == 0 or u == d:
        return None  
    
    p = (exp(r * dt) - d) / (u - d)
    option_tree = np.zeros((N + 1, N + 1))
    
    # Compute the leaves of the option tree (the payoff at maturity)
    for j in range(N + 1):
        if option_type == 'call':
            option_tree[N, j] = max(0, S * (u ** j) * (d ** (N - j)) - K)
        elif option_type == 'put':
            option_tree[N, j] = max(0, K - S * (u ** j) * (d ** (N - j)))
    
    # Iterate backwards through the tree to compute the option price at each node
    for i in range(N - 1, -1, -1):
        for j in range(i + 1):
            option_tree[i, j] = exp(-r * dt) * (p * option_tree[i + 1, j + 1] + (1 - p) * option_tree[i + 1, j])
            if option_type == 'call':
                option_tree[i, j] = max(option_tree[i, j], S * (u ** j) * (d ** (i - j)) - K)
            elif option_type == 'put':
                option_tree[i, j] = max(option_tree[i, j], K - S * (u ** j) * (d ** (i - j)))
    
    # Return the option price at the root of the tree
    return option_tree[0, 0]

def fill_missing_iv_interpolation(data, iv_column):
    data[iv_column] = data.groupby('dte')[iv_column].apply(lambda x: x.replace(0, np.nan).interpolate())
    data[iv_column] = data[iv_column].fillna(data[iv_column].mean())
    return data

def calculate_fair_price_with_checks(row, r=0.015):
    S = row['underlying_last']
    K = row['strike']
    T = row['dte']
    c_iv = row['c_iv'] / 100
    p_iv = row['p_iv'] / 100
    if T <= 0 or S <= 0 or K <= 0 or (c_iv <= 0 and p_iv <= 0):
        return pd.Series({'c_fair_price': None, 'p_fair_price': None})
    c_fair_price = american_option_price(S, K, T, r, c_iv, option_type='call') if c_iv > 0 else None
    p_fair_price = american_option_price(S, K, T, r, p_iv, option_type='put') if p_iv > 0 else None
    return pd.Series({'c_fair_price': c_fair_price, 'p_fair_price': p_fair_price})

# Read the processed data and convert IV columns to numeric values.
df = pd.read_csv('C:/Users/Santiago/Desktop/Portfolio/DTE_Project/DTE_Data.txt', sep='\t')
df['c_iv'] = pd.to_numeric(df['c_iv'], errors='coerce').fillna(0)
df['p_iv'] = pd.to_numeric(df['p_iv'], errors='coerce').fillna(0)

# Fill missing IV values using interpolation.
df_filled_c_iv = fill_missing_iv_interpolation(df, 'c_iv')
df_filled_iv = fill_missing_iv_interpolation(df_filled_c_iv, 'p_iv')

# Convert pandas DataFrame to Dask DataFrame for efficient computation.
ddf = pd.from_pandas(df_filled_iv, npartitions=1000)

# Calculate fair prices for call and put options using Dask (it's more efficient for large data).
ddf['c_fair_price'] = ddf.apply(
    lambda row: american_option_price(row['underlying_last'], row['strike'], row['dte'], 0.015, row['c_iv'] / 100, 'call')
                if row['underlying_last'] > 0 and row['strike'] > 0 and row['dte'] > 0 and row['c_iv'] > 0 else None, 
    axis=1, 
    meta=(None, 'float64'))

ddf['p_fair_price'] = ddf.apply(
    lambda row: american_option_price(row['underlying_last'], row['strike'], row['dte'], 0.015, row['p_iv'] / 100, 'put')
                if row['underlying_last'] > 0 and row['strike'] > 0 and row['dte'] > 0 and row['p_iv'] > 0 else None, 
    axis=1,
    meta=(None, 'float64'))

# Convert Dask DataFrame back to pandas DataFrame.
DF = ddf.compute()












































