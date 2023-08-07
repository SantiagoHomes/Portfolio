import os
import dask.dataframe as dd
import QuantLib as ql
from scipy import stats
import matplotlib.pyplot as plt

# Define the columns to import and their data types
columns_to_import = [' [UNDERLYING_LAST]', ' [DTE]', ' [STRIKE]', ' [C_IV]', ' [P_IV]', ' [C_LAST]', ' [P_LAST]', ' [QUOTE_DATE]']
column_types = {
    ' [UNDERLYING_LAST]': 'float64',
    ' [DTE]': 'float64',
    ' [STRIKE]': 'float64',
    ' [C_IV]': 'float64',
    ' [P_IV]': 'object', # Initial import as object to handle potential non-numeric values
    ' [C_LAST]': 'float64',
    ' [P_LAST]': 'float64',
    ' [QUOTE_DATE]': 'object' # Import as object, will convert to datetime later
}

# Get a list of all the files in the directory
files = os.listdir("C:/Users/Santiago/Desktop/Portfolio/DTE_Project/SPY_2022_DATA/")
files = ["C:/Users/Santiago/Desktop/Portfolio/DTE_Project/SPY_2022_DATA/" + file for file in files if file.startswith('spy_01x_')]

# Read all files into a Dask DataFrame
df_all = dd.read_csv(files, usecols=columns_to_import, dtype=column_types)

# Convert ' [P_IV]' and ' [QUOTE_DATE]' to appropriate data types
df_all[' [P_IV]'] = dd.to_numeric(df_all[' [P_IV]'], errors='coerce')
df_all[' [QUOTE_DATE]'] = dd.to_datetime(df_all[' [QUOTE_DATE]'])

# Option pricing function
def calculate_option_price(row):
    # Parameters
    underlying_price = row[' [UNDERLYING_LAST]']
    strike_price = row[' [STRIKE]']
    volatility = row[' [C_IV]']
    dividend_rate = 0.0  # assumption
    risk_free_rate = 0.015  # assumption
    days_to_expiration = row[' [DTE]']
    calculation_date = ql.Date.todaysDate()

    # Define calendar and day counter
    calendar = ql.UnitedStates(ql.UnitedStates.NYSE)
    day_counter = ql.Actual365Fixed()

    # Define American exercise
    expiration_date = calculation_date + int(days_to_expiration)
    exercise = ql.AmericanExercise(calculation_date, expiration_date)

    # American option
    payoff = ql.PlainVanillaPayoff(ql.Option.Call, strike_price)
    american_option = ql.VanillaOption(payoff, exercise)

    # Set evaluation date
    ql.Settings.instance().evaluationDate = calculation_date

    # Construct the European Option
    spot_handle = ql.QuoteHandle(ql.SimpleQuote(underlying_price))
    flat_ts = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, risk_free_rate, day_counter))
    dividend_yield = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, dividend_rate, day_counter))
    flat_vol_ts = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(calculation_date, calendar, volatility, day_counter))
    bsm_process = ql.BlackScholesMertonProcess(spot_handle, dividend_yield, flat_ts, flat_vol_ts)

    # Use binomial tree (Cox-Ross-Rubinstein)
    steps = 200
    binomial_engine = ql.BinomialVanillaEngine(bsm_process, "crr", steps)
    american_option.setPricingEngine(binomial_engine)

    # Theoretical price
    return american_option.NPV()

# Calculate the fair prices
df_all['fair_price'] = df_all.map_partitions(lambda df: df.apply(calculate_option_price, axis=1), meta=('fair_price', 'f8'))

# Calculate the price differences
df_all['call_price_diff'] = df_all[' [C_LAST]'] - df_all['fair_price']
df_all['put_price_diff'] = df_all[' [P_LAST]'] - df_all['fair_price']

# Get summary statistics
call_price_diff_summary = df_all['call_price_diff'].describe().compute()
put_price_diff_summary = df_all['put_price_diff'].describe().compute()

# Print summary statistics
print("Call Price Difference Summary Statistics:\n", call_price_diff_summary)
print("\nPut Price Difference Summary Statistics:\n", put_price_diff_summary)

# Plot histograms of price differences
df_all[['call_price_diff', 'put_price_diff']].compute().hist(bins=50, alpha=0.5)
plt.title('Histograms of Call and Put Price Differences')
plt.xlabel('Price Difference')
plt.ylabel('Frequency')
plt.show()

# Perform t-tests for price differences
call_ttest_result = stats.ttest_1samp(df_all['call_price_diff'].dropna().compute(), 0)
put_ttest_result = stats.ttest_1samp(df_all['put_price_diff'].dropna().compute(), 0)

# Print t-test results
print("T-test result for call price difference: ", call_ttest_result)
print("T-test result for put price difference: ", put_ttest_result)

# Segment the data by days to expiration and compute summary statistics for each group
grouped = df_all.groupby(' [DTE]')
grouped_summary = grouped[['call_price_diff', 'put_price_diff']].describe().compute()

# Print the grouped summary statistics
print(grouped_summary)





































# def convert_unix_to_readable(df, unix_cols):
#     for col in unix_cols:
#         df[col] = pd.to_datetime(df[col], unit='s')
#     return df

# def clean_column_name(column_name):
#     return column_name.replace("[", "").replace("]", "").strip().lower()

# DF = []

# df_dict = {
#     "df_01": df_01,
#     "df_02": df_02,
#     "df_03": df_03,
#     "df_04": df_04,
#     "df_05": df_05,
#     "df_06": df_06,
#     "df_07": df_07,
#     "df_08": df_08,
#     "df_09": df_09,
#     "df_10": df_10,
#     "df_11": df_11,
#     "df_12": df_12
# }

# epsilon = 0.9

# for df_name in df_dict:
#     temp = df_dict[df_name][df_dict[df_name][' [DTE]'].abs() <= epsilon]
#     temp = temp.compute()
#     DF.append(temp)

# results = pd.concat(df, ignore_index=True)

# results = convert_unix_to_readable(results,['[QUOTE_UNIXTIME]',' [EXPIRE_UNIX]'])
# results.columns = [clean_column_name(column) for column in results.columns]    
# results.to_csv(r'C:\Users\Santiago\Documents\DTE_DATA.txt', sep='\t', index=False)
















# df = pd.concat(df_dict, ignore_index=True)

# df.replace('', np.nan, )

# column_types = {
#     ' [UNDERLYING_LAST]': 'float64',
#     ' [DTE]': 'float64',
#     ' [STRIKE]': 'float64',
#     ' [C_IV]': 'float64',
#     ' [P_IV]': 'float64',
#     ' [C_LAST]': 'float64',
#     ' [P_LAST]': 'float64',
# }

# df = df.astype(column_types)

# df[' [QUOTE_DATE]'] = pd.to_datetime(df[' [QUOTE_DATE]'])
















































