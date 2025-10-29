
# %% 
# Import your module

from data_reader_minutecandle import MinuteDataReader, main_jupyter

# Create an instance
reader = MinuteDataReader()

# List available symbols
symbols = reader.list_available_symbols()
print("Available symbols:", symbols[:10])  # Show first 10

# Load data for a specific symbol
df = reader.load_symbol_data('CADHEA', start_date='2024-09-19', end_date='2024-09-23')

# Describe the data
reader.describe_data('CADHEA')

# Plot the data
reader.plot_symbol('CADHEA', period='1min')

reader.export_to_csv('CADHEA', output_path=None)
# Or use the main_jupyter function
# main_jupyter(['--list'])
# main_jupyter(['--plot', 'TVSMOT', '--period', '1D'])
# main_jupyter(['--describe', 'TVSMOT'])
# %%
