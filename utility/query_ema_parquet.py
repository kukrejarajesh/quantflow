
from ema_consolidated_query import EODDataQuery
from config import example_tokens, instruments_to_process
import pandas as pd

# Assuming df is your DataFrame
output_file = 'stock_above_ema.xlsx'


# index=False prevents writing the DataFrame index as a column in Excel
instruments=example_tokens
query = EODDataQuery()

print("\n" + "="*80)
print("Example 6: Find instruments above EMA 50")
print("="*80)
above_ema = query.get_instruments_above_ema(instruments, ema_period=9)
if above_ema is not None and len(above_ema) > 0:
    print(above_ema)
    above_ema.to_excel(output_file, index=False) 
else:
    print("No instruments found above EMA 50")