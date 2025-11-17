Order Flow strategy
Mean_SD_Calculator to calculate mean and sd for all instruments for date range specified, output parquet gets saved in F:\working\2024\Zerodha\breeze_data_service\metrics_rollup\quarterly
Update the list of instruments you want to track for a day. in config.py instruments_to_process field
Run the Metric_Calculation_Scheduler, it has been scheduled to run TickLevelMetrics and Rollup_metrics_batch and Generate_z_score calculator; interval is also defined as of now lets say 15min
Tick_Level_Metrics one need to define date i.e. folder in which it will look for data and run the process. And also it only process the instruments which are defined in config.py instruments_to_process field
To process all instruments one need to uncomment the code 
#uncomment below to process all instruments
            if int(instrument_token) not in instruments_to_process:
                print(f"Skipping instrument token: {instrument_token}")
                continue
Rollup_metrics_batch again date need to be specified, to make sure it looks for data in that folder
Generate_z_score: One need to specify clearly from which parquet mean and s.d will be read, date need to be specified to process data from that folder

This 15 minute real time update of data can be monitored via zscore_dashboard... 
zscore_dashboard_v2 created to have instrument and date filter

