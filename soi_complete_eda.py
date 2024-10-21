import pandas as pd



soi_990_ez_dir = 'input_files/2023/Form 990-EZ extract XLSX (2023).csv'
soi_990_dir = 'input_files/2023/Form 990 extract XLSX (2023).csv'

soi_990_df = pd.read_csv(soi_990_dir)
soi_990_ez_df = pd.read_csv(soi_990_ez_dir)

WGI_dir = 'input_files/WGI/WGI_6.0_EIN_10-6-2024.csv'
WGI_df = pd.read_csv(WGI_dir,skiprows=1)

soi_990_df.rename(columns={'ein':'EIN'},inplace=True)

soi_df = pd.concat([soi_990_df,soi_990_ez_df])

expanded_df = WGI_df.merge(soi_df,how='left',on='EIN')

