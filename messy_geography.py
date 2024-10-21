import pandas as pd

df = pd.read_csv('messy_geography.csv')

df['Town'] = df['Region Town'].str.extract(r'(\w+)$')

df['Region'] = df['Region Town'].str.split().str[:-1].str.join(' ')

df.drop(columns=['Region Town'], inplace=True)

df.to_csv('geography.csv')



zip_codes = pd.read_csv('MA Zip Codes.csv')

zip_codes.str.explode('County')

county_idx = []
for i, row in zip_codes.iterrows():
    if 'County' in row['ZIP Code']:
        county_idx.append(i)

county_idx.append(len(zip_codes))
row_start = [idx - 3 for idx in county_idx]

header = ['ZIP Code', 'Type', 'Common Cities', 'County', 'Area Codes']
rows = []
current_row_idx = 0
current_row = {'Area Code':''}
for i, row in zip_codes.iterrows():
    if i in row_start:
        rows.append(current_row)
        current_row_idx += 1
        current_row = {'Area Code':''}
    diff = i - row_start[current_row_idx]
    value = row['ZIP Code'] 
    print(i, current_row_idx, differecnce)
    if diff == 0:
        current_row['Zip Code'] = value 
    elif diff == 1:
        current_row['Type'] = value 
    elif diff == 2:
        current_row['Common Cities'] = value 
    elif diff == 3:
        current_row['County'] = value 
    elif diff >= 4:
        current_row['Area Code'] = current_row['Area Code'] + value 


    #     new_row = {}
    #     new_row['Zip Code'] = value
    # if i + 2 in county_idx:
