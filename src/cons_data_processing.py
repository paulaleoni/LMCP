'''
combine data in single file
'''

import pandas as pd
from zipfile import ZipFile
from pathlib import Path


wd = Path.cwd()
folder = 'data'

with ZipFile(wd.parent/folder/'consumption.zip', 'r') as zip:
    file_list = []
    for filename in zip.namelist():
        if filename.endswith('.txt') & (filename.__contains__('archive') == False) :
            file_list.append(filename) # all files with txt ending in list
    # make dataframe out of first file
    df = pd.read_csv(zip.open(file_list[0]), sep = '|')
    for f in range(1,len(file_list)):
        new = pd.read_csv(zip.open(file_list[f]), sep = '|')
        if all(new.columns == df.columns): # exclude files that have different structure
            df = df.append(new) # append all files to a df
    
    zip.close()

# add Kilifi data
zip = ZipFile(wd.parent/folder/'consumption.zip')
kilifi = pd.read_csv(zip.open('consumption/Kilifi_data_20210308.txt'), sep = '|', parse_dates=['PURCHASE_DATE', 'CONNECTION_DATE'])

kilifi.rename(columns = {'COUNTY':'county', 'REFERENCE_':'zrefrence','CUSTOMERNAME':'name', 'METER':'meternumber', 'INCMS_CUSTOMER_NAME':'incms_name', 'CONNECTION_DATE':'meterinstdate', 'PURCHASE_DATE':'vending_date', 'AMOUNT_KES':'amount','UNITS_KWH':'units', 'AMOUNT_LCMP_LOAN':'debt_collected'}, inplace = True)

df = df.append(kilifi)

# export to csv
df.to_csv(wd.parent/'data'/'cons_data_all.zip', index=False,compression={'method': 'zip', 'archive_name': 'cons_data_all.csv'})
