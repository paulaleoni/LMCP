from pathlib import Path
import pandas as pd
from zipfile import ZipFile
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

wd = Path.cwd()
folder = 'data'
path_figure = wd.parent/'figures'/'post_pre_paid'

# load data
zipf = ZipFile(wd.parent/folder/'post_pre_paid.zip')
post = pd.read_csv(zipf.open('Postpaid_AFDB_TX_Data_20220126.txt'),sep = '|')
pre = pd.read_csv(zipf.open('Prepaid_AFDB_TX_Data_20220126.txt'), sep = '|')
pp = post.append(pre).reset_index(drop=True)

pp.columns = pp.columns.str.lower()


pp.loc[pp['billing_date'].isnull(),'billing_date'] = pp.loc[pp['billing_date'].isnull(),'date_of_vend']
pp = pp.drop('date_of_vend',axis=1)
pp['billing_date'] = pd.to_datetime(pp['billing_date'])

pp.loc[pp['id_bill'].isnull(),'id_bill'] = pp.loc[pp['id_bill'].isnull(),'receipt_no']
pp = pp.drop('receipt_no',axis=1)

# add rows if same meternumber and vending_date
id_cols = ['serial_num', 'billing_date']
collapse = pp.groupby(id_cols)[['amount', 'units', 'collected']].sum()
collapse.reset_index(inplace = True)

pp = pp.drop(columns=['amount', 'units', 'collected'])
df = pp.merge(collapse, how='left').drop_duplicates(id_cols)

# sort df by meternumber and vending date
df = df.sort_values(by = id_cols)

# time elapsed between purchases 
df['time_elapsed'] = df['billing_date'].diff() 
df.loc[(df['serial_num'] != df['serial_num'].shift(1)) ,'time_elapsed'] = np.nan

# get year
df['year'] = df['billing_date'].dt.year

# yearmonth
df['month'] = df['billing_date'].dt.month

df['yearmonth'] = df['year'].astype(str) + '/' + df['month'].astype(str) 

# get weekday
weekdays= ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
df['dayofweek'] = pd.Categorical(df['billing_date'].dt.day_name(), categories=weekdays, ordered=True)


# get day of month
df['dayofmonth'] = df['billing_date'].dt.day


# first vending date per meter
first_vend = df.groupby(['serial_num'])['billing_date'].min().reset_index().rename(columns = {'billing_date':'first_billing_date'})
df = df.merge(first_vend, on=['serial_num'], how = 'left')


############# figures ##############

# day of week histogram
categories = df['dayofweek'].value_counts(sort=False).index
counts = df['dayofweek'].value_counts(sort=False,normalize=True).values
fig, ax = plt.subplots(tight_layout=True)
ax.bar(categories, counts, width=0.5)
fig.savefig(path_figure/'dayofweek.png')

# day of month histogram
categories = df['dayofmonth'].value_counts().sort_index().index
counts = df['dayofmonth'].value_counts(normalize=True).sort_index().values
fig, ax = plt.subplots(tight_layout=True)
ax.bar(categories, counts, width=0.5)
fig.savefig(path_figure/'dayofmonth.png')

# time series
ts = df.groupby(['billing_date','offered_service'])[['amount','units']].sum().reset_index()
ts['offered_service'] = pd.Categorical(ts['offered_service'])
colors = {'POSTPAID':'blue', 'PREPAID':'orange'}

offser = ['POSTPAID','PREPAID']
for os in offser:
    data = ts[ts.offered_service ==os]
    fig, ax =plt.subplots()
    ax.set_ylabel('units', color = 'red') 
    ax.plot(data.billing_date,data.units, color='red', alpha=.6)
    ax2 =ax.twinx()
    ax2.set_ylabel('amount', color = 'blue') 
    ax2.plot(data.billing_date,data.amount, color='blue', alpha=.6)
    plt.title(f'{os}')
    fig.savefig(path_figure/f'ts_{os}.png')

# histogram monthly usage

yearmonth = df.groupby(['serial_num','yearmonth','offered_service'])['units'].sum().reset_index()

yearmonth = yearmonth.groupby(['serial_num','offered_service'])['units'].mean().rename('units_monthly_mean').reset_index()


postpaid = yearmonth.offered_service == 'POSTPAID'
post975 = yearmonth.units_monthly_mean[postpaid] < yearmonth.units_monthly_mean[postpaid].quantile(.975)
post025 = yearmonth.units_monthly_mean[postpaid] > yearmonth.units_monthly_mean[postpaid].quantile(.025)

prepaid = yearmonth.offered_service == 'PREPAID'
pre975 = yearmonth.units_monthly_mean[prepaid] < yearmonth.units_monthly_mean[prepaid].quantile(.975)
pre025 = yearmonth.units_monthly_mean[prepaid] > yearmonth.units_monthly_mean[prepaid].quantile(.025)


fig, ax = plt.subplots()
colors = {'POSTPAID':'blue','PREPAID':'red'}
for p in ['POSTPAID','PREPAID']:
    ppaid = yearmonth.offered_service == p
    data = yearmonth.units_monthly_mean[ppaid]
    p975 = data < data.quantile(.975)
    p025 = data > data.quantile(.025)
    sns.histplot(x = data[p975 & p025], ax=ax, stat='percent', kde=True, alpha=.4, label = p + ' 95%', color=colors[p])
ax.set_xlabel('average monthy usage per meter')
plt.legend()
fig.savefig(path_figure/f'hist_usage.png')
