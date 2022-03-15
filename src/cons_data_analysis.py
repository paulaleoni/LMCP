'''
summary statistics, histograms, etc
Input: 
'''

from datetime import timedelta
import zipfile
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator, PercentFormatter
from datetime import timedelta
from zipfile import ZipFile


wd = Path.cwd()
folder = 'data'

# load data
zip = ZipFile(wd.parent/folder/'cons_data_all.zip')
df = pd.read_csv(zip.open('cons_data_all.csv'), parse_dates=['vending_date', 'meterinstdate'], dtype={'meternumber':str, 'zrefrence':str})


#*#########################
#! CHECK DATA
#*#########################

df.describe()
nmis = df.shape[0] - df.amount.count() # 1705 rows have missing values for amount, etc.
# all missings come from kilifi
df.loc[df.amount.isnull() & (df.county == 'KILIFI'),].shape[0] == nmis


#*#########################
#! PREPARE DATA
#*#########################

# 15512 names Xx
# df.loc[df.name == 'Xx',]

# add rows if same meternumber and vending_date
collapse = df.groupby(['meternumber', 'vending_date'])[['amount', 'units', 'debt_collected']].sum()
collapse.reset_index(inplace = True)

# drop columns amount units and debt, add the collapse data and drop duplicates
df = df.drop(columns = ['amount', 'units', 'debt_collected'])
df = df.merge(collapse, on = ['meternumber', 'vending_date'], how='left').drop_duplicates(['meternumber', 'vending_date'])

# sort df by meternumber and vending date
df = df.sort_values(by = ['meternumber', 'vending_date'])

# time elapsed between purchases 
df['time_elapsed'] = df['vending_date'].diff() 
df.loc[(df.meternumber != df.meternumber.shift(1)) ,'time_elapsed'] = np.nan

# get net amount
df['amount_net'] = df.amount - df.debt_collected

# get year
df['year'] = df.vending_date.dt.year

# first vending date per customer
first_vend = df.groupby(['meternumber'])['vending_date'].min()
first_vend = first_vend.rename('first_vending_date')
df = df.merge(first_vend, left_on=['meternumber'], right_index=True, how = 'left')

# last vending date
last_vend = df.groupby(['meternumber'])['vending_date'].max()
last_vend = last_vend.rename('last_vending_date')
df = df.merge(last_vend, left_on=['meternumber'], right_index=True, how = 'left')

# keep one row for each meter 
df_cust = df.drop_duplicates(['meternumber']).reset_index(drop=True)
df_cust = df_cust.drop(columns=['vending_date', 'amount', 'units', 'debt_collected', 'time_elapsed', 'amount_net', 'year'])

# year of first purchase
df_cust['year'] = df_cust.first_vending_date.dt.year


# number of purchases per meter % per year / 6 months
no_purchase = df.meternumber.value_counts()
no_purchase = no_purchase.rename('no_purchase')

df_cust = df_cust.merge(no_purchase, left_on='meternumber', right_index=True, how = 'left')
df_cust = df_cust.reset_index(drop=True)

# no of purchases per year
no_purch_year = df.value_counts(subset = ['meternumber', 'year']).rename('no_purch_year')
df = df.merge(no_purch_year, how = 'left', left_on=['meternumber', 'year'], right_index=True)

# gap between first purchase and meterinstdate
df_cust['gap'] = df_cust.first_vending_date - df_cust.meterinstdate

# make time series: check periods between vending date and first vending date
df['days_since_inst'] = df.vending_date - df.first_vending_date # days from first to vending


# make panel
period = 90 # 3-month period
max_p = (df.days_since_inst.max()/period).days # maximum number of periods
# create new column
df['period_since_inst'] = np.nan
# loop over periods
for p in range(1,max_p+1):
    # assign periods
    bool = (df.days_since_inst >= timedelta(days = (p-1)*period)) & (df.days_since_inst < timedelta(days = p*period))
    df.loc[bool,'period_since_inst'] = p

# created aggregated variables
amount_pp = df.groupby(['meternumber', 'period_since_inst'])[['amount', 'amount_net', 'units']].sum()
amount_pp.reset_index(inplace = True)

df_cust_panel = df_cust.merge(amount_pp, on=['meternumber'])


# export df of customers
df_cust_panel.to_csv(wd.parent/'data'/'cons_data_pmeter_panel.zip', index=False, compression={'method': 'zip', 'archive_name': 'cons_data_pmeter_panel.csv'})


#*#########################
#! HISTOGRAMS
#*#########################

# @DANA: if you want to check entries with a negative gap (first vending is before meterinstdate) - 9253 entries
# df_cust.loc[df_cust.gap < timedelta(days = 0),]

# unique counties
county = df.county.unique()
for c in county:

    fig,ax=plt.subplots(2,2, figsize = (10,5))
    fig.suptitle(c)

    # meterinstdate
    input = df_cust.loc[df_cust.county == c,'meterinstdate'].dt.year
    ax[0,0].hist(input, density = True)
    ax[0,0].set_xticks([2016, 2017, 2018, 2019, 2020, 2021])
    ax[0,0].tick_params(axis="x")
    ax[0,0].set_title('date of installation')

    # gap
    time = 'D' # D: days, M: months
    input = df_cust.loc[df_cust.county == c,'gap'].astype(f'timedelta64[{time}]')
    binwidth = 30
    ax[0,1].hist(input, bins=range(int(input.min()), int(input.max()) + binwidth , binwidth), density = True)
    ax[0,1].xaxis.set_major_locator(MaxNLocator(7)) 
    ax[0,1].tick_params(axis="x")
    ax[0,1].set_title(f'time between installation and first purchase in {time}')
    #ax[0,1].set_xlim(0) # if you only want to see only positive values

    # number of purchases % make percent on yaxis
    input = df.loc[df.county == c,'no_purch_year']
    binwidth = 5
    ax[1,0].hist(input, bins=range(int(input.min()), int(input.max()) + binwidth , binwidth), density = True)
    ax[1,0].xaxis.set_major_locator(MaxNLocator(7)) 
    ax[1,0].tick_params(axis="x")
    ax[1,0].set_title('# purchases in a year')

    # time elapsed
    input = df.loc[df.county == c,'time_elapsed'].dt.days
    binwidth = 5
    ax[1,1].hist(input, bins=range(int(input.min()), int(input.max()) + binwidth , binwidth), density = True)
    ax[1,1].xaxis.set_major_locator(MaxNLocator(7)) 
    ax[1,1].tick_params(axis="x")
    ax[1,1].set_title('days between vendings')
    ax[1,1].set_xlim(1,250)
    
    plt.tight_layout()

    fig.savefig(wd.parent/f'figures/histograms_{c}.png')
    plt.close()

#*#########################
#! TIME SERIES
#*#########################
# df_cust_panel: customer-period level, amount and units aggregated

'''
plt.scatter(df_cust_panel.period_since_inst, df_cust_panel.amount)
plt.title('amount per period since meterinstdate - period = 90 days')
plt.savefig(wd.parent/'figures'/'amount_scatter.png')
plt.close()

plt.scatter(df_cust_panel.period_since_inst, df_cust_panel.amount_net)
plt.title('net amount per period since meterinstdate - period = 90 days')
plt.savefig(wd.parent/'figures'/'amount_net_scatter.png')
plt.close()

plt.scatter(df_cust_panel.period_since_inst, df_cust_panel.units)
plt.title('units per period since meterinstdate - period = 90 days')
plt.savefig(wd.parent/'figures'/'amount_net_scatter.png')
plt.close()
'''
  
# line plot # take median of all customer-period level pairs
periods = df_cust_panel.groupby(['period_since_inst', 'year'])[['amount', 'amount_net', 'units']].median()
periods.reset_index(inplace=True)

# units
fig, ax = plt.subplots()
for y in periods.year.unique():
    input = periods.loc[periods.year == y,]
    ax.plot(input.period_since_inst, input.units, label = f'{int(y)}')
    ax.set_title('median of units')
    ax.set_xlabel(f'periods since first purchase (period  = {period} days)')
    ax.legend()

#fig.savefig(wd.parent/'figures'/'units_year.png')
#plt.close()

# amount
fig, ax = plt.subplots()
for y in periods.year.unique():
    input = periods.loc[periods.year == y,]
    ax.plot(input.period_since_inst, input.amount, label = f'{int(y)}')
    ax.set_title('median of amount')
    ax.set_xlabel(f'periods since first purchase (period  = {period} days)')
    ax.legend()

#fig.savefig(wd.parent/'figures'/'amount_year.png')
#plt.close()

# amount net
fig, ax = plt.subplots()
for y in periods.year.unique():
    input = periods.loc[periods.year == y,]
    ax.plot(input.period_since_inst, input.amount_net, label = f'{int(y)}')
    ax.set_title('median of net amount')
    ax.set_xlabel(f'periods since first purchase (period  = {period} days)')
    ax.legend()

#fig.savefig(wd.parent/'figures'/'amount_net_year.png')
#plt.close()