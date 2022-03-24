'''
merge post- and prepaid data with survey
 
 fuzzy matching
'''

import pandas as pd
#pd.set_option('max_columns', None)
from zipfile import ZipFile
from pathlib import Path
import re
import numpy as np
#import difflib

from fuzzywuzzy import process, fuzz
# avoid warning
import logging
logging.getLogger().setLevel(logging.ERROR)

########################
# FUNCTIONS
#######################  

def get_match(df, col, df_choices, col_choices, common, cutoff = 80, method = fuzz.ratio):
    '''
    get best match for each entry in df[col]
    common: column which needs to be the same for both dataframes (transno)
    cutoff: minimum similarity
    method: get more info here: https://pypi.org/project/fuzzywuzzy/
    '''
    # initialize dictionary
    matches = {}
    # loop through rows in df_choices
    for i,r in df_choices[df_choices[common] == df[common]].iterrows():
        # extract entries from col_choices
        row_pos = [r[c] for c in col_choices if r[c] is not np.nan]
        # find the best match among col_choices
        try: match_row = process.extractOne(df[col],row_pos,score_cutoff=cutoff,scorer = method)
        except: continue
        # add to dictionary
        if match_row is not None: 
            matches[i] = match_row[1] 
    # find the best match among all rows        
    if len(matches) >0:
        matches_sorted = dict(sorted(matches.items(), key=lambda x:x[1]))
        max_match = (list(matches_sorted.keys())[0], list(matches_sorted.values())[0])
        # return best match (index in df_choices, similarity score)
        return max_match

def match_and_merge(df1, df2, newcol, df1_col, df2_cols, common = 'transno', cutoff = 80, fuzzy = fuzz.ratio):
    '''
    matches values based on function get_match and merges them
    '''    
    # define new column and apply get_match
    df1[newcol] = df1.apply(get_match, args = (df1_col, df2, df2_cols, common, cutoff, fuzzy), axis = 1)

    # extract the index
    df1[f'{newcol}_index'] = df1[newcol].apply(lambda row: row[0] if row is not None else np.nan)

    # remove non informative entries
    df1_clean = df1[df1[newcol].notnull() & (df1[newcol] != '')]

    # perform inner merge of dataframes
    merge = df2.merge(df1_clean, how='inner',left_on=['county','transno',df2.index.values], right_on = ['county','transno',f'{newcol}_index'])
    
    # drop index columns
    merge = merge.drop([f'{newcol}_index'], axis=1)
    df1 = df1.drop([f'{newcol}_index'], axis=1, inplace=True)
    # return the merged data
    return merge 

########################
# DATA
#######################
wd = Path.cwd()

zip = ZipFile(wd.parent/'data'/'post_pre_paid.zip')

post = pd.read_csv(zip.open('Postpaid_AFDB_TX_Data_20220126.txt'), sep = '|', dtype={'SERIAL_NUM': str, 'ACCOUNT_NO':str})
# load serial_num and account_num as strings for better handling
pre = pd.read_csv(zip.open('Prepaid_AFDB_TX_Data_20220126.txt'), sep = '|',dtype={'SERIAL_NUM': str, 'ACCOUNT_NO':str})

zip_survey = ZipFile(wd.parent/'data'/'survey.zip')

survey = pd.read_stata(zip_survey.open('survey/workingsample8.dta'))

########################
# PREPARE survey
#########################

# keep relevant columns
survey = survey[['county', 'transno','transname', 'a1_7','a3_15','a3_22','hh_member1','hh_member2', 'hh_member3', 'hh_member4', 'hh_member5','hh_member6','hh_member7','hh_member8','hh_member9','hh_member10','hh_member11','hh_member12','hh_member13','hh_member14','hh_member15', 'l1_1','l1_2']]

# define treatment if xx in HH_xx > 32
survey['treatment'] = survey['a1_7'].apply(lambda row: 1 if int(re.sub(r'\D','',row)) < 32 else 0)


# a3_15 - name of respondent
# a3_22 - name of hh head

# extract numbers # meternumber # accountnumber
numbers = ['l1_1','l1_2']
survey[numbers] = survey[numbers].replace('', np.nan)
survey[numbers] = survey[numbers].replace('0', np.nan)
survey[numbers] = survey[numbers].replace(regex= '[\s_-]+', value = np.nan)
#survey[numbers] = survey[numbers].replace(regex= '\d+(\d+)', value = np.nan)

survey[numbers] = survey[numbers].apply(lambda row: re.match('([\d]*)\D',row) if not np.nan else row)
survey[numbers] = survey[numbers].replace(regex = r'[aA-zZ]', value = np.nan)


survey['county'] = survey['county'].str.lower()

survey['a3_15'] = survey['a3_15'].str.casefold()
# remove leading and trailing spaces
survey['a3_15'] = survey['a3_15'].replace(regex=r"^\s+|\s+$", value = '')
# remove duplicated space
survey['a3_15'] = survey['a3_15'].replace(regex=r" +", value = ' ')

# transno
survey['transno'] = survey['transno'].str.casefold()
survey['transno'] = survey['transno'].replace(regex=r',',value=' ')
survey['transno'] = survey['transno'].replace(regex=r"^\s+|\s+$", value = '')
survey['transno'] = survey['transno'].replace(regex=r" +", value = ' ')

########################
# PREPARE pre-post
#########################

# merge pre and post data, drop duplicates
pp = post.append(pre).reset_index(drop=True)

# select relevant columns
pp = pp[['COUNTY','TXNUMBER','TRANSNO','FULL_NAME','SERIAL_NUM','ACCOUNT_NO','OFFERED_SERVICE']].drop_duplicates().reset_index(drop=True)

pp.columns = pp.columns.str.lower()

# remove non informative entries
pp = pp.dropna(subset=['full_name','serial_num','account_no'])


pp['county'] = pp['county'].str.lower()

pp['full_name'] = pp['full_name'].str.casefold()
# remove leading and trailing spaces
pp['full_name'] = pp['full_name'].replace(regex=r"^\s+|\s+$", value = '')
# remove duplicated space
pp['full_name'] = pp['full_name'].replace(regex=r" +", value = ' ')

# transno
pp['transno'] = pp['transno'].str.casefold()
pp['transno'] = pp['transno'].replace(regex=r',',value=' ')
pp['transno'] = pp['transno'].replace(regex=r"^\s+|\s+$", value = '')
pp['transno'] = pp['transno'].replace(regex=r" +", value = ' ')


# check if all transformers in pp exist in survey
##set(survey.transno).intersection(set(pp.transno))
pp['survey_trans'] = pp['transno'].apply(lambda row: 1 if row in set(survey.transno) else 0)
#print(pp[pp.survey_trans == 0]) 
#print(survey[survey.transno == '41755 kwini market'])

# !!! I assume that 'kwni market' = '41755 kwini market'
pp.loc[pp.transno == 'kwni market','transno'] =  '41755 kwini market'
pp['survey_trans'] = pp['transno'].apply(lambda row: 1 if row in set(survey.transno) else 0)
#print('should be an empty dataframe:\n')
#print(pp[pp.survey_trans == 0]) 

pp = pp.drop('survey_trans', axis=1)


'''
-> this allows to use the column 'transno' for matching
'''


########################
# MERGE based on name, serial- and account number
#########################

merge_serial = match_and_merge(pp, survey, newcol = 'closest_serial',df1_col = 'serial_num',df2_cols=['l1_1','l1_2'],cutoff = 95, fuzzy=fuzz.ratio)

merge_account = match_and_merge(pp, survey, newcol = 'closest_account',df1_col = 'account_no',df2_cols=['l1_1','l1_2'],cutoff = 95, fuzzy=fuzz.ratio)

merge_name = match_and_merge(pp, survey, newcol = 'closest_name',df1_col = 'full_name',df2_cols=['a3_15','a3_22','hh_member1','hh_member2', 'hh_member3', 'hh_member4','hh_member5', 'hh_member5'],cutoff = 85, fuzzy=fuzz.token_set_ratio)

# concat all merged data
merged = pd.concat([merge_serial, merge_account, merge_name])
# drop if duplicates in all columns accept ['closest_serial','closest_name','closest_account']
merged = merged.drop_duplicates(subset=merged.columns.difference(['closest_serial','closest_name','closest_account']))


# problem: some entries seem to be from the same person but different account or serial numbers or missing
# solution: put them in a list in a new column

dups_serial = merged.groupby(['full_name','transno', 'offered_service'])['serial_num'].apply(lambda x: list(x)).reset_index().rename(columns ={'serial_num':'serial_list'})

merged = merged.merge(dups_serial, how ='left', on=['full_name','transno', 'offered_service'])

dups_account =  merged.groupby(['full_name','transno', 'offered_service'])['account_no'].apply(lambda x: list(x)).reset_index().rename(columns ={'account_no':'account_list'})

merged = merged.merge(dups_account, how ='left', on=['full_name','transno', 'offered_service'])

merged = merged.drop_duplicates(subset = ['full_name','transno', 'offered_service'])

# duplicates
duplicates = merged[merged[['full_name','transno']].duplicated(keep='first')].sort_values(['full_name']).shape[0]
#reasons: offered_service

print('unique merges:', merged.shape[0]-duplicates)
print('survey entries:', survey.shape[0])


# check duplicates

cols = survey.columns.tolist()
cols.append('offered_service')

# show all duplicates
with pd.option_context("display.max_rows", None, "display.max_columns", None):
    display(merged[merged[cols].duplicated(keep=False)].sort_values(survey.columns.tolist()).drop(['serial_num','account_no','txnumber'], axis=1))

# get percentage of treatment in matching
merged.groupby(['treatment'])['county'].count()/merged.shape[0]


# randomly select x% of merged dataframe
with pd.option_context("display.max_rows", None, "display.max_columns", None):
    display(merged.sample(frac = .05))


