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
import difflib

from fuzzywuzzy import process, fuzz
# avoid warning
import logging
logging.getLogger().setLevel(logging.ERROR)

########################
# FUNCTIONS
#######################  

def get_match(df, col, df_choices, col_choices, common, cutoff, method = fuzz.ratio):
    '''
    '''
    matches = {}
    for i,r in df_choices[df_choices[common] == df[common]].iterrows():
        row_pos = [r[c] for c in col_choices if r[c] is not np.nan]
        try: match_row = process.extractOne(df[col],row_pos,score_cutoff=cutoff,scorer = method)
        except: continue
        if match_row is not None: 
            matches[i] = match_row[1] 
    #match_dict = dict(matches)
    # get the best match   
    #match = max(match_dict, key=match_dict.get)
    if len(matches) >0:
        matches_sorted = dict(sorted(matches.items(), key=lambda x:x[1]))
        max_match = (list(matches_sorted.keys())[0], list(matches_sorted.values())[0])
        #if len(matches) >1: 
            #max_match2 = (list(matches_sorted.keys())[1], list(matches_sorted.values())[1])
            #return max_match, max_match2
        return max_match
        #max(matches, key=matches.get)

def match_and_merge(df1, df2, newcol, df1_col, df2_cols, common = 'transno', cutoff = 80, fuzzy = fuzz.ratio):
    '''
    matches values based on function get_match and merges them
    '''    
    df1[newcol] = df1.apply(get_match, args = (df1_col, df2, df2_cols, common, cutoff, fuzzy), axis = 1)

    df1[f'{newcol}_index'] = df1[newcol].apply(lambda row: row[0] if row is not None else np.nan)

    df1_clean = df1[df1[newcol].notnull() & (df1[newcol] != '')]

    merge = df2.merge(df1_clean, how='inner',left_on=['county','transno',df2.index.values], right_on = ['county','transno',f'{newcol}_index'])
    
    merge = merge.drop([f'{newcol}_index'], axis=1)

    df1 = df1.drop([f'{newcol}_index'], axis=1, inplace=True)

    return merge 

########################
# DATA
#######################
wd = Path.cwd()

zip = ZipFile(wd.parent/'data'/'post_pre_paid.zip')

post = pd.read_csv(zip.open('Postpaid_AFDB_TX_Data_20220126.txt'), sep = '|', dtype={'SERIAL_NUM': str, 'ACCOUNT_NO':str})

pre = pd.read_csv(zip.open('Prepaid_AFDB_TX_Data_20220126.txt'), sep = '|',dtype={'SERIAL_NUM': str, 'ACCOUNT_NO':str})

zip_survey = ZipFile(wd.parent/'data'/'survey.zip')

survey = pd.read_stata(zip_survey.open('survey/workingsample8.dta'))

########################
# PREPARE survey
#########################

# keep relevant columns
survey = survey[['county', 'transno','transname', 'a1_7','a3_15','a3_22','hh_member1','hh_member2', 'hh_member3', 'hh_member4', 'hh_member5', 'l1_1','l1_2']]

survey['treatment'] = survey['a1_7'].apply(lambda row: 1 if int(re.sub(r'\D','',row)) < 32 else 0)
#cleanup_yesno = {'b1_4': {'Yes':1,'No':0},'c1_3':{'Yes':1,'No':0}}
#survey = survey.replace(cleanup_yesno) 

#survey[survey.b1_4 != survey.c1_3]

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

#survey[numbers] = survey[numbers].astype(float)

survey['county'] = survey['county'].str.lower()
#survey['trans_no'] = survey['trans_no'].astype(int).astype(str)
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


##set(survey.transno).intersection(set(pp.transno))
pp['survey_trans'] = pp['transno'].apply(lambda row: 1 if row in set(survey.transno) else 0)
#print(pp[pp.survey_trans == 0]) 
#print(survey[survey.transno == '41755 kwini market'])

# !!! I assume that 'kwni market' = '41755 kwini market'
pp.loc[pp.transno == 'kwni market','transno'] =  '41755 kwini market'
pp['survey_trans'] = pp['transno'].apply(lambda row: 1 if row in set(survey.transno) else 0)
#print('should be an empty dataframe:\n')
print(pp[pp.survey_trans == 0]) 

pp = pp.drop('survey_trans', axis=1)

# extract transformer number
#pp['trans_no'] = pp['transno'].apply(lambda row: re.match('([\d]*)\D*([\d]*)\D',row).groups()[0])
'''
-> this allows to use the column 'transno' for matching
'''


########################
# MERGE based on name and serial- and account number
#########################

merge_serial = match_and_merge(pp, survey, newcol = 'closest_serial',df1_col = 'serial_num',df2_cols=['l1_1','l1_2'],cutoff = 95, fuzzy=fuzz.ratio)

merge_account = match_and_merge(pp, survey, newcol = 'closest_account',df1_col = 'account_no',df2_cols=['l1_1','l1_2'],cutoff = 95, fuzzy=fuzz.ratio)

merge_name = match_and_merge(pp, survey, newcol = 'closest_name',df1_col = 'full_name',df2_cols=['a3_15','a3_22','hh_member1','hh_member2', 'hh_member3', 'hh_member4','hh_member5', 'hh_member5'],cutoff = 85, fuzzy=fuzz.token_set_ratio)

merged = pd.concat([merge_serial, merge_account, merge_name])
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

# add unmatched entries in survey

cols = survey.columns.tolist()
cols.append('offered_service')

merged[merged[cols].duplicated(keep=False)].sort_values(survey.columns.tolist()).drop(['serial_num','account_no','txnumber'], axis=1)

merged.groupby(['treatment'])['county'].count()/merged.shape[0]

#survey['matched'] = survey.merge(merged[survey.columns.tolist()].drop_duplicates(), how='left', indicator=True, on = survey.columns.tolist())['_merge'].ne('left_only')
