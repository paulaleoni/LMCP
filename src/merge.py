'''
merge post- and prepaid data with survey
 
 fuzzy matching
'''

from sys import displayhook
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
        dct= {'survey_i':max_match[0], 'pp_i':df.name, 'score':max_match[1]}
        return dct

def match_and_merge(df1, df2, newcol, df1_col, df2_cols, common = 'transno', cutoff = 80, fuzzy = fuzz.ratio):
    '''
    matches values based on function get_match and merges them
    '''    
    # define new column and apply get_match
    df1[newcol] = df1.apply(get_match, args = (df1_col, df2, df2_cols, common, cutoff, fuzzy), axis = 1)

    # extract the index
    df1[f'{newcol}_index'] = df1[newcol].apply(lambda row: row['survey_i'] if row is not None else np.nan)

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
survey = survey[['county', 'transno','transname', 'a1_7','a3_15','a3_22','hh_member1','hh_member2', 'hh_member3', 'hh_member4', 'hh_member5','hh_member6','hh_member7','hh_member8','hh_member9','hh_member10','hh_member11','hh_member12','hh_member13','hh_member14','hh_member15', 'l1_1','l1_2','lmcp']]

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

# how to choose algorithm : https://pypi.org/project/fuzzywuzzy/


###################  merge on serial number ###################
#  df with merges on serial number
merge_serial = match_and_merge(pp, survey, newcol = 'closest_serial',df1_col = 'serial_num',df2_cols=['l1_1','l1_2'],cutoff = 65, fuzzy=fuzz.ratio)

'''
1.	If there is a 100 score match based on the meter (serial) number, remove that TX observation from the list of possible matches to reduce duplicate matches
'''

# list of rows in survey that got matched by serial_num at a score of minimum 
min_score = 100

lst_survey_merged = merge_serial['closest_serial'].apply(lambda row: row['survey_i'] if row['score'] >= min_score else None).dropna().tolist()
lst_pp_merged = merge_serial['closest_serial'].apply(lambda row: row['pp_i'] if row['score'] >= min_score else None).dropna().tolist()


###################  merge on account number ###################
# df with merges on account number that are not already merged before
df_input = survey.drop(lst_survey_merged)
pp_input = pp.loc[~pp.index.isin(lst_pp_merged)]

merge_account = match_and_merge(pp_input, df_input, newcol = 'closest_account',df1_col = 'account_no',df2_cols=['l1_1','l1_2'], cutoff = 65, fuzzy=fuzz.ratio)

# get rows that are merged having at least minimum score
lst_survey_merged_acc = merge_account['closest_account'].apply(lambda row: row['survey_i'] if row['score'] >= min_score else None).dropna().tolist()
lst_survey_merged_acc.extend(lst_survey_merged)

lst_pp_merged_acc = merge_account['closest_account'].apply(lambda row: row['pp_i'] if row['score'] >= min_score else None).dropna().tolist()
lst_pp_merged_acc.extend(lst_pp_merged)

###################  merge on names ###################

# df with merges on name that are not already merged before
names_list = ['a3_15','a3_22','hh_member1','hh_member2', 'hh_member3', 'hh_member4', 'hh_member5','hh_member6','hh_member7','hh_member8','hh_member9','hh_member10','hh_member11','hh_member12','hh_member13','hh_member14','hh_member15']

df_input = survey.drop(lst_survey_merged_acc)
pp_input = pp.loc[~pp.index.isin(lst_pp_merged_acc)]

merge_name = match_and_merge(pp_input, df_input, newcol = 'closest_name',df1_col = 'full_name',df2_cols=names_list,cutoff = 70, fuzzy=fuzz.token_set_ratio)

################### concat all merged data  ###################
merged = pd.concat([merge_serial, merge_account, merge_name])


'''
2.	For serial number matched at a high score that is not 100 (e.g.  90<=score<100) , let’s also check what the matching score is for the names of the SAME matched observation. If that is high enough (e.g. score>=75) OR there is any overlap in the name, then we declare this as a good match. 
'''
def good_match(df, cols, ser_min = 90, name_min = 75):
    '''
    declare good matches
    '''
    # if score serial = 100
    if (df[cols[0]] not in [None, np.nan]):
        if (df[cols[0]]['score'] == 100):
            return True
    # if score serial between 90-100 and score name > 75
        elif (df[cols[1]] not in [None, np.nan]):
            # check if same match
            same_survey = df[cols[0]]['survey_i'] == df[cols[1]]['survey_i']
            same_pp = df[cols[0]]['pp_i'] == df[cols[1]]['pp_i']
            # check serial score
            score_ser = (df[cols[0]]['score'] >= ser_min) & (df[cols[0]]['score'] < 100)
            # check name score
            score_name = df[cols[1]]['score'] >= name_min
            # if all True
            bool = same_survey + same_pp + score_name + score_ser == 4
            return bool
        else: return False
    else: return False



merged['good_match'] = merged.apply(lambda row: good_match(row, ['closest_serial', 'closest_name']), axis = 1)


'''
3.	We still have some duplicate matches where one of the matches has a higher score, are we not keeping the matches with the highest score among duplicates  and dropping the rest?
'''

# get the highest score among, serial, account and name matches
def highest_score(df, cols):
    list = [x['score'] for x in df[cols] if (x is not np.nan) & (x is not None)]
    return max(list)

merged['highest_score'] = merged.apply(lambda row: highest_score(row,['closest_serial','closest_account','closest_name']), axis=1)

def highest_dup(df, cols = survey.columns.tolist()):
    '''
    when duplicates keep only the ones with highest score
    '''
    # get all rows
    bool = merged[cols].isin(df[cols].values.ravel()).all(axis=1)
    # maximum score in group
    max_dup_score = merged.loc[bool, 'highest_score'].max()
    # return True if row has highest score or is declared as good match
    yes = (df['highest_score'] >= max_dup_score) or df['good_match'] 
    return yes

high_dups = merged.apply(lambda row: highest_dup(row), axis=1)

merged = merged[high_dups]


#############################

# problem: some entries seem to be from the same person but different account or serial numbers or missing
# solution: put them in a list in a new column

identifier = survey.columns.tolist()
identifier.extend(['full_name', 'offered_service'])

dups_serial = merged.groupby(identifier)['serial_num'].apply(list).reset_index().rename(columns ={'serial_num':'serial_list'})

merged = merged.merge(dups_serial, how ='left', on=identifier)

dups_account =  merged.groupby(identifier)['account_no'].apply(list).reset_index().rename(columns ={'account_no':'account_list'})

merged = merged.merge(dups_account, how ='left', on=identifier)

merged = merged.drop_duplicates(subset = identifier)



'''
in the remaining duplicates, perform a name matching based on a different algorithm
'''

# list of names columns in survey: names_list

algo_match = fuzz.token_sort_ratio
merged['name_match_score'] = merged.apply(lambda row: process.extractOne(row.full_name, row[names_list], scorer=algo_match)[1] , axis=1)

# keep duplicates with highes match
def dups_name_match(df, cols = survey.columns.tolist()):
    '''
    when duplicates keep only the ones with highest name_match_score
    '''
    # get all rows
    bool = merged[cols].isin(df[cols].values.ravel()).all(axis=1)
    # maximum score in group
    max_dup_score = merged.loc[bool, 'name_match_score'].max()
    # return True if row has highest score 
    bool = (df['name_match_score'] >= max_dup_score) 
    return bool
    
# boolean serios
dups_highest_name = merged.apply(lambda row: dups_name_match(row), axis=1)

# keep those with highest match among duplicates
merged = merged[dups_highest_name]

# save as csv
merged.to_csv(wd.parent/'data'/'survey_prepost_matched.csv', index=False)
###############################
'''
4.	After doing step 3 above, can we have some summary stats: what proportion of the matches has a score of 100 based on serial id (I think it was about 850 observations)? What proportion has a score above 90 but not equal to 100?
'''

# you might restriction the matching of those who have a highest score above x
#merged = merged[merged['highest_score'] >= 85]

ser100 = merged[merged['closest_serial'].apply(lambda row: row['score'] == 100 if row is not None else False)].shape[0]

ser90 = merged[merged['closest_serial'].apply(lambda row: row['score'] in range(90,100) if row is not None else False)].shape[0]

print('Proportion score 100 based on serial number:', ser100/merged.shape[0])

print('Proportion score 90-100 based on serial number:', ser90/merged.shape[0])

print('Proportion score >= 90:', merged[merged.highest_score >=90].shape[0]/merged.shape[0])

# get percentage of treatment in matching
print('percentage share of lmcp: \n')
print(merged.groupby(['lmcp'])['county'].count()/merged.shape[0],'\n')

#####################################################

# filter for good merges
#good_merged = merged[merged['score'] > 90]

#print('unique merges:', merged.shape[0]-duplicates)
#print('survey entries:', survey.shape[0])

#good_rows = good_merged.shape[0]
#print(f'number of rows: {good_rows}')

# get percentage of treatment in matching
#print('percentage share of lmcp: \n')
#print(good_merged.groupby(['lmcp'])['county'].count()/good_merged.shape[0],'\n')

# check duplicates

#cols = survey.columns.tolist()
#cols.append('offered_service')

#print('all duplicated rows: \n')
#duplicates = good_merged[good_merged[cols].duplicated(keep=False)].shape[0]
#print(f'N = {duplicates}')
#with pd.option_context("display.max_rows", None, "display.max_columns", None):
 #   display(good_merged[good_merged[cols].duplicated(keep=False)].sort_values(survey.columns.tolist(), axis=0).drop(['hh_member5','hh_member6','hh_member7','hh_member8','hh_member9','hh_member10','hh_member11','hh_member12','hh_member13','hh_member14','hh_member15'], axis=1))


# randomly select x% of merged dataframe
#with pd.option_context("display.max_rows", None, "display.max_columns", None):
#x = 0.05
#    display(merged.sample(frac = x))



#good_merged.to_csv(wd.parent/'data'/'survey_prepost_matched_good.csv')

