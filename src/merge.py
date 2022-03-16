'''
merge post- and prepaid data with survey
'''

import pandas as pd
#pd.set_option('max_columns', None)
from zipfile import ZipFile
from pathlib import Path
import re
import numpy as np

wd = Path.cwd()

########################
# DATA
#######################

zip = ZipFile(wd.parent/'data'/'post_pre_paid.zip')

post = pd.read_csv(zip.open('Postpaid_AFDB_TX_Data_20220126.txt'), sep = '|', dtype={'SERIAL_NUM': str, 'ACCOUNT_NO':str})

pre = pd.read_csv(zip.open('Prepaid_AFDB_TX_Data_20220126.txt'), sep = '|',dtype={'SERIAL_NUM': str, 'ACCOUNT_NO':str})

zip_survey = ZipFile(wd.parent/'data'/'survey.zip')

survey = pd.read_stata(zip_survey.open('survey/workingsample8.dta'))

########################
# PREPARE survey
#########################

# keep relevant columns
survey = survey[['county', 'transno','transname', 'a1_7','a3_15','a3_22','hh_member1','hh_member2', 'l1_1','l1_2', 'trans_no']]


# extract numbers 
numbers = ['l1_1','l1_2']
survey[numbers] = survey[numbers].replace('', np.nan)
survey[numbers] = survey[numbers].replace('0', np.nan)
survey[numbers] = survey[numbers].replace(regex= '[\s_-]+', value = np.nan)
#survey[numbers] = survey[numbers].replace(regex= '\d+(\d+)', value = np.nan)

survey[numbers] = survey[numbers].apply(lambda row: re.match('([\d]*)\D',row) if not np.nan else row)
survey[numbers] = survey[numbers].replace(regex = r'[aA-zZ]', value = np.nan)

#survey[numbers] = survey[numbers].astype(float)

survey['county'] = survey['county'].str.lower()
survey['trans_no'] = survey['trans_no'].astype(int).astype(str)
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

# extract transformer number
pp['trans_no'] = pp['transno'].apply(lambda row: re.match('([\d]*)\D*([\d]*)\D',row).groups()[0])

# there seems to be one transformer with 2 numbers, so extract the second one also
pp['trans_alternative'] = pp['transno'].apply(lambda row: re.match('([\d]*)\D*([\d]*)\D',row).groups()[1])


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
print(pp[pp.survey_trans == 0]) 
print(survey[survey.transno == '41755 kwini market'])

# !!! I assume that 'kwni market' = '41755 kwini market'
pp.loc[pp.transno == 'kwni market','transno'] =  '41755 kwini market'
pp['survey_trans'] = pp['transno'].apply(lambda row: 1 if row in set(survey.transno) else 0)
print('should be an empty dataframe:\n')
print(pp[pp.survey_trans == 0]) 

pp = pp.drop('survey_trans', axis=1)

'''
-> this allows me to use the column 'transno' for matching
'''


########################
# MERGE based on name
#########################

merged = survey.merge(pp,how='left',left_on=['county','transno','a3_15'],right_on=['county','transno','full_name'])

# works for 87 hh, some are duplicated
merged[merged.full_name.notnull()]


### tests
survey.loc[(survey.county == 'kitui') & (survey.transno == '66686 local tx mwingi substation'),]
pp.loc[(pp.county == 'kitui') & (pp.transno == '66686 local tx mwingi substation'),]

########################
# difflib
#########################
import difflib

survey['closest_pp_name'] = ''

for tr in survey.transno:
    df = survey[survey.transno == tr]
    pp_names = pp.loc[pp.transno == tr,'full_name'].dropna().drop_duplicates().tolist()

    names_dict = {} # survey name : pp name
    for w in df.a3_15:
        match = difflib.get_close_matches(w,pp_names, cutoff=.6, n=1)
        if len(match) > 0:
            names_dict[w] = match[0]

    survey.loc[survey.transno == tr,'closest_pp_name'] = survey.loc[survey.transno == tr,'a3_15'].apply(lambda row: names_dict[row] if row in names_dict.keys() else np.nan)

merged2 = survey.merge(pp,how='left',left_on=['county','transno','closest_pp_name'],right_on=['county','transno','full_name'])

with pd.option_context('display.max_rows', None,):
    print(merged2.loc[merged2.full_name.notnull(),['a3_15','closest_pp_name','full_name']])

'''
########################
# COMPARE numbers
#########################


serialnum = pp['serial_num'].tolist()
accountnum = pp['account_no'].tolist()
l1_1 = survey['l1_1'].tolist()
l1_2 = survey['l1_2'].tolist()

# 
survey['serial_num'] = survey['l1_1'].apply(lambda row: row if row in set(serialnum) else np.nan)
survey['serial_num'] = survey['l1_2'].apply(lambda row: row if row in set(serialnum) else row)

survey['account_no'] = survey['l1_1'].apply(lambda row: row if row in set(accountnum) else np.nan)
survey['account_no'] = survey['l1_2'].apply(lambda row: row if row in set(accountnum) else row)

#set(accountnum).intersection(set(a3_20))

# merge based on county, transformer, serialnum
merged = survey.merge(pp.drop(['trans_alternative','account_no'],axis=1),how='left',on=['county','trans_no','serial_num'])
merged = merged.merge(pp[['county','trans_alternative','serial_num']],how='left',left_on=['county','trans_no','serial_num'],right_on=['county','trans_alternative','serial_num'])

# it worked on 15 hh
len(merged.serial_num.drop_duplicates())

# merge based on county, transformer, serialnum
merged = merged.merge(pp[['county','trans_no','account_no']],how='left',on=['county','trans_no','account_no'])
merged = merged.merge(pp[['county','trans_alternative','account_no']],how='left',left_on=['county','trans_no','account_no'],right_on=['county','trans_alternative','account_no'])

# it worked on 15 hh !!! the same !!!
len(merged.account_no.drop_duplicates())
'''


