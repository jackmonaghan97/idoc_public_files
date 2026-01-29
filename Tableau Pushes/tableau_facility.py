
#%%
'''

Version:      Python 3.12.3

Author:       Jack Monaghan
Raw Source  : IDOC population datasets (https://idoc.illinois.gov/reportsandstatistics/populationdatasets.html) 

write to    : /encrypted/data/Justice_Counts/Prisons/JC Upload/admissions.csv
            : /encrypted/data/Justice_Counts/Prisons/JC Upload/releases.csv
            : /encrypted/data/Justice_Counts/Prisons/JC Upload/population.csv

            
'''
import itertools
import pandas as pd
import psycopg2
import os

def extract_pgres(query : str) -> pd.DataFrame:

    query = 'SELECT * FROM ' + query 
    cursor.execute(query)

    col = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    df_ = pd.DataFrame(data = data, columns = col)

    return df_

def rename_inst(col : pd.Series) -> pd.Series:
   
    omit = [' CC', ' R&C', ' Male']
    for o in omit: col = col.str.replace(o, '', regex = False)
    col = col.str.strip(' ')

    replace_other = ['Missing', 'Transportation']
    col = col.replace(replace_other, 'Other')

    return col

if __name__ == '__main__':

    # retrieve credentials 
    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    
    # if not defined default to the user
    if username == None:
        os.environ['POSTGRES_USER'] = input('ccjda username:')  
        os.environ['POSTGRES_PASSWORD'] = input('password:')

            # reset credentials
        username = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')

    # Connect to db
    conn = psycopg2.connect(
        dbname='archives',
        user = username,
        password = password,
        host = 'ccjda1.icjia.org',
        port="5432" )
    cursor = conn.cursor()

    df_admits   = extract_pgres('justice_counts.idoc_public_admissions')
    df_pop      = extract_pgres('justice_counts.idoc_public_pop')
    df_exits    = extract_pgres('justice_counts.idoc_public_exits')
   
    # Close the connection
    cursor.close()
    conn.close()

    # -------------------------------------------------
    # Upload Karl's Key
    # -------------------------------------------------

    file_path = '/encrypted/data/Justice_Counts/Miscellaneous Tasks/Mapping csv/'
    file_name = 'idoc_public_map.xlsx'
  
    col = ['state_description', 'Justice_Counts']
    key_df = pd.read_excel(file_path + file_name, usecols = col)
  
    where  = key_df['state_description'].duplicated()
    key_df = key_df[~where]

    col = 'Justice_Counts'
    key_df[col] = key_df[col] + ' Offense'

    # Merges Karl's key
    df_pop = df_pop.merge(
        key_df, how = 'left', left_on = 'hofnscd', 
        right_on = 'state_description')
    
    # -------------------------------------------------
    # Create scaffolding table to ensure all combinations
    # -------------------------------------------------

    # CC stands cof Correctional Center
    # ATC stands for Adult Transition Center, and there's only
    # one ATC entry in the population data
    # remove transportion and missin enries

    '''
    test = df_admits['recpcntr'].str.replace(' R&C', '')
    test = test.str.replace(' Special Unit', '')
    test = test.str.replace(' Psych Unit', '')
    test = test.str.replace(' R & C', '')
    test = test.str.replace(' STC', '')
    test = test.str.replace('  Treat Cntr General Pop', '')

    test.unique()

    1. Remove CC and
    2. But don't remove:
        'Like Skills Center', 'Reentry Center',
        '
    4. T

    '''

    # ATC are different from CC but there still in the custody they are still being 
    # housed by 

    # Remove CC
    inst = pd.Series(df_pop['prtinst'].unique())
    inst = inst.str.replace(' CC', '')
    inst = inst.str.replace(' ATC', '')
    where = inst.isin(['Transportation', 'Missing'])
    inst = inst.loc[~where]

    breakdowns  = pd.concat([
        pd.Series(df_pop['race'].unique()),
        pd.Series(df_pop['sex'].unique()),
        pd.Series(df_pop['Justice_Counts'].unique())])
    quarters    = pd.to_datetime(df_pop['record_date'], errors='coerce').unique()

    scaffolding = pd.DataFrame(
        list(itertools.product(inst, breakdowns, quarters)),
        columns=['facility', 'breakdown', 'quarter'])
    where = scaffolding.isna().any(axis = 1)
    scaffolding = scaffolding.loc[~where]

    # -------------------------------------------------
    # Edits 
    # -------------------------------------------------

    # ensure the date columns are in dt format
    df_admits['admitdt'] = pd.to_datetime(df_admits['admitdt'], errors='coerce')
    df_exits['exitdt'] = pd.to_datetime(df_exits['exitdt'], errors='coerce')
    df_pop['record_date'] = pd.to_datetime(df_pop['record_date'], errors='coerce')

    # summarize by quarters
    df_admits['quarter'] = df_admits['admitdt'].dt.to_period('Q')
    df_exits['quarter'] = df_exits['exitdt'].dt.to_period('Q')
    df_pop['quarter'] = df_pop['record_date'].dt.to_period('Q')

    # edit institution names
    df_pop['prtinst'] = rename_inst(df_pop['prtinst'])
    df_admits['recpcntr'] = rename_inst(df_admits['recpcntr'])
    df_exits['relinst'] = rename_inst(df_exits['relinst'])

    # -------------------------------------------------
    # Group Breakdowns & Population
    # -------------------------------------------------

    # group population
    by = ['prtinst', 'quarter']
    result = df_pop.groupby(by = by)['docnbr'].count()
    result = result.reset_index()
    result.rename(columns = {'docnbr' : 'prison_pop'}, inplace = True)
    col = ['breakdown', 'breakdown_category']
    result[col] = 'Total'

    # group race
    by_race = ['prtinst', 'quarter', 'race']
    result_ = df_pop.groupby(by = by_race)['docnbr'].count()
    result_ = result_.reset_index()
    rename = {'docnbr' : 'prison_pop', 'race' : 'breakdown'}
    result_.rename(columns = rename, inplace = True)
    result_['breakdown_category'] = 'Race'

    result = pd.concat([result, result_])    # retype some races
    in_race = ['Not Assigned', 'Unknown']
    where = result['breakdown'].isin(in_race)
    result.loc[where, 'breakdown'] = 'Other'

    # group sex
    by_sex = ['prtinst', 'quarter', 'sex']
    result_ = df_pop.groupby(by = by_sex)['docnbr'].count()
    result_ = result_.reset_index()
    rename = {'docnbr' : 'prison_pop', 'sex' : 'breakdown'}
    result_.rename(columns = rename, inplace = True)
    result_['breakdown_category'] = 'Sex'

    result = pd.concat([result, result_])    # retype some races
    where = result['breakdown'] == 'B'
    result = result.loc[~where]

    # group offense type
    by_hoff = ['prtinst', 'quarter', 'Justice_Counts']
    result_ = df_pop.groupby(by = by_hoff)['docnbr'].count()
    result_ = result_.reset_index()
    rename = {'docnbr' : 'prison_pop', 'Justice_Counts' : 'breakdown'}
    result_.rename(columns = rename, inplace = True)
    result_['breakdown_category'] = 'Offense Type'

    result = pd.concat([result, result_])


    # -------------------------------------------------
    # Add Admits and Exits
    # -------------------------------------------------

    # group admits
    by_adm = ['recpcntr', 'quarter']
    result_ = df_admits[by_adm].value_counts()
    result_ = result_.reset_index()
    result_.rename(columns = {'count' : 'admissions'}, inplace = True)
    result_[['breakdown', 'breakdown_category']] = 'Total'
    result_.rename(columns = {'recpcntr' : 'prtinst'}, inplace = True)

    # merge
    col = ['prtinst', 'quarter', 'breakdown', 'breakdown_category']
    result = result.merge(result_, on = col, how = 'left')

    # group exits
    by_ext = ['relinst', 'quarter']
    result_ = df_exits[by_ext].value_counts()
    result_ = result_.reset_index()
    result_.rename(columns = {'count' : 'exits'}, inplace = True)
    result_[['breakdown', 'breakdown_category']] = 'Total'
    result_.rename(columns = {'relinst' : 'prtinst'}, inplace = True)

    # merge
    col = ['prtinst', 'quarter', 'breakdown', 'breakdown_category']
    result = result.merge(result_, on = col, how = 'left')

    # retype for pgres
    result['quarter'] = result['quarter'].astype('datetime64[ns]')
    result['prison_pop'] = result['prison_pop'].fillna(0).astype(int)

    result.reset_index(drop = True, inplace = True)
    result['id'] = result.index + 1

    col = ['admissions', 'exits']
    result[col] = result[col].fillna(-9999)

    # -------------------------------------------------
    # Upload to Tableau
    # -------------------------------------------------

    # Connect to db
    conn = psycopg2.connect(
        dbname='archives',
        user = username,
        password = password,
        host = 'ccjda1.icjia.org',
        port="5432" )
    cursor = conn.cursor()

    pgres_name = 'tableau.idoc_population_facility'
    print(f'---- Upload {pgres_name} to PostGres')

    # Truncate the table to remove existing data
    query = 'TRUNCATE TABLE ' + pgres_name
    cursor.execute(query)

    # iterate and upload each row
    for index, row in result.iterrows():
        columns = ', '.join(row.index)
        values = ', '.join(['%s'] * len(row))
        insert_query = f'INSERT INTO {pgres_name} ({columns}) VALUES ({values})'
        
        cursor.execute(insert_query, tuple(row))

    # Commit the transaction
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()
