
'''
Version:      Python 3.12.3

Author:       Jack Monaghan
Raw Source  : IDOC population datasets (https://idoc.illinois.gov/reportsandstatistics/populationdatasets.html) 

write to    : /encrypted/data/Justice_Counts/Prisons/JC Upload/admissions.csv
            : /encrypted/data/Justice_Counts/Prisons/JC Upload/releases.csv
            : /encrypted/data/Justice_Counts/Prisons/JC Upload/population.csv

            
'''

import pandas as pd
import psycopg2
import os
import itertools

def extract_pgres(query : str) -> pd.DataFrame:

    query = 'SELECT * FROM ' + query 
    cursor.execute(query)

    col = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()

    df_ = pd.DataFrame(data = data, columns = col)

    return df_

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
    df_cen_pop  = extract_pgres('justice_counts.geoid_population')

    # don't know what's going on with the entires that say state
    # but their values are clearly Illinois' ucgid code 
    where = df_cen_pop['variable'] == 'state'
    df_cen_pop = df_cen_pop.loc[~where]

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
    
    where = df_pop['Justice_Counts'] == 'Unknown Offense'
    df_pop.loc[where, 'Justice_Counts'] = 'Missing Offense Record'

    # -------------------------------------------------
    # Create scaffolding table to ensure all combinations
    # -------------------------------------------------

    ucgid       = df_cen_pop['ucgid'].unique()
    breakdowns  = pd.concat([
        pd.Series(df_cen_pop['variable'].unique()),
        pd.Series(key_df['Justice_Counts'].unique())])
    quarters    = pd.to_datetime(df_pop['record_date'], errors='coerce').unique()

    scaffolding = pd.DataFrame(
        list(itertools.product(ucgid, breakdowns, quarters)),
        columns=['ucgid', 'breakdown', 'quarter'])
    scaffolding['year'] = scaffolding['quarter'].dt.year

    where = (scaffolding['quarter'].dt.year < 2024) & (scaffolding['quarter'].dt.year > 2008)
    scaffolding = scaffolding.loc[where]

    # -------------------------------------------------
    # Add census population values
    # -------------------------------------------------

    scaffolding = scaffolding.merge(
        df_cen_pop, how = 'left',
        left_on = ['ucgid', 'year', 'breakdown'],
        right_on= ['ucgid', 'year', 'variable'])
    scaffolding = scaffolding.rename(columns = {'value' : 'gen_population'})

    # force county names
    col = ['ucgid', 'county_name']
    unique = df_cen_pop.loc[~df_cen_pop[col].duplicated(), col]
    scaffolding = scaffolding.merge(
        unique, how = 'left', on = 'ucgid')
    scaffolding.drop(columns = 'county_name_x', inplace = True)
    scaffolding.rename(columns = {'county_name_y' : 'county_name'}, inplace = True)

    # -------------------------------------------------
    # Urban
    # -------------------------------------------------

    df_urban = pd.read_excel(
        "/encrypted/data/Justice_Counts/Miscellaneous Tasks/2020_UA_COUNTY.xlsx",
        usecols = ['STATE_NAME', 'COUNTY', 'POPPCT_URB'])
    
    # select only Illinois
    where = df_urban['STATE_NAME'] == 'Illinois'
    df_urban = df_urban.loc[where]
    where = df_urban['POPPCT_URB'] > 0.7
    
    # Define urban counties
    where = df_urban['POPPCT_URB'] > 0.7
    urban_counties = df_urban.loc[where, 'COUNTY']
    scaffolding['urban'] = scaffolding['ucgid'].str.replace('17', '').astype(int).isin(urban_counties)

    # -------------------------------------------------
    # Format dt for merge & aggregations
    # -------------------------------------------------

    # ensure the date columns are in dt format
    df_admits['admitdt'] = pd.to_datetime(df_admits['admitdt'], errors='coerce')
    df_exits['exitdt'] = pd.to_datetime(df_exits['exitdt'], errors='coerce')
    df_pop['record_date'] = pd.to_datetime(df_pop['record_date'], errors='coerce')

    # summarize by quarters
    df_admits['quarter'] = df_admits['admitdt'].dt.to_period('Q')
    df_exits['quarter'] = df_exits['exitdt'].dt.to_period('Q')
    df_pop['quarter'] = df_pop['record_date'].dt.to_period('Q')

    # -------------------------------------------------
    # Compute breakdowns for population
    # -------------------------------------------------

    # So sometime DeKalb is spelled Dekalb so I'm creating this key
    # which can also act as a merge key to group data
    key = df_pop['stnccty'].str.lower()
    key = key.str.replace(' ', '')
    df_pop['key'] = key

    # group population
    by = ['key', 'quarter']
    result = df_pop.groupby(by = by)['docnbr'].count()
    result = result.reset_index()
    result.rename(columns = {'docnbr' : 'prison_pop'}, inplace = True)
    col = ['breakdown', 'breakdown_category']
    result[col] = 'Total'

    # group race
    by_race = ['key', 'quarter', 'race']
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
    by_sex = ['key', 'quarter', 'sex']
    result_ = df_pop.groupby(by = by_sex)['docnbr'].count()
    result_ = result_.reset_index()
    rename = {'docnbr' : 'prison_pop', 'sex' : 'breakdown'}
    result_.rename(columns = rename, inplace = True)
    result_['breakdown_category'] = 'Sex'

    result = pd.concat([result, result_])    # retype some races
    where = result['breakdown'] == 'B'
    result = result.loc[~where]

    # group offense type
    by_hoff = ['key', 'quarter', 'Justice_Counts']
    result_ = df_pop.groupby(by = by_hoff)['docnbr'].count()
    result_ = result_.reset_index()
    rename = {'docnbr' : 'prison_pop', 'Justice_Counts' : 'breakdown'}
    result_.rename(columns = rename, inplace = True)
    result_['breakdown_category'] = 'Offense Type'

    result = pd.concat([result, result_])

    # -------------------------------------------------
    # Add Names from population table
    # -------------------------------------------------

    # add key
    key = scaffolding['county_name'].str
    key = key.replace(' County, Illinois', '')
    key = key.str.lower()
    key = key.str.replace(' ', '')
    scaffolding['key'] = key

    # merge
    scaffolding['quarter'] = scaffolding['quarter'].dt.to_period('Q')
    result = scaffolding.merge(
        result, how = 'left',
        on = ['quarter', 'key', 'breakdown'])

    result['prison_pop'] = result['prison_pop'].fillna(0)

    # -------------------------------------------------
    # Add Admits and Exits
    # -------------------------------------------------
    
    # which can also act as a merge key to group data
    key = df_admits['stnccty'].str.lower()
    key = key.str.replace(' ', '')
    df_admits['key'] = key

    # group admits
    by = ['key', 'quarter']
    result_ = df_admits[by].value_counts()
    result_ = result_.reset_index()
    result_.rename(columns = {'count' : 'admissions'}, inplace = True)
    result_['breakdown'] = 'Total'

    # merge
    col = ['key', 'quarter', 'breakdown']
    result = result.merge(result_, on = col, how = 'left')

    # which can also act as a merge key to group data
    key = df_exits['stnccty'].str.lower()
    key = key.str.replace(' ', '')
    df_exits['key'] = key

    # group exits
    result_ = df_exits[by].value_counts()
    result_ = result_.reset_index()
    result_.rename(columns = {'count' : 'exits'}, inplace = True)
    result_['breakdown'] = 'Total'
    
    # merge
    col = ['key', 'quarter', 'breakdown']
    result = result.merge(result_, on = col, how = 'left')
    
    col = ['admissions', 'exits']
    result[col] = result[col].fillna(-9999)

    # -------------------------------------------------
    # Final edits for Tableay & PostGres
    # -------------------------------------------------

    # retype
    result['quarter'] = result['quarter'].astype('datetime64[ns]')
    
    # Fit geo-names for Tableau
    result['county_name'] = result['county_name'].str.replace(' County, Illinois', '')
    result['state'] = 'Illinois'

    # Drop unnecessary columns
    result.drop(columns = ['key', 'year', 'variable'], inplace = True)

    # Create PK id columns
    result.reset_index(drop = True, inplace = True)
    result['id'] = result.index + 1

    # -------------------------------------------------
    # Upload to Tableau
    # -------------------------------------------------

    # id breakdown pairs
    col = ['breakdown', 'breakdown_category']
    where = (~result[col].duplicated()) & (~result['breakdown_category'].isna())
    pairs = result.loc[where, col]
    result = result.merge(pairs, on = 'breakdown', how = 'left')

    # Change columns
    result.drop(columns = 'breakdown_category_x', inplace = True)
    result.rename(
        columns = {'breakdown_category_y' : 'breakdown_category'},
        inplace = True)

    # All the nulls are offense type
    where = result['breakdown_category'].isna()
    result.loc[where, 'breakdown_category'] = 'Offense Type'
    
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

    pgres_name = 'tableau.idoc_population_stnccty'
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