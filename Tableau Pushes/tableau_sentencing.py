
#%%

import pandas as pd
import duckdb
import itertools

def extract_pgres(query : str) -> pd.DataFrame:

    query = 'SELECT * FROM ' + query 
    conn.execute(query)

    col = [desc[0] for desc in conn.description]
    data = conn.fetchall()

    df_ = pd.DataFrame(data = data, columns = col)

    return df_

def pandas_to_sql(dtype):
    
    if dtype.kind in {"i"}:      # integer
        return "INTEGER"
    
    if dtype.kind in {"f"}:      # float
        return "DOUBLE PRECISION"
    
    if dtype.kind in {"b"}:      # boolean
        return "BOOLEAN"
    
    if dtype.kind in {"M"}:      # datetime64
        return "TIMESTAMP"
    
    if dtype.name == "category":
        return "VARCHAR"
    
    return "VARCHAR"             # fallback for object/string

def generate_create_table_sql(df_ : pd.DataFrame, table_name):
    
    
    cols = []
    for col, dtype in df_.dtypes.items():
        sql_type = pandas_to_sql(dtype)
        cols.append(f'"{col}" {sql_type}')
    
    cols_sql = ",\n  ".join(cols)

    # upload to duckdb
    db_path = r"C:\Users\jackm\OneDrive\Documents\duckdb_cli-windows-amd64\my_database.duckdb"
    con = duckdb.connect(db_path)

    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql});
    """)

def upload_pgres(df_ : pd.DataFrame, pgres_name : str) -> None:

    # upload to duckdb
    db_path = r"C:\Users\jackm\OneDrive\Documents\duckdb_cli-windows-amd64\my_database.duckdb"
    con = duckdb.connect(db_path)

    #### FUNCTION

    con.execute(f'TRUNCATE TABLE {pgres_name}')
    con.register('df_view', df_)

    # 3. Insert fresh data
    con.execute(f"""
        INSERT INTO {pgres_name}
        SELECT * FROM df_view;
    """)

    con.close()

if __name__ == '__main__':

    # ------------------------------------------------
    # Extract from DuckDB
    # ------------------------------------------------

    # connect
    db_path = r"C:\Users\jackm\OneDrive\Documents\duckdb_cli-windows-amd64\my_database.duckdb"
    conn = duckdb.connect(db_path)

    # save
    df_pop      = extract_pgres('idoc_public_population')
    df_cen_pop  = extract_pgres('cesnsus_illinois_county_population')

    # don't know what's going on with the entires that say state
    # but their values are clearly Illinois' ucgid code 
    where = df_cen_pop['variable'] == 'state'
    df_cen_pop = df_cen_pop.loc[~where]

    conn.close()

    # -------------------------------------------------
    # Create scaffolding table to ensure all combinations
    # -------------------------------------------------

    
    age = (df_pop['record_date'] - df_pop['birthdt'])
    age = (age.dt.days/360).fillna(0).astype(int)
    age = age // 10 * 10
    df_pop

#%%

    # -------------------------------------------------
    # Create scaffolding table to ensure all combinations
    # -------------------------------------------------
#%%
    ucgid       = df_cen_pop['ucgid'].unique()
    breakdowns  = pd.Series(df_cen_pop['variable'].unique())
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
    by_hoff = ['key', 'quarter', 'hofnscd']
    result_ = df_pop.groupby(by = by_hoff)['docnbr'].count()
    result_ = result_.reset_index()
    rename = {'docnbr' : 'prison_pop', 'hofnscd' : 'breakdown'}
    result_.rename(columns = rename, inplace = True)
    result_['breakdown_category'] = 'Offense'

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
    # Upload to sql
    # -------------------------------------------------

    result = result.convert_dtypes()
    generate_create_table_sql(result, 'tableau_idoc_sentencing')
    
    upload_pgres(result, 'tableau_idoc_sentencing')