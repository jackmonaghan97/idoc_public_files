

# import packages
import duckdb
from OpenExcel import OpenExcel
from WebRetrieve import WebRetrieve

import os
import pandas as pd
import numpy as np
import time
import psycopg2
import pandas as pd
import multiprocessing as mp

def run(url : str, type : str) -> pd.DataFrame:
    
    print(f'Downloading {type} data...', end='\r', flush=True)
    start_wr = time.time()

    # retieve from website
    wb = WebRetrieve(url = url, type = type)
    wb.execute()
    dir = wb.save_directory

    end_wr = time.time()
    print(f'Opening {type} excels...', end='\r', flush=True)
    start_open = time.time()

    # grab all excels out of storage
    file_list = os.listdir(dir)                                     # 1. find all pop files
    package = {c : OpenExcel(dir + '/', c) for c in file_list}      # 2. set up open package
    files   = {c : excel.open() for c, excel in package.items()}    # 3. open all excels

    end_open = time.time()
    print(f'Complete {type}:                ', flush=True)
    print(f'[Web Retreive: {end_wr - start_wr}] \n[Open Excels: {start_open - end_open}]')

    # merge & return
    return pd.concat(files.values(), ignore_index=True)

def remove(df_ : pd.DataFrame) -> None:

    col = list(df_.columns)
    col.remove('record_date')

    where_a = df_.duplicated(keep = False)
    where_t = df_[col].duplicated(keep = 'first')
    where = where_t & ~where_a

    df_ = df_.loc[~where]

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

    start = time.time()

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

    end = time.time()
    print(f'---- Complete {pgres_name} to PostGres: {end - start} seconds')

if __name__ == '__main__':

    df_pop = run(
        url = 'https://idoc.illinois.gov/reportsandstatistics/prison-population-data-sets.html',
        type = 'pop')
    df_adm = run(
        url = 'https://idoc.illinois.gov/reportsandstatistics/prison-admission-data-sets.html',
        type = 'adm')
    df_ext = run(
        url= 'https://idoc.illinois.gov/reportsandstatistics/prison-exit-data-sets.html',
        type = 'ext')

    # remove dupilcate entries -------------- TEMP

    # drop duplicate years
    where = df_adm['record_date'].str.contains('CY')
    df_adm = df_adm.loc[where]

    # drop duplicate years
    where = df_ext['record_date'].str.contains('CY')
    df_ext = df_ext.loc[where]

    # add exit dt -------------- TEMP

    df_ext['exitdt'] = df_ext['actdisdt']
    where = df_ext['exitdt'].isna()
    df_ext.loc[where, 'exitdt'] = df_ext.loc[where, 'actmsrdt']

    # --------------------------------------------------------------
    # upload to postgre server
    # --------------------------------------------------------------

    for df_ in df_adm, df_ext, df_pop:

        df_.fillna(np.nan, inplace = True)              # fill na for pgres
        df_.replace([np.nan], [None], inplace = True)   # fill na for pgres
        try: df_.drop(columns=['year'], inplace = True) # drop quarter cols
        except: pass

    df_pop = df_pop.convert_dtypes()
    generate_create_table_sql(df_pop, 'idoc_public_population')
    df_adm = df_adm.convert_dtypes()
    generate_create_table_sql(df_adm, 'idoc_public_admissions')
    df_ext = df_ext.convert_dtypes()
    generate_create_table_sql(df_ext, 'idoc_public_exits')

    upload_pgres(df_pop, 'idoc_public_population')
    upload_pgres(df_adm, 'idoc_public_admissions')
    upload_pgres(df_ext, 'idoc_public_exits')