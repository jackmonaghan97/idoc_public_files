
# import packages
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

def upload_pgres(df_ : pd.DataFrame, pgres_name : str) -> None:

    start = time.time()

    # Connect to db
    conn = psycopg2.connect(
        dbname='archives',
        user = username,
        password = password,
        host = 'ccjda1.icjia.org',
        port="5432" )
    cursor = conn.cursor()

    print(f'---- Upload {pgres_name} to PostGres')

    # Truncate the table to remove existing data
    query = 'TRUNCATE TABLE ' + pgres_name
    cursor.execute(query)

    # iterate and upload each row
    for index, row in df_.iterrows():
        columns = ', '.join(row.index)
        values = ', '.join(['%s'] * len(row))
        insert_query = f'INSERT INTO {pgres_name} ({columns}) VALUES ({values})'
        
        cursor.execute(insert_query, tuple(row))

    # Commit the transaction
    conn.commit()
    cursor.close()
    conn.close()

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

    # retrieve credentials
    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')

    # if not defined default to the user
    if username == None:

        print('USER INPUT COMMANDS...')
        os.environ['POSTGRES_USER'] = input('ccjda username:')  
        os.environ['POSTGRES_PASSWORD'] = input('password:')

            # reset credentials
        username = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')

    params = [
        (df_adm, 'justice_counts.idoc_public_admissions'),
        (df_pop, 'justice_counts.idoc_public_pop'),
        (df_ext, 'justice_counts.idoc_public_exits')
    ]

    with mp.Pool(processes=len(params)) as pool:
        results = pool.starmap(upload_pgres, params)
