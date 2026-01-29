#%%
'''
Version     : Python 3.12.3

Raw Source  : IDOC population datasets (https://idoc.illinois.gov/reportsandstatistics/populationdatasets.html) 
Author      : Jack Monaghan

writes to   : justice_counts.idoc_public_admissions
            : justice_counts.idoc_public_pop
            : justice_counts.idoc_public_exits
            : justice_counts.idoc_prob_pop

'''

import pandas as pd
from dateutil.parser import parse

class OpenExcel:

    def __init__(self, directory : str, file : str):

        self.directory = directory
        self.file = file
        self.df = pd.DataFrame

        excel_file = pd.ExcelFile(directory + file)
        self.sheet = excel_file.sheet_names[0]

    def header(self) -> None:

        # The following for loop find the column in which the data starts. It does 
        # this by going through the unnamed columns (df.columns) and finding which
        # column is not null at row 10. Roughly that should be the starting column.
        
        for IDOC_c in self.df.columns:

            if pd.notna(self.df[IDOC_c].iloc[10]):break
            else: pass # exit loop when found
        
        # Next we need to find the correct header row. This should be where the 
        # first column is 'IDOC #'.
        
        where   = self.df[IDOC_c] == 'IDOC #'      # find header 
        header  = self.df.loc[where].index[0]
        
        self.df.columns   = self.df.iloc[header]             # put correct header as header
        self.df           = self.df.iloc[header + 1:]        # drop nan rows
        
        where = self.df['Name'].isna()
        self.df = self.df.loc[~where]

    def type_name(self) -> None:
        
        self.df.rename(columns= {'Current Admission Type' : 'Admission Type'}, inplace = True)
        self.df.rename(columns= {'Custody    Date' : 'Custody Date'}, inplace = True)
        self.df.columns = self.df.columns.str.strip('3')


        date_col = list(self.df.columns[1:])
        date_col = [c for c in date_col if 'Date' in c]
        for c in date_col:

            wrong = self.df[c]
            correct = pd.to_datetime(wrong, format='%m%d%Y', errors='coerce')
            
            self.df[c] = correct

        renaming = {
            
            # basic
            'IDOC #'            : 'docnbr',
            'Name'              : 'fullname',
            'Date of Birth'     : 'birthdt',
            'Sex'               : 'sex',
            'Race'              : 'race',
            'Holding Offense'   : 'hofnscd',
            'Admission Type'    : 'admtyp',
            'Sentence Date'     : 'sntdt',
            'Sentencing County' : 'stnccty',
            
            # admissions
            'Reception Center' : 'recpcntr',
            'Admission Date' : 'admitdt',

            # population
            'Actual Mandatory Supervised Release (MSR) Date' : 'actmsrdt', 
            'Actual Discharge Date' : 'actdisdt',
            'Discharge Reason' : 'discrsn', 
            'Special Release Reason' :'sperlsrsn',
            'Releasing Institution' : 'relinst',

            # exits
            'Parent Institution' : 'prtinst',
            'Custody Date': 'cstdt'}

        # select cols for renaming
        select = [col for col in renaming if col in self.df.columns]
        self.df = self.df[select]

        # rename
        self.df = self.df.rename(columns=renaming)

        types = {
            # basic
            'docnbr' : str,
            'fullname' : str,
            'birthdt' : 'datetime64[ns]',
            'sex' : str,
            'race' : str,
            'hofnscd' : str,
            'admtyp' : str,
            'sntdt' : 'datetime64[ns]',
            'stnccty' : str,
            
            # admissions
            'recpcntr' : str,
            'admitdt' : 'datetime64[ns]',

            # population
            'actmsrdt' : 'datetime64[ns]', 
            'actdisdt' : 'datetime64[ns]',
            'discrsn' : str, 
            'sperlsrsn' : str,
            'relinst' : str,

            # exits
            'prtinst' : str,
            'cstdt' : 'datetime64[ns]'}
        
        current_types = {col: types[col] for col in self.df.columns if col in types}
        self.df = self.df.astype(current_types)

    def date(self) -> None:

        if 'pop' in self.directory:
            
            date = self.sheet.split(' ')[-1]
            date = parse(date)
            self.df['record_date'] = date

        else:

            date = self.sheet.split(' ')[0]
            self.df['record_date'] = date

    def open(self) -> pd.DataFrame:

        self.df = pd.read_excel(self.directory + self.file) 
        self.header()
        self.type_name()
        self.date()

        return self.df

if __name__ == '__main__':

    dire = '/encrypted/data/Justice_Counts/idoc_public_files/pop_20250207_135127/'
    file = 'dec-2011-prison-stock-pop.xls'

    excel = OpenExcel(directory = dire, file = file)
    df = excel.open()


# %%
