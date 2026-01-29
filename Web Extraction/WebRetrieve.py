
'''
A class to retrieve and manage Excel files from an IDOC URL and saves them in a 
JCIP folder.

'''

#%%

# import packages
from bs4 import BeautifulSoup
from datetime import datetime
import time
import requests
import shutil
import os

class WebRetrieve:

    def __init__(self, url : str, type : str):

        # initialize class variables
        self.url = url
        self.type = type
        self.directory = '/encrypted/data/Justice_Counts/idoc_public_files'
        self.save_directory = ''
        self.excel_files = []

    def create_folder(self) -> None:

        # create a file_name that is timestamped
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f'{self.type}_{timestamp}'
        self.save_directory = os.path.join(self.directory, file_name) 

        # generate file
        os.makedirs(self.save_directory, exist_ok=True)

    def fetch_excels(self) -> None:

        # fetch 
        response = requests.get(self.url)
        html_content = response.content

        soup = BeautifulSoup(html_content, 'html.parser')   # parse
        links = soup.find_all('a', href=True)               # find links

        # select sub-folders that are excels
        self.excel_files = [link['href'] for link in links if link['href'].endswith('.xlsx') or link['href'].endswith('.xls')]

    def save_excels(self) -> None:

        # check if excels have not been properly found
        if len(self.excel_files) == 0: raise KeyError('Excel files have not been fetched. Execute WebRetrieve.fetch_excels')
        elif len(self.save_directory) == 0: raise KeyError('Directory has not been generated. Execute WebRetrieve.create_folder')

        # save all excels
        for excel_file in self.excel_files:
            self.save_single_excel(excel_file)
    
    def save_single_excel(self, excel_file) -> None:
            
            # ff the link is relative, make it absolute
            if not excel_file.startswith('http'):
                excel_file = requests.compat.urljoin(self.url, excel_file)
            
            response = requests.get(excel_file) # fetch excel
            filename = os.path.basename(excel_file) # get file name
            
            # save file in directory
            file_path = os.path.join(self.save_directory, filename)
            with open(file_path, 'wb') as f:
                f.write(response.content)
    
    def find_outdated(self) -> None:
        
        '''
        We keep two folders:
            1. the most resent webscrapping,
            2. a backup of the previous webscapping.

        This method finds the two most recent folders and removes
        the remaining outdated folder. The method also asks the
        user's permission before deleting the files.
        '''


        # find all files of that dtypes
        all_files = os.listdir(self.directory)
        all_files = [f for f in all_files if self.type in f]

        # parse out dates
        dates = [f[4:] for f in all_files]
        dates = [datetime.strptime(d, '%Y%m%d_%H%M%S') for d in dates]

        storage = dict(zip(dates, all_files))

        # sort and select most recent
        dates = sorted(dates, reverse=True)
        drop_dates = dates[2:]

        drop_files = []
        for d in drop_dates:
            drop_files.append(storage[d])

        self.delete_files(drop_files)

    def delete_files(self, drop : list) -> None:

        for file in drop:

            path = self.directory + '/' + file
            shutil.rmtree(path)
            
    def execute(self) -> None:

        self.create_folder()
        self.fetch_excels()
        self.save_excels()
        self.find_outdated()

if __name__ == '__main__':

    url = 'https://idoc.illinois.gov/reportsandstatistics/prison-population-data-sets.html'

    wb = WebRetrieve(url = url, type = 'pop')
    wb.execute()


# %%
