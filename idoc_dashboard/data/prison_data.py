import pandas as pd

prison_data = pd.read_csv('data\prison_data.csv')
prison_data['quarter'] = pd.to_datetime(prison_data['quarter'], format='%Y-%m-%d')
prison_data = prison_data.sort_values('gen_population', ascending=False)
