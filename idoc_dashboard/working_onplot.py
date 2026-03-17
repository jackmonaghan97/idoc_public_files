#%%

import matplotlib.pyplot as plt
import plotly.express as px
import pandas as pd

# Load data and compute static values
from data.prison_data import prison_data

prison_data = pd.read_csv('data\prison_data.csv')
prison_data['quarter'] = pd.to_datetime(prison_data['quarter'], format='%Y-%m-%d')
prison_data = prison_data.sort_values('gen_population', ascending=False)

dates = prison_data['quarter'].dt.date.sort_values().unique()
dates_rng = (dates[0], dates[-1])

where = prison_data['breakdown_category'] == 'Race'
to_plot = prison_data.loc[where].groupby(by = 'breakdown')
to_plot = to_plot[['prison_pop', 'gen_population']].sum().reset_index()
to_plot = to_plot.sort_values('prison_pop', ascending=False)

to_plot["pct"] = to_plot["prison_pop"] / to_plot["gen_population"]

fig, ax = plt.subplots(figsize=(9, 5))

px.box(to_plot, x='breakdown', y='prison_pop', color='breakdown')

# Add percentage labels
for i, row in to_plot.iterrows():
    ax.text(
        i,
        row["prison_pop"],
        f"{row['pct']:.2%}",      # formats as 12.3%
        ha="center",
        va="bottom",
        fontsize=9,
        fontweight="bold")

plt.show()
# %%

import plotly.express as px
import pandas as pd

import plotly.io as pio
pio.renderers.default = "browser"

prison_data = pd.read_csv('data/prison_data.csv')
prison_data['quarter'] = pd.to_datetime(prison_data['quarter'])

where = prison_data['breakdown_category'] == 'Race'

to_plot = (
    prison_data.loc[where]
    .groupby(by = ['quarter', 'breakdown'])['prison_pop']
    .sum()
    .reset_index()
    .sort_values('prison_pop', ascending=False)
).reset_index()

# 1. Add the text argument so the chart knows WHAT to display
fig = px.area(
    to_plot, 
    x="quarter", 
    y="prison_pop", 
    color="breakdown")

# 2. Change "outside" to a valid enumeration value
fig.update_traces(textposition="top center") 

fig.update_layout(yaxis_title="Prison Population")
fig.show()

# %%
