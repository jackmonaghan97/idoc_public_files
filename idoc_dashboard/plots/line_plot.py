
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def line_plot(df: pd.DataFrame, bkd_c : str):

    
    # Aggregate
    where = ((df['breakdown_category'] == bkd_c))
    df = df.loc[where]
    df_sum = df.groupby(by = ['quarter', 'breakdown'])['prison_pop'].sum().reset_index()

    # Create figure
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(df_sum, x="quarter", y="prison_pop",
                 hue="breakdown",ax=ax, color="steelblue")

    fig.patch.set_alpha(0)   # transparent figure background
    ax.set_facecolor("none") # transparent axes background

    # Titles + labels
    ax.set_title(f"Prison Population by {bkd_c}", pad=20)
    ax.set_xlabel("")
    ax.set_ylabel("")

    sns.despine()

    return fig
