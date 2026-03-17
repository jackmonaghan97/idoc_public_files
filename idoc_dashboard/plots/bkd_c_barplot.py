

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import textwrap

def bar_plot(df: pd.DataFrame, bkd_c : str):
    
    # Aggregate
    
    where = (
        (df['quarter'].dt.year == 2019) &
        (df['quarter'].dt.month == 4) &
        (df['breakdown_category'] == bkd_c))
    
    df = df.loc[where]

    df_sum = df.groupby("breakdown", as_index=False)[["prison_pop", "gen_population"]].sum()
    df_sum = df_sum.sort_values(by = 'prison_pop', ascending=False).reset_index()
    df_sum["pct"] = df_sum["prison_pop"] / df_sum["gen_population"]

    df_sum["wrapped_label"] = df_sum["breakdown"].apply(lambda x: "\n".join(textwrap.wrap(x, width=12)))


    # Create figure
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.barplot(df_sum, x="wrapped_label", y="prison_pop", ax=ax, color="steelblue")

    # Add percentage labels
    for i, row in df_sum.iterrows():
        ax.text(
            i,
            row["prison_pop"],
            f"{row['pct']:.2%}",      # formats as 12.3%
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold"
        )

    fig.patch.set_alpha(0)   # transparent figure background
    ax.set_facecolor("none") # transparent axes background

    plt.xticks(rotation=45, ha="right")

    # Titles + labels
    ax.set_title(f"Prison Population by {bkd_c}", pad=20)
    ax.set_xlabel("")
    ax.set_ylabel("")

    sns.despine()

    return fig