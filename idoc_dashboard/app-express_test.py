#%%
from server import server

import faicons as fa
import plotly.express as px
import polars as pl

# Load data and compute static values

# I think this is the data
from data.prison_data import prison_data
from shinywidgets import output_widget, render_plotly
from shiny import reactive, render
from shiny import App, reactive, render, ui
from shinywidgets import render_plotly

from shiny import render
from data.sql_data_collector import extract_pgres
from plots.bkd_c_barplot import bar_plot
from plots.line_plot import line_plot
from data.prison_data import prison_data

dates = prison_data['quarter'].dt.date.sort_values().unique()

ICONS = {
    "user": fa.icon_svg("user", "regular"),
    "wallet": fa.icon_svg("wallet"),
    "currency-dollar": fa.icon_svg("dollar-sign"),
    "ellipsis": fa.icon_svg("ellipsis"),
}

app_ui = ui.page_sidebar(
    
    ui.sidebar(
        ui.input_slider(
                "date_range",
                "Select date range",
                min=dates[0],
                max=dates[-1],
                value=[dates[0], dates[-1]]),

        ui.input_checkbox_group(
            "date_range",
            "Select date range",
            ["Rural", "Urban"],
            selected=["Rural", "Urban"],
            inline=True),

        ui.input_action_button("reset", "Reset filter")
    ),

    ui.layout_columns(
        
        ui.value_box(
            "People incarcerated", ui.output_ui("total_tippers"),
            showcase=ICONS["user"]),
        ui.value_box(
            "Incarcerated per 100K", ui.output_ui("average_tip"), 
            showcase=ICONS["wallet"]),
        ui.value_box(
            "Don't know yet", ui.output_ui("average_bill"),
            showcase=ICONS["currency-dollar"]),
        fill=False),

    ui.layout_columns(
        ui.card(
            ui.card_header("Test this data"),
            ui.output_data_frame("table"),
            full_screen=True
        )),
        
    ui.layout_columns(
        ui.card(
            ui.card_header("will this work?"),
            ui.output_plot("line_graph"),
            full_screen=True
        )),
        
    title="Restaurant tipping",
    fillable=True,        
        
)



def server(input, output, session):

    # Load data once at startup
    df = extract_pgres()

    @output
    @render.plot
    
    def sex_bar(): return bar_plot(df, bkd_c = 'Sex')

    @output
    @render.plot

    def race_bar(): return bar_plot(df, bkd_c = 'Race')

    @output
    @render.plot

    def line_graph(): return line_plot(df , bkd_c = 'Race')

    @output
    @render.data_frame

    def table(): return render.DataGrid(
        prison_data.loc[
            prison_data[
                'breakdown'] == 'Total',
                ['county_name', 'prison_pop', 'gen_population']])

app = App(app_ui, server)


# %%


