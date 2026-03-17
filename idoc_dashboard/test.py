
import faicons as fa
from numpy import where
import plotly.express as px
import pandas as pd

# Load data and compute static values
from data.prison_data import prison_data
from shiny import App, reactive, render, ui
from shinywidgets import output_widget, render_plotly

prison_data = pd.read_csv('data\prison_data.csv')
prison_data['quarter'] = pd.to_datetime(prison_data['quarter'], format='%Y-%m-%d')
prison_data = prison_data.sort_values('gen_population', ascending=False)

dates = prison_data['quarter'].dt.date.sort_values().unique()
dates_rng = (dates[0], dates[-1])

dir = 'C:\\Users\\jackm\\OneDrive\\Documents\\git_projects\\idoc_public_files\\idoc_dashboard\\style.css'

ICONS = {
    "user": fa.icon_svg("user", "regular"),
    "wallet": fa.icon_svg("wallet"),
    "currency-dollar": fa.icon_svg("dollar-sign"),
    "ellipsis": fa.icon_svg("ellipsis"),
}

# Add page title and sidebar
app_ui = ui.page_sidebar(
    
    ui.sidebar(
        ui.input_slider(
            "test_slider",
            "Daily Population Records",
            min=dates_rng[0],
            max=dates_rng[1],
            value=dates_rng[1]),
        
        ui.input_checkbox_group(
            "total_bill",
            "Bill amount",
            ["Rural", "Urban"],
            selected=["Rural", "Urban"],
            inline=True),
        
        ui.input_action_button("reset", "Reset filter"),
        open="desktop"),

    ui.layout_columns(
        
        ui.value_box(
            
            "Total incarcerated",
            ui.output_ui("total_in"),
            showcase=ICONS["user"]),
            
        ui.value_box(
            
            "Percent Incarcerated",
            ui.output_ui("percent_in"),
            showcase=ICONS["wallet"]),
            
        ui.value_box(
            
            "Percent Year Change",
            ui.output_ui("year_change"),
            showcase=ICONS["currency-dollar"]),

        fill=False),

    ui.layout_columns(
        
        ui.card(
            
            ui.card_header("Bar Plot"),
            output_widget("bar_plot"),
            full_screen=True),
    
        ui.card(
            
            ui.card_header("Area Graph"),
            output_widget("area_graph"),
            full_screen=True),
    
        ),
    
    ui.include_css(dir),
    title="Restaurant tipping",
    fillable=True)

def server(input, output, session):
    
    @reactive.calc
    def total_calculation():

        date = input.test_slider()

        where = prison_data['quarter'].dt.date == date
        where_total = prison_data['breakdown'].str.strip().eq('Total')
        total = prison_data.loc[where & where_total, 'prison_pop'].sum()
        return total

    @render.ui
    def total_in():
        return ui.h3(f"{total_calculation():,}")

    @render.ui
    def percent_in():
        
        where = prison_data['quarter'] == dates_rng[0]
        where_total = prison_data['breakdown'] == 'Total'
        total = total_calculation()
        total_gen = prison_data.loc[where & where_total, 'gen_population'].sum()
        
        return total/total_gen

    @render.ui
    def year_change():
        
        previous_year = dates_rng[0] - pd.DateOffset(years=1)
        
        where = prison_data['quarter'] == previous_year
        where_total = prison_data['breakdown'] == 'Total'
        prev_year_total = prison_data.loc[where & where_total, 'prison_pop'].sum()
        current_total = total_calculation()

        return (current_total - prev_year_total)/prev_year_total

    @render_plotly
    def bar_plot():

        where = prison_data['breakdown_category'] == 'Race'
        to_plot = (
            prison_data.loc[where]
            .groupby('breakdown')[['prison_pop', 'gen_population']]
            .sum()
            .reset_index()
            .sort_values('prison_pop', ascending=False)
        )

        to_plot["pct"] = to_plot["prison_pop"] / to_plot["gen_population"]

        fig = px.bar(
            to_plot,
            x="breakdown",
            y="prison_pop",
            text=to_plot["pct"].map(lambda x: f"{x:.2%}")
        )

        fig.update_traces(textposition="outside")
        fig.update_layout(yaxis_title="Prison Population")

        return fig

    @render_plotly
    def area_graph():

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

        fig.add_vline(
            x=pd.Timestamp(dates_rng[0]),   # or any date in your data
            line_width=2,
            line_dash="dash",
            line_color="red"
        )


        # 2. Change "outside" to a valid enumeration value
        fig.update_traces(textposition="top center") 
        fig.update_layout(yaxis_title="Prison Population")

        return fig

app = App(app_ui, server)


