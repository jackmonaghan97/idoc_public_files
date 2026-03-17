
from shiny import render
from data.sql_data_collector import extract_pgres
from plots.bkd_c_barplot import bar_plot
from plots.line_plot import line_plot
from data.prison_data import prison_data

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
    @render.plot

    def table(): return render.DataGrid(prison_data)