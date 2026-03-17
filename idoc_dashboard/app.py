from shiny import App, ui
from server import server

card_style = """
    background-color: #f4f9fd;
    padding: 20px;
    border-radius: 24px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.15);
"""
app_ui = ui.page_fluid(

    # header
    ui.h3(
        "Prison Population Dashboard",
        style="margin-top: 25px; margin-bottom: 50px; margin-left: 100px;"),

    ui.layout_columns(

        # ---- SIDEBAR ----
        ui.column(
            3,
            ui.div(
                ui.h4("Filters"),
                ui.input_select("year", "Year", [2020, 2021, 2022]),
                ui.input_select("sex", "Sex", ["Male", "Female"]),
                ui.input_slider("age", "Age", 18, 80, 30),
                style="""
                    background-color: #f4f9fd;
                    padding: 12px;   /* was 20px */
                    border-radius: 12px;
                    box-shadow: 0 2px 6px rgba(0,0,0,0.1);
                """


            )
        ),

        # ---- MAIN CONTENT ----
        ui.column(12, # ROW 1
            ui.div(ui.output_plot("line_graph"),
                style=card_style),

            # ROW 2
            ui.layout_columns(
                ui.column(12, ui.div(ui.output_plot("sex_bar"),  style=card_style)),
                ui.column(12, ui.div(ui.output_plot("race_bar"), style=card_style)),
                ui.column(12, ui.div(ui.output_plot("age_bar"),  style=card_style)),
            )
        )
    ),

    style="""
        max-width: 2000px;
        margin-left: auto;
        margin-right: auto;
        --bs-gutter-y: 5px;
    """
)

app = App(app_ui, server)