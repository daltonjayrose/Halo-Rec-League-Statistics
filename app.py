from dash import Dash, html, dcc, callback, Output, Input, dash_table
import plotly.express as px
import pandas as pd
from sqlalchemy import URL, create_engine, text
import os
from dotenv import load_dotenv

# Load Environment Variables
load_dotenv(".env")

# Connect to the PostgreSQL Database
connection_string = URL.create(
    'postgresql',
    username=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    host=os.getenv("PGHOST"),
    database=os.getenv("PGDATABASE")
)

engine = create_engine(connection_string)


# Get game tables for specific match
def get_game_tables_for_match(match):
    query = text(f"""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'match{match:02d}_game%';
""")
    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result]


# Generate query for specific match
def generate_match_query(match):
    game_tables = get_game_tables_for_match(match)
    if not game_tables:
        raise ValueError(f"No game tables found for match {match:02d}")

    union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in game_tables])
    full_query = f"""
        SELECT 
            "Player",
            SUM("Kills") AS "Kills",
            SUM("Deaths") AS "Deaths",
            SUM("Assists") AS "Assists",
            SUM("DamageDone") AS "DamageDone",
            SUM("DamageTaken") AS "DamageTaken",
            CASE 
                WHEN SUM("Deaths") > 0 THEN TO_CHAR(SUM("Kills")::FLOAT / SUM("Deaths")::FLOAT, 'FM999999999.00') 
                ELSE '0.00' 
            END AS "KD",
            CASE 
                WHEN SUM("Deaths") > 0 THEN TO_CHAR((SUM("Kills") + SUM("Assists"))::FLOAT / SUM("Deaths")::FLOAT, 'FM999999999.00') 
                ELSE '0.00' 
            END AS "KDA",
            SUM("ShotsFired") AS "ShotsFired",
            SUM("ShotsLanded") AS "ShotsLanded",
            CASE 
                WHEN SUM("ShotsFired") > 0 THEN TO_CHAR(SUM("ShotsLanded")::FLOAT / SUM("ShotsFired")::FLOAT, 'FM999999999.00') 
                ELSE '0.00' 
            END AS "Accuracy",
            CASE 
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("Kills")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00') 
                ELSE '0.00' 
            END AS "AvgKills",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("Assists")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgAssists",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("Deaths")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgDeaths",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("DamageDone")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgDamageDone",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("DamageTaken")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgDamageTaken"
        FROM ({union_query}) combined_stats
        GROUP BY "Player";
    """
    return full_query


# Generate query for all matches
def generate_all_matches_query():
    query = text("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename LIKE 'match%_game%';
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        tables = [row[0] for row in result]

    if not tables:
        raise ValueError("No match tables found in the database.")

    union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in tables])
    full_query = f"""
        SELECT 
            "Player",
            SUM("Kills") AS "Kills",
            SUM("Deaths") AS "Deaths",
            SUM("Assists") AS "Assists",
            SUM("DamageDone") AS "DamageDone",
            SUM("DamageTaken") AS "DamageTaken",
            CASE 
                WHEN SUM("Deaths") > 0 THEN TO_CHAR(SUM("Kills")::FLOAT / SUM("Deaths")::FLOAT, 'FM999999999.00') 
                ELSE '0.00' 
            END AS "KD",
            CASE 
                WHEN SUM("Deaths") > 0 THEN TO_CHAR((SUM("Kills") + SUM("Assists"))::FLOAT / SUM("Deaths")::FLOAT, 'FM999999999.00') 
                ELSE '0.00' 
            END AS "KDA",
            SUM("ShotsFired") AS "ShotsFired",
            SUM("ShotsLanded") AS "ShotsLanded",
            CASE 
                WHEN SUM("ShotsFired") > 0 THEN TO_CHAR(SUM("ShotsLanded")::FLOAT / SUM("ShotsFired")::FLOAT, 'FM999999999.00') 
                ELSE '0.00' 
            END AS "Accuracy",
            CASE 
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("Kills")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00') 
                ELSE '0.00' 
            END AS "AvgKills",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("Assists")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgAssists",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("Deaths")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgDeaths",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("DamageDone")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgDamageDone",
            CASE
                WHEN COUNT(DISTINCT "MatchId") > 0 THEN TO_CHAR(SUM("DamageTaken")::FLOAT / COUNT(DISTINCT "MatchId"), 'FM999999999.00')
                ELSE '0.00'
            END AS "AvgDamageTaken"
        FROM ({union_query}) combined_stats
        GROUP BY "Player";
    """
    return full_query


# Configure players whose statistics are displayed
selected_players = ['Walrus Boots', 'OgFragnetism', 'Slimbo TDS', 'THE GOOOOOOOSE', 'ZOMBIE IC', 'ShaneVersionOne']

# By default, display statistics from all matches, from selected players
query = generate_all_matches_query()
df = pd.read_sql_query(query, con=engine)
filtered_df = df[df['Player'].isin(selected_players)]

app = Dash(__name__)
server = app.server

app.title = "Halo Rec League Stats"

app.layout = html.Div([
    html.H1(children='Halo Rec League Stats', style={'textAlign': 'center'}),
    html.Div([
        dcc.Dropdown(['All', 'Match #01', 'Match #02', 'Match #03'], 'All', id='main-dropdown', searchable=False),
        html.Div(id='dd-output-container')
    ]),
    html.Br(),
    dcc.Loading(
        id="loading",
        type="circle",
        children=[
            dash_table.DataTable(
                id='datatable-id',
                columns=[{"name": col, "id": col} for col in filtered_df.columns],
                data=filtered_df.to_dict('records'),
                style_cell={'textAlign': 'center', 'font-family': 'Roboto'},
                editable=False,
                sort_action="native",
                sort_by=[{"column_id": "Kills", "direction": "desc"}],
                column_selectable=False,
                row_selectable=False,
                row_deletable=False,
                selected_columns=[],
                selected_rows=[0],
                page_action="native",
                page_current=0,
                page_size=10
            )
        ]
    )
])


@app.callback(
    Output('datatable-id', 'data'),
    Input('main-dropdown', 'value')
)
def update_output(value):
    if value == "All":
        query = generate_all_matches_query()
    elif value.startswith("Match"):
        match_number = int(value.split("#")[1])
        query = generate_match_query(match=match_number)
    else:
        return []

    df = pd.read_sql_query(query, con=engine)
    modified_df = df[df['Player'].isin(selected_players)]
    return modified_df.to_dict('records')


if __name__ == '__main__':
    app.run_server(debug=False,
                   port=8000)  # It's always good practice to specify the port, even though there is a default
