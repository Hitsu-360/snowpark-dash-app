import os

from pydoc import classname
from dash import Dash, html, dcc, dash_table, Input, Output, ctx, exceptions
from dotenv import load_dotenv
from pathlib import Path
from snowflake_handler import SnowflakeHandler

import dash_bootstrap_components as dbc
import pandas

# Setting env path
dotenv_path = Path('../.env')
load_dotenv(dotenv_path=dotenv_path)

connection_parameters = {
    "account": os.getenv('ACCOUNT'),
    "user": os.getenv('USER'),
    "password": os.getenv('PASS'),
    "role": os.getenv('ROLE'),
    "warehouse": os.getenv('WAREHOUSE'),
    "database": os.getenv('DATABASE'),
    "schema": os.getenv('SCHEMA')
}    

snowflake = SnowflakeHandler(connection_parameters)

snowflake.init_session()

df = pandas.DataFrame(snowflake.get_table_data('<your_table_name>'))

snowflake.close_session()

app = Dash(external_stylesheets=[dbc.themes.LUX])

app.layout = html.Div([
    html.Div([
        html.Br(),
        html.H1('Snowpark Dash App'),
        html.P('An app where you can edit a Snowflake table using the UI.', className='lead text-muted'),
    ], className='container'),
    html.Hr(),
    html.Div([
        html.H5('Your table'),
        dash_table.DataTable(
            id='data-table',
            data=df.to_dict('records'),
            columns=[{'id': i, 'name': i} for i in df.columns],
            editable=True
        ),
        html.Br(),
        html.Button('Submit', 'submit-button', className='btn btn-primary'),
        html.Div('', 'display-div')
    ], className='container')
])

@app.callback(
    Output('display-div', 'children'),
    Input('data-table', 'data'),
    Input('submit-button', 'n_clicks'),
    prevent_initial_call=True
)
def show(data, n_clicks):

    if ctx.triggered_id == 'submit-button':
        print('Submitted')
    elif ctx.triggered_id == 'data-table':
        raise exceptions.PreventUpdate

    return str(data)

if __name__ == '__main__':
    app.run_server(debug=True)