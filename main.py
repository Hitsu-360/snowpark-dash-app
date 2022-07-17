import os

from pydoc import classname
from turtle import width
from urllib import response
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

df = pandas.DataFrame(snowflake.get_table_data('<your-table-name>'))

schemas = snowflake.get_schemas_by_database('<your-database>')

df_schemas = pandas.DataFrame(schemas)['name']

tables = snowflake.get_tables_by_schema('<your-schema>')

df_tables = pandas.DataFrame(tables)['name']

snowflake.close_session()

app = Dash(title='Snowpark Dash App', external_stylesheets=[dbc.themes.LUX])

app.layout = html.Div([
    html.Div([
        html.Br(),
        html.H1('Snowpark Dash App'),
        html.P('An app where you can edit a Snowflake table using the UI.', className='lead text-muted'),
    ], className='container'),
    html.Hr(),
    html.Div([
         html.Div([
            html.H5('Select you schema:'),
            dcc.Dropdown(df_schemas)
        ]),
        html.Br(),
        html.Div([
            html.H5('Select you table:'),
            dcc.Dropdown(df_tables)
        ]),
        html.Br(),
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
            html.Br(),
            html.Br(),
            html.Div('', 'display-div', className="alert alert-success", style= {'display': 'none'})
        ])
    ], className='container')
])

@app.callback(
    Output('display-div', 'children'),
    Output('display-div', 'className'),
    Output('display-div', 'style'),
    Input('data-table', 'data'),
    Input('submit-button', 'n_clicks'),
    prevent_initial_call=True
)
def show(data, n_clicks):
    
    if ctx.triggered_id == 'submit-button':

        snowflake = SnowflakeHandler(connection_parameters)

        snowflake.init_session()

        response = snowflake.save_table_data(data, '<your-table-name>', 'overwrite')

        snowflake.close_session()

        if response['status'] == 'success':

            return response['message'], 'alert alert-success', {'display': 'block'}

        elif response['status'] == 'error':

            return response['message'], 'alert alert-danger', {'display': 'block'}

    elif ctx.triggered_id == 'data-table':
        raise exceptions.PreventUpdate

    return None, None, None

if __name__ == '__main__':
    app.run_server(debug=True)