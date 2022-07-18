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

df_databases = []
df_schemas = []
df_tables = []

snowflake = SnowflakeHandler(connection_parameters)

snowflake.init_session()

# df = pandas.DataFrame(snowflake.get_table_data('<your-table-name>'))
df = pandas.DataFrame(None)

databases = snowflake.get_databases()

df_databases = pandas.DataFrame(databases)['name']

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
            html.Div([
                html.H6('Select you database:'),
                dcc.Dropdown(df_databases, id='databases-dropdown')
            ], className='col-md-3'),
            html.Div([
                html.H6('Select you schema:'),
                dcc.Dropdown(df_schemas, id='schemas-dropdown')
            ], className='col-md-3'),
            html.Div([
                html.H6('Select you table:'),
                dcc.Dropdown(df_tables, id='tables-dropdown')
            ], className='col-md-3'),
             html.Div([
                html.Button('Query Table', 'query-button', className='btn btn-primary btn-sm'),
            ], className='col-md-3')
        ], className='row'),
        html.Br(),
        html.Div([
            html.H5('', id='table-title'),
            dash_table.DataTable(
                id='data-table',
                data=[],
                columns=[],
                editable=True
            ),
            html.Br(),
            html.Button('Submit', 'submit-button', className='btn btn-primary btn-sm'),
            html.Br(),
            html.Br(),
            html.Div('', 'display-div', className="alert alert-success", style= {'display': 'none'})
        ])
    ], className='container'),
    dcc.Store('selected-table-value')
])

@app.callback(
    Output('schemas-dropdown', 'options'),
    Input('databases-dropdown', 'value'),
    prevent_initial_call=True
)
def selectDatabase(database):

    if database:

        snowflake = SnowflakeHandler(connection_parameters)

        snowflake.init_session()

        schemas = snowflake.get_schemas_by_database(database)

        df_schemas = pandas.DataFrame(schemas)['name']

        snowflake.close_session()

        return df_schemas
    
    raise exceptions.PreventUpdate  

@app.callback(
    Output('tables-dropdown', 'options'),
    Input('databases-dropdown', 'value'),
    Input('schemas-dropdown', 'value'),
    prevent_initial_call=True
)
def selectSchema(database, schema):

    if schema:

        snowflake = SnowflakeHandler(connection_parameters)

        snowflake.init_session()

        tables = snowflake.get_tables_by_schema(f'{database}.{schema}')

        df_tables = pandas.DataFrame(tables)['name']

        snowflake.close_session()

        return df_tables
    
    raise exceptions.PreventUpdate 

@app.callback(
    Output('selected-table-value', 'data'),
    Input('databases-dropdown', 'value'),
    Input('schemas-dropdown', 'value'),
    Input('tables-dropdown', 'value'),
    prevent_initial_call=True
)
def selectTable(database, schema, table):

    if database and schema and table:

        return f'{database}.{schema}.{table}'
    
    raise exceptions.PreventUpdate 

@app.callback(
    Output('table-title', 'children'),
    Output('data-table', 'data'),
    Output('data-table', 'columns'),
    Input('selected-table-value', 'data'),
    Input('query-button', 'n_clicks'),
    prevent_initial_call=True
)
def query(selected_table_value, n_clicks):

    if ctx.triggered_id == 'query-button':
        if selected_table_value:
            snowflake = SnowflakeHandler(connection_parameters)

            snowflake.init_session() 

            df_query_data = pandas.DataFrame(snowflake.get_table_data(selected_table_value))

            snowflake.close_session()

            return selected_table_value, df_query_data.to_dict('records'), [{'id': i, 'name': i} for i in df_query_data.columns]
    else:
        raise exceptions.PreventUpdate
    
    return None, None, None

@app.callback(
    Output('display-div', 'children'),
    Output('display-div', 'className'),
    Output('display-div', 'style'),
    Input('selected-table-value', 'data'),
    Input('data-table', 'data'),
    Input('submit-button', 'n_clicks'),
    prevent_initial_call=True
)
def submit(select_table_value, data, n_clicks):
    
    if ctx.triggered_id == 'submit-button':

        snowflake = SnowflakeHandler(connection_parameters)

        snowflake.init_session()

        response = snowflake.save_table_data(data, select_table_value, 'overwrite')

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