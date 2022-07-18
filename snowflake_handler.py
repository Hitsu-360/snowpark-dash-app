from urllib import response
from snowflake.snowpark import Session

class SnowflakeHandler:

    def __init__(self, connection_parameters):

        self.session = Session.builder.configs(connection_parameters)

    def init_session(self):
        self.session = self.session.create()

    def close_session(self):
        self.session.close()

    def get_databases(self):

        return self.session.sql(f'show databases').collect()

    def get_schemas_by_database(self, database):

        return self.session.sql(f'show schemas in database {database}').collect()

    def get_tables_by_schema(self, schema):

        return self.session.sql(f'show tables in schema {schema}').collect()

    def get_table_data(self, table_name):

        return self.session.table(table_name).collect()

    def save_table_data(self, data, table_name, mode):

        try:

            snowpark_dataframe = self.session.create_dataframe(data)
            
            print(f'Saving data to table ({table_name}) with mode ({mode})')
            
            status = snowpark_dataframe.write.mode(mode).save_as_table(table_name)

            print(status)

            return { 'status': 'success', 'message': 'Saved Successfully'}
        
        except NameError: 
            
            print(f'Exception: {NameError}')
        
            return { 'status': 'error', 'message': str(NameError)}
        
        except: 

            return { 'status': 'error', 'message': 'Something went wrong'}

        finally:
        
            print('Save Operation Finished')

            