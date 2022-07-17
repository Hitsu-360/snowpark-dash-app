from snowflake.snowpark import Session

class SnowflakeHandler:

    def init(self, connection_parameters):

        self.session = Session.builder.configs(connection_parameters)

    def init_session(self):
        self.session.create()

    def close_session(self):
        self.session.close()

    def get_table_data(self, table_name):

        return self.session.table(table_name).collect()

    def save_table_data(self, data, table_name, mode):

        snowpark_dataframe = self.session.create_dataframe(data.to_dict('list'))

        try:
            print(f'Saving data to table ({table_name}) with mode ({mode})')
            snowpark_dataframe.write.mode(mode).save_as_table(table_name)
        except NameError: 
            print(f'Exception: {NameError}')
        finally:
            print('Save Operation Finished')