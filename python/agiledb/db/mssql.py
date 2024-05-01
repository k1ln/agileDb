import pymssql

class AgileMssql:
    def __init__(self, config, config_database):
        self.connection = None
        self.cursor = None
        self.type = None
        self.host = None
        self.port = None
        self.name = None    
        self.user = None
        self.password = None
        self.config = config
        self.config_database = config_database 

    def configure_mssql(self):
        self.host = self.config_database["host"]
        self.port = self.config_database["port"]
        self.name = self.config_database["name"]
        self.user = self.config_database["user"]
        self.password = self.config_database["password"]
        self.connection = pymssql.connect(self.host + ":" + self.port, self.user, self.password, self.name)
        self.cursor = self.connection.cursor(as_dict=True)
        self.initialize_database_mssql()
        self.initialize_mssql_tables()
        self.initialize_mssql_types()
        self.initialize_mssql_types_columns()
        self.initialize_mssql_types_indexes()

    def execute_and_commit(self, sql):
        self.cursor.execute(sql)
        self.connection.commit()

    def create_create_main_table_sql_string(self):
        return """if not exists (select * from sysobjects where name='agile_main' and xtype='U')
            CREATE TABLE agile_main ( 
                agile_id uniqueidentifier DEFAULT NEWID(),
                agile_type nvarchar(max),
                data nvarchar(max)
            )"""

    def create_create_main_table_index_sql_string(self):
        return """IF NOT EXISTS(SELECT * FROM sys.indexes WHERE Name = 'IDX_NC_id_data')
        CREATE NONCLUSTERED INDEX IDX_NC_id_data ON agile_main(agile_id) INCLUDE(data)"""
    
    def initialize_database_mssql(self):
        sql = self.create_create_main_table_sql_string()
        self.execute_and_commit(sql)
        sql = self.create_create_main_table_index_sql_string()
        self.execute_and_commit(sql)

    def create_create_table_string(self, table):
        return f"""if not exists (select * from sysobjects where name='agile_{table}' and xtype='U')
                CREATE TABLE agile_{table} ( 
                agile_id uniqueidentifier DEFAULT NEWID(),
                agile_type nvarchar(max),
                data  nvarchar(max));"""

    def initialize_mssql_tables(self):
        if self.config['tables'] is None:
            return
            
        tables = self.config['tables']
        for table, table_object in tables.items():
            sql = self.create_create_table_string(table)
            self.execute_and_commit(sql)
        
    def initialize_mssql_types(self):
        if self.config['types'] is None:
            return
            
        db_types = self.config['types']
        for db_type, db_type_object in db_types.items():
            type_table = self.get_type_table(db_type)
            db_type_object["table"] = type_table
            self.move_types_to_right_table(type_table, db_type)

    def create_get_column_name_string(self, change_table):
        return f"""SELECT column_name 
        FROM information_schema.columns
        WHERE TABLE_NAME = '{change_table}'"""

    def get_all_column_names(self, change_table):
        column_sql = self.create_get_column_name_string(change_table)
        print(column_sql)
        self.execute_and_commit(column_sql)
        all_columns = self.cursor.fetchall()
        all_columns_list = []
        for item in all_columns :
            all_columns_list.append(item['column_name'])
        return all_columns_list

    def create_add_column_sql(self, change_table, column, column_type):
        if column_type == "TEXT":
            column_type = "nvarchar(max)"
        return f"""IF COL_LENGTH('{change_table}' , '{column}') IS NULL
BEGIN
    ALTER TABLE {change_table} ADD a AS CAST(JSON_VALUE(data,'$.{column}') as {column_type}) PERSISTED
END"""

    def create_column_update_sql(self, change_table, column, column_type):
        return f"""UPDATE {change_table} SET
        {column} = CAST(JSON_VALUE(data,'$.{column}') AS {column_type})"""  

    def change_table_columns_due_to_config(self, db_type_object):
        type_table = db_type_object["table"]
        change_table = "agile_main"
        if type_table != None:
            change_table = type_table
        columns = db_type_object['columns']
        all_columns = self.get_all_column_names(change_table)
        for column, column_type in columns.items():
            if column not in all_columns:
                sql = self.create_add_column_sql(change_table, column, column_type)
                print(sql)
                self.execute_and_commit(sql)

    def initialize_mssql_types_columns(self):
        if self.config['types'] is None:
            return
        db_types = self.config['types']
        for db_type, db_type_object in db_types.items():
            self.change_table_columns_due_to_config(db_type_object)

    def create_index_sql(self, change_table, index):
        index_name = index.replace(",", "_")
        return f"""IF NOT EXISTS(SELECT * FROM sys.indexes WHERE Name = 'IDX_{change_table}{index_name}')
        CREATE NONCLUSTERED INDEX IDX_{change_table}{index_name} ON {change_table}(agile_id) INCLUDE({index})"""

    def create_mssql_indices(self, db_type, db_type_object):
        type_table = db_type_object["table"]
        change_table = "agile_main"
        if type_table != None:
            change_table = type_table
        indices = db_type_object['indices']
        for index, index_string in indices.items():
            sql = self.create_index_sql(change_table, index)     
            print(sql)
            self.execute_and_commit(sql)
            
    def initialize_mssql_types_indexes(self):
        if self.config['types'] is None:
            return
        db_types = self.config['types']
        for db_type, db_type_object in db_types.items():
            self.create_mssql_indices(db_type, db_type_object)
    
    def create_agile_table_sql(self):
        return """select * from information_schema.tables 
        where table_name like 'agile_%' 
        and table_name not like '%_index'"""

    def create_insert_agile_data_sql(self, type_table, agile_table):
        return f"""INSERT INTO {str(type_table)}
        (agile_id, agile_type, data) 
        SELECT agile_id, agile_type, data FROM {agile_table['TABLE_NAME']}                
        WHERE agile_type=%s"""

    def move_types_to_right_table(self, type_table, db_type):
        if type_table == None:
            return 
        sql = self.create_agile_table_sql()
        self.execute_and_commit
        self.cursor.execute(sql)
        agile_tables = self.cursor.fetchall()
        for agile_table in agile_tables:
            if type_table != agile_table['TABLE_NAME']:
                sql = self.create_insert_agile_data_sql(type_table, agile_table)
                self.cursor.execute(sql, (db_type,))
                sql = f"""Delete FROM {agile_table['TABLE_NAME']} WHERE agile_type=%s"""
                self.cursor.execute(sql, (db_type,))
                self.connection.commit()

    def get_type_table(self, requested_type):
        if self.config['tables'] == None:
            return None
        tables = self.config['tables']
        for table, table_object in tables.items():
            types = table_object['types']
            for table_type, table_object in types.items():
                if table_type == requested_type:
                    return "agile_" + table