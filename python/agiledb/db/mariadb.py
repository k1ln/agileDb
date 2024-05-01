import mariadb

class AgileMariaDb:
    def __init__(self, config, config_database):
        """
        Initialize the AgileMariaDb class with configuration and database configuration.

        Parameters:
        config (dict): The configuration dictionary.
        config_database (dict): The database configuration dictionary.
        """
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

    def configure_maria_db(self):
        """
        Configure the MariaDB database using the provided configuration.
        """
        self.host = self.config_database["host"]
        try:
            self.port = int(self.config_database["port"])
        except ValueError:
            print(f"Invalid port number: {self.config_database['port']}")
            self.port = None  # or set a default port number
        self.name = self.config_database["name"]
        self.user = self.config_database["user"]
        self.password = self.config_database["password"]
        conn_params = {
            "user" : self.user,
            "password" : self.password,
            "host" : self.host,
            "database" : self.name,
            "port":self.port,
        }

        self.connection = mariadb.connect(**conn_params)
        self.cursor = self.connection.cursor(dictionary=True)
        self.initialize_database_maria_db()
        self.initialize_maria_db_tables()
        self.initialize_maria_db_types()
        self.initialize_maria_db_types_columns()
        self.initialize_maria_db_types_indexes()

    def execute_and_commit(self, sql):
        """
        Execute the given SQL query and commit the transaction.

        Parameters:
        sql (str): The SQL query to execute.
        """
        self.cursor.execute(sql)
        self.connection.commit()

    def create_create_main_table_sql_string(self):
        """
        Create an SQL query string for creating the main table.
        """
        return """CREATE TABLE IF NOT EXISTS agile_main ( 
            agile_id UUID NOT NULL DEFAULT UUID() primary key,
            agile_type TEXT,
            data JSON
        );"""

    def initialize_database_maria_db(self):
        """
        Initialize the MariaDB database by creating the main table.
        """
        sql = self.create_create_main_table_sql_string()
        self.execute_and_commit(sql)
        
    def create_create_table_string(self, table):
        """
        Create an SQL query string for creating a table.

        Parameters:
        table (str): The name of the table.
        """
        return f"""CREATE TABLE IF NOT EXISTS agile_{table} ( 
                agile_id UUID NOT NULL DEFAULT UUID() primary key,
                agile_type TEXT,
                data JSON);"""

    def initialize_maria_db_tables(self):
        """
        Initialize the MariaDB tables as per the configuration.
        """
        if self.config['tables'] is None:
            return
            
        tables = self.config['tables']
        for table, table_object in tables.items():
            sql = self.create_create_table_string(table)
            self.execute_and_commit(sql)
        
    def initialize_maria_db_types(self):
        """
        Initialize the MariaDB types as per the configuration.
        """
        if self.config['types'] is None:
            return
            
        db_types = self.config['types']
        for db_type, db_type_object in db_types.items():
            type_table = self.get_type_table(db_type)
            db_type_object["table"] = type_table
            self.move_types_to_right_table(type_table, db_type)

    def create_get_column_name_string(self, change_table):
        """
        Create an SQL query string to get all column names of a table.

        Parameters:
        change_table (str): The name of the table.
        """
        return f"""SELECT column_name 
        FROM information_schema.columns
        WHERE TABLE_NAME = '{change_table}'"""

    def get_all_column_names(self, change_table):
        """
        Get all column names of a table.

        Parameters:
        change_table (str): The name of the table.
        """
        column_sql = self.create_get_column_name_string(change_table)
        self.execute_and_commit(column_sql)
        all_columns = self.cursor.fetchall()
        all_columns_list = []
        for item in all_columns :
            all_columns_list.append(item['column_name'])
        return all_columns_list

    def create_add_column_sql(self, change_table, column, column_type):
        """
        Create an SQL query string to add a new column to a table.

        Parameters:
        change_table (str): The name of the table.
        column (str): The name of the column.
        column_type (str): The type of the column.
        """
        return f"""ALTER TABLE {change_table} 
        ADD COLUMN IF NOT EXISTS {column} {column_type} as (JSON_VALUE(data,'$.{column}')) STORED;"""
    
    def create_column_update_sql(self, change_table, column, column_type):
        """
        Create an SQL query string to update a column in a table.

        Parameters:
        change_table (str): The name of the table.
        column (str): The name of the column.
        column_type (str): The type of the column.
        """
        return f"""UPDATE {change_table} SET
        {column} = CAST(JSON_VALUE(data,'$.{column}') AS {column_type})"""  

    def change_table_columns_due_to_config(self, db_type_object):
        """
        Change the table columns as per the configuration.

        Parameters:
        db_type_object (dict): The database type object from the configuration.
        """
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

    def initialize_maria_db_types_columns(self):
        """
        Initialize the MariaDB types columns as per the configuration.
        """
        if self.config['types'] is None:
            return
        db_types = self.config['types']
        for db_type, db_type_object in db_types.items():
            self.change_table_columns_due_to_config(db_type_object)

    def create_index_sql(self, change_table, index):
        """
        Create an SQL query string to create an index.

        Parameters:
        change_table (str): The name of the table.
        index (str): The name of the index.
        """
        index_name = index.replace(",","_") 
        return f"""CREATE INDEX IDX_{change_table}{index_name} ON {change_table}(agile_id)"""
        
    def create_maria_db_indices(self, db_type, db_type_object):
        """
        Create MariaDB indices for a type.

        Parameters:
        db_type (str): The database type.
        db_type_object (dict): The database type object from the configuration.
        """
        type_table = db_type_object["table"]
        change_table = "agile_main"
        if type_table != None:
            change_table = type_table
        indices = db_type_object['indices']
        for index, index_string in indices.items():
            sql = self.create_index_sql(change_table, index)     
            try:
                self.execute_and_commit(sql)
            except mariadb.Error as e:
                print("Index already exists or an Error occured: ",e)
            
    def initialize_maria_db_types_indexes(self):
        """
        Initialize the MariaDB types indices as per the configuration.
        """
        if self.config['types'] is None:
            return
        db_types = self.config['types']
        for db_type, db_type_object in db_types.items():
            self.create_maria_db_indices(db_type, db_type_object)
    
    def create_agile_table_sql(self):
        """
        Create an SQL query string to select all tables that start with 'agile_' and do not end with '_index'.
        """
        return """select * from information_schema.tables 
        where table_name like 'agile_%' 
        and table_name not like '%_index'"""

    def create_insert_agile_data_sql(self, type_table, agile_table):
        """
        Create an SQL query string to insert data from one table to another.

        Parameters:
        type_table (str): The name of the table to insert data into.
        agile_table (str): The name of the table to get data from.
        """
        return f"""INSERT INTO {str(type_table)}
        (agile_id, agile_type, data) 
        SELECT agile_id, agile_type, data FROM {agile_table['TABLE_NAME']}                
        WHERE agile_type=%s"""

    def move_types_to_right_table(self, type_table, db_type):
        """
        Move types to the right table.

        Parameters:
        type_table (str): The name of the table to move types to.
        db_type (str): The database type.
        """
        if type_table == None:
            return 
        sql = self.create_agile_table_sql()
        self.execute_and_commit(sql)
        self.cursor.execute(sql)
        agile_tables = self.cursor.fetchall()
        for agile_table in agile_tables:
            if type_table != agile_table['TABLE_NAME']:
                sql = self.create_insert_agile_data_sql(type_table, agile_table)
                print(sql)
                self.cursor.execute(sql, (db_type,))
                sql = f"""Delete FROM {agile_table['TABLE_NAME']} WHERE agile_type=%s"""
                self.cursor.execute(sql, (db_type,))
                self.connection.commit()

    def get_type_table(self, requested_type):
        """
        Get the table of a requested type.

        Parameters:
        requested_type (str): The requested type.
        """
        if self.config['tables'] == None:
            return None
        tables = self.config['tables']
        for table, table_object in tables.items():
            types = table_object['types']
            for table_type, table_object in types.items():
                if table_type == requested_type:
                    return "agile_" + table