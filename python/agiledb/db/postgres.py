import psycopg2
from psycopg2.extras import RealDictCursor

class AgilePostgres:
    def __init__(self,config,config_database):
        """
        Constructor for the AgilePostgres class. Initializes the class with the given configuration and database configuration.

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

    def configure_postgres(self):
        """
        Configures the PostgreSQL database using the database configuration provided during initialization.
        """
        self.host = self.config_database["host"]
        self.port = self.config_database["port"]
        self.name = self.config_database["name"]
        self.user = self.config_database["user"]
        self.password = self.config_database["password"]
        connection_string = f"host={self.host} port={self.port} dbname={self.name} user={self.user} password={self.password}"
        self.connection = psycopg2.connect(connection_string)
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        self.initialize_database_postgres()
        self.initialize_postgres_tables()
        self.initialize_postgres_types()
        self.initialize_postgres_types_columns()
        self.initialize_postgres_types_indexes()

    def execute_and_commit(self,sql):
        """
        Executes the given SQL query and commits the transaction.

        Parameters:
        sql (str): The SQL query to execute.
        """
        self.cursor.execute(sql)
        self.connection.commit()

    def create_create_main_table_slql_string(self):
        """
        Creates the SQL query string for creating the main table.

        Returns:
        str: The SQL query string.
        """
        return """CREATE TABLE IF NOT EXISTS public.agile_main ( 
            agile_id uuid DEFAULT gen_random_uuid(),
            agile_type TEXT,
            data jsonb
        );"""

    def create_create_main_table_index_sql_string(self):
        """
        Creates the SQL query string for creating an index on the main table.

        Returns:
        str: The SQL query string.
        """
        return """CREATE INDEX IF NOT EXISTS agile_main_data_idx ON public.agile_main ("data");"""
    
    def initialize_database_postgres(self):
        """
        Initializes the PostgreSQL database by creating the main table and its index.
        """
        sql = self.create_create_main_table_slql_string()
        self.execute_and_commit(sql)
        sql = self.create_create_main_table_index_sql_string()
        self.execute_and_commit(sql)

    def create_create_table_string(self,table):
        """
        Creates the SQL query string for creating a table.

        Parameters:
        table (str): The name of the table.

        Returns:
        str: The SQL query string.
        """
        return f"""CREATE TABLE IF NOT EXISTS public.agile_{table} ( 
                agile_id uuid DEFAULT gen_random_uuid(),
                agile_type TEXT,
                data jsonb);"""

    def initialize_postgres_tables(self):
        """
        Initializes the tables in the PostgreSQL database as per the configuration.
        """
        if self.config['tables'] is None:
            return
            
        tables = self.config['tables']
        for table, table_object in tables.items():
            sql = self.create_create_table_string(table)
            self.execute_and_commit(sql)
        
    def initialize_postgres_types(self):
        """
        Initializes the types in the PostgreSQL database as per the configuration.
        """
        if self.config['types'] is None:
            return
            
        db_types = self.config['types']
        for db_type,db_type_object in db_types.items():
            type_table = self.get_type_table(db_type)
            db_type_object["table"] = type_table
            self.move_types_to_right_table(type_table,db_type)

    def create_get_column_name_string(self,change_table):
        """
        Creates the SQL query string for getting the column names of a table.

        Parameters:
        change_table (str): The name of the table.

        Returns:
        str: The SQL query string.
        """
        return f"""SELECT column_name 
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = '{change_table}'"""

    def get_all_column_names(self,change_table):
        """
        Gets all the column names of a table.

        Parameters:
        change_table (str): The name of the table.

        Returns:
        list: The list of column names.
        """
        column_sql = self.create_get_column_name_string(change_table)
        self.execute_and_commit(column_sql)
        all_columns = self.cursor.fetchall()
        all_columns_list = []
        for item in all_columns :
            all_columns_list.append(item['column_name'])
        return all_columns_list

    def create_add_column_sql(self,change_table,column,column_type):
        """
        Creates the SQL query string for adding a column to a table.

        Parameters:
        change_table (str): The name of the table.
        column (str): The name of the column.
        column_type (str): The type of the column.

        Returns:
        str: The SQL query string.
        """
        return f"""ALTER TABLE {change_table} ADD COLUMN IF NOT EXISTS {column} {column_type}
        GENERATED ALWAYS AS (data->'{column}') STORED;"""

    def create_column_update_sql(self,change_table,column,column_type):
        """
        Creates the SQL query string for updating a column in a table.

        Parameters:
        change_table (str): The name of the table.
        column (str): The name of the column.
        column_type (str): The type of the column.

        Returns:
        str: The SQL query string.
        """
        return f"""UPDATE {change_table} SET  
        {column}=CAST(data->>'{column}' AS {column_type})"""  
  
    def change_table_columns_due_to_config(self,db_type_object):
        """
        Changes the columns of a table due to configuration.

        Parameters:
        db_type_object (dict): The database type object.
        """
        type_table = db_type_object["table"]
        change_table = "agile_main"
        if type_table != None:
            change_table = type_table
        columns = db_type_object['columns']
        all_columns = self.get_all_column_names(change_table)
        for column,column_type in columns.items():
            if column not in all_columns:
                sql = self.create_add_column_sql(change_table,column,column_type)
                self.execute_and_commit(sql)
                #sql = self.create_column_update_sql(change_table,column,column_type)
                #self.execute_and_commit(sql)

    def initialize_postgres_types_columns(self):
        """
        Initializes the columns of the types in the PostgreSQL database as per the configuration.
        """
        if self.config['types'] is None:
            return
        db_types = self.config['types']
        for db_type,db_type_object in db_types.items():
            self.change_table_columns_due_to_config(db_type_object)

    def create_index_sql(self,change_table,index):
        """
        Creates the SQL query string for creating an index on a table.

        Parameters:
        change_table (str): The name of the table.
        index (str): The name of the index.

        Returns:
        str: The SQL query string.
        """
        index_name = index.replace(",","_")
        index_column_name = index.replace(",","\",\"")    
        return f"""CREATE INDEX IF NOT EXISTS 
            {change_table}{index_name}_idx
            ON public.{change_table} (\"{index_column_name}\")"""

    def create_postgres_indices(self,db_type,db_type_object):
        """
        Creates the indices for a type in the PostgreSQL database.

        Parameters:
        db_type (str): The name of the type.
        db_type_object (dict): The database type object.
        """
        type_table = db_type_object["table"]
        change_table = "agile_main"
        if type_table != None:
            change_table = type_table
        indices = db_type_object['indices']
        for index,indexString in indices.items():
            sql = self.create_index_sql(change_table,index)     
            self.execute_and_commit(sql)
            
    def initialize_postgres_types_indexes(self):
        """
        Initializes the indices of the types in the PostgreSQL database as per the configuration.
        """
        if self.config['types'] is None:
            return
        db_types = self.config['types']
        for db_type,db_type_object in db_types.items():
            self.create_postgres_indices(db_type,db_type_object)
    
    def create_agile_table_sql(self):
        """
        Creates the SQL query string for selecting all tables that start with 'agile_' and do not end with '_index'.

        Returns:
        str: The SQL query string.
        """
        return """select * from information_schema.tables 
        where table_name like 'agile_%' 
        and table_name not like '%_index'"""

    def create_insert_agile_data_sql(self,type_table,agile_table):
        """
        Creates the SQL query string for inserting data from one table to another.

        Parameters:
        type_table (str): The name of the target table.
        agile_table (dict): The source table.

        Returns:
        str: The SQL query string.
        """
        return f"""INSERT INTO {str(type_table)}
        (agile_id, agile_type, data) 
        SELECT agile_id, agile_type, data FROM {agile_table['table_name']}                
        WHERE agile_type=%s"""

    def move_types_to_right_table(self,type_table,db_type):
        """
        Moves the types to the right table.

        Parameters:
        type_table (str): The name of the target table.
        db_type (str): The name of the type.
        """
        if type_table == None:
            return 
        sql = self.create_agile_table_sql()
        self.execute_and_commit
        self.cursor.execute(sql)
        agile_tables = self.cursor.fetchall()
        for agile_table in agile_tables:
            if type_table != agile_table['table_name']:
                sql = self.create_insert_agile_data_sql(type_table,agile_table)
                self.cursor.execute(sql,(db_type,))
                sql = f"""Delete FROM {agile_table['table_name']} WHERE agile_type=%s"""
                self.cursor.execute(sql,(db_type,))
                self.connection.commit()

    def get_type_table(self,requestedType):
        """
        Gets the table for a requested type.

        Parameters:
        requestedType (str): The name of the requested type.

        Returns:
        str: The name of the table for the requested type.
        """
        if self.config['tables'] == None:
            return None
        tables = self.config['tables']
        for table, table_object in tables.items():
            types = table_object['types']
            for tableType, table_object in types.items():
                if tableType == requestedType:
                    return "agile_" + table