import json
import re
import agiledb.db.postgres as postgresLib
import agiledb.db.mssql as mssqlLib
import agiledb.db.mariaDb as mariaDbLib


class Database:
    def __init__(self):
        self.config = None
        self.cursor = None
        self.connection = None
        self.config_database = None
        self.type_cache = {}
        self.operator_list = ["=", "LIKE", ">", "<", "<=", ">="]

    def configure(self, config_json):
        self.config = config_json
        self.config_database = config_json["database"]
        self.type = self.config_database["type"]
        if self.type == "postgres":
            postgres = postgresLib.AgilePostgres(
                self.config,
                self.config_database
            )
        
            postgres.configure_postgres()
            self.cursor = postgres.cursor
            self.connection = postgres.connection
            return
        if self.type == "mssql":
            mssql = mssqlLib.AgileMssql(self.config, self.config_database)
            mssql.configure_mssql()
            self.cursor = mssql.cursor
            self.connection = mssql.connection
            return
        if self.type == "mariaDb":
            mariaDb = mariaDbLib.AgileMariaDb(self.config,self.config_database)
            mariaDb.configure_maria_db()
            print("mariaDb.cursor")
            print(mariaDb.cursor)
            self.cursor = mariaDb.cursor
            self.connection = mariaDb.connection
            return
      
    def get_from_json(self, str, json):
        """
        Retrieves a value from a JSON object based on the provided key.

        This function takes a key and a JSON object as input.
        If the key exists in the JSON object, its corresponding value is
        returned.
        If the key does not exist in the JSON object, None is returned.

        Args:
            str (str): The key for which the value should be retrieved.
            json (dict): The JSON object from which the value should be
            retrieved.

        Returns:
            obj: The value corresponding to the provided key if it exists,
            None otherwise.
        """
        obj = None
        if str in json:
            obj = json[str]
        return obj

    def parse_where(self, where_string, single_where, str_sql_tuple):
        """
        Parses a WHERE clause for a SQL query.

        This function takes a WHERE clause string, a dictionary representing
        a single WHERE condition, and a tuple representing the SQL string
        as input.
        The function iterates over the keys in the single WHERE condition.
        If the key is not 'operator' and 'where', it is considered as a column
        and its corresponding value is considered as the value for the WHERE
        condition.
        If the key is 'operator', its corresponding value is considered as
        the operator for the WHERE condition.
        The function then checks if the column exists in the configuration
        for the given type.
        If it does, the column, operator, and value are added to the WHERE
        clause string.
        If it doesn't, a JSONB query is added to the WHERE clause string.
        The function then returns the updated WHERE clause string and tuple.

        Args:
            where_string (str): The WHERE clause string to be parsed.
            single_where (dict): A dictionary representing a single WHERE
            condition.
            str_sql_tuple (tuple): The tuple representing the SQL string.

        Returns:
            tuple: A tuple containing the updated WHERE clause string and
            tuple.
        """
        column = ""
        value = ""
        operator = "="
        for att in list(single_where.keys()):
            if att != "operator" and att != "where":
                column = att
                value = single_where[att]
            elif att == "operator":
                temp_operator = single_where[att]
                if temp_operator in self.operator_list:
                    operator = temp_operator
            if self.keys_exists(self.config, "types", type, "columns", column): 
                where_string += column + " " + operator + " %s "
                str_sql_tuple += (value,)
            else:
                where_string += " data->>%s " + operator + " %s "
                str_sql_tuple += (column, value)
        return where_string, str_sql_tuple
    
    def keys_exists(self, element, *keys):
        if not isinstance(element, dict):
            raise AttributeError('keys_exists() expects dict as first argument.')
        if len(keys) == 0:
            raise AttributeError('keys_exists() expects at least two arguments, one given.')
        _element = element
        for key in keys:
            try:
                _element = _element[key]
            except KeyError:
                return False
        return True

    def add_column_to_string_sql(self, str_sql, str_sql_tuple, type_, column):
        """
        Adds a column to a SQL string.

        This function takes a SQL string, a tuple representing the SQL string,
        the type of the record, and a column name as input.
        The function checks if the column is not 'agile_id' and 'agile_type',
        and if the column exists in the configuration for the given type.
        If the conditions are met, the column is added to the SQL string.
        The function then returns the updated SQL string and tuple.

        Args:
            str_sql (str): The SQL string to which the column should be added.
            str_sql_tuple (tuple): The tuple representing the SQL string.
            type_ (str): The type of the record.
            column (str): The name of the column to be added.

        Returns:
            tuple: A tuple containing the updated SQL string and tuple.
        """
        if column != "agile_id" and column != "agile_type":
            if self.keys_exists(self.config,"types",type_,"columns",column):
                str_sql += column+" "
            else:
                if self.type == "postgres":
                    str_sql += "data->>%s as \""+re.sub('[^A-Za-z0-9_]+', '', column)+"\" "
                    str_sql_tuple += (column,)
                elif self.type == "mssql":
                    str_sql += "JSON_VALUE(data,%s) as \""+re.sub('[^A-Za-z0-9_]+', '', column)+"\" "
                    str_sql_tuple += ("$."+column,)
                elif self.type == "mariaDb":
                    str_sql += "JSON_VALUE(data,%s) as \""+re.sub('[^A-Za-z0-9_]+', '', column)+"\" "
                    str_sql_tuple += ("$."+column,)    
        else: 
            if column == "agile_id":
                if self.type == "mariaDb":
                    str_sql += "CAST(agile_id as CHAR CHARACTER SET utf8) as agile_id" + " "
                else:
                    str_sql += "CAST(agile_id as varchar(max)) as agile_id" + " "
            else:
                str_sql += column+" "
        return str_sql,str_sql_tuple

    def get(self,jsonObject):
        type = jsonObject["type"]
        columns = self.get_from_json("columns",jsonObject)
        where = self.get_from_json("where",jsonObject)
        strSQL = "SELECT "
        strSQLTuple = ()
        if columns == None or len(columns) == 0:
            strSQL += "* "
        else:
            first = True
            for column in columns:
                if first == False:strSQL += ","  
                else:first = False
                strSQL,strSQLTuple = self.add_column_to_string_sql(strSQL,strSQLTuple,type,column)
        if self.keys_exists(self.config,"types",type,"table"):
            strSQL += "FROM "+self.config["types"][type]["table"]+" "
        else:    
            strSQL += "FROM agile_main "
        strSQL += "WHERE agile_type=%s "
        strSQLTuple += (type,)
        whereString = ""
        if where != None:
            for singleWhere in where:
                whereString += " AND "    
                whereString,strSQLTuple = self.parse_where(whereString,singleWhere,strSQLTuple)
                #Parse wjere object here
        strSQL += whereString
        print ("SQL")
        print (strSQL)

        self.cursor.execute(strSQL,strSQLTuple)
        if self.type =="postgres":
            self.connection.commit()
        result = self.cursor.fetchall()
        return json.dumps(result)
    
    def post(self, json_object):
        """
        Inserts a new record into the database.

        This function takes a JSON object as input, which should contain the type of the record and the data to be inserted.
        The function constructs an SQL INSERT statement based on the input and executes it.
        If the operation is successful, the function returns the ID of the newly inserted record.

        Args:
            json_object (dict): A dictionary containing the type of the record and the data to be inserted.

        Returns:
            str: The ID of the newly inserted record as a string.

        Raises:
            Exception: If there is an error executing the SQL statement.
        """
        agile_type= json_object["type"]
        data = json_object["data"]
        table_name = "main"
        if self.keys_exists(self.config, "types", agile_type, "table"):
            table_name = self.config["types"][agile_type]["table"]
        sql = """INSERT INTO agile_"""+table_name+""" (agile_type,data"""
        values = " VALUES (%s, %s"
        #if self.keys_exists(self.config,"types",type,"columns"):
        #    for column, _ in self.config["types"][type]["columns"].items():
        #        sql += ","+column
        #        values += ", %s"
        sql += ") "
        values += ") "
        if self.type=="postgres" or self.type=="mariaDb":
            sql += values + " RETURNING agile_id;"  
        if self.type=="mssql":
           sql += " OUTPUT Inserted.agile_id " + values +";"
        print ("SQL")
        print(sql)
        self.cursor.execute(sql,(agile_type, json.dumps(data)))
        print("After COmmand")
        id = self.cursor.fetchone()['agile_id']
        self.connection.commit()
        return str(id)
        
    def put(self,jsonObject):
        id = jsonObject["agile_id"]
        type = jsonObject["type"]
        data = jsonObject["data"]
        tableName = "main"
        if self.keys_exists(self.config,"types",type,"table"):
            tableName = self.config["types"][type]["table"]
        sql = "UPDATE agile_"+tableName+" set data=%s"
        sql_tuple = (json.dumps(data),id)
        #if self.keys_exists(self.config,"types",type,"columns"):
        #    for column, _ in self.config["types"][type]["columns"].items():
        #        if self.keys_exists(jsonObject,"data",column):
        #            sql += ","+column+"=%s"
        #            sqlTuple += (jsonObject["data"][column],)
        sql += """ WHERE agile_id=%s"""
        self.cursor.execute(sql, sql_tuple)
        self.connection.commit()

    def is_number(n):
        try:
            float(n)   # Type-casting the string to `float`.
                    # If string is not a valid `float`, 
                    # it'll raise `ValueError` exception
        except ValueError:
            return False
        return True

    def patch(self,jsonObject):
        #Here You can Put RAWSQL 
        if "enableRawSQL" in self.config and \
        self.config["enableRawSQL"] == True:
            sql = jsonObject["sql"]
            self.cursor.execute(sql)
            if self.type=="postgres":
                self.connection.commit()
            arr = self.cursor.fetchall()
            if self.type=="mssql":
                for arrElement in arr:
                    for attr in arrElement:
                        attrValue = arrElement[attr]
                        attributeType = type(attrValue).__name__
                        if attributeType == 'UUID':
                            arrElement[attr] = str(attrValue)
            return json.dumps(arr)
        else:
            return json.dumps({"error": "RawSQL is not enabled!"})
    
    def delete(self, json_object):
        """
        Deletes a record or records from the database.

        This function takes a JSON object as input, which should contain the 
        ID or IDs of the records to be deleted and the type of the record.
        The function constructs an SQL DELETE statement based on the input and 
        executes it.

        Args:
            json_object (dict): A dictionary containing the objects
            to be deleted and the type of the record.

        Raises:
            Exception: If there is an error executing the SQL statement.
        """
        id = json_object["agile_id"]
        type = json_object["type"]
        table_name = "agile_main"
        if self.keys_exists(self.config, "types", type, "table"):
            table_name = self.config["types"][type]["table"]
        if isinstance(id, list) is True:
            for single_id in id:
                sql = "DELETE FROM "+table_name+" WHERE agile_id=%s"
                self.cursor.execute(sql, (single_id,))
        else:
            sql = "DELETE FROM "+table_name+" WHERE agile_id=%s"
            self.cursor.execute(sql, (id,))
        self.connection.commit()