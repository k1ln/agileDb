import psycopg2
import json
import agiledb.drivers
from bottle import route, run, request

# Load the configuration file and initialize the database
config_file = open("config.json", "r").read()
config_file_dict = json.loads(config_file)
db_type = config_file_dict["database"]["type"]
port = config_file_dict["server"]["port"]
db = agiledb.drivers.Database()
db.configure(config_file_dict)

def check_if_set_and_true(lookup_str, config):
    """
    This function checks if a given key exists in the configuration and if it's set to True.
    
    Parameters:
    lookup_str (str): The key to look for in the configuration.
    config (dict): The configuration dictionary.

    Returns:
    bool: True if the key exists and its value is True, False otherwise.
    """
    return lookup_str in config and config[lookup_str]

@route('/', method="GET")
def get():
    """
    This function handles GET requests to the root URL. It tries to get data from the database.
    
    Returns:
    str: The data retrieved from the database or an error message.
    """
    try:
        return db.get(request.json)
    except Exception as error:
        print("Error", error)
        if check_if_set_and_true("showDbErrors", db.config["server"]):
            return str(error)

@route('/', method="POST")
def post():
    """
    This function handles POST requests to the root URL. It tries to post data to the database.
    
    Returns:
    str: A success message with the ID of the created record or an error message.
    """
    try:
        return_id = db.post(request.json)
        return json.dumps({"result": "OK", "id": return_id})
    except Exception as error:
        print(error)
        if check_if_set_and_true("showDbErrors", db.config["server"]):
            return str(error)

@route('/', method="PUT")
def put():
    """
    This function handles PUT requests to the root URL. It tries to update a record in the database.
    
    Returns:
    str: A success message or an error message.
    """
    try:
        db.put(request.json)
        return json.dumps({"result": "OK"})
    except Exception as error:
        if check_if_set_and_true("showDbErrors", db.config["server"]):
            return str(error)

@route('/', method="PATCH")
def patch():
    """
    This function handles PATCH requests to the root URL. It tries to partially update a record in the database.
    
    Returns:
    str: The updated data or an error message.
    """
    try:
        return db.patch(request.json)
    except Exception as error:
        if check_if_set_and_true("showDbErrors", db.config["server"]):
            return str(error)

@route('/', method="DELETE")
def delete():
    """
    This function handles DELETE requests to the root URL. It tries to delete a record from the database.
    
    Returns:
    str: A success message or an error message.
    """
    try:
        db.delete(request.json)
        return json.dumps({"result": "OK"})
    except Exception as error:
        if check_if_set_and_true("showDbErrors", db.config["server"]):
            return str(error)

# Start the server
run(host='localhost', port=port, debug=True)